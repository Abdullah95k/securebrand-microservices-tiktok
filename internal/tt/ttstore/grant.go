package ttstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
)

// Grants are what Grant gave, role by role.
type Grants map[string]string

// Grant gives each component's role its rights on schema tt (M18), as the
// role that owns the schema (the one that migrates). Every writer (api,
// persister, scheduler, crawler) reads and writes every table's rows and
// creates partitions through tt.ensure_partition, but owns nothing, so it
// can neither change nor drop the schema, truncate a table or rewrite the
// migrations' record; the reader only reads. The same rights reach the
// tables later migrations and partitions add (default privileges, and
// Grant runs after every migration). A role that does not exist fails the
// call: the platform creates the roles, tks only grants.
//
// Rights are by kind, not table by table: the components share code paths
// (the API registers accounts through the same package discovery uses), so
// a per-table list could not be proven complete before production.
func Grant(ctx context.Context, pool *pgxpool.Pool, roles config.PostgresRoles) (Grants, error) {
	out := Grants{}
	var dbName string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&dbName); err != nil {
		return nil, err
	}
	db := pgx.Identifier{dbName}.Sanitize()
	writers := []string{roles.API, roles.Persister, roles.Scheduler, roles.Crawler}
	for i, r := range append(writers, roles.Reader) {
		if r == "" {
			continue
		}
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, r).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("ttstore: grant: role %q does not exist (the platform creates it)", r)
		}
		role := pgx.Identifier{r}.Sanitize()
		writer := i < len(writers)
		tables, seqs, kind := "SELECT", "USAGE, SELECT", "read"
		stmts := []string{"GRANT CONNECT ON DATABASE " + db + " TO " + role}
		if writer {
			tables, seqs, kind = "SELECT, INSERT, UPDATE, DELETE", "USAGE, SELECT, UPDATE", "write"
			// The persister stages rows in temporary tables.
			stmts = append(stmts, "GRANT TEMPORARY ON DATABASE "+db+" TO "+role)
		}
		stmts = append(stmts,
			"GRANT USAGE ON SCHEMA tt TO "+role,
			"GRANT "+tables+" ON ALL TABLES IN SCHEMA tt TO "+role,
			"GRANT "+seqs+" ON ALL SEQUENCES IN SCHEMA tt TO "+role,
			"ALTER DEFAULT PRIVILEGES IN SCHEMA tt GRANT "+tables+" ON TABLES TO "+role,
			"ALTER DEFAULT PRIVILEGES IN SCHEMA tt GRANT "+seqs+" ON SEQUENCES TO "+role)
		if writer {
			stmts = append(stmts,
				"REVOKE INSERT, UPDATE, DELETE ON "+VersionTable+" FROM "+role,
				"GRANT EXECUTE ON FUNCTION tt.ensure_partition(text, text, timestamptz, timestamptz, text[]) TO "+role)
		}
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			for _, s := range stmts {
				if _, err := tx.Exec(ctx, s); err != nil {
					return fmt.Errorf("ttstore: grant: %s: %w", strings.SplitN(s, " TO ", 2)[0], err)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		out[r] = kind
	}
	return out, nil
}
