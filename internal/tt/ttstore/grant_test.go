package ttstore_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// The components' roles (M18): a writer reads and writes rows and creates
// partitions, but cannot change the schema or the migrations' record; the
// reader only reads; a role the platform has not created fails the grant.
func TestGrantRoles(t *testing.T) {
	ctx := context.Background()
	dsn := testenv.EmptyPostgres(t)
	owner, err := ttstore.Open(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	if err := ttstore.MigrateUp(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := ttstore.Grant(ctx, owner, config.PostgresRoles{API: "tks_no_such_role"}); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("missing role: %v", err)
	}
	w, wdsn := testenv.Role(t, owner, dsn)
	r, rdsn := testenv.Role(t, owner, dsn)
	g, err := ttstore.Grant(ctx, owner, config.PostgresRoles{Persister: w, Reader: r})
	if err != nil || g[w] != "write" || g[r] != "read" {
		t.Fatalf("grant: %v %v", g, err)
	}
	// Granting again (the next migration) is harmless.
	if _, err := ttstore.Grant(ctx, owner, config.PostgresRoles{Persister: w, Reader: r}); err != nil {
		t.Fatal(err)
	}
	writer, err := ttstore.Open(ctx, wdsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(writer.Close)
	reader, err := ttstore.Open(ctx, rdsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	if err := ttstore.CheckSchema(ctx, writer); err != nil {
		t.Fatal(err)
	}

	// A partition years back, created by the writer as the tables' owner.
	old := time.Date(2019, 3, 10, 0, 0, 0, 0, time.UTC)
	for _, tb := range []string{"posts", "post_metrics"} {
		if err := ttstore.Tables.EnsureFor(ctx, writer, tb, old); err != nil {
			t.Fatalf("%s: %v", tb, err)
		}
	}
	if _, err := ttstore.EnsurePartitions(ctx, writer, time.Now(), store.PartitionWindow{MonthsBack: 1, MonthsAhead: 1, DaysAhead: 2}); err != nil {
		t.Fatal(err)
	}
	var owned string
	if err := owner.QueryRow(ctx, `SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'posts_y2019m03'`).Scan(&owned); err != nil || owned == w {
		t.Fatalf("partition owned by %q: %v", owned, err)
	}
	// Rows in tables the migrations and the function made after the grant.
	if _, err := writer.Exec(ctx, `INSERT INTO client_params (key, value) VALUES ('m18', 'x')`); err != nil {
		t.Fatalf("writer insert: %v", err)
	}
	if _, err := writer.Exec(ctx, `UPDATE client_params SET value = 'y' WHERE key = 'm18'`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := reader.QueryRow(ctx, `SELECT count(*) FROM client_params`).Scan(&n); err != nil || n == 0 {
		t.Fatalf("reader select: %d %v", n, err)
	}
	denied := func(who string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "42501") && !strings.Contains(err.Error(), "must be owner") && !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("%s: want permission denied, got %v", who, err)
		}
	}
	_, err = reader.Exec(ctx, `INSERT INTO client_params (key, value) VALUES ('r', 'x')`)
	denied("reader insert", err)
	_, err = writer.Exec(ctx, `DROP TABLE client_params`)
	denied("writer drop", err)
	_, err = writer.Exec(ctx, `TRUNCATE posts`)
	denied("writer truncate", err)
	_, err = writer.Exec(ctx, `ALTER TABLE accounts ADD COLUMN m18 int`)
	denied("writer alter", err)
	_, err = writer.Exec(ctx, `DELETE FROM goose_db_version`)
	denied("writer migrations' record", err)
	_, err = writer.Exec(ctx, `CREATE TABLE tt.m18 (x int)`)
	denied("writer create", err)
	// The function makes only partitions of tt's partitioned tables.
	_, err = writer.Exec(ctx, `SELECT tt.ensure_partition('accounts', 'accounts_y2019m03', now(), now() + interval '1 day', '{}')`)
	if err == nil {
		t.Fatal("ensure_partition made a partition of accounts")
	}
	_, err = reader.Exec(ctx, `SELECT tt.ensure_partition('posts', 'posts_y2018m01', '2018-01-01', '2018-02-01', '{}')`)
	denied("reader partition", err)
}
