package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// partitionLockKey serialises partition DDL across processes.
const partitionLockKey = 4_300_001

// Monthly partitioned tables (by post month or capture month).
var monthlyTables = []string{"posts", "comments", "page_snapshots"}

// Daily partitioned tables.
var dailyTables = []string{"post_metrics"}

// lz4 columns are large text/jsonb columns that compress well.
var lz4Columns = map[string][]string{
	"posts":    {"text", "text_ranges", "links", "extra"},
	"comments": {"text", "text_ranges", "attachments", "extra"},
}

// PartitionWindow describes which partitions must exist.
type PartitionWindow struct {
	MonthsBack  int // monthly tables: how many months before now
	MonthsAhead int
	DaysBack    int // daily tables
	DaysAhead   int
}

// EnsurePartitions creates every missing partition inside the window.
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, w PartitionWindow) (int, error) {
	now = now.UTC()
	created := 0
	start := monthStart(now).AddDate(0, -w.MonthsBack, 0)
	end := monthStart(now).AddDate(0, w.MonthsAhead+1, 0)
	for m := start; m.Before(end); m = m.AddDate(0, 1, 0) {
		for _, t := range monthlyTables {
			ok, err := ensureRange(ctx, pool, t, monthName(t, m), m, m.AddDate(0, 1, 0))
			if err != nil {
				return created, err
			}
			if ok {
				created++
			}
		}
	}
	dstart := dayStart(now).AddDate(0, 0, -w.DaysBack)
	dend := dayStart(now).AddDate(0, 0, w.DaysAhead+1)
	for d := dstart; d.Before(dend); d = d.AddDate(0, 0, 1) {
		for _, t := range dailyTables {
			ok, err := ensureRange(ctx, pool, t, dayName(t, d), d, d.AddDate(0, 0, 1))
			if err != nil {
				return created, err
			}
			if ok {
				created++
			}
		}
	}
	return created, nil
}

// EnsurePartitionFor creates the partition of table that holds ts. The
// persister calls it when an insert fails with "no partition".
func EnsurePartitionFor(ctx context.Context, pool *pgxpool.Pool, table string, ts time.Time) error {
	ts = ts.UTC()
	for _, t := range dailyTables {
		if t == table {
			d := dayStart(ts)
			_, err := ensureRange(ctx, pool, table, dayName(table, d), d, d.AddDate(0, 0, 1))
			return err
		}
	}
	m := monthStart(ts)
	_, err := ensureRange(ctx, pool, table, monthName(table, m), m, m.AddDate(0, 1, 0))
	return err
}

// IsNoPartition reports whether err is Postgres "no partition of relation".
func IsNoPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && strings.Contains(pgErr.Message, "no partition")
}

func ensureRange(ctx context.Context, pool *pgxpool.Pool, table, name string, from, to time.Time) (bool, error) {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", partitionLockKey); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		ff := ""
		if table == "posts" || table == "comments" {
			ff = " WITH (fillfactor = 85)"
		}
		ddl := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')%s",
			pgx.Identifier{name}.Sanitize(), pgx.Identifier{table}.Sanitize(),
			from.Format(time.RFC3339), to.Format(time.RFC3339), ff)
		if _, err := tx.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("store: create partition %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	for _, col := range lz4Columns[table] {
		// lz4 needs a server built with lz4 (official images are); ignore otherwise.
		q := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET COMPRESSION lz4",
			pgx.Identifier{name}.Sanitize(), pgx.Identifier{col}.Sanitize())
		if _, err := pool.Exec(ctx, q); err != nil {
			slog.Debug("store: lz4 not applied", "partition", name, "err", err)
			break
		}
	}
	return true, nil
}

var partName = regexp.MustCompile(`^(\w+)_(y(\d{4})m(\d{2})|d(\d{8}))$`)

// DropPartitionsBefore drops partitions of table whose whole range ends at
// or before cutoff. It returns the dropped partition names.
func DropPartitionsBefore(ctx context.Context, pool *pgxpool.Pool, table string, cutoff time.Time) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.relname
		FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = $1`, table)
	if err != nil {
		return nil, err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	var dropped []string
	for _, n := range names {
		m := partName.FindStringSubmatch(n)
		if m == nil || m[1] != table {
			continue
		}
		var upper time.Time
		if m[3] != "" {
			start, _ := time.Parse("2006-01", m[3]+"-"+m[4])
			upper = start.AddDate(0, 1, 0)
		} else {
			start, _ := time.Parse("20060102", m[5])
			upper = start.AddDate(0, 0, 1)
		}
		if !upper.After(cutoff) {
			if _, err := pool.Exec(ctx, "DROP TABLE "+pgx.Identifier{n}.Sanitize()); err != nil {
				return dropped, fmt.Errorf("store: drop %s: %w", n, err)
			}
			dropped = append(dropped, n)
		}
	}
	return dropped, nil
}

func monthStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func dayStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func monthName(table string, m time.Time) string {
	return fmt.Sprintf("%s_y%04dm%02d", table, m.Year(), int(m.Month()))
}

func dayName(table string, d time.Time) string {
	return fmt.Sprintf("%s_d%s", table, d.Format("20060102"))
}
