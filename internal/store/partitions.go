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

// ddlLockTimeout bounds how long partition DDL waits for its table's lock;
// on timeout it fails and is tried again later.
const ddlLockTimeout = "5s"

// partitionLockKey serialises partition DDL across processes.
const partitionLockKey = 4_400_001

// Tables names a schema's partitioned tables. The names are unqualified:
// they resolve through the pool's search_path (tks pins schema tt).
type Tables struct {
	// Monthly are partitioned by month (post month or capture month).
	Monthly []string
	// Daily are partitioned by day.
	Daily []string
	// LZ4 are each table's large text and jsonb columns that compress well.
	LZ4 map[string][]string
}

// FBTables are the copied Facebook schema's partitioned tables.
var FBTables = Tables{
	Monthly: []string{"posts", "comments", "page_snapshots"},
	Daily:   []string{"post_metrics"},
	LZ4: map[string][]string{
		"posts":    {"text", "text_ranges", "links", "extra"},
		"comments": {"text", "text_ranges", "attachments", "extra"},
	},
}

// PartitionWindow describes which partitions must exist.
type PartitionWindow struct {
	MonthsBack  int // monthly tables: how many months before now
	MonthsAhead int
	DaysBack    int // daily tables
	DaysAhead   int
	// NotBefore skips a table's partitions that end at or before its time:
	// those retention drops, so creating them would only churn locks.
	NotBefore map[string]time.Time
}

// wanted reports whether a partition of table ending at end is kept.
func (w PartitionWindow) wanted(table string, end time.Time) bool {
	nb, ok := w.NotBefore[table]
	return !ok || end.After(nb)
}

// EnsurePartitions creates every missing partition of the copied Facebook
// schema's tables inside the window.
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, w PartitionWindow) (int, error) {
	return FBTables.Ensure(ctx, pool, now, w)
}

// Ensure creates every missing partition of t's tables inside the window.
func (t Tables) Ensure(ctx context.Context, pool *pgxpool.Pool, now time.Time, w PartitionWindow) (int, error) {
	now = now.UTC()
	created := 0
	start := monthStart(now).AddDate(0, -w.MonthsBack, 0)
	end := monthStart(now).AddDate(0, w.MonthsAhead+1, 0)
	for m := start; m.Before(end); m = m.AddDate(0, 1, 0) {
		for _, tb := range t.Monthly {
			if !w.wanted(tb, m.AddDate(0, 1, 0)) {
				continue
			}
			ok, err := ensureRange(ctx, pool, tb, monthName(tb, m), m, m.AddDate(0, 1, 0), t.LZ4[tb])
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
		for _, tb := range t.Daily {
			if !w.wanted(tb, d.AddDate(0, 0, 1)) {
				continue
			}
			ok, err := ensureRange(ctx, pool, tb, dayName(tb, d), d, d.AddDate(0, 0, 1), t.LZ4[tb])
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
	return FBTables.EnsureFor(ctx, pool, table, ts)
}

// EnsureFor creates the partition of one of t's tables that holds ts.
func (t Tables) EnsureFor(ctx context.Context, pool *pgxpool.Pool, table string, ts time.Time) error {
	ts = ts.UTC()
	for _, d := range t.Daily {
		if d == table {
			d := dayStart(ts)
			_, err := ensureRange(ctx, pool, table, dayName(table, d), d, d.AddDate(0, 0, 1), t.LZ4[table])
			return err
		}
	}
	m := monthStart(ts)
	_, err := ensureRange(ctx, pool, table, monthName(table, m), m, m.AddDate(0, 1, 0), t.LZ4[table])
	return err
}

// IsNoPartition reports whether err is Postgres "no partition of relation".
func IsNoPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && strings.Contains(pgErr.Message, "no partition")
}

func ensureRange(ctx context.Context, pool *pgxpool.Pool, table, name string, from, to time.Time, lz4 []string) (bool, error) {
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
		// Creating a partition locks its parent: wait a bounded time behind
		// long readers instead of queueing every other query behind us.
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+ddlLockTimeout+"'"); err != nil {
			return err
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
	for _, col := range lz4 {
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
			err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+ddlLockTimeout+"'"); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, "DROP TABLE "+pgx.Identifier{n}.Sanitize())
				return err
			})
			if err != nil {
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
