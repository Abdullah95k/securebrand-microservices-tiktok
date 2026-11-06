package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

// testTables are two partitioned tables of the test's own: one by month, one
// by day.
var testTables = Tables{Monthly: []string{"events"}, Daily: []string{"counts"}}

func partitioned(t *testing.T) *pgxpool.Pool {
	t.Helper()
	db := testenv.Postgres(t)
	if _, err := db.Exec(context.Background(), `
		CREATE TABLE events (at timestamptz NOT NULL, body text) PARTITION BY RANGE (at);
		CREATE TABLE counts (at timestamptz NOT NULL, n bigint) PARTITION BY RANGE (at)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func partitionsOf(t *testing.T, db *pgxpool.Pool, table string) map[string]bool {
	t.Helper()
	rows, err := db.Query(context.Background(), `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = $1`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		out[n] = true
	}
	return out
}

// The window's partitions are created once: a partition retention drops
// (NotBefore) is never created, and the next run creates and drops nothing
// (each statement would lock the table).
func TestEnsureDoesNotChurnPartitions(t *testing.T) {
	db := partitioned(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	w := PartitionWindow{MonthsBack: 3, MonthsAhead: 1, DaysBack: 2, DaysAhead: 1,
		NotBefore: map[string]time.Time{"events": time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}}
	n, err := testTables.Ensure(ctx, db, now, w)
	if err != nil {
		t.Fatal(err)
	}
	events, counts := partitionsOf(t, db, "events"), partitionsOf(t, db, "counts")
	// events: September to December (August ends at NotBefore); counts: 4 days.
	if n != 8 || len(events) != 4 || len(counts) != 4 || events["events_y2026m08"] || !events["events_y2026m12"] || !counts["counts_d20261107"] {
		t.Fatalf("created %d: events %v, counts %v", n, events, counts)
	}
	if _, err := db.Exec(ctx, `CREATE TABLE ddl_log (tag text);
		CREATE FUNCTION log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
		BEGIN INSERT INTO ddl_log VALUES (tg_tag); END $$;
		CREATE EVENT TRIGGER log_ddl ON ddl_command_start WHEN TAG IN ('CREATE TABLE', 'DROP TABLE') EXECUTE FUNCTION log_ddl()`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = db.Exec(context.Background(), `DROP EVENT TRIGGER log_ddl`) }()
	if n, err := testTables.Ensure(ctx, db, now, w); err != nil || n != 0 {
		t.Fatalf("second run created %d (%v)", n, err)
	}
	var ddl int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM ddl_log`).Scan(&ddl); err != nil || ddl != 0 {
		t.Fatalf("the second run ran %d partition DDL statements (%v)", ddl, err)
	}
}

// A row with no partition is reported as such, EnsureFor creates the one
// that holds it, and DropPartitionsBefore drops only partitions that end by
// the cutoff.
func TestEnsureForAndDrop(t *testing.T) {
	db := partitioned(t)
	ctx := context.Background()
	at := time.Date(2025, 3, 15, 8, 0, 0, 0, time.UTC)
	_, err := db.Exec(ctx, `INSERT INTO events VALUES ($1, 'x')`, at)
	if !IsNoPartition(err) {
		t.Fatalf("insert without a partition: %v", err)
	}
	for _, ts := range []time.Time{at, at.AddDate(0, 1, 0)} {
		if err := testTables.EnsureFor(ctx, db, "events", ts); err != nil {
			t.Fatal(err)
		}
	}
	if err := testTables.EnsureFor(ctx, db, "counts", at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO events VALUES ($1, 'x')`, at); err != nil {
		t.Fatal(err)
	}
	if p := partitionsOf(t, db, "counts"); !p["counts_d20250315"] || len(p) != 1 {
		t.Fatalf("counts partitions %v", p)
	}
	dropped, err := DropPartitionsBefore(ctx, db, "events", time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || len(dropped) != 1 || dropped[0] != "events_y2025m03" {
		t.Fatalf("dropped %v (%v)", dropped, err)
	}
	if p := partitionsOf(t, db, "events"); !p["events_y2025m04"] || len(p) != 1 {
		t.Fatalf("events partitions after the drop %v", p)
	}
}

// Partition DDL behind a long reader gives up after its lock timeout, so
// the queries queued behind it aren't held up until the reader ends.
func TestPartitionDDLDoesNotWaitForever(t *testing.T) {
	db := partitioned(t)
	ctx := context.Background()
	reader, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	if _, err := reader.Exec(ctx, `SELECT count(*) FROM events`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = testTables.EnsureFor(ctx, db, "events", time.Date(2001, 1, 15, 0, 0, 0, 0, time.UTC))
	if err == nil || time.Since(start) > 20*time.Second {
		t.Fatalf("partition creation behind a reader: %v after %s", err, time.Since(start))
	}
}
