package ttstore_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// Every table of ADR 0007 §4.
var tables = []string{
	"accounts", "account_usernames", "account_snapshots", "posts", "post_index", "post_mentions",
	"post_metrics", "music", "comments", "people", "media", "media_acks",
	"api_clients", "jobs", "crawl_cursors", "account_schedule", "account_registrations", "post_refresh",
	"operations", "client_params", "canary_runs",
}

func tablesIn(t *testing.T, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, schema string) []string {
	t.Helper()
	rows, err := db.Query(context.Background(), `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND NOT c.relispartition ORDER BY 1`, schema)
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// Schema tt migrates up, all the way down and up again on Postgres; every
// table is in tt and nothing lands in public, and goose's version table is
// tt's own.
func TestMigrateUpDownUp(t *testing.T) {
	ctx := context.Background()
	dsn := testenv.EmptyPostgres(t)
	db, err := ttstore.Open(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := ttstore.CheckSchema(ctx, db); !errors.Is(err, ttstore.ErrWrongSchema) {
		t.Fatalf("an unmigrated schema passed the check: %v", err)
	}
	check := func(stage string) {
		got := tablesIn(t, db, "tt")
		want := append(slices.Clone(tables), "goose_db_version")
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s: tables in tt %v, want %v", stage, got, want)
		}
		if pub := tablesIn(t, db, "public"); len(pub) != 0 {
			t.Fatalf("%s: tables in public: %v", stage, pub)
		}
	}
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	check("up")
	if err := ttstore.CheckSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := ttstore.MigrateDownTo(ctx, db, 0); err != nil {
		t.Fatal(err)
	}
	if got := tablesIn(t, db, "tt"); !slices.Equal(got, []string{"goose_db_version"}) {
		t.Fatalf("down left %v", got)
	}
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	check("up again")
	st, err := ttstore.MigrateStatus(ctx, db)
	if err != nil || len(st) == 0 {
		t.Fatalf("%v %v", st, err)
	}
	for _, l := range st {
		if statusState(l) != "applied" {
			t.Fatalf("not applied: %s", l)
		}
	}

	// A pool that does not pin search_path (pg.Open) resolves names in
	// public: the start-up check refuses it.
	plain, err := pg.Open(ctx, dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if err := ttstore.CheckSchema(ctx, plain); !errors.Is(err, ttstore.ErrWrongSchema) {
		t.Fatalf("an unpinned pool passed the check: %v", err)
	}
	// A DSN's own search_path cannot move a pool off tt.
	other, err := ttstore.Open(ctx, dsn+"&search_path=public", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := ttstore.CheckSchema(ctx, other); err != nil {
		t.Fatal(err)
	}
}

func statusState(line string) string {
	var v int
	var state, path string
	_, _ = fmt.Sscan(line, &v, &state, &path)
	return state
}

// The schema holds what the pipeline writes: partitions for the backfill
// window, a row in every table, the operation seed in tt.operations read
// back by the registry, and the copied partition code's names resolving in
// tt.
func TestSchemaHoldsData(t *testing.T) {
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	n, err := ttstore.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 13, MonthsAhead: 2, DaysBack: 1, DaysAhead: 2})
	if err != nil {
		t.Fatal(err)
	}
	// 16 months (2025-10 to 2027-01) of posts, comments and snapshots, 4
	// days of metrics.
	if n != 16*3+4 {
		t.Fatalf("%d partitions", n)
	}
	var where string
	if err := db.QueryRow(ctx, `SELECT relnamespace::regnamespace::text FROM pg_class WHERE relname = 'posts_y2025m11'`).Scan(&where); err != nil || where != "tt" {
		t.Fatalf("partition in %q (%v)", where, err)
	}

	key := []byte{1, 2, 3}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	created := now.Add(-48 * time.Hour)
	exec(`INSERT INTO accounts (user_id, sec_uid, username, author_key, follower_count) VALUES (7, 'MS4w', 'someone', $1, 15858019)`, key)
	exec(`INSERT INTO account_usernames VALUES (7, 'someone', now(), now())`)
	exec(`INSERT INTO account_snapshots (user_id, captured_at, follower_count) VALUES (7, $1, 15858019)`, now)
	exec(`INSERT INTO posts (created_at, post_id, user_id, kind, hashtags, is_pinned, labels, play_count, content_hash, first_seen_at, last_seen_at)
		VALUES ($1, 7400000000000000001, 7, 'photo', '{news}', true, '{"ai_generated": false}', 10, $2, now(), now())`, created, key)
	exec(`INSERT INTO post_index VALUES (7400000000000000001, 7, $1, 'photo')`, created)
	exec(`INSERT INTO post_mentions (post_id, author_key, offsets) VALUES (7400000000000000001, $1, '{3,9}')`, key)
	exec(`INSERT INTO post_metrics (post_id, captured_at, user_id, play_count, repost_count) VALUES (7400000000000000001, $1, 7, 10, 1)`, now)
	exec(`INSERT INTO music (music_id, title, original) VALUES (9, 'sound', true)`)
	exec(`INSERT INTO comments (post_created_at, post_id, comment_id, user_id, depth, parent_comment_id, author_key, content_hash, first_seen_at, last_seen_at)
		VALUES ($1, 7400000000000000001, 7400000000000000002, 7, 1, 7400000000000000003, $2, $2, now(), now())`, created, key)
	exec(`INSERT INTO people (author_key, user_id, predicted_age_group, first_seen_at, last_seen_at) VALUES ($1, 8, 'band', now(), now())`, key)
	exec(`INSERT INTO media (media_id, source_media_id, kind, variant, post_id) VALUES ('00000000-0000-0000-0000-000000000001', '7400000000000000001', 'photo', 'original', 7400000000000000001)`)
	exec(`INSERT INTO media_acks VALUES ('00000000-0000-0000-0000-000000000001', 'analysis', 'processed', NULL, now())`)
	exec(`INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ('00000000-0000-0000-0000-0000000000a1', 'c', 'p', '\x00', '{data:read}')`)
	exec(`INSERT INTO jobs (job_id, api_client_id, kind, lane, mode, account_ref, user_id, params, status)
		VALUES ('00000000-0000-0000-0000-0000000000b1', '00000000-0000-0000-0000-0000000000a1', 'on_demand', 0, 'full', 'u:someone', 7, '{}', 'queued')`)
	exec(`INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', 7, 'partial')`)
	exec(`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at) VALUES ('u:someone', 7, 9900, now())`)
	exec(`INSERT INTO account_registrations (api_client_id, account_ref) VALUES ('00000000-0000-0000-0000-0000000000a1', 'u:someone')`)
	exec(`INSERT INTO post_refresh (post_id, user_id, post_created_at, next_refresh_at) VALUES (7400000000000000001, 7, $1, now())`, created)
	exec(`INSERT INTO canary_runs (started_at, finished_at, status, report) VALUES (now(), now(), 'ok', '{}')`)
	for _, tb := range tables {
		var rows int
		if err := db.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{tb}.Sanitize()).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows == 0 && tb != "operations" && tb != "client_params" {
			t.Errorf("%s: no row", tb)
		}
	}
	// sec_uid is unique: one account per secUid.
	if _, err := db.Exec(ctx, `INSERT INTO accounts (user_id, sec_uid, author_key) VALUES (8, 'MS4w', $1)`, key); err == nil {
		t.Fatal("a second account took the same sec_uid")
	}
	// Retention drops whole partitions, in tt.
	dropped, err := store.DropPartitionsBefore(ctx, db, "posts", time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC))
	slices.Sort(dropped)
	if err != nil || !slices.Equal(dropped, []string{"posts_y2025m10", "posts_y2025m11"}) {
		t.Fatalf("dropped %v (%v)", dropped, err)
	}

	// The operation seed lands in tt.operations and the registry reads it
	// from there.
	seed, err := ttclient.Seed()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.ApplySeedTo(ctx, db, ttstore.Registry, seed); err != nil {
		t.Fatal(err)
	}
	if err := registry.ApplySeedTo(ctx, db, ttstore.Registry, seed); err != nil {
		t.Fatalf("seeding twice: %v", err)
	}
	exec(`INSERT INTO client_params (key, value) VALUES ('tz_name', 'UTC')`)
	reg, err := registry.NewDBFrom(ctx, db, ttstore.Registry, seed)
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.All()) != len(seed.Queries) {
		t.Fatalf("%d operations, seed has %d", len(reg.All()), len(seed.Queries))
	}
	q, err := reg.Get("creator_list")
	if err != nil || q.Method != "GET" || q.DocID != "creator" {
		t.Fatalf("creator_list: %+v %v", q, err)
	}
	if reg.ClientParams()["tz_name"] != "UTC" || reg.ClientParams()["aid"] != "1988" {
		t.Fatalf("client params: %v", reg.ClientParams())
	}
	q.Status = registry.StatusActive
	q.DocID = "creator-v2"
	if err := registry.UpsertTo(ctx, db, ttstore.Registry, q); err != nil {
		t.Fatal(err)
	}
	all, err := registry.ListFrom(ctx, db, ttstore.Registry)
	if err != nil || len(all) != len(seed.Queries)+1 {
		t.Fatalf("%d entries (%v)", len(all), err)
	}
}
