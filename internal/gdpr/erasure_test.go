package gdpr

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

func TestRetryAfter(t *testing.T) {
	for n, want := range map[int]time.Duration{1: time.Minute, 2: 2 * time.Minute, 5: 16 * time.Minute, 9: 4 * time.Hour, 40: 4 * time.Hour} {
		if got := retryAfter(n); got != want {
			t.Errorf("retryAfter(%d) = %v, want %v", n, got, want)
		}
	}
}

// A request that fails on a passing error stays pending and is retried
// once its backoff is over; a request no retry can complete fails at once.
func TestErasureRetries(t *testing.T) {
	db := testenv.Postgres(t)
	brokers := testenv.Kafka(t)
	ctx := context.Background()
	prefix := testenv.TopicPrefix()

	down, err := kafka.NewProducer(kafka.ClientConfig{Brokers: []string{"127.0.0.1:1"}, TopicPrefix: prefix},
		kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	up, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()

	author, bogus := uuid.New(), uuid.New()
	for _, q := range []struct {
		id      uuid.UUID
		subject string
	}{{author, "author"}, {bogus, "bogus"}} {
		if _, err := db.Exec(ctx, `INSERT INTO erasure_requests (id, subject_type, author_key, input, status)
			VALUES ($1, $2, '\x0102'::bytea, '{}', 'pending')`, q.id, q.subject); err != nil {
			t.Fatal(err)
		}
	}
	state := func(id uuid.UUID) (status string, attempts int, due bool) {
		t.Helper()
		if err := db.QueryRow(ctx, `SELECT status, attempts, coalesce(next_attempt_at <= now(), true) FROM erasure_requests WHERE id = $1`,
			id).Scan(&status, &attempts, &due); err != nil {
			t.Fatal(err)
		}
		return
	}

	if err := (&Eraser{DB: db, Producer: down}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if s, n, due := state(author); s != "pending" || n != 1 || due {
		t.Fatalf("after a passing failure: %s, %d attempts, due %v", s, n, due)
	}
	if s, n, _ := state(bogus); s != "failed" || n != 1 {
		t.Fatalf("an invalid request: %s, %d attempts", s, n)
	}

	// Not due yet: nothing happens, even with Kafka back.
	if err := (&Eraser{DB: db, Producer: up}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if s, n, _ := state(author); s != "pending" || n != 1 {
		t.Fatalf("retried before its backoff: %s, %d", s, n)
	}
	if _, err := db.Exec(ctx, `UPDATE erasure_requests SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, author); err != nil {
		t.Fatal(err)
	}
	if err := (&Eraser{DB: db, Producer: up}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := state(author); s != "done" {
		t.Fatalf("after the retry: %s", s)
	}

	// The last allowed attempt fails the request for good.
	last := uuid.New()
	if _, err := db.Exec(ctx, `INSERT INTO erasure_requests (id, subject_type, author_key, input, status, attempts)
		VALUES ($1, 'author', '\x0103'::bytea, '{}', 'pending', $2)`, last, MaxErasureAttempts-1); err != nil {
		t.Fatal(err)
	}
	if err := (&Eraser{DB: db, Producer: down}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if s, n, _ := state(last); s != "failed" || n != MaxErasureAttempts {
		t.Fatalf("after the last attempt: %s, %d", s, n)
	}
}

// A writer's Guard waits for an erasure that holds the lock, and then sees
// its suppression entry: it can't check before the erasure commits and
// write after.
func TestGuardWaitsForErasure(t *testing.T) {
	db := testenv.Postgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	erasure, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = erasure.Rollback(ctx) }()
	if err := lockExclusive(ctx, erasure); err != nil {
		t.Fatal(err)
	}
	if _, err := erasure.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('author', 'abcd'), ('page', '7')`); err != nil {
		t.Fatal(err)
	}

	type result struct {
		author, page bool
		err          error
	}
	got := make(chan result, 1)
	go func() {
		writer, err := db.Begin(ctx)
		if err != nil {
			got <- result{err: err}
			return
		}
		defer func() { _ = writer.Rollback(ctx) }()
		sup, err := Guard(ctx, writer, [][]byte{{0xab, 0xcd}}, []int64{7})
		if err != nil {
			got <- result{err: err}
			return
		}
		got <- result{author: sup.AuthorSuppressed([]byte{0xab, 0xcd}), page: sup.PageSuppressed(7)}
	}()
	select {
	case r := <-got:
		t.Fatalf("the writer did not wait for the erasure: %+v", r)
	case <-time.After(500 * time.Millisecond):
	}
	if err := erasure.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if r := <-got; r.err != nil || !r.author || !r.page {
		t.Fatalf("after the erasure: %+v", r)
	}
}

// An author erasure deletes the author's media even when the comment isn't
// stored yet, and scrubs the author's mentions in other people's text; a
// page erasure deletes the page's comments and metrics through its posts.
func TestErasureReachesEverything(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: testenv.Kafka(t), TopicPrefix: testenv.TopicPrefix()})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	created := time.Now().UTC().Add(-time.Hour)
	for _, table := range []string{"posts", "comments"} {
		if err := store.EnsurePartitionFor(ctx, db, table, created); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.EnsurePartitionFor(ctx, db, "post_metrics", time.Now()); err != nil {
		t.Fatal(err)
	}
	mention := `[{"offset":0,"length":6,"entity_type":"User","entity_id":"100001","name":"Erased","url":"u"},{"entity_type":"User","entity_id":"100002","name":"Kept"}]`
	for _, q := range []string{
		`INSERT INTO posts (post_created_at, post_id, page_id, post_type, text, text_ranges, content_hash, first_seen_at, last_seen_at)
			VALUES ($1, 1, 9, 'status', 'hi Erased', $2, '\x01', now(), now()), ($1, 2, 8, 'status', 'other page', NULL, '\x01', now(), now())`,
		`INSERT INTO post_index (post_id, page_id, post_created_at) VALUES (1, 9, $1), (2, 8, $1)`,
		`INSERT INTO comments (post_created_at, post_id, comment_id, page_id, depth, author_key, text, text_ranges, content_hash, first_seen_at, last_seen_at)
			VALUES ($1, 1, 10, 9, 0, '\xbb', 'Erased: thanks', $2, '\x01', now(), now()), ($1, 2, 20, 8, 0, '\xbb', 'stays', NULL, '\x01', now(), now()),
				($1, 1, 11, 9, 0, '\xaa', 'the erased author, in the thread', NULL, '\x01', now(), now())`,
		`INSERT INTO post_metrics (post_id, captured_at, page_id) VALUES (1, now(), 9), (2, now(), 8)`,
		`INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, comment_id, page_id, status, author_key, alt_text)
			VALUES ('00000000-0000-0000-0000-0000000000a1', 'f', 'orig', 'photo', 'comment', 999, 7, 'discovered', '\xaa', 'a person smiling')`,
		// Page 8, scheduled under its vanity (before its id was known) and
		// under its id, with a job that names it by vanity.
		`INSERT INTO pages (page_id, vanity, name) VALUES (8, 'Brand8', 'Brand')`,
		`INSERT INTO page_schedule (page_ref, page_id, interval_seconds, next_run_at) VALUES ('v:brand8', NULL, 3600, now()), ('id:8', 8, 3600, now())`,
		`INSERT INTO jobs (job_id, kind, lane, mode, page_ref, params, status) VALUES (gen_random_uuid(), 'scheduled', 1, 'full', 'v:brand8', '{}', 'completed')`,
	} {
		var args []any
		if strings.Contains(q, "$2") {
			args = []any{created, mention}
		} else if strings.Contains(q, "$1") {
			args = []any{created}
		}
		if _, err := db.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, q := range []string{
		// The author row is already gone (a retry after the transaction that
		// deleted it): the Facebook id comes from the request.
		`INSERT INTO erasure_requests (id, subject_type, author_key, input, status) VALUES (gen_random_uuid(), 'author', '\xaa', '{"author_id":"100001"}', 'pending')`,
		`INSERT INTO erasure_requests (id, subject_type, page_id, input, status) VALUES (gen_random_uuid(), 'page', 8, '{}', 'pending')`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if err := (&Eraser{DB: db, Producer: prod}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]int{
		`SELECT count(*) FROM erasure_requests WHERE status = 'done'`:     2,
		`SELECT count(*) FROM erasure_requests WHERE input ? 'author_id'`: 0,
		`SELECT count(*) FROM media`:                                      0,
		`SELECT count(*) FROM comments WHERE comment_id = 10 AND text_ranges->0->>'entity_id' IS NULL
			AND text_ranges->0->>'name' IS NULL AND text_ranges->1->>'entity_id' = '100002' AND text = 'Erased: thanks'`: 1,
		`SELECT count(*) FROM posts WHERE post_id = 1 AND text_ranges->0->>'entity_id' IS NULL AND text_ranges->0->>'offset' = '0'`: 1,
		`SELECT count(*) FROM comments WHERE page_id = 8`:                                  0,
		`SELECT count(*) FROM post_metrics WHERE page_id = 8`:                              0,
		`SELECT count(*) FROM post_metrics WHERE page_id = 9`:                              1,
		`SELECT count(*) FROM comments WHERE page_id = 9`:                                  2,
		`SELECT count(*) FROM page_schedule WHERE enabled`:                                 0,
		`SELECT count(*) FROM page_schedule WHERE page_ref LIKE 'v:%'`:                     0,
		`SELECT count(*) FROM page_schedule WHERE page_ref = 'id:8'`:                       1,
		`SELECT count(*) FROM jobs WHERE page_ref = 'id:8'`:                                1,
		`SELECT count(*) FROM pages WHERE page_id = 8 AND vanity IS NULL AND name IS NULL`: 1,
	} {
		var n int
		if err := db.QueryRow(ctx, q).Scan(&n); err != nil || n != want {
			t.Errorf("%s = %d (%v), want %d", q, n, err, want)
		}
	}
}

// An erasure that couldn't send a media's deletion event leaves the row as
// it was, so the retried erasure finds the media again, sends it, and
// deletes the row.
func TestErasureMediaEventsAreNotLost(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	down, err := kafka.NewProducer(kafka.ClientConfig{Brokers: []string{"127.0.0.1:1"}}, kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	up, err := kafka.NewProducer(kafka.ClientConfig{Brokers: testenv.Kafka(t), TopicPrefix: testenv.TopicPrefix()})
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	for _, q := range []string{
		`INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, page_id, status, author_key, s3_bucket, s3_key)
			VALUES ('00000000-0000-0000-0000-0000000000b1', 'f', 'orig', 'photo', 'comment', 7, 'ready', '\xcc', 'media', 'media/00/b1.jpg')`,
		`INSERT INTO erasure_requests (id, subject_type, author_key, input, status) VALUES (gen_random_uuid(), 'author', '\xcc', '{}', 'pending')`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	status := func() (media, request string) {
		_ = db.QueryRow(ctx, `SELECT status FROM media`).Scan(&media)
		_ = db.QueryRow(ctx, `SELECT status FROM erasure_requests`).Scan(&request)
		return
	}
	if err := (&Eraser{DB: db, Producer: down}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if m, r := status(); m != "ready" || r != "pending" {
		t.Fatalf("with Kafka down: media %q, request %q", m, r)
	}
	if _, err := db.Exec(ctx, `UPDATE erasure_requests SET next_attempt_at = now()`); err != nil {
		t.Fatal(err)
	}
	if err := (&Eraser{DB: db, Producer: up}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if m, r := status(); m != "" || r != "done" {
		t.Fatalf("after the retry: media %q, request %q", m, r)
	}
}
