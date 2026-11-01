package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

type env struct {
	db   *pgxpool.Pool
	trk  *jobs.Tracker
	prod *kafka.Producer
	disp *dispatch.Dispatcher
	cfg  *config.Config
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	brokers := testenv.Kafka(t)
	prefix := testenv.TopicPrefix()
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, ClientID: "sched-test", TopicPrefix: prefix, Linger: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	var specs []kafka.TopicSpec
	for _, n := range []string{topics.Job, topics.Task(tkstaskv1.Kind_KIND_ACCOUNT, tkstaskv1.Lane_LANE_P1)} {
		specs = append(specs, kafka.TopicSpec{Name: n, Partitions: 1})
	}
	if err := kafka.EnsureTopics(ctx, prod.Client(), prefix, specs, 1); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	trk := jobs.New(testenv.Redis(t))
	return &env{db: db, trk: trk, prod: prod, cfg: cfg, disp: &dispatch.Dispatcher{DB: db, Jobs: trk, Producer: prod}}
}

func (e *env) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, err := e.db.Exec(context.Background(), q, args...); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	lim := &tkstaskv1.Limits{MaxAgeSeconds: 86400}
	req := func(ref string) dispatch.Request {
		return dispatch.Request{Kind: dispatch.KindOnDemand, Lane: tkstaskv1.Lane_LANE_P1, Mode: dispatch.ModeAuto, AccountRef: ref, Limits: lim}
	}
	// A user id alone cannot be read.
	if _, _, err := e.disp.Create(ctx, req("id:42"), 0); !errors.Is(err, dispatch.ErrUnknownAccount) {
		t.Fatalf("id: of an unknown account: %v", err)
	}
	// A new username: a backfill.
	j, created, err := e.disp.Create(ctx, req("u:new_account"), 0)
	if err != nil || !created || j.Mode != dispatch.ModeFull || j.UserID != nil {
		t.Fatalf("new account: %+v %v %v", j, created, err)
	}
	if st, err := e.trk.Status(ctx, j.ID.String()); err != nil || st.Spawned != 1 || st.Pending != 1 {
		t.Errorf("root task not registered: %+v %v", st, err)
	}
	// A stored account, renamed since, whose list was read: found by its
	// old name, checked incrementally, with its read times.
	e.exec(t, `INSERT INTO accounts (user_id, sec_uid, username, author_key, status, profile_refreshed_at, page_read_at)
		VALUES (7, 'SEC7', 'now_named', '\x01', 'active', now(), now())`)
	e.exec(t, `INSERT INTO account_usernames (user_id, username, first_seen_at, last_seen_at) VALUES
		(7, 'old_name', now() - interval '2 days', now() - interval '1 day'), (7, 'now_named', now(), now())`)
	e.exec(t, `INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', 7, 'complete')`)
	a, ok, err := dispatch.Resolve(ctx, e.db, "u:old_name")
	if err != nil || !ok || a.UserID != 7 || a.Username != "now_named" || a.ListCheckedAt == nil {
		t.Fatalf("resolve by an old username: %+v %v %v", a, ok, err)
	}
	sc := dispatch.Scope("u:old_name", a)
	if sc.UserId != 7 || sc.SecUid != "SEC7" || sc.Username != "now_named" || sc.AccountSeenAt == nil || sc.PageSeenAt == nil {
		t.Errorf("scope %+v", sc)
	}
	a.Status = "private"
	if sc := dispatch.Scope("u:old_name", a); sc.AccountSeenAt != nil || sc.PageSeenAt != nil {
		t.Error("a private account's scope keeps its read times: its page would not be read again")
	}
	for _, ref := range []string{"u:old_name", "sec:SEC7", "id:7"} {
		j, _, err := e.disp.Create(ctx, req(ref), 0)
		if err != nil || j.Mode != dispatch.ModeIncremental || j.UserID == nil || *j.UserID != 7 {
			t.Errorf("%s: %+v %v", ref, j, err)
		}
	}
	// Idempotency and the client's cap.
	client := uuid.New()
	e.exec(t, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, 'c', 'tks_x', '\x00', '{}')`, client)
	r := req("u:capped")
	r.ClientID, r.IdempotencyKey = &client, "k1"
	first, created, err := e.disp.Create(ctx, r, 1)
	if err != nil || !created {
		t.Fatal(err)
	}
	again, created, err := e.disp.Create(ctx, r, 1)
	if err != nil || created || again.ID != first.ID {
		t.Errorf("same idempotency key: %+v %v %v", again, created, err)
	}
	r.IdempotencyKey = "k2"
	if _, _, err := e.disp.Create(ctx, r, 1); !errors.Is(err, dispatch.ErrTooManyJobs) {
		t.Errorf("over the client's cap: %v", err)
	}
	// Visibility: another client sees neither the job nor its existence.
	other := uuid.New()
	if _, err := e.disp.GetFor(ctx, first.ID, &other, dispatch.Read); err == nil {
		t.Error("another client reads the job")
	}
	if _, err := e.disp.GetFor(ctx, first.ID, &client, dispatch.Write); err != nil {
		t.Errorf("its client cannot read the job: %v", err)
	}
}

func TestReaper(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	id := uuid.New()
	e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at)
		VALUES ($1, 'scheduled', 2, 'incremental', 'u:stuck', '{}', 'running', now() - interval '2 hours')`, id)
	e.exec(t, `INSERT INTO account_schedule (account_ref, interval_seconds, next_run_at, last_job_id) VALUES ('u:stuck', 9900, now(), $1)`, id)
	if err := e.trk.Spawn(ctx, id.String(), "task-1"); err != nil {
		t.Fatal(err)
	}
	r := &Reaper{DB: e.db, Jobs: e.trk, Producer: e.prod, After: time.Millisecond, MaxAge: time.Hour}
	time.Sleep(1100 * time.Millisecond) // activity is kept in seconds
	if err := r.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	var failures int
	if err := e.db.QueryRow(ctx, `SELECT j.status, s.consecutive_failures FROM jobs j JOIN account_schedule s ON s.last_job_id = j.job_id
		WHERE j.job_id = $1`, id).Scan(&status, &failures); err != nil {
		t.Fatal(err)
	}
	if status != "partial" || failures != 1 {
		t.Errorf("stalled job: %s, schedule failures %d", status, failures)
	}
}

func TestMaintenance(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	now := time.Now().UTC()
	old := now.AddDate(0, 0, -400)
	for _, tb := range []string{"comments", "post_metrics", "account_snapshots"} {
		if err := ttstore.Tables.EnsureFor(ctx, e.db, tb, old); err != nil {
			t.Fatal(err)
		}
	}
	e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at, finished_at)
		VALUES ($1, 'scheduled', 2, 'incremental', 'u:a', '{}', 'completed', $2, $2)`, uuid.New(), old)
	e.exec(t, `INSERT INTO people (author_key, first_seen_at, last_seen_at) VALUES ('\x01', $1, $1), ('\x02', now(), now())`, old)
	var before int64
	if err := e.db.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relname ~ $1`, fmt.Sprintf("_(y%04dm%02d|d%s)$",
		old.Year(), int(old.Month()), old.Format("20060102"))).Scan(&before); err != nil || before != 3 {
		t.Fatalf("%d old partitions before maintenance (%v), want 3", before, err)
	}
	m := &Maintenance{DB: e.db, Cfg: e.cfg}
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var n int64
	for q, want := range map[string]int64{`SELECT count(*) FROM jobs`: 0, `SELECT count(*) FROM people`: 1} {
		if err := e.db.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Errorf("%s: %d, want %d", q, n, want)
		}
	}
	month := fmt.Sprintf("y%04dm%02d", old.Year(), int(old.Month()))
	for _, part := range []string{"comments_" + month, "account_snapshots_" + month, "post_metrics_d" + old.Format("20060102")} {
		var exists bool
		if err := e.db.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, part).Scan(&exists); err != nil || exists {
			t.Errorf("partition %s past retention still there (%v)", part, err)
		}
	}
	// Partitions ahead exist for writes to come.
	if err := e.db.QueryRow(ctx, `SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'posts'`).
		Scan(&n); err != nil || n < 13 {
		t.Errorf("%d post partitions (%v), want a year and more", n, err)
	}
}
