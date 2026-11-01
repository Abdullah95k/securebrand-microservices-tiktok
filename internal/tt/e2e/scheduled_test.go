package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scheduler"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

var (
	p1 = int16(tkstaskv1.Lane_LANE_P1)
	p2 = int16(tkstaskv1.Lane_LANE_P2)
)

// sched is the scheduler's planner and finalizer on the stack, with the
// world's clock.
type sched struct {
	s   *Stack
	pl  *scheduler.Planner
	fin *scheduler.Finalizer
}

func newSched(s *Stack) *sched {
	disp := &dispatch.Dispatcher{DB: s.DB, Jobs: s.Tracker, Producer: s.Producer}
	return &sched{s: s,
		pl:  &scheduler.Planner{DB: s.DB, Dispatch: disp, Cfg: s.Cfg, Now: s.World.Clock},
		fin: &scheduler.Finalizer{DB: s.DB, Jobs: s.Tracker, Producer: s.Producer, Cfg: s.Cfg, Timeout: time.Minute, Now: s.World.Clock}}
}

// register puts accounts on the schedule, due now.
func (sc *sched) register(t *testing.T, accs ...*fakett.Account) {
	t.Helper()
	for _, a := range accs {
		if _, err := sc.s.DB.Exec(context.Background(), `INSERT INTO account_schedule (account_ref, interval_seconds, next_run_at)
			VALUES ($1, $2, $3)`, "u:"+a.Username, int(sc.s.Cfg.TT.Freshness.ListInterval.Seconds()), sc.s.World.Clock()); err != nil {
			t.Fatal(err)
		}
	}
}

// job is a finished job row.
type job struct {
	kind, mode, status string
	lane               int16
	params             map[string]any
	counters           map[string]int64
}

// tick runs the planner once, waits for the jobs it started and finalises
// them; it returns them by account reference.
func (sc *sched) tick(t *testing.T) map[string]job {
	t.Helper()
	ctx := context.Background()
	before := map[string]*uuid.UUID{}
	rows, err := sc.s.DB.Query(ctx, `SELECT account_ref, last_job_id FROM account_schedule`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var ref string
		var id *uuid.UUID
		if err := rows.Scan(&ref, &id); err != nil {
			t.Fatal(err)
		}
		before[ref] = id
	}
	rows.Close()
	if err := sc.pl.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	started := map[string]uuid.UUID{}
	rows, err = sc.s.DB.Query(ctx, `SELECT account_ref, last_job_id FROM account_schedule WHERE last_job_id IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var ref string
		var id uuid.UUID
		if err := rows.Scan(&ref, &id); err != nil {
			t.Fatal(err)
		}
		if b := before[ref]; b == nil || *b != id {
			started[ref] = id
		}
	}
	rows.Close()
	ids := make([]string, 0, len(started))
	for _, id := range started {
		ids = append(ids, id.String())
	}
	sc.s.Wait(t, 3*time.Minute, ids...)
	out := map[string]job{}
	deadline := time.Now().Add(time.Minute)
	for ref, id := range started {
		for {
			if err := sc.fin.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			var j job
			var params, counters []byte
			if err := sc.s.DB.QueryRow(ctx, `SELECT kind, mode, status, lane, params, coalesce(counters, '{}') FROM jobs WHERE job_id = $1`, id).
				Scan(&j.kind, &j.mode, &j.status, &j.lane, &params, &counters); err != nil {
				t.Fatal(err)
			}
			if j.status != "running" {
				_ = json.Unmarshal(params, &j.params)
				_ = json.Unmarshal(counters, &j.counters)
				out[ref] = j
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s of %s not finalised", id, ref)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return out
}

// jobEvents reads tt.job.v1 until it holds n events.
func jobEvents(t *testing.T, s *Stack, n int) []*commonv1.JobEvent {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Cfg.Kafka.Brokers...), kgo.ConsumeTopics(s.Cfg.Kafka.TopicPrefix+topics.Job),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []*commonv1.JobEvent
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("%d job events, want %d", len(out), n)
		}
		fs.EachRecord(func(r *kgo.Record) {
			ev := &commonv1.JobEvent{}
			if err := proto.Unmarshal(r.Value, ev); err != nil {
				t.Fatal(err)
			}
			out = append(out, ev)
		})
	}
	return out
}

// TestScheduledChecks runs tks's scheduler as tks-scheduler wires it (the
// planner and the finalizer) with the crawler and the persister against
// fakett, and checks after every round that the store equals fakett's
// world (build brief §8, A6) and the schedule moved on:
//  1. registered accounts are backfilled (backfill lane), finalised with a
//     tt.job.v1 event each, their comments' coverage recorded, and every
//     metrics walk stamped;
//  2. nothing is started before the list interval has passed;
//  3. 7 h later: incremental checks find a new post, and a post whose
//     count went up after one comment was deleted and others added is read
//     whole, so the finalizer marks the comment and its replies deleted;
//  4. a metrics walk replaces the check when due, in the backfill lane;
//  5. an account gone private is checked again only after a day.
func TestScheduledChecks(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 11, Accounts: 3, PostsPerAccount: 12, MaxComments: 14, MaxReplies: 4,
		PostSpacing: 30 * time.Hour}})
	w := s.World
	accs := w.Accounts()
	ctx := context.Background()
	// A fresh post with comments and replies, for the deletion in round 3.
	fresh := w.AddPost(accs[1], 6)
	w.AddReplies(fresh.Comments[0], 2)
	sc := newSched(s)
	sc.register(t, accs...)
	none := truth{}

	// 1. Backfills.
	got := sc.tick(t)
	if len(got) != len(accs) {
		t.Fatalf("%d jobs started, want %d", len(got), len(accs))
	}
	for ref, j := range got {
		if j.kind != dispatch.KindBackfill || j.mode != dispatch.ModeFull || j.lane != p2 || j.status != "completed" {
			t.Errorf("%s: job %+v, want a completed backfill in p2", ref, j)
		}
	}
	for _, a := range accs {
		sameStore(t, s, a, none)
	}
	evs := jobEvents(t, s, len(accs))
	for _, ev := range evs {
		if ev.Status != commonv1.JobStatus_JOB_STATUS_COMPLETED || ev.SubjectId == 0 || ev.Platform != commonv1.Platform_PLATFORM_TIKTOK ||
			ev.Completeness.GetPosts() == 0 {
			t.Errorf("job event %+v", ev)
		}
	}
	var nulls, unwalked int64
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FILTER (WHERE user_id IS NULL OR last_success_at IS NULL OR consecutive_failures > 0),
		count(*) FILTER (WHERE NOT (walks ? $1 AND walks ? $2)) FROM account_schedule`,
		scheduler.WalkKey(s.Cfg.TT.Freshness.MetricsWalks[0]), scheduler.WalkKey(s.Cfg.TT.Freshness.MetricsWalks[1])).Scan(&nulls, &unwalked); err != nil {
		t.Fatal(err)
	}
	if nulls != 0 || unwalked != 0 {
		t.Errorf("schedule after the backfill: %d rows without user id or success, %d without both walks", nulls, unwalked)
	}
	if n := count(t, s, `SELECT count(*) FROM posts WHERE comment_count > 0 AND comments_state <> 'complete'`); n != 0 {
		t.Errorf("%d posts with comments not recorded complete", n)
	}
	if n := count(t, s, `SELECT count(*) FROM posts WHERE comment_count > 0 AND completeness < 1`); n != 0 {
		t.Errorf("%d posts read whole with a completeness under 1", n)
	}
	if t.Failed() {
		t.FailNow()
	}

	// 2. Not due yet.
	w.Advance(time.Hour)
	if got := sc.tick(t); len(got) != 0 {
		t.Errorf("%d jobs an hour after the backfill", len(got))
	}

	// 3. Checks: a new post, and a deleted comment on a post whose count
	// still went up (past its 6 h comment window).
	w.Advance(6 * time.Hour)
	np := w.AddPost(accs[0], 3)
	gone := fresh.Comments[0]
	w.DeleteComment(fresh, gone)
	w.AddComments(fresh, 5) // 8 before, 5 after the deletion, 10 now
	got = sc.tick(t)
	if len(got) != len(accs) {
		t.Fatalf("%d checks, want %d", len(got), len(accs))
	}
	for ref, j := range got {
		if j.kind != dispatch.KindScheduled || j.mode != dispatch.ModeIncremental || j.lane != p1 || j.status != "completed" {
			t.Errorf("%s: job %+v, want a completed incremental check in p1", ref, j)
		}
	}
	if d := got["u:"+accs[1].Username].counters["comments_deleted"]; d != 3 {
		t.Errorf("%d comments marked deleted, want 3 (a comment and its 2 replies)", d)
	}
	for _, a := range accs {
		sameStore(t, s, a, none)
	}
	if n := count(t, s, `SELECT count(*) FROM comments WHERE (comment_id = $1 OR parent_comment_id = $1) AND status = 'deleted'
		AND deleted_at IS NOT NULL`, gone.ID); n != 3 {
		t.Errorf("%d rows of the deleted comment marked deleted, want 3", n)
	}
	if n := count(t, s, `SELECT count(*) FROM posts WHERE post_id = $1`, np.ID); n != 1 {
		t.Error("the new post was not stored")
	}
	if t.Failed() {
		t.FailNow()
	}

	// 4. A metrics walk is due for one account: it replaces the check.
	w.Advance(3*time.Hour + 30*time.Minute) // past 2 h 45 min and its jitter (±10 %)
	if _, err := s.DB.Exec(ctx, `UPDATE account_schedule SET walks = '{}' WHERE account_ref = $1`, "u:"+accs[2].Username); err != nil {
		t.Fatal(err)
	}
	got = sc.tick(t)
	walk := got["u:"+accs[2].Username]
	if walk.mode != dispatch.ModeRefresh || walk.lane != p2 || walk.status != "completed" ||
		walk.params["walk"] != scheduler.WalkKey(s.Cfg.TT.Freshness.MetricsWalks[1]) {
		t.Errorf("walk job %+v, want the deepest walk in p2", walk)
	}
	if j := got["u:"+accs[0].Username]; j.mode != dispatch.ModeIncremental {
		t.Errorf("the other accounts' jobs: %+v", j)
	}
	if n := count(t, s, `SELECT count(*) FROM account_schedule WHERE account_ref = $1 AND walks ? $2 AND walks ? $3`,
		"u:"+accs[2].Username, scheduler.WalkKey(s.Cfg.TT.Freshness.MetricsWalks[0]), scheduler.WalkKey(s.Cfg.TT.Freshness.MetricsWalks[1])); n != 1 {
		t.Error("the walk was not recorded")
	}
	for _, a := range accs {
		sameStore(t, s, a, none)
	}

	// 5. An account gone private: its check records the status, and the
	// next one waits a day.
	w.Advance(3*time.Hour + 30*time.Minute) // past 2 h 45 min and its jitter (±10 %)
	w.Mutate(func() { accs[0].Private = true })
	got = sc.tick(t)
	if j := got["u:"+accs[0].Username]; j.status != "completed" || j.counters["accounts_private"] != 1 {
		t.Errorf("private account's job %+v", j)
	}
	var status string
	var next time.Time
	if err := s.DB.QueryRow(ctx, `SELECT a.status, s.next_run_at FROM account_schedule s JOIN accounts a USING (user_id)
		WHERE s.account_ref = $1`, "u:"+accs[0].Username).Scan(&status, &next); err != nil {
		t.Fatal(err)
	}
	if status != "private" || next.Before(w.Clock().Add(s.Cfg.TT.Freshness.AccountRefresh-time.Minute)) {
		t.Errorf("private account: status %s, next check %v (now %v)", status, next, w.Clock())
	}
}
