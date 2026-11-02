package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
)

// delay returns tks_post_store_delay_seconds{mode}'s count and sum.
func delay(t *testing.T, mode string) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	if err := metrics.PostStoreDelay.WithLabelValues(mode).(prometheus.Histogram).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

func checks(kind, result, window string) float64 {
	var m dto.Metric
	_ = metrics.Checks.WithLabelValues(kind, result, window).Write(&m)
	return m.GetCounter().GetValue()
}

func gauge(g prometheus.Gauge) float64 {
	var m dto.Metric
	_ = g.Write(&m)
	return m.GetGauge().GetValue()
}

// TestObjectiveMetrics measures build brief §8's A1 and A2 end to end, as
// tks-scheduler and tks-persister publish them, against fakett with the
// world's clock:
//  1. backfills count as backfills within their window, and their posts
//     are not new (no delay observed; first_mode full);
//  2. a post published 1.5 h before the next check is stored 1.5 h after
//     its createTime (the histogram and posts.stored_at agree), and the
//     checks count as successful within their interval;
//  3. the leader's gauges: the oldest successful check, and an account
//     whose daily refresh is more than a day overdue;
//  4. checks that end after their interval count as late.
func TestObjectiveMetrics(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 31, Accounts: 2, PostsPerAccount: 5, MaxComments: 2, MaxReplies: 1,
		PostSpacing: 30 * time.Hour}})
	w := s.World
	accs := w.Accounts()
	ctx := context.Background()
	sc := newSched(s)
	sc.register(t, accs...)
	n := float64(len(accs))

	// 1. Backfills.
	bf0 := checks("backfill", "completed", "in")
	inc0, sum0 := delay(t, "incremental")
	full0, _ := delay(t, "full")
	for ref, j := range sc.tick(t) {
		if j.kind != dispatch.KindBackfill || j.status != "completed" {
			t.Fatalf("%s: %+v", ref, j)
		}
	}
	if got := checks("backfill", "completed", "in") - bf0; got != n {
		t.Errorf("%v backfills counted in their window, want %v", got, n)
	}
	if c, _ := delay(t, "incremental"); c != inc0 {
		t.Errorf("%d delays observed for backfilled posts", c-inc0)
	}
	if c, _ := delay(t, "full"); c != full0 {
		t.Error("a backfill's posts were counted as new")
	}
	if got := count(t, s, `SELECT count(*) FROM posts WHERE first_mode = 'full' AND stored_at IS NOT NULL`); got != int64(5*len(accs)) {
		t.Errorf("%d backfilled posts with first_mode full and stored_at, want %d", got, 5*len(accs))
	}

	// 2. A new post, 1.5 h before the next check.
	c0 := checks("check", "completed", "in")
	w.Advance(2 * time.Hour)
	np := w.AddPost(accs[0], 0)
	w.Advance(90 * time.Minute) // past 2 h 45 min and its jitter (±10 %)
	for ref, j := range sc.tick(t) {
		if j.mode != dispatch.ModeIncremental || j.status != "completed" {
			t.Fatalf("%s: %+v", ref, j)
		}
	}
	if got := checks("check", "completed", "in") - c0; got != n {
		t.Errorf("%v checks counted successful in their window, want %v", got, n)
	}
	c, sum := delay(t, "incremental")
	if c-inc0 != 1 || sum-sum0 != 5400 {
		t.Errorf("new posts' delays: %d observed, %v s in all; want 1 of 5400 s (1.5 h)", c-inc0, sum-sum0)
	}
	var mode string
	var d time.Duration
	if err := s.DB.QueryRow(ctx, `SELECT first_mode, stored_at - created_at FROM posts WHERE post_id = $1`, np.ID).Scan(&mode, &d); err != nil {
		t.Fatal(err)
	}
	if mode != "incremental" || d != 90*time.Minute {
		t.Errorf("new post: first_mode %q, stored %v after its createTime; want incremental, 1h30m", mode, d)
	}

	// 3. The leader's gauges.
	if err := sc.pl.TierGauges(ctx); err != nil {
		t.Fatal(err)
	}
	if v := gauge(metrics.CheckOldest); v != 0 {
		t.Errorf("oldest successful check %v s ago, want 0 (both just checked)", v)
	}
	if v := gauge(metrics.RefreshOverdue); v != 0 {
		t.Errorf("%v accounts' refresh overdue, want 0", v)
	}
	if _, err := s.DB.Exec(ctx, `UPDATE accounts SET profile_refreshed_at = $1 WHERE username = $2`,
		w.Clock().Add(-49*time.Hour), accs[1].Username); err != nil {
		t.Fatal(err)
	}
	w.Advance(7 * time.Hour)
	if err := sc.pl.TierGauges(ctx); err != nil {
		t.Fatal(err)
	}
	if v := gauge(metrics.CheckOldest); v < (7 * time.Hour).Seconds() {
		t.Errorf("oldest successful check %v s ago, want 7 h (A1's 6 h passed)", v)
	}
	if v := gauge(metrics.RefreshOverdue); v != 1 {
		t.Errorf("%v accounts' refresh overdue, want 1", v)
	}

	// 4. Checks that end after their interval are late.
	late0 := checks("check", "completed", "late")
	sc.beforeFinal = func() { w.Advance(3 * time.Hour) }
	sc.tick(t)
	sc.beforeFinal = nil
	if got := checks("check", "completed", "late") - late0; got != n {
		t.Errorf("%v checks counted late, want %v", got, n)
	}
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}
}

// TestBudgetPause runs the traffic budget as tks-scheduler (the
// controller, the planner's stretch) and tks-crawler (the gate on the
// backfill lanes) wire it, against fakett:
//  1. with room in the budget, a backfill runs;
//  2. once the day's traffic exceeds the budget, backfills pause: a new
//     account's backfill is queued but reads nothing, while the other
//     account's check (p1) still runs, and the planner stretches the
//     checks' interval;
//  3. with the budget raised, the backfill resumes and the store equals
//     the world.
func TestBudgetPause(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 41, Accounts: 2, PostsPerAccount: 6, MaxComments: 2, MaxReplies: 1,
		PostSpacing: 30 * time.Hour}, Config: func(c *config.Config) {
		c.Scheduler.Budget = config.Budget{Enabled: true, RequestsPerDay: 1 << 40, Interval: time.Minute, Stretch: 2,
			StretchTiers: []string{"T4", "T3", "T2"}, ProjectionSpan: time.Hour}
	}})
	w := s.World
	accs := w.Accounts()
	ctx := context.Background()
	ns := s.Cfg.Kafka.TopicPrefix
	gate := s.crawler().Budget
	if gate == nil {
		t.Fatal("the crawler has no budget gate")
	}
	sc := newSched(s)
	sc.pl.Budget = func(ctx context.Context) ratelimit.BudgetState {
		st, err := ratelimit.LoadBudgetState(ctx, s.Redis, ns)
		if err != nil {
			t.Error(err)
		}
		return st
	}
	// budget publishes the budget's state with the given daily request
	// budget, as tks-scheduler's controller does, and has the crawler's
	// gate read it.
	budget := func(perDay int64) ratelimit.BudgetState {
		t.Helper()
		b := s.Cfg.Scheduler.Budget
		b.RequestsPerDay = perDay
		if err := ratelimit.NewBudgetController(s.Redis, ns, b).Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if err := gate.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		return gate.State()
	}

	// 1. A backfill within the budget; the crawler reports its traffic.
	sc.register(t, accs[0])
	sc.tick(t)
	// Its requests reach the budget's day as the crawler flushes them
	// (every 2 s); the day is read once they stopped growing.
	var used int64
	for deadline := time.Now().Add(30 * time.Second); ; {
		time.Sleep(2500 * time.Millisecond)
		u := budget(1 << 40).Used.Requests[2]
		if u > 0 && u == used {
			break
		}
		if used = u; time.Now().After(deadline) {
			t.Fatal("the backfill's requests never settled in the budget")
		}
	}

	// 2. Over budget: backfills pause, checks go on, intervals stretch.
	st := budget(used / 2)
	if !st.PauseP2 || st.Stretch != 2 {
		t.Fatalf("budget state over budget: %+v", st)
	}
	if v := gauge(metrics.BudgetBackfillPaused); v != 1 {
		t.Errorf("tks_budget_backfill_paused = %v, want 1", v)
	}
	sc.register(t, accs[1])
	w.Advance(3*time.Hour + 30*time.Minute) // accs[0]'s check is due too
	now := w.Clock()
	if err := sc.pl.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var backfill, check uuid.UUID
	var next time.Time
	if err := s.DB.QueryRow(ctx, `SELECT last_job_id FROM account_schedule WHERE account_ref = $1`, "u:"+accs[1].Username).Scan(&backfill); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRow(ctx, `SELECT last_job_id, next_run_at FROM account_schedule WHERE account_ref = $1`, "u:"+accs[0].Username).
		Scan(&check, &next); err != nil {
		t.Fatal(err)
	}
	if earliest := now.Add(time.Duration(float64(2*s.Cfg.TT.Freshness.ListInterval) * 0.89)); next.Before(earliest) {
		t.Errorf("next check at %v, want the interval stretched twice (after %v)", next, earliest)
	}
	s.Wait(t, 3*time.Minute, check.String()) // p1 is not held back
	time.Sleep(3 * time.Second)              // time for a backfill that was not held back
	if n := count(t, s, `SELECT count(*) FROM accounts WHERE username = $1`, accs[1].Username); n != 0 {
		t.Fatal("the paused backfill read the account")
	}
	if st, err := s.Tracker.Status(ctx, backfill.String()); err != nil || st.Pending == 0 || st.Counters["requests"] != 0 {
		t.Fatalf("paused backfill: %+v (%v), want queued with no request", st, err)
	}

	// 3. The budget raised: the backfill resumes.
	if st := budget(1 << 40); st.PauseP2 || st.Stretch != 1 {
		t.Fatalf("budget state with room again: %+v", st)
	}
	s.Wait(t, 3*time.Minute, backfill.String())
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}
}
