package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
)

// The planner lets new accounts start only as admission allows, the
// owner's first, and never holds up an admitted account's check
// (docs/tiktok/discovery-plan.md §5.3).
func TestPlannerAdmission(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	e.cfg.TT.Admission.PerHour = 2
	client := uuid.New()
	e.exec(t, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, 'c', 'c', '\x00', '{accounts:write}')`, client)
	// An account monitored for long, due now.
	e.exec(t, `INSERT INTO account_schedule (account_ref, interval_seconds, next_run_at, admitted_at) VALUES ('u:old', 9900, $1, $2)`,
		now.Add(-time.Minute), now.Add(-30*24*time.Hour))
	// Five new accounts: two discovered (registered first), three the owner's.
	for i, origin := range []string{accounts.OriginDiscovery, accounts.OriginDiscovery, accounts.OriginOwner, accounts.OriginOwner, accounts.OriginOwner} {
		prio := accounts.PriorityOwner
		if origin == accounts.OriginDiscovery {
			prio = accounts.PriorityDiscovered + 50
		}
		if _, err := accounts.Register(ctx, e.db, accounts.Registration{ClientID: client, Ref: fmt.Sprintf("u:new%d", i), How: accounts.HowAPI,
			Origin: origin, Priority: prio, DefaultInterval: 9900 * time.Second, Enabled: true, Now: now.Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	p := &Planner{DB: e.db, Dispatch: e.disp, Cfg: e.cfg, Now: func() time.Time { return now.Add(10 * time.Second) }}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	jobsFor := func() map[string]string {
		rows, err := e.db.Query(ctx, `SELECT account_ref, mode FROM jobs`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var r, m string
			_ = rows.Scan(&r, &m)
			out[r] = m
		}
		return out
	}
	got := jobsFor()
	// The old account's check, and the first two of the owner's backfills.
	want := map[string]string{"u:old": dispatch.ModeFull, "u:new2": dispatch.ModeFull, "u:new3": dispatch.ModeFull}
	if len(got) != len(want) {
		t.Fatalf("jobs %v, want %v", got, want)
	}
	for k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("no job for %s: %v", k, got)
		}
	}
	// Within the hour nothing more starts; the queue holds three.
	p.Now = func() time.Time { return now.Add(30 * time.Minute) }
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(jobsFor()); n != 3 {
		t.Errorf("%d jobs within the hour, want 3", n)
	}
	var queued int64
	_ = e.db.QueryRow(ctx, `SELECT count(*) FROM account_schedule WHERE admitted_at IS NULL`).Scan(&queued)
	if queued != 3 {
		t.Errorf("%d queued, want 3", queued)
	}
	// An hour on: the owner's last, then the higher-priority discovered one.
	p.Now = func() time.Time { return now.Add(71 * time.Minute) }
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	got = jobsFor()
	if _, ok := got["u:new4"]; !ok {
		t.Errorf("the owner's third account did not start: %v", got)
	}
	if _, ok := got["u:new0"]; !ok {
		t.Errorf("the first discovered account did not start: %v", got)
	}
	if _, ok := got["u:new1"]; ok {
		t.Errorf("more than two admitted in the hour: %v", got)
	}
}

// Accounts that went quiet get the lighter checks of tt.freshness.inactive,
// spaced by how long they have been so; a check that finds one active puts
// it back on its interval (discovery-plan §5.4).
func TestInactiveChecks(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	clock := now
	f := &Finalizer{DB: e.db, Cfg: e.cfg, Now: func() time.Time { return clock }}
	jid := uuid.New()
	e.exec(t, `INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at, last_job_id)
		VALUES ('u:q', 1, 9900, $1, $1, $2)`, now.Add(9900*time.Second), jid)
	check := func(status string, wantNext time.Duration, wantSince *time.Time) {
		t.Helper()
		if err := f.schedule(ctx, jid, "completed", dispatch.ModeIncremental, nil, clock, &dispatch.Account{UserID: 1, Status: status}); err != nil {
			t.Fatal(err)
		}
		var next time.Time
		var since *time.Time
		if err := e.db.QueryRow(ctx, `SELECT next_run_at, inactive_since FROM account_schedule WHERE account_ref = 'u:q'`).Scan(&next, &since); err != nil {
			t.Fatal(err)
		}
		if wantNext > 0 && !next.Equal(clock.Add(wantNext)) {
			t.Errorf("%s at +%v: next in %v, want %v", status, clock.Sub(now), next.Sub(clock), wantNext)
		}
		if (since == nil) != (wantSince == nil) || since != nil && !since.Equal(*wantSince) {
			t.Errorf("%s: inactive since %v, want %v", status, since, wantSince)
		}
	}
	day := 24 * time.Hour
	check("private", day, &now)
	clock = now.Add(6 * day)
	check("private", day, &now)
	clock = now.Add(8 * day)
	check("private", 7*day, &now)
	// Active again: back to its interval (the planner's next_run_at stays).
	clock = now.Add(15 * day)
	e.exec(t, `UPDATE account_schedule SET next_run_at = $1`, clock.Add(9900*time.Second))
	check("active", 9900*time.Second, nil)
	// Gone: weekly for 30 days, then monthly.
	check("unavailable", 7*day, &clock)
	start := clock
	clock = start.Add(31 * day)
	check("banned", 30*day, &start)
}

// Importing 10,000 accounts keeps the admission pace and leaves the checks
// of accounts already monitored on time (discovery-plan §14).
func TestImportTenThousand(t *testing.T) {
	if testing.Short() {
		t.Skip("10,000 registrations")
	}
	e := newEnv(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	e.cfg.TT.Admission.PerHour = 25
	client := uuid.New()
	e.exec(t, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, 'c', 'c', '\x00', '{accounts:write}')`, client)
	for i := range 20 {
		e.exec(t, `INSERT INTO account_schedule (account_ref, interval_seconds, next_run_at, admitted_at) VALUES ($1, 9900, $2, $3)`,
			fmt.Sprintf("u:monitored%02d", i), now.Add(-time.Minute), now.Add(-30*24*time.Hour))
	}
	var b []byte
	for i := range 10_000 {
		b = fmt.Appendf(b, "@acct%05d\n", i)
	}
	lines, err := accounts.Parse(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rep, err := accounts.Import(ctx, e.db, e.cfg, lines, accounts.ImportOptions{ClientID: client, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("imported 10,000 lines in %v", time.Since(start).Round(time.Millisecond))
	if rep.Added != 10_000 || rep.Queued != 10_000 || rep.Queue.PerHour != 25 || rep.Queue.EstimatedFinish == nil ||
		*rep.Queue.DrainHours != 400 {
		t.Fatalf("report: added %d queued %d queue %+v", rep.Added, rep.Queued, rep.Queue)
	}
	e.cfg.Scheduler.PlannerBatch = 1000
	p := &Planner{DB: e.db, Dispatch: e.disp, Cfg: e.cfg, Now: func() time.Time { return now.Add(time.Minute) }}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var checks, backfills int64
	_ = e.db.QueryRow(ctx, `SELECT count(*) FILTER (WHERE account_ref LIKE 'u:monitored%'), count(*) FILTER (WHERE account_ref LIKE 'u:acct%')
		FROM jobs`).Scan(&checks, &backfills)
	if checks != 20 || backfills != 25 {
		t.Errorf("%d checks of monitored accounts (want 20) and %d new accounts started (want 25)", checks, backfills)
	}
}

// A due check whose account's previous job is still going: a metrics walk
// the budget holds in the paused p2 lane is cancelled and the check runs
// now (the walk stays due); any other job still going makes the slot wait
// SlotRetry, not a whole interval.
func TestPlannerSlotsWithJobStillGoing(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	for i, ref := range []string{"u:walked", "u:checking"} {
		uid := int64(90 + i)
		e.exec(t, `INSERT INTO accounts (user_id, sec_uid, username, author_key, status, profile_refreshed_at, page_read_at)
			VALUES ($1, $2, $3, '\x01', 'active', now(), now())`, uid, fmt.Sprintf("SEC%d", uid), ref[2:])
		e.exec(t, `INSERT INTO account_usernames (user_id, username, first_seen_at, last_seen_at) VALUES ($1, $2, now(), now())`, uid, ref[2:])
		e.exec(t, `INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', $1, 'complete')`, uid)
	}
	walk, check := uuid.New(), uuid.New()
	e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at)
		VALUES ($1, 'scheduled', 3, 'refresh', 'u:walked', '{}', 'running', $3), ($2, 'scheduled', 2, 'incremental', 'u:checking', '{}', 'running', $3)`,
		walk, check, now.Add(-time.Hour))
	// u:checking's check is still walking its list (its cursor was saved
	// before the check began), so it is not a check beside comment work.
	e.exec(t, `UPDATE crawl_cursors SET updated_at = $1 WHERE scope_id = 91`, now.Add(-2*time.Hour))
	for ref, job := range map[string]uuid.UUID{"u:walked": walk, "u:checking": check} {
		e.exec(t, `INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at, last_job_id)
			SELECT $1, user_id, 9900, $2, $3, $4 FROM account_usernames WHERE username = $5`, ref, now.Add(-time.Minute), now.Add(-30*24*time.Hour), job, ref[2:])
	}
	p := &Planner{DB: e.db, Dispatch: e.disp, Cfg: e.cfg, Now: func() time.Time { return now },
		Budget: func(context.Context) ratelimit.BudgetState { return ratelimit.BudgetState{PauseP2: true, Stretch: 1} }}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var st string
	if err := e.db.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, walk).Scan(&st); err != nil || st != "cancelling" {
		t.Fatalf("held walk %q (%v), want cancelling", st, err)
	}
	var mode string
	if err := e.db.QueryRow(ctx, `SELECT j.mode FROM account_schedule s JOIN jobs j ON j.job_id = s.last_job_id
		WHERE s.account_ref = 'u:walked'`).Scan(&mode); err != nil || mode != dispatch.ModeIncremental {
		t.Fatalf("u:walked's job %q (%v), want the check", mode, err)
	}
	var last uuid.UUID
	var next time.Time
	if err := e.db.QueryRow(ctx, `SELECT last_job_id, next_run_at FROM account_schedule WHERE account_ref = 'u:checking'`).Scan(&last, &next); err != nil {
		t.Fatal(err)
	}
	if last != check {
		t.Errorf("u:checking got a new job while its check runs")
	}
	if d := next.Sub(now); d <= 0 || d > SlotRetry+time.Second {
		t.Errorf("u:checking next check in %s, want within %s", d, SlotRetry)
	}
}

// While p2 runs (no budget pause) but a backfill or walk is still reading
// an account's history there, its list checks go on beside it: a read
// account whose walk runs gets a p1 incremental check (the walk keeps
// running), and so does one whose last job ended while an older walk still
// runs (never a second walk, though one is due); an account never read
// waits for its backfill, which is its first read.
func TestPlannerCheckBesideHistory(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	refs := []string{"u:walking", "u:after", "u:backfilling"}
	for i, ref := range refs {
		uid := int64(70 + i)
		e.exec(t, `INSERT INTO accounts (user_id, sec_uid, username, author_key, status, profile_refreshed_at, page_read_at)
			VALUES ($1, $2, $3, '\x01', 'active', now(), now())`, uid, fmt.Sprintf("SEC%d", uid), ref[2:])
		e.exec(t, `INSERT INTO account_usernames (user_id, username, first_seen_at, last_seen_at) VALUES ($1, $2, now(), now())`, uid, ref[2:])
		if ref != "u:backfilling" {
			e.exec(t, `INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', $1, 'complete')`, uid)
		}
	}
	walk, oldWalk, done, backfill := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at) VALUES
		($1, 'scheduled', 3, 'refresh', 'u:walking', '{}', 'running', $5),
		($2, 'scheduled', 3, 'refresh', 'u:after', '{}', 'running', $5),
		($3, 'scheduled', 2, 'incremental', 'u:after', '{}', 'completed', $5),
		($4, 'backfill', 3, 'full', 'u:backfilling', '{}', 'running', $5)`,
		walk, oldWalk, done, backfill, now.Add(-time.Hour))
	for ref, job := range map[string]uuid.UUID{"u:walking": walk, "u:after": done, "u:backfilling": backfill} {
		e.exec(t, `INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at, last_job_id)
			SELECT $1, user_id, 9900, $2, $3, $4 FROM account_usernames WHERE username = $5`, ref, now.Add(-time.Minute), now.Add(-30*24*time.Hour), job, ref[2:])
	}
	p := &Planner{DB: e.db, Dispatch: e.disp, Cfg: e.cfg, Now: func() time.Time { return now },
		Budget: func(context.Context) ratelimit.BudgetState { return ratelimit.BudgetState{Stretch: 1} }}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{"u:walking", "u:after"} {
		var mode string
		var lane int16
		if err := e.db.QueryRow(ctx, `SELECT j.mode, j.lane FROM account_schedule s JOIN jobs j ON j.job_id = s.last_job_id
			WHERE s.account_ref = $1`, ref).Scan(&mode, &lane); err != nil {
			t.Fatal(err)
		}
		if mode != dispatch.ModeIncremental || lane != 2 {
			t.Errorf("%s's new job %s in lane %d, want the p1 incremental check", ref, mode, lane)
		}
	}
	var running int
	_ = e.db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE job_id IN ($1, $2) AND status = 'running'`, walk, oldWalk).Scan(&running)
	if running != 2 {
		t.Errorf("%d of the 2 walks still running, want both", running)
	}
	var walks int
	_ = e.db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE account_ref = 'u:after' AND mode = 'refresh'`).Scan(&walks)
	if walks != 1 {
		t.Errorf("u:after has %d walks, want its one running walk and no second", walks)
	}
	var last uuid.UUID
	var next time.Time
	if err := e.db.QueryRow(ctx, `SELECT last_job_id, next_run_at FROM account_schedule WHERE account_ref = 'u:backfilling'`).Scan(&last, &next); err != nil {
		t.Fatal(err)
	}
	if last != backfill {
		t.Errorf("u:backfilling, never read, got a job beside its backfill")
	}
	if d := next.Sub(now); d <= 0 || d > SlotRetry+time.Second {
		t.Errorf("u:backfilling next check in %s, want within %s", d, SlotRetry)
	}
}

// A running check whose list walk has ended (the list cursor saved after
// it began) waits only for comment work: the next check runs beside it,
// at most two at once. A check still walking its list waits.
func TestPlannerCheckBesideComments(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	refs := []string{"u:listed", "u:listing", "u:twoopen"}
	for i, ref := range refs {
		uid := int64(60 + i)
		e.exec(t, `INSERT INTO accounts (user_id, sec_uid, username, author_key, status, profile_refreshed_at, page_read_at)
			VALUES ($1, $2, $3, '\x01', 'active', now(), now())`, uid, fmt.Sprintf("SEC%d", uid), ref[2:])
		e.exec(t, `INSERT INTO account_usernames (user_id, username, first_seen_at, last_seen_at) VALUES ($1, $2, now(), now())`, uid, ref[2:])
		// The list cursor was saved 30 minutes ago.
		e.exec(t, `INSERT INTO crawl_cursors (scope_type, scope_id, state, updated_at) VALUES ('account_list', $1, 'complete', $2)`,
			uid, now.Add(-30*time.Minute))
	}
	listed, listing, two1, two2 := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at) VALUES
		($1, 'scheduled', 2, 'incremental', 'u:listed', '{}', 'running', $5),
		($2, 'scheduled', 2, 'incremental', 'u:listing', '{}', 'running', $6),
		($3, 'scheduled', 2, 'incremental', 'u:twoopen', '{}', 'running', $5),
		($4, 'scheduled', 2, 'incremental', 'u:twoopen', '{}', 'running', $5)`,
		listed, listing, two1, two2, now.Add(-time.Hour), now.Add(-10*time.Minute))
	for ref, job := range map[string]uuid.UUID{"u:listed": listed, "u:listing": listing, "u:twoopen": two2} {
		e.exec(t, `INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at, last_job_id)
			SELECT $1, user_id, 9900, $2, $3, $4 FROM account_usernames WHERE username = $5`, ref, now.Add(-time.Minute), now.Add(-30*24*time.Hour), job, ref[2:])
	}
	p := &Planner{DB: e.db, Dispatch: e.disp, Cfg: e.cfg, Now: func() time.Time { return now },
		Budget: func(context.Context) ratelimit.BudgetState { return ratelimit.BudgetState{Stretch: 1} }}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var last uuid.UUID
	var mode string
	var lane int16
	if err := e.db.QueryRow(ctx, `SELECT s.last_job_id, j.mode, j.lane FROM account_schedule s JOIN jobs j ON j.job_id = s.last_job_id
		WHERE s.account_ref = 'u:listed'`).Scan(&last, &mode, &lane); err != nil {
		t.Fatal(err)
	}
	if last == listed || mode != dispatch.ModeIncremental || lane != 2 {
		t.Errorf("u:listed: job %s %s lane %d, want a new p1 check beside the listed one", last, mode, lane)
	}
	for ref, job := range map[string]uuid.UUID{"u:listing": listing, "u:twoopen": two2} {
		if err := e.db.QueryRow(ctx, `SELECT last_job_id FROM account_schedule WHERE account_ref = $1`, ref).Scan(&last); err != nil {
			t.Fatal(err)
		}
		if last != job {
			t.Errorf("%s got a new job; want it to wait", ref)
		}
	}
}

func TestSlotFor(t *testing.T) {
	for _, c := range []struct {
		status, mode      string
		lane              int16
		paused, firstRead bool
		want              slot
	}{
		{"completed", dispatch.ModeRefresh, 3, true, true, slotGo},
		{"running", dispatch.ModeRefresh, 3, true, false, slotCancelWalk},
		{"running", dispatch.ModeRefresh, 3, false, true, slotWait},   // p2 runs: the walk stands in for the check
		{"running", dispatch.ModeFull, 3, true, false, slotWait},      // a backfill, no light first reads: nothing can run before it
		{"running", dispatch.ModeFull, 3, true, true, slotCancelWalk}, // a held backfill gives way to the light first read
		{"running", dispatch.ModeFull, 3, false, true, slotWait},      // a backfill p2 is running
		{"running", dispatch.ModeIncremental, 2, true, true, slotWait},
		{"finalizing", dispatch.ModeRefresh, 3, true, true, slotWait},
		{"cancelling", dispatch.ModeRefresh, 3, true, true, slotWait},
	} {
		if got := slotFor(c.status, c.mode, c.lane, c.paused, c.firstRead); got != c.want {
			t.Errorf("%+v: %v", c, got)
		}
	}
}

// While the budget pauses p2, a never-read account's backfill held there
// gives way to the light first read in p1.
func TestPlannerHeldBackfillGivesWayToFirstRead(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	e.exec(t, `INSERT INTO accounts (user_id, sec_uid, username, author_key, status, profile_refreshed_at, page_read_at)
		VALUES (95, 'SEC95', 'fresh', '\x01', 'active', now(), now())`)
	e.exec(t, `INSERT INTO account_usernames (user_id, username, first_seen_at, last_seen_at) VALUES (95, 'fresh', now(), now())`)
	backfill := uuid.New()
	e.exec(t, `INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at)
		VALUES ($1, 'backfill', 3, 'full', 'u:fresh', '{}', 'running', $2)`, backfill, now.Add(-time.Hour))
	e.exec(t, `INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at, last_job_id)
		VALUES ('u:fresh', 95, 9900, $1, $2, $3)`, now.Add(-time.Minute), now.Add(-2*time.Hour), backfill)
	p := &Planner{DB: e.db, Dispatch: e.disp, Cfg: e.cfg, Now: func() time.Time { return now },
		Budget: func(context.Context) ratelimit.BudgetState { return ratelimit.BudgetState{PauseP2: true, Stretch: 1} }}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var st string
	if err := e.db.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, backfill).Scan(&st); err != nil || st != "cancelling" {
		t.Fatalf("held backfill %q (%v), want cancelling", st, err)
	}
	var mode string
	var lane int16
	var maxAge int64
	var first bool
	if err := e.db.QueryRow(ctx, `SELECT j.mode, j.lane, coalesce((j.params->'limits'->>'max_age_seconds')::bigint, -1),
			coalesce((j.params->>'first_read')::boolean, false)
		FROM account_schedule s JOIN jobs j ON j.job_id = s.last_job_id WHERE s.account_ref = 'u:fresh'`).Scan(&mode, &lane, &maxAge, &first); err != nil {
		t.Fatal(err)
	}
	if mode != dispatch.ModeIncremental || lane != 2 || maxAge != 3*86400 || !first {
		t.Fatalf("u:fresh's job %s in lane %d, max age %d s, first_read %v; want the 3-day first read in p1", mode, lane, maxAge, first)
	}
}
