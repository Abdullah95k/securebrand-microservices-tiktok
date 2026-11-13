package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// candidates is a Discoverer on an empty schema tt with the Iraq keyword
// list loaded, at a fixed clock.
type candidates struct {
	t   *testing.T
	db  *pgxpool.Pool
	d   *Discoverer
	now time.Time
}

func newCandidates(t *testing.T) *candidates {
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
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	if _, err := ttstore.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 14, MonthsAhead: 1, DaysBack: 1, DaysAhead: 3}); err != nil {
		t.Fatal(err)
	}
	seed, err := IraqSeed()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ImportSeed(ctx, db, seed); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	c := &candidates{t: t, db: db, now: now}
	c.d = &Discoverer{DB: db, Cfg: cfg, Now: func() time.Time { return c.now }}
	return c
}

func (c *candidates) exec(q string, args ...any) {
	c.t.Helper()
	if _, err := c.db.Exec(context.Background(), q, args...); err != nil {
		c.t.Fatalf("%s: %v", q, err)
	}
}

// stored writes an account as an entry check would have stored it, with
// posts made ages before now in regions.
func (c *candidates) stored(uid int64, bio string, ages []time.Duration, regions []string) {
	c.t.Helper()
	c.exec(`INSERT INTO accounts (user_id, sec_uid, username, bio, author_key) VALUES ($1, $2, $3, $4, '\x00')`,
		uid, secOf(uid), "user", bio)
	for i, age := range ages {
		c.exec(`INSERT INTO posts (created_at, post_id, user_id, kind, region, caption, content_hash, first_seen_at, last_seen_at)
			VALUES ($1, $2, $3, 'video', $4, 'a post', '\x00', $1, $1)`, c.now.Add(-age), uid*100+int64(i), uid, regions[i])
	}
}

func secOf(uid int64) string { return "MS4wLjABAAAAtest" + time.Unix(uid, 0).UTC().Format("150405") }

// found observes the account uid by a mention.
func (c *candidates) found(uid int64) Observed {
	c.t.Helper()
	o, err := c.d.Observe(context.Background(), Finding{UserID: uid, SecUID: secOf(uid), Username: "user", How: "mention", Via: "7"})
	if err != nil {
		c.t.Fatal(err)
	}
	return o
}

// checked runs a found account's entry check: claimed, its job ended with
// final and counters, decided.
func (c *candidates) checked(ref, final string, counters map[string]int64) string {
	c.t.Helper()
	ctx := context.Background()
	claimed, err := c.d.Claim(ctx, 100)
	if err != nil {
		c.t.Fatal(err)
	}
	job := uuid.New()
	for _, cl := range claimed {
		if cl.Ref == ref {
			if err := c.d.Started(ctx, ref, job); err != nil {
				c.t.Fatal(err)
			}
		} else if err := c.d.Unclaim(ctx, cl.Ref); err != nil {
			c.t.Fatal(err)
		}
	}
	st, err := c.d.Decide(ctx, Entry{JobID: job, Final: final, Counters: counters})
	if err != nil {
		c.t.Fatal(err)
	}
	return st
}

func (c *candidates) state(ref string) (state, reason string) {
	c.t.Helper()
	var r *string
	if err := c.db.QueryRow(context.Background(), `SELECT state, reason FROM discovery_candidates WHERE account_ref = $1`, ref).Scan(&state, &r); err != nil {
		c.t.Fatal(err)
	}
	if r != nil {
		reason = *r
	}
	return state, reason
}

var read = map[string]int64{"entry_read": 1, "posts": 1}

// The entry check's decisions: an Iraqi place in the bio qualifies; no
// recent post, no post at all, a private account and a failed read go on
// the dormant list (the read after max_attempts tries); a cancelled check
// goes back to the queue.
func TestDecide(t *testing.T) {
	c := newCandidates(t)
	day := 24 * time.Hour
	cases := []struct {
		uid      int64
		bio      string
		ages     []time.Duration
		regions  []string
		counters map[string]int64
		state    string
		reason   string
	}{
		{1, "مطعم في بغداد", []time.Duration{day}, []string{""}, read, StateQualified, ""},
		{2, "", []time.Duration{day, 2 * day}, []string{"US", "IQ"}, read, StateQualified, ""},
		{3, "hello", []time.Duration{day}, []string{"US"}, read, StateNotIraqi, ""},
		{4, "Baghdad", []time.Duration{100 * day}, []string{"IQ"}, read, StateDormant, ReasonInactive},
		{5, "Baghdad", nil, nil, map[string]int64{"entry_read": 1}, StateDormant, ReasonNeverPosted},
	}
	for _, k := range cases {
		c.stored(k.uid, k.bio, k.ages, k.regions)
		o := c.found(k.uid)
		if st := c.checked(o.Ref, "completed", k.counters); st != k.state {
			t.Errorf("account %d: %s, want %s", k.uid, st, k.state)
		}
		if st, r := c.state(o.Ref); st != k.state || r != k.reason {
			t.Errorf("account %d: stored %s (%s), want %s (%s)", k.uid, st, r, k.state, k.reason)
		}
	}
	// A private account (its status stored by the check).
	c.stored(6, "", nil, nil)
	c.exec(`UPDATE accounts SET status = 'private' WHERE user_id = 6`)
	o := c.found(6)
	if st := c.checked(o.Ref, "completed", map[string]int64{"accounts_private": 1}); st != StateDormant {
		t.Errorf("private: %s", st)
	}
	// Failed reads: tried again, then dormant as unreadable.
	o = c.found(7)
	for i, want := range []string{StatePending, StatePending, StateDormant} {
		if st := c.checked(o.Ref, "partial", map[string]int64{"tasks_failed": 1}); st != want {
			t.Fatalf("failed read %d: %s, want %s", i+1, st, want)
		}
	}
	if _, r := c.state(o.Ref); r != ReasonUnreadable {
		t.Errorf("failed reads: %s", r)
	}
	// A cancelled check goes back to the queue, its attempts kept.
	o = c.found(8)
	if st := c.checked(o.Ref, "cancelled", nil); st != StatePending {
		t.Errorf("cancelled: %s", st)
	}
	// A dormant account's date: checked plus recheck_after.
	var next time.Time
	if err := c.db.QueryRow(context.Background(), `SELECT next_check_at FROM discovery_candidates WHERE user_id = 4`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if !next.Equal(c.now.Add(c.d.Cfg.TT.Discovery.Entry.RecheckAfter)) {
		t.Errorf("next check %v", next)
	}
}

// Admit registers qualified accounts under the discovery client, the
// highest score first, paced behind the owner's by score, up to the day's
// cap when one is set; one a client removed meanwhile is blocked instead.
// Quiet moves the discovered ones that went quiet to the dormant list and
// stops their checks, never an owner's.
func TestAdmitAndQuiet(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	day := 24 * time.Hour
	c.d.Cfg.TT.Discovery.MaxNewPerDay = 2
	for uid, regions := range map[int64][]string{11: {"IQ"}, 12: {"IQ"}, 13: {"IQ"}, 14: {"IQ"}} {
		c.stored(uid, "", []time.Duration{day}, regions)
		o := c.found(uid)
		c.checked(o.Ref, "completed", read)
	}
	// 12 also writes a phone number: the higher score goes first.
	c.exec(`UPDATE accounts SET bio = '+964 770 123 4567' WHERE user_id = 12`)
	c.exec(`UPDATE discovery_candidates SET score = 2 WHERE user_id = 12`)
	client, err := EnsureClient(ctx, c.db, c.d.Cfg.TT.Discovery.Client)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := EnsureClient(ctx, c.db, c.d.Cfg.TT.Discovery.Client); err != nil || again != client {
		t.Fatalf("EnsureClient again: %v %v", again, err)
	}
	someone := uuid.New()
	c.exec(`INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, 'c', 'c', '\x00', '{}')`, someone)
	if err := accounts.Block(ctx, c.db, someone, "sec:"+secOf(13), 13, c.now); err != nil {
		t.Fatal(err)
	}
	n, err := c.d.Admit(ctx, client, 10)
	if err != nil || n != 2 {
		t.Fatalf("admitted %d: %v", n, err)
	}
	if st, _ := c.state("sec:" + secOf(12)); st != StateAdded {
		t.Errorf("highest score: %s", st)
	}
	var prio int
	var origin string
	if err := c.db.QueryRow(ctx, `SELECT admit_priority, origin FROM account_schedule WHERE account_ref = $1`, "sec:"+secOf(12)).
		Scan(&prio, &origin); err != nil {
		t.Fatal(err)
	}
	if prio != accounts.PriorityDiscovered+200 || origin != accounts.OriginDiscovery {
		t.Errorf("schedule: priority %d, origin %s", prio, origin)
	}
	// The day's cap is spent; the next day admits the rest.
	if n, _ := c.d.Admit(ctx, client, 10); n != 0 {
		t.Errorf("admitted %d past the cap", n)
	}
	c.now = c.now.Add(day)
	if n, _ := c.d.Admit(ctx, client, 10); n != 1 {
		t.Errorf("next day admitted %d, want 1", n)
	}
	if st, _ := c.state("sec:" + secOf(13)); st != StateBlocked {
		t.Errorf("removed: %s", st)
	}

	// Quiet: 11 went private; 12's last post is too old; an owner's account
	// as quiet stays.
	c.exec(`UPDATE account_schedule SET admitted_at = $1, last_success_at = $1`, c.now)
	c.exec(`UPDATE account_schedule SET inactive_since = $1 WHERE user_id = 11`, c.now)
	c.exec(`UPDATE accounts SET status = 'private' WHERE user_id = 11`)
	c.exec(`INSERT INTO crawl_cursors (scope_type, scope_id, ordering, newest_seen_at, state) VALUES ('account_list', 12, 'default', $1, 'complete'),
		('account_list', 14, 'default', $2, 'complete')`, c.now.Add(-100*day), c.now.Add(-day))
	c.exec(`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at, last_success_at, inactive_since)
		VALUES ('u:owned', 99, 9900, $1, $1, $1, $1)`, c.now)
	if n, err := c.d.Quiet(ctx, 100); err != nil || n != 2 {
		t.Fatalf("quiet moved %d: %v", n, err)
	}
	if st, r := c.state("sec:" + secOf(11)); st != StateDormant || r != ReasonPrivate {
		t.Errorf("private discovered account: %s (%s)", st, r)
	}
	if st, r := c.state("sec:" + secOf(12)); st != StateDormant || r != ReasonInactive {
		t.Errorf("quiet discovered account: %s (%s)", st, r)
	}
	var on int
	if err := c.db.QueryRow(ctx, `SELECT count(*) FROM account_schedule WHERE enabled`).Scan(&on); err != nil || on != 2 {
		t.Errorf("%d schedules still checked, want 2 (the active discovered one, the owner's): %v", on, err)
	}
	// Found again by a source while dormant: counted, not read.
	if o := c.found(12); o.State != StateDormant || o.Monitored {
		t.Errorf("dormant discovered account found again: %+v", o)
	}
	// Found again after its date: one entry check.
	c.now = c.now.Add(181 * day)
	if o := c.found(12); o.State != StatePending {
		t.Errorf("found after its date: %+v", o)
	}
}

// Recover puts back a check whose job ended without a decision.
func TestRecover(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	o := c.found(21)
	if _, err := c.d.Claim(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.d.Recover(ctx, 15*time.Minute); n != 0 {
		t.Errorf("recovered %d fresh checks", n)
	}
	c.now = c.now.Add(time.Hour)
	if n, err := c.d.Recover(ctx, 15*time.Minute); err != nil || n != 1 {
		t.Fatalf("recovered %d: %v", n, err)
	}
	if st, _ := c.state(o.Ref); st != StatePending {
		t.Errorf("recovered: %s", st)
	}
}

// A lowered max_running holds at once: the checks beyond it, the most
// recently queued, are returned to be cancelled, those already cancelling
// are not counted, and a cancelled check's account goes back to pending
// with no attempt counted.
func TestExcess(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	jobs := map[string]uuid.UUID{}
	var refs []string
	for i := int64(31); i <= 35; i++ {
		c.found(i)
		claimed, err := c.d.Claim(ctx, 1)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim: %v %v", claimed, err)
		}
		job := uuid.New()
		if err := c.d.Started(ctx, claimed[0].Ref, job); err != nil {
			t.Fatal(err)
		}
		status := "running"
		if i == 32 {
			status = "cancelling"
		}
		c.exec(`INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at) VALUES ($1, 'entry', 3, 'entry', $2, '{}', $3, now())`,
			job, claimed[0].Ref, status)
		jobs[claimed[0].Ref] = job
		refs = append(refs, claimed[0].Ref)
		c.now = c.now.Add(time.Minute)
	}
	got, err := c.d.Excess(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Running, oldest first: 31, 33, 34, 35 (32 is cancelling); 2 kept.
	want := []uuid.UUID{jobs[refs[3]], jobs[refs[4]]}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("excess %v, want %v", got, want)
	}
	if got, _ := c.d.Excess(ctx, 10); len(got) != 0 {
		t.Fatalf("excess under the cap: %v", got)
	}
	if _, err := c.d.Decide(ctx, Entry{JobID: jobs[refs[4]], Final: "cancelled"}); err != nil {
		t.Fatal(err)
	}
	if st, _ := c.state(refs[4]); st != StatePending {
		t.Fatalf("cancelled check: %s, want pending", st)
	}
	if n := one[int](t, c, `SELECT attempts FROM discovery_candidates WHERE account_ref = $1`, refs[4]); n != 0 {
		t.Fatalf("attempts %d after a cancel", n)
	}
}
