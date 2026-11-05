package e2e

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
	ttapi "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/api"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scheduler"
)

// disco is the scheduler's discovery loop on the stack, with the
// planner and the finalizer of sched and the world's clock.
type disco struct {
	s  *Stack
	sc *sched
	d  *discovery.Discoverer
	l  *scheduler.Discovery
}

func newDisco(s *Stack) *disco {
	sc := newSched(s)
	d := &discovery.Discoverer{DB: s.DB, Cfg: s.Cfg, Now: s.World.Clock}
	disp := &dispatch.Dispatcher{DB: s.DB, Jobs: s.Tracker, Producer: s.Producer, Now: s.World.Clock}
	l := &scheduler.Discovery{D: d, Dispatch: disp}
	sc.fin.Entry = l.Entry
	return &disco{s: s, sc: sc, d: d, l: l}
}

// observe records a finding of acc by a source.
func (dc *disco) observe(t *testing.T, acc *fakett.Account, how, via string, mod func(*discovery.Finding)) discovery.Observed {
	t.Helper()
	f := discovery.Finding{UserID: acc.UID, SecUID: acc.SecUID, Username: acc.Username, How: how, Via: via}
	if mod != nil {
		mod(&f)
	}
	o, err := dc.d.Observe(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// round runs the discovery loop once, waits for the entry checks it
// started and finalises them (deciding each account), then runs the loop
// again, which registers the accounts that qualified. It returns how many
// entry checks it ran.
func (dc *disco) round(t *testing.T) int {
	t.Helper()
	ctx := context.Background()
	if err := dc.l.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := dc.s.DB.Query(ctx, `SELECT job_id FROM discovery_candidates WHERE state = 'checking' AND job_id IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id.String())
	}
	rows.Close()
	dc.s.Wait(t, 3*time.Minute, ids...)
	deadline := time.Now().Add(time.Minute)
	for count(t, dc.s, `SELECT count(*) FROM discovery_candidates WHERE state = 'checking'`) > 0 {
		if err := dc.sc.fin.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("entry checks not decided")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := dc.l.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	return len(ids)
}

// candidate reads a found account's state and dormant reason.
func (dc *disco) candidate(t *testing.T, acc *fakett.Account) (state, reason string) {
	t.Helper()
	var r *string
	if err := dc.s.DB.QueryRow(context.Background(), `SELECT state, reason FROM discovery_candidates WHERE user_id = $1`, acc.UID).
		Scan(&state, &r); err != nil {
		t.Fatalf("candidate %s: %v", acc.Username, err)
	}
	if r != nil {
		reason = *r
	}
	return state, reason
}

func (dc *disco) want(t *testing.T, acc *fakett.Account, state, reason string) {
	t.Helper()
	if st, r := dc.candidate(t, acc); st != state || r != reason {
		t.Errorf("%s: %s (%s), want %s (%s)", acc.Username, st, r, state, reason)
	}
}

// TestEntryCheckAndDormantList runs discovery's entry check end to end
// against fakett (docs/tiktok/discovery-plan.md §3, §4): each found account
// is read once (its list's first page, stored like a monitored account's,
// with no list watermark), then added (a post made in Iraq), kept as not
// Iraqi (yet), or put on the dormant list (quiet, private, gone); the
// account added is registered to the discovery client, paced behind the
// owner's and backfilled in full; a not-Iraqi account that new signals lift
// qualifies with no read; the cheap pre-check, a removed account and an
// account already monitored cost no read; a dormant account found again is
// read again only after its date; and a discovered account that goes quiet
// moves to the dormant list, its checks stopped and its data kept.
func TestEntryCheckAndDormantList(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 41, Accounts: 1, PostsPerAccount: 8, PostSpacing: 30 * time.Hour},
		Config: func(c *config.Config) {
			c.TT.Discovery.Enabled = true
			c.TT.Discovery.Sources = map[string]bool{"mention": true, "following": true, "search": true}
			c.Limits.IncludeComments = false
		}})
	w := s.World
	ctx := context.Background()
	dc := newDisco(s)
	owner := w.Accounts()[0]
	dc.sc.register(t, owner)
	dc.sc.tick(t) // the owner's account, backfilled
	via := strconv.FormatInt(owner.UID, 10)
	day := 24 * time.Hour

	iraqi := w.AddAccount("found_iraqi", "travel and food", 5, 2*day, day)
	quiet := w.AddAccount("found_quiet", "", 3, 120*day, day)
	private := w.AddAccount("found_private", "", 3, day, day)
	gone := w.AddAccount("found_gone", "", 3, day, day)
	plain := w.AddAccount("found_plain", "hello", 4, day, day)
	never := w.AddAccount("found_never", "", 0, day, day)
	w.Mutate(func() {
		iraqi.Posts[3].Region = "IQ"
		private.Private = true
		gone.Gone = true
	})
	for _, a := range []*fakett.Account{iraqi, quiet, private, gone, plain, never} {
		if o := dc.observe(t, a, "mention", via, nil); o.State != discovery.StatePending || o.Ref != "sec:"+a.SecUID {
			t.Fatalf("%s found: %+v", a.Username, o)
		}
	}
	// The cheap pre-check: a private account a source shows as private
	// goes on the dormant list unread; a removed one stays out; one already
	// monitored only gains the source.
	precheck := w.AddAccount("found_precheck", "", 2, day, day)
	if o := dc.observe(t, precheck, "following", via, func(f *discovery.Finding) { yes := true; f.Private = &yes }); o.State != discovery.StateDormant {
		t.Fatalf("pre-checked private: %+v", o)
	}
	removed := w.AddAccount("found_removed", "Baghdad", 2, day, day)
	someone := uuid.New()
	if _, err := s.DB.Exec(ctx, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, 'c', 'c', '\x00', '{}')`, someone); err != nil {
		t.Fatal(err)
	}
	if err := accounts.Block(ctx, s.DB, someone, "sec:"+removed.SecUID, removed.UID, w.Clock()); err != nil {
		t.Fatal(err)
	}
	if o := dc.observe(t, removed, "mention", via, nil); o.State != discovery.StateBlocked {
		t.Fatalf("removed: %+v", o)
	}
	if o := dc.observe(t, owner, "mention", "1", nil); !o.Monitored || o.Ref != "u:"+owner.Username {
		t.Fatalf("monitored: %+v", o)
	}
	if n := count(t, s, `SELECT count(*) FROM account_sources WHERE account_ref = $1 AND how = 'mention'`, "u:"+owner.Username); n != 1 {
		t.Errorf("monitored account: %d mention sources", n)
	}

	if n := dc.round(t); n != 6 {
		t.Fatalf("%d entry checks, want 6", n)
	}
	dc.want(t, iraqi, discovery.StateAdded, "")
	dc.want(t, quiet, discovery.StateDormant, discovery.ReasonInactive)
	dc.want(t, private, discovery.StateDormant, discovery.ReasonPrivate)
	dc.want(t, gone, discovery.StateDormant, discovery.ReasonGone)
	dc.want(t, plain, discovery.StateNotIraqi, "")
	dc.want(t, never, discovery.StateDormant, discovery.ReasonNeverPosted)
	dc.want(t, precheck, discovery.StateDormant, discovery.ReasonPrivate)
	dc.want(t, removed, discovery.StateBlocked, "")
	// What an entry check read is stored (discovery-plan §10), with no list
	// watermark: the added account's first read is still a backfill.
	if n := count(t, s, `SELECT count(*) FROM posts WHERE user_id = $1`, plain.UID); n != 4 {
		t.Errorf("not-Iraqi account: %d posts stored, want 4", n)
	}
	if n := count(t, s, `SELECT count(*) FROM crawl_cursors WHERE scope_id = ANY($1)`, []int64{iraqi.UID, plain.UID, quiet.UID}); n != 0 {
		t.Errorf("%d list watermarks after entry checks, want 0", n)
	}
	var ev string
	if err := s.DB.QueryRow(ctx, `SELECT evidence::text FROM discovery_candidates WHERE user_id = $1`, iraqi.UID).Scan(&ev); err != nil {
		t.Fatal(err)
	}
	if want := `"signal": "posts_in_iraq"`; !contains(ev, want) {
		t.Errorf("evidence %s lacks %s", ev, want)
	}

	// The added account: the discovery client's, paced behind the owner's
	// by its score (1.5: a post made in Iraq, strong, and the mention by a
	// monitored account, medium), then backfilled in full by the planner.
	var origin string
	var prio int
	var admitted *time.Time
	if err := s.DB.QueryRow(ctx, `SELECT origin, admit_priority, admitted_at FROM account_schedule WHERE account_ref = $1`,
		"sec:"+iraqi.SecUID).Scan(&origin, &prio, &admitted); err != nil {
		t.Fatal(err)
	}
	if origin != accounts.OriginDiscovery || prio != accounts.PriorityDiscovered+150 || admitted != nil {
		t.Errorf("added schedule: origin %s, priority %d, admitted %v", origin, prio, admitted)
	}
	if n := count(t, s, `SELECT count(*) FROM account_registrations r JOIN api_clients c ON c.id = r.api_client_id
		WHERE r.account_ref = $1 AND c.name = $2`, "sec:"+iraqi.SecUID, s.Cfg.TT.Discovery.Client); n != 1 {
		t.Errorf("added account: %d registrations to the discovery client", n)
	}
	jobs := dc.sc.tick(t)
	if j := jobs["sec:"+iraqi.SecUID]; j.mode != "full" || j.status != "completed" {
		t.Fatalf("added account's first read: %+v", j)
	}
	if n := count(t, s, `SELECT count(*) FROM posts WHERE user_id = $1`, iraqi.UID); n != 5 {
		t.Errorf("added account: %d posts, want 5", n)
	}

	// New signals lift the not-Iraqi account with no read: a second medium
	// signal (followed by a monitored account) next to the mention.
	if o := dc.observe(t, plain, "mention", "2", nil); o.State != discovery.StateNotIraqi {
		t.Fatalf("one medium signal: %+v", o)
	}
	if o := dc.observe(t, plain, "following", via, nil); o.State != discovery.StateQualified {
		t.Fatalf("two medium signals: %+v", o)
	}
	if n := dc.round(t); n != 0 {
		t.Errorf("rescoring read %d accounts, want 0", n)
	}
	dc.want(t, plain, discovery.StateAdded, "")

	// A dormant account found again before its date: counted, not read.
	w.Advance(90 * day)
	if o := dc.observe(t, quiet, "search", "kw", nil); o.State != discovery.StateDormant {
		t.Fatalf("dormant, found early: %+v", o)
	}
	if n := dc.round(t); n != 0 {
		t.Errorf("a dormant account found early was read (%d checks)", n)
	}
	// After its date: one entry check, still quiet, its date moved on.
	w.Advance(91 * day)
	var before time.Time
	if err := s.DB.QueryRow(ctx, `SELECT next_check_at FROM discovery_candidates WHERE user_id = $1`, quiet.UID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if o := dc.observe(t, quiet, "search", "kw", nil); o.State != discovery.StatePending {
		t.Fatalf("dormant, found after its date: %+v", o)
	}
	if n := dc.round(t); n != 1 {
		t.Errorf("%d entry checks for the dormant account found again, want 1", n)
	}
	dc.want(t, quiet, discovery.StateDormant, discovery.ReasonInactive)
	var after time.Time
	if err := s.DB.QueryRow(ctx, `SELECT next_check_at FROM discovery_candidates WHERE user_id = $1`, quiet.UID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.After(before.Add(170 * day)) {
		t.Errorf("next check %v, was %v: not moved on", after, before)
	}

	// Discovered accounts that went quiet (no post for 90 days) move to
	// the dormant list: their checks stop, their data stays. The owner's
	// account, as quiet, keeps its checks.
	if err := dc.l.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	dc.want(t, iraqi, discovery.StateDormant, discovery.ReasonInactive)
	var enabled bool
	if err := s.DB.QueryRow(ctx, `SELECT enabled FROM account_schedule WHERE account_ref = $1`, "sec:"+iraqi.SecUID).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Error("a quiet discovered account is still checked")
	}
	if n := count(t, s, `SELECT count(*) FROM posts WHERE user_id = $1`, iraqi.UID); n != 5 {
		t.Errorf("quiet discovered account: %d posts kept, want 5", n)
	}
	if n := count(t, s, `SELECT count(*) FROM account_schedule WHERE account_ref = $1 AND enabled`, "u:"+owner.Username); n != 1 {
		t.Error("the owner's quiet account was moved off the schedule")
	}
	if n := count(t, s, `SELECT count(*) FROM dormant_accounts`); n != 6 {
		t.Errorf("%d accounts on the dormant list, want 6", n)
	}

	// The API lists the dormant list, paged, with names only for pii:read.
	srv := &ttapi.Server{Cfg: s.Cfg, DB: s.DB, Redis: s.Redis, Now: w.Clock}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	key, _, err := ttapi.CreateClient(ctx, s.DB, "reviewer", []string{ttapi.ScopeDiscoveryRead}, 0)
	if err != nil {
		t.Fatal(err)
	}
	api := &client{t: t, url: hs.URL, key: key}
	var page struct {
		Accounts []map[string]any `json:"accounts"`
		Counts   map[string]int   `json:"counts"`
		Next     string           `json:"next"`
	}
	api.want(200, "GET", "/discovery/accounts?state=dormant&limit=4", nil, &page)
	if len(page.Accounts) != 4 || page.Next == "" || page.Counts["dormant"] != 6 || page.Counts["added"] != 1 {
		t.Fatalf("dormant list, first page: %d accounts, next %q, counts %v", len(page.Accounts), page.Next, page.Counts)
	}
	for _, a := range page.Accounts {
		if a["username"] != nil || a["reason"] == nil || a["next_check_at"] == nil {
			t.Errorf("dormant account %v", a)
		}
	}
	next := page.Next
	page.Accounts, page.Next = nil, ""
	api.want(200, "GET", "/discovery/accounts?state=dormant&limit=4&after="+next, nil, &page)
	if len(page.Accounts) != 2 || page.Next != "" {
		t.Errorf("dormant list, second page: %d accounts, next %q", len(page.Accounts), page.Next)
	}
	api.want(400, "GET", "/discovery/accounts?state=asleep", nil, nil)
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
