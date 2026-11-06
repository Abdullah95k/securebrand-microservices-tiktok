package discovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
)

// The review page's search: every filter, both orders and their cursors,
// and the sources of each account.
func TestSearch(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	day := 24 * time.Hour
	// 21 to 25 found a day apart by a mention from 7; 23 also by search
	// (keyword 4); scores 0.5 to 2.5.
	for i, uid := range []int64{21, 22, 23, 24, 25} {
		c.now = c.now.Add(day)
		c.found(uid)
		c.exec(`UPDATE discovery_candidates SET score = $2, state = 'not_iraqi' WHERE user_id = $1`, uid, 0.5*float64(i+1))
	}
	if _, err := c.d.Observe(ctx, Finding{UserID: 23, SecUID: secOf(23), How: SearchSource, Via: "4", At: c.now}); err != nil {
		t.Fatal(err)
	}
	ids := func(vs []View) []int64 {
		var out []int64
		for _, v := range vs {
			out = append(out, *v.UserID)
		}
		return out
	}
	search := func(f Filter) ([]View, string) {
		t.Helper()
		out, next, err := Search(ctx, c.db, f, false)
		if err != nil {
			t.Fatal(err)
		}
		return out, next
	}
	eq := func(name string, got []int64, want ...int64) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: %v, want %v", name, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s: %v, want %v", name, got, want)
			}
		}
	}
	all, next := search(Filter{})
	eq("latest found first", ids(all), 25, 24, 23, 22, 21)
	if next != "" {
		t.Fatalf("a last page has a cursor: %q", next)
	}
	if all[2].Sources == nil || len(all[2].Sources) != 2 || all[2].Sources[0] != "mention" || all[2].Sources[1] != "search" {
		t.Fatalf("23's sources: %v", all[2].Sources)
	}
	if all[0].Username != nil || all[0].ID == 0 {
		t.Fatalf("without pii: %+v", all[0])
	}
	page, next := search(Filter{Limit: 2})
	eq("page 1", ids(page), 25, 24)
	page, _ = search(Filter{Limit: 2, After: next})
	eq("page 2", ids(page), 23, 22)
	page, next = search(Filter{Limit: 2, ByScore: true})
	eq("by score", ids(page), 25, 24)
	page, _ = search(Filter{Limit: 2, ByScore: true, After: next})
	eq("by score, page 2", ids(page), 23, 22)
	if _, _, err := Search(ctx, c.db, Filter{ByScore: true, After: "12"}, false); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("an id cursor in score order: %v", err)
	}
	page, _ = search(Filter{How: SearchSource, Via: "4"})
	eq("keyword 4", ids(page), 23)
	page, _ = search(Filter{How: SearchSource, Via: "5"})
	eq("keyword 5", ids(page))
	page, _ = search(Filter{How: "mention"})
	eq("mention", ids(page), 25, 24, 23, 22, 21)
	lo, hi := 1.0, 2.0
	page, _ = search(Filter{MinScore: &lo, MaxScore: &hi})
	eq("scores 1 to 2", ids(page), 24, 23, 22)
	start := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	page, _ = search(Filter{Since: start.Add(2 * day), Until: start.Add(4 * day)})
	eq("found days 2 and 3", ids(page), 23, 22)
	page, _ = search(Filter{State: StateDormant})
	eq("dormant", ids(page))

	// Labels are on the schedule of an added account.
	client, _ := EnsureClient(ctx, c.db, c.d.Cfg.TT.Discovery.Client)
	c.exec(`UPDATE discovery_candidates SET state = 'added', schedule_ref = account_ref WHERE user_id = 21`)
	c.exec(`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at) SELECT account_ref, 21, 9900, now()
		FROM discovery_candidates WHERE user_id = 21`)
	c.exec(`INSERT INTO account_labels (api_client_id, account_ref, label) SELECT $1, account_ref, 'list:x' FROM discovery_candidates
		WHERE user_id = 21`, client)
	page, _ = search(Filter{Label: "list:x"})
	eq("label, any client", ids(page), 21)
	other := uuid.New()
	page, _ = search(Filter{Label: "list:x", LabelClient: &other})
	eq("label of another client", ids(page))
}

// Removing a found account: blocked for the caller, discovery's
// registration gone and its schedule off; an account another client
// registered stays monitored for them; found again, it stays out; the
// caller's registration lifts the block.
func TestRemove(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	day := 24 * time.Hour
	for _, uid := range []int64{31, 32} {
		c.stored(uid, "", []time.Duration{day}, []string{"IQ"})
		o := c.found(uid)
		c.checked(o.Ref, "completed", read)
	}
	client, err := EnsureClient(ctx, c.db, c.d.Cfg.TT.Discovery.Client)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := c.d.Admit(ctx, client, 10); err != nil || n != 2 {
		t.Fatalf("admitted %d: %v", n, err)
	}
	reviewer, owner := uuid.New(), uuid.New()
	c.exec(`INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, 'r', 'r', '\x00', '{}'), ($2, 'o', 'o', '\x00', '{}')`,
		reviewer, owner)
	// The owner registered 32 too.
	if _, err := accounts.Register(ctx, c.db, accounts.Registration{ClientID: owner, Ref: "sec:" + secOf(32), UserID: 32, Origin: accounts.OriginOwner,
		DefaultInterval: c.d.Cfg.TT.Freshness.ListInterval, Enabled: true, Now: c.now}); err != nil {
		t.Fatal(err)
	}
	idOf := func(uid int64) int64 {
		var id int64
		if err := c.db.QueryRow(ctx, `SELECT id FROM discovery_candidates WHERE user_id = $1`, uid).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	enabled := func(uid int64) bool {
		var on bool
		if err := c.db.QueryRow(ctx, `SELECT enabled FROM account_schedule WHERE user_id = $1`, uid).Scan(&on); err != nil {
			t.Fatal(err)
		}
		return on
	}
	r, err := Remove(ctx, c.db, client, reviewer, idOf(31), c.now)
	if err != nil || r.State != StateBlocked || !r.MonitoringStopped || r.StillMonitored {
		t.Fatalf("remove 31: %+v %v", r, err)
	}
	if enabled(31) {
		t.Fatal("31 is still checked")
	}
	if again, err := Remove(ctx, c.db, client, reviewer, idOf(31), c.now); err != nil || again.MonitoringStopped {
		t.Fatalf("remove 31 again: %+v %v", again, err)
	}
	r, err = Remove(ctx, c.db, client, reviewer, idOf(32), c.now)
	if err != nil || r.MonitoringStopped || !r.StillMonitored {
		t.Fatalf("remove 32: %+v %v", r, err)
	}
	if !enabled(32) {
		t.Fatal("the owner's 32 stopped")
	}
	var discReg int
	if err := c.db.QueryRow(ctx, `SELECT count(*) FROM account_registrations WHERE api_client_id = $1`, client).Scan(&discReg); err != nil || discReg != 0 {
		t.Fatalf("discovery still holds %d registrations: %v", discReg, err)
	}
	if _, err := Remove(ctx, c.db, client, reviewer, 999999, c.now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an unknown id: %v", err)
	}
	// Found again: it stays out.
	if o := c.found(31); o.State != StateBlocked || o.Monitored {
		t.Fatalf("31 found again: %+v", o)
	}
	if st, _ := c.state("sec:" + secOf(31)); st != StateBlocked {
		t.Fatalf("31: %s", st)
	}
	// The reviewer registers 31 itself: its own block is lifted.
	if _, err := accounts.Register(ctx, c.db, accounts.Registration{ClientID: reviewer, Ref: "sec:" + secOf(31), UserID: 31, Origin: accounts.OriginOwner,
		DefaultInterval: c.d.Cfg.TT.Freshness.ListInterval, Enabled: true, Now: c.now}); err != nil {
		t.Fatal(err)
	}
	if blocked, err := accounts.Blocked(ctx, c.db, "sec:"+secOf(31), 31); err != nil || blocked {
		t.Fatalf("31 still blocked after the reviewer registered it: %v", err)
	}
}

// Keyword hits count the accounts each keyword's searches found; the
// daily figures count found and added accounts by source and the entry
// checks' reads.
func TestKeywordHitsAndDaily(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	for _, uid := range []int64{41, 42} {
		if _, err := c.d.Observe(ctx, Finding{UserID: uid, SecUID: secOf(uid), How: SearchSource, Via: "4", At: c.now}); err != nil {
			t.Fatal(err)
		}
	}
	// 42 is found by keyword 4 twice: one account.
	if _, err := c.d.Observe(ctx, Finding{UserID: 42, SecUID: secOf(42), How: SearchSource, Via: "4", At: c.now}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.d.Observe(ctx, Finding{UserID: 43, SecUID: secOf(43), How: SearchSource, Via: "5", At: c.now.Add(-24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	c.found(41)
	hits, err := KeywordHits(ctx, c.db, []int64{4, 5, 6})
	if err != nil || hits[4] != 2 || hits[5] != 1 || hits[6] != 0 {
		t.Fatalf("hits %v: %v", hits, err)
	}
	c.exec(`UPDATE discovery_candidates SET state = 'added', added_at = $1 WHERE user_id = 41`, c.now)
	job := func(kind string, at time.Time, requests, wire int) {
		c.exec(`INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, counters, created_at)
			VALUES ($1, $2, 2, 'full', 'u:x', '{}', 'completed', $3, $4)`, uuid.New(), kind, map[string]int{"requests": requests, "wire_bytes": wire}, at)
	}
	job("entry", c.now, 2, 1000)
	job("entry", c.now.Add(-time.Hour), 3, 500)
	job("scheduled", c.now, 50, 99999) // monitoring: not discovery's
	days, err := Daily(ctx, c.db, c.now.AddDate(0, 0, -1), c.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(days) != 2 || days[0].Day != "2026-11-04" || days[1].Day != "2026-11-05" {
		t.Fatalf("days: %+v", days)
	}
	today, yesterday := days[1], days[0]
	if today.Found["search"] != 2 || today.Found["mention"] != 1 || yesterday.Found["search"] != 1 {
		t.Fatalf("found: today %v, yesterday %v", today.Found, yesterday.Found)
	}
	if today.Added["search"] != 1 || today.Added["mention"] != 1 || len(yesterday.Added) != 0 {
		t.Fatalf("added: today %v, yesterday %v", today.Added, yesterday.Added)
	}
	if r := today.Reads["entry"]; r.Jobs != 2 || r.Requests != 5 || r.WireBytes != 1500 || len(today.Reads) != 1 {
		t.Fatalf("reads: %+v", today.Reads)
	}
}
