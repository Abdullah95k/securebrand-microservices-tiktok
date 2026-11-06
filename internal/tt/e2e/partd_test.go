package e2e

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	ttapi "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/api"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

// lists runs the discovery loop once, waits for the follow-list reads it
// started and finalises them, and returns how many it started.
func (dc *disco) lists(t *testing.T) int {
	t.Helper()
	ctx := context.Background()
	if err := dc.l.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := dc.s.DB.Query(ctx, `SELECT job_id FROM jobs WHERE kind = 'follows' AND status = 'running'`)
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
	for count(t, dc.s, `SELECT count(*) FROM jobs WHERE kind = 'follows' AND status IN ('running', 'finalizing')`) > 0 {
		if err := dc.sc.fin.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("list reads not finalised")
		}
		time.Sleep(100 * time.Millisecond)
	}
	dc.s.WaitPersisted(t, ctx)
	return len(ids)
}

// TestDiscoveryPartD runs discovery part D end to end against fakett
// (docs/tiktok/discovery-plan.md §9): every monitored account's following
// list (unless its profile hides it: no call then) and followers list are
// read, across a yield; every link is stored; the people new to a list
// are counted by the leader: a monitored one gains the source with no
// read, a private one and one with no post go on the dormant list with no
// read (the pre-checks), the rest get one entry check each, and the one
// that posts from Iraq is added. A day later the followers list is read
// from the top down to the people already stored: only the new followers
// are read and checked; a source switched off reads nothing.
func TestDiscoveryPartD(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 47, Accounts: 2, PostsPerAccount: 3, PostSpacing: 30 * time.Hour},
		Config: func(c *config.Config) {
			c.TT.Discovery.Enabled = true
			c.TT.Discovery.Sources = map[string]bool{"following": true, "follower": true}
			c.TT.Discovery.Follows.PageSize = 5
			c.TT.Discovery.Follows.MaxPages = 4
		}})
	w := s.World
	ctx := context.Background()
	dc := newDisco(s)
	owner, other := w.Accounts()[0], w.Accounts()[1]
	day := 24 * time.Hour

	star := w.AddAccount("pd_star", "", 3, day, day) // followed by the owner; posts from Iraq
	w.Mutate(func() { star.Posts[0].Region = "IQ" })
	shy := w.AddAccount("pd_shy", "", 2, day, day) // private
	w.Mutate(func() { shy.Private = true })
	quiet := w.AddAccount("pd_quiet", "", 0, day, day) // never posted
	var fans []*fakett.Account
	for i := range 10 {
		fans = append(fans, w.AddAccount(fmt.Sprintf("pd_fan%d", i), "", 2, 2*day, day))
	}
	w.AddFollowing(owner, other)
	w.AddFollowing(owner, star)
	for i := len(fans) - 1; i >= 0; i-- {
		w.AddFollower(owner, fans[i])
	}
	w.AddFollower(owner, quiet)
	w.AddFollower(owner, shy) // newest
	w.Mutate(func() {
		other.FollowingHidden = true
		other.FollowerList = []*fakett.Account{}
	})

	dc.sc.register(t, owner, other)
	dc.sc.tick(t) // the first reads: the profiles say who hides the following list
	s.WaitPersisted(t, ctx)
	if v := count(t, s, `SELECT coalesce(following_visibility, 0) FROM accounts WHERE user_id = $1`, other.UID); v != 2 {
		t.Fatalf("other's following_visibility %d, want 2", v)
	}

	if n := dc.lists(t); n != 3 {
		t.Fatalf("%d list reads, want 3 (the owner's two, other's followers)", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'follows' AND user_id = $1 AND mode = 'following'`, other.UID); n != 0 {
		t.Errorf("other's hidden following list was read %d times", n)
	}
	list := func(acc *fakett.Account, dir string) (state, stop string, done bool) {
		t.Helper()
		var st *string
		if err := s.DB.QueryRow(ctx, `SELECT state, last_stop, deep_done FROM follow_lists WHERE user_id = $1 AND direction = $2`,
			acc.UID, dir).Scan(&state, &st, &done); err != nil {
			t.Fatalf("%s's %s list: %v", acc.Username, dir, err)
		}
		if st != nil {
			stop = *st
		}
		return state, stop, done
	}
	if st, stop, _ := list(other, "following"); st != "hidden" || stop != "hidden" {
		t.Errorf("other's following list: %s, %s", st, stop)
	}
	if st, stop, done := list(owner, "follower"); st != "ok" || stop != "end" || !done {
		t.Errorf("the owner's followers list: %s, %s, done %v", st, stop, done)
	}
	if n := count(t, s, `SELECT count(*) FROM follows WHERE user_id = $1 AND direction = 'follower'`, owner.UID); n != 12 {
		t.Errorf("%d follower links, want 12", n)
	}
	if n := count(t, s, `SELECT count(*) FROM follows WHERE user_id = $1 AND direction = 'following'`, owner.UID); n != 2 {
		t.Errorf("%d following links, want 2", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_sightings WHERE how = 'follower'`); n != 12 {
		t.Errorf("%d follower sightings, want 12", n)
	}

	// The leader counts them: the monitored account gains the source, the
	// private one and the one with no post go dormant unread, the rest get
	// one entry check each.
	checks := dc.round(t)
	for range 3 {
		if count(t, s, `SELECT count(*) FROM discovery_candidates WHERE state IN ('pending', 'checking')`) == 0 {
			break
		}
		checks += dc.round(t)
	}
	if checks != 11 {
		t.Fatalf("%d entry checks, want 11 (the star and 10 fans)", checks)
	}
	dc.want(t, star, discovery.StateAdded, "")
	dc.want(t, shy, discovery.StateDormant, discovery.ReasonPrivate)
	dc.want(t, quiet, discovery.StateDormant, discovery.ReasonNeverPosted)
	for _, f := range fans {
		dc.want(t, f, discovery.StateNotIraqi, "")
	}
	if n := count(t, s, `SELECT count(*) FROM account_sources WHERE user_id = $1 AND how = 'following'`, other.UID); n != 1 {
		t.Errorf("the monitored account followed: %d sources, want 1", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE user_id = $1`, other.UID); n != 0 {
		t.Errorf("the monitored account became a candidate")
	}

	// A day later, two new followers: the re-read stops at the people
	// already stored and only the new ones are checked.
	w.Advance(25 * time.Hour)
	n1 := w.AddAccount("pd_new1", "", 2, day, day)
	n2 := w.AddAccount("pd_new2", "", 2, day, day)
	w.AddFollower(owner, n1)
	w.AddFollower(owner, n2)
	before := count(t, s, `SELECT pages_read FROM follow_lists WHERE user_id = $1 AND direction = 'follower'`, owner.UID)
	dc.lists(t) // the added star's lists are new too
	if st, stop, _ := list(owner, "follower"); st != "ok" || stop != "known" {
		t.Errorf("the owner's followers re-read: %s, %s", st, stop)
	}
	if p := count(t, s, `SELECT pages_read FROM follow_lists WHERE user_id = $1 AND direction = 'follower'`, owner.UID) - before; p != 2 {
		t.Errorf("the re-read took %d pages, want 2 (one with the new, one of known people)", p)
	}
	if n := count(t, s, `SELECT count(*) FROM follows WHERE user_id = $1 AND direction = 'follower'`, owner.UID); n != 14 {
		t.Errorf("%d follower links after the re-read, want 14", n)
	}
	checks = dc.round(t)
	if checks != 2 {
		t.Errorf("%d entry checks after the re-read, want 2", checks)
	}
	dc.want(t, n1, discovery.StateNotIraqi, "")
	dc.want(t, n2, discovery.StateNotIraqi, "")

	// The API serves the links, newest first seen first, paged; who each
	// person is only with pii:read.
	srv := &ttapi.Server{Cfg: s.Cfg, DB: s.DB, Redis: s.Redis, Now: w.Clock}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	var page struct {
		Direction string           `json:"direction"`
		List      map[string]any   `json:"list"`
		Follows   []map[string]any `json:"follows"`
		Next      string           `json:"next_cursor"`
	}
	for _, c := range []struct {
		scopes []string
		pii    bool
	}{{[]string{ttapi.ScopeDataRead}, false}, {[]string{ttapi.ScopeDataRead, ttapi.ScopePIIRead}, true}} {
		key, _, err := ttapi.CreateClient(ctx, s.DB, fmt.Sprintf("reader-%v", c.pii), c.scopes, 0)
		if err != nil {
			t.Fatal(err)
		}
		api := &client{t: t, url: hs.URL, key: key}
		page.Follows, page.Next = nil, ""
		api.want(200, "GET", "/accounts/id:"+strconv.FormatInt(owner.UID, 10)+"/follows?limit=10", nil, &page)
		if page.Direction != "follower" || len(page.Follows) != 10 || page.Next == "" || page.List["state"] != "ok" {
			t.Fatalf("followers, first page: %s, %d links, next %q, list %v", page.Direction, len(page.Follows), page.Next, page.List)
		}
		for _, f := range page.Follows {
			if f["author_key"] == "" || (f["person"] != nil) != c.pii {
				t.Errorf("pii %v: link %v", c.pii, f)
			}
		}
		if c.pii {
			if p, _ := page.Follows[0]["person"].(map[string]any); p == nil || (p["username"] != n1.Username && p["username"] != n2.Username) {
				t.Errorf("newest link %v, want a new follower", page.Follows[0])
			}
		}
		next := page.Next
		page.Follows, page.Next = nil, ""
		api.want(200, "GET", "/accounts/id:"+strconv.FormatInt(owner.UID, 10)+"/follows?limit=10&cursor="+next, nil, &page)
		if len(page.Follows) != 4 || page.Next != "" {
			t.Errorf("followers, second page: %d links, next %q", len(page.Follows), page.Next)
		}
		api.want(200, "GET", "/accounts/id:"+strconv.FormatInt(other.UID, 10)+"/follows?direction=following", nil, &page)
		if page.List["state"] != "hidden" || len(page.Follows) != 0 {
			t.Errorf("other's following: list %v, %d links", page.List, len(page.Follows))
		}
		api.want(400, "GET", "/accounts/id:"+strconv.FormatInt(owner.UID, 10)+"/follows?direction=sideways", nil, nil)
	}

	// Switched off: the lists are not read.
	s.Cfg.TT.Discovery.Sources = map[string]bool{}
	w.Advance(25 * time.Hour)
	if n := dc.lists(t); n != 0 {
		t.Errorf("%d list reads with the sources off", n)
	}
}
