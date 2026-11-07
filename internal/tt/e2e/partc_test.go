package e2e

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	contracts "github.com/abdullah95k/securebrand-contracts"
	"github.com/abdullah95k/securebrand-contracts/conformance"
	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

// pages runs the discovery loop once, waits for the discovery page reads
// it started and finalises them, and returns how many it started.
func (dc *disco) pages(t *testing.T) int {
	t.Helper()
	ctx := context.Background()
	if err := dc.l.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := dc.s.DB.Query(ctx, `SELECT job_id FROM jobs WHERE kind = 'discover' AND status = 'running'`)
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
	for count(t, dc.s, `SELECT count(*) FROM jobs WHERE kind = 'discover' AND status IN ('running', 'finalizing')`) > 0 {
		if err := dc.sc.fin.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("discovery page reads not finalised")
		}
		time.Sleep(100 * time.Millisecond)
	}
	dc.s.WaitPersisted(t, ctx)
	return len(ids)
}

// TestDiscoveryPartC runs discovery part C end to end against fakett
// (docs/tiktok/discovery-plan.md §8): the hashtag and the sound two
// monitored accounts both use become targets, as do their recent posts'
// related posts; each is read once, signed through the crawler's client,
// on an identity bootstrapped on a monitored account's page; every post
// found is stored with the way it was found, its author as an account,
// and each author is counted by the leader: a monitored one gains the
// source with no read, the others get one entry check each, and the one
// that posts from Iraq is added. Before their next read nothing is read
// again; a source switched off reads nothing. (Keyword search and the feed
// run the same stage: internal/tt/stage's tests.)
func TestDiscoveryPartC(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 61, Accounts: 2, PostsPerAccount: 3, PostSpacing: 20 * time.Hour},
		Config: func(c *config.Config) {
			c.TT.Discovery.Enabled = true
			c.TT.Discovery.Sources = map[string]bool{"hashtag": true, "sound": true, "related": true}
			c.TT.Discovery.Hashtags.Keywords = false // the Iraq list's terms: a target each
			c.TT.Discovery.Hashtags.MinAccounts, c.TT.Discovery.Sounds.MinAccounts = 2, 2
		}})
	w := s.World
	ctx := context.Background()
	dc := newDisco(s)
	owner, other := w.Accounts()[0], w.Accounts()[1]
	day := 24 * time.Hour
	const tag, sound = "pcshared", int64(4242)

	star := w.AddAccount("pc_star", "", 3, day, day) // posts from Iraq
	var taggers []*fakett.Account
	for i := range 4 {
		taggers = append(taggers, w.AddAccount(fmt.Sprintf("pc_tag%d", i), "", 2, day, day))
	}
	w.Mutate(func() {
		for _, a := range append([]*fakett.Account{owner, other, star}, taggers...) {
			for _, p := range a.Posts {
				p.Hashtags = append(p.Hashtags, tag)
				p.Music.ID = sound
			}
		}
		star.Posts[0].Region = "IQ"
	})

	dc.sc.register(t, owner, other)
	dc.sc.tick(t) // the first reads store the monitored accounts' posts
	s.WaitPersisted(t, ctx)

	if n := dc.pages(t); n < 3 {
		t.Fatalf("%d discovery page reads, want the hashtag, the sound and related posts", n)
	}
	target := func(source, key string) (state, stop string) {
		t.Helper()
		var st *string
		if err := s.DB.QueryRow(ctx, `SELECT state, last_stop FROM discovery_targets WHERE source = $1 AND key = $2`, source, key).
			Scan(&state, &st); err != nil {
			t.Fatalf("target %s %s: %v", source, key, err)
		}
		if st != nil {
			stop = *st
		}
		return state, stop
	}
	if st, stop := target("hashtag", tag); st != "ok" || stop != "end" {
		t.Errorf("the hashtag: %s, %s", st, stop)
	}
	if st, stop := target("sound", strconv.FormatInt(sound, 10)); st != "ok" || stop != "end" {
		t.Errorf("the sound: %s, %s", st, stop)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_targets WHERE source = 'related' AND last_stop IS NOT NULL AND next_read_at IS NULL`); n != 6 {
		t.Errorf("%d related posts read once, want 6 (the monitored accounts' posts)", n)
	}
	for _, a := range append([]*fakett.Account{star}, taggers...) {
		if n := count(t, s, `SELECT count(*) FROM posts WHERE user_id = $1 AND discovered_via IN ('hashtag', 'sound', 'related')`, a.UID); n == 0 {
			t.Errorf("%s: no post stored as found", a.Username)
		}
		if n := count(t, s, `SELECT count(*) FROM accounts WHERE user_id = $1`, a.UID); n != 1 {
			t.Errorf("%s: the author not stored", a.Username)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_sightings WHERE how = 'hashtag' AND via = $1 AND region IS NOT NULL`, tag); n == 0 {
		t.Fatal("no hashtag finds recorded")
	}

	// The leader counts the finds: the monitored accounts gain the source,
	// the others get their entry check.
	checks := dc.round(t)
	for range 3 {
		if count(t, s, `SELECT count(*) FROM discovery_candidates WHERE state IN ('pending', 'checking')`) == 0 {
			break
		}
		checks += dc.round(t)
	}
	if checks != 5 {
		t.Errorf("%d entry checks, want 5 (the star and 4 taggers)", checks)
	}
	dc.want(t, star, discovery.StateAdded, "")
	for _, a := range taggers {
		dc.want(t, a, discovery.StateNotIraqi, "")
	}
	if n := count(t, s, `SELECT count(*) FROM account_sources WHERE user_id = $1 AND how = 'hashtag' AND via = $2`, other.UID, tag); n != 1 {
		t.Errorf("the monitored account found by the hashtag: %d sources, want 1", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE user_id IN ($1, $2)`, owner.UID, other.UID); n != 0 {
		t.Errorf("a monitored account became a candidate")
	}
	if n := count(t, s, `SELECT count(*) FROM account_sources WHERE user_id = $1 AND how = 'hashtag' AND evidence->>'region' <> ''`, star.UID); n != 1 {
		t.Errorf("the star's hashtag source carries no region")
	}

	// Not due yet: nothing is read again (the added star's new posts' related
	// posts aside).
	reads := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'discover' AND mode IN ('hashtag', 'sound')`)
	dc.pages(t)
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'discover' AND mode IN ('hashtag', 'sound')`); n != reads {
		t.Errorf("%d hashtag and sound reads before they were due, want %d", n, reads)
	}

	// A keyword search's find (the persister's record of it; search itself
	// needs an Iraq-placed tier) reaches the shared contract with the
	// shared keyword list's id.
	s.Cfg.TT.Discovery.Sources["search"] = true
	var kwID int64
	var grp, lang, term string
	if err := s.DB.QueryRow(ctx, `INSERT INTO discovery_keywords (term, normalized, language, grp) VALUES ('Baghdad', 'baghdad', 'en', 'baghdad')
		RETURNING id, grp, language, term`).Scan(&kwID, &grp, &lang, &term); err != nil {
		t.Fatal(err)
	}
	tg := taggers[0]
	if _, err := s.DB.Exec(ctx, `INSERT INTO discovery_sightings (how, found_in, via, region, user_id, sec_uid, username, post_id, seen_at)
		VALUES ('search', 'search', $1, 'US', $2, $3, $4, $5, $6)`, strconv.FormatInt(kwID, 10), tg.UID, tg.SecUID, tg.Username,
		tg.Posts[0].ID, w.Clock()); err != nil {
		t.Fatal(err)
	}
	if err := dc.l.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, `SELECT count(*) FROM account_sources WHERE user_id = $1 AND how = 'search' AND via = $2`, tg.UID,
		strconv.FormatInt(kwID, 10)); n != 1 {
		t.Fatalf("the search find: %d sources, want 1", n)
	}
	pub := &content.Candidates{E: &content.Emitter{DB: s.DB, Producer: s.Producer, Options: conformance.Options{Production: true}}}
	if _, err := pub.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	want := contracts.KeywordID(grp, lang, term)
	found := false
	for _, m := range contractRecords(t, s, content.TopicCandidate, int(topicRecords(t, s, content.TopicCandidate))) {
		c := m.(*discoveryv1.Candidate)
		if c.Account.GetHandle() != tg.Username {
			continue
		}
		for _, ev := range c.Evidence {
			if ev.KeywordId == want && ev.Keyword == term && ev.Language == lang && ev.ContentId != "" {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("no candidate event of %s with the keyword %q", tg.Username, want)
	}

	// Switched off: nothing is read.
	s.Cfg.TT.Discovery.Sources = map[string]bool{}
	w.Advance(8 * day)
	if n := dc.pages(t); n != 0 {
		t.Errorf("%d page reads with the sources off", n)
	}
}
