package discovery

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Part C's planning (docs/tiktok/discovery-plan.md §8): search is off
// without a tier; the keywords become search targets and their one-word
// terms hashtag targets, following the keyword list's switches; the
// hashtags and sounds enough monitored accounts used become targets (not
// the skipped ones, not one account's own), and stop being read once no
// longer used; monitored accounts' recent posts get their related posts
// read once; the feed is one target. Claims take never read targets
// first, skip one whose read still runs and push the claim back by
// retry_after.
func TestPageTargets(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	d := c.d
	dc := &d.Cfg.TT.Discovery
	dc.Enabled = true
	dc.Sources = map[string]bool{"search": true, "hashtag": true, "sound": true, "related": true, "feed": true}
	if src := d.PageSources(); slices.Contains(src, SourceSearch) || len(src) != 4 {
		t.Fatalf("sources without a search tier: %v", src)
	}
	dc.Search.Tier = "iraq"
	sources := d.PageSources()
	if len(sources) != 5 {
		t.Fatalf("sources %v", sources)
	}
	dc.Hashtags.MinAccounts, dc.Sounds.MinAccounts = 2, 2
	dc.Hashtags.Skip = []string{"#FYP"}

	// Three monitored accounts and one that is not: their recent posts'
	// hashtags and sounds.
	for _, uid := range []int64{1, 2, 3, 4} {
		c.exec(`INSERT INTO accounts (user_id, sec_uid, username, author_key) VALUES ($1, $2, $3, '\x00')`, uid, secOf(uid),
			"user"+strconv.FormatInt(uid, 10))
	}
	for _, uid := range []int64{1, 2, 3} {
		c.exec(`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at)
			VALUES ($1, $2, 9900, now(), now())`, "id:"+strconv.FormatInt(uid, 10), uid)
	}
	post := func(uid, id int64, age time.Duration, tags []string, music int64, plays int64) {
		ids := make([]int64, len(tags))
		for i := range tags {
			ids[i] = int64(100 + i)
		}
		c.exec(`INSERT INTO posts (created_at, post_id, user_id, kind, hashtags, hashtag_ids, music_id, play_count, content_hash,
			first_seen_at, last_seen_at) VALUES ($1, $2, $3, 'video', $4, $5, $6, $7, '\x00', $1, $1)`,
			c.now.Add(-age), id, uid, tags, ids, music, plays)
	}
	post(1, 11, time.Hour, []string{"zzshared", "fyp", "solo1"}, 77, 10)
	post(2, 21, 2*time.Hour, []string{"zzshared", "fyp"}, 77, 50)
	post(3, 31, 5*24*time.Hour, []string{"zzshared"}, 88, 5) // older than related's window
	post(4, 41, time.Hour, []string{"solo1"}, 88, 900)       // not monitored

	if err := d.SeedTargets(ctx, sources, true); err != nil {
		t.Fatal(err)
	}
	keywords := one[int64](t, c, `SELECT count(*) FROM discovery_keywords WHERE enabled`)
	if n := one[int64](t, c, `SELECT count(*) FROM discovery_targets WHERE source = 'search' AND enabled`); n != keywords {
		t.Fatalf("%d search targets, want %d", n, keywords)
	}
	if n := one[int64](t, c, `SELECT count(*) FROM discovery_targets WHERE source = 'hashtag' AND origin = 'keyword'`); n == 0 {
		t.Fatal("no keyword hashtags")
	}
	if n := one[int64](t, c, `SELECT count(*) FROM discovery_targets WHERE source = 'hashtag' AND key LIKE '% %'`); n != 0 {
		t.Fatalf("%d hashtags of several words", n)
	}
	used := func(src string) []string {
		t.Helper()
		rows, err := c.db.Query(ctx, `SELECT key FROM discovery_targets WHERE source = $1 AND origin = 'monitored' AND enabled ORDER BY key`, src)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var k string
			if err := rows.Scan(&k); err != nil {
				t.Fatal(err)
			}
			out = append(out, k)
		}
		return out
	}
	if got := used("hashtag"); !slices.Equal(got, []string{"zzshared"}) {
		t.Errorf("used hashtags %v, want [zzshared] (fyp skipped, solo1 by one monitored account)", got)
	}
	if got := used("sound"); !slices.Equal(got, []string{"77"}) {
		t.Errorf("used sounds %v, want [77]", got)
	}
	if id := one[int64](t, c, `SELECT coalesce(item_id, 0) FROM discovery_targets WHERE source = 'hashtag' AND key = 'zzshared'`); id != 100 {
		t.Errorf("zzshared's id %d, want 100 (from the posts)", id)
	}
	rel := one[int64](t, c, `SELECT count(*) FROM discovery_targets WHERE source = 'related'`)
	if rel != 2 {
		t.Errorf("%d related targets, want 2 (the monitored posts in the window)", rel)
	}
	if n := one[int64](t, c, `SELECT count(*) FROM discovery_targets WHERE source = 'feed'`); n != 1 {
		t.Errorf("%d feed targets", n)
	}

	// A keyword turned off stops its search; a hashtag no longer used
	// stops being read.
	kw := one[int64](t, c, `SELECT min(id) FROM discovery_keywords WHERE enabled`)
	c.exec(`UPDATE discovery_keywords SET enabled = false WHERE id = $1`, kw)
	c.exec(`DELETE FROM posts WHERE post_id IN (21, 31)`)
	if err := d.SeedTargets(ctx, sources, true); err != nil {
		t.Fatal(err)
	}
	if on := one[bool](t, c, `SELECT enabled FROM discovery_targets WHERE source = 'search' AND key = $1`, strconv.FormatInt(kw, 10)); on {
		t.Error("a keyword turned off is still searched")
	}
	if got := used("hashtag"); len(got) != 0 {
		t.Errorf("used hashtags %v after zzshared lost a user", got)
	}

	// Claims: never read first; a running read is not claimed again.
	c.exec(`UPDATE discovery_targets SET next_read_at = NULL WHERE source <> 'feed' AND source <> 'related'`)
	c.exec(`UPDATE discovery_targets SET first_read_at = $1 WHERE source = 'related' AND key = '11'`, c.now.Add(-time.Hour))
	got, err := d.ClaimTargets(ctx, sources, 2)
	if err != nil || len(got) != 2 || got[0].Source == SourceRelated && got[0].Key == "11" {
		t.Fatalf("claimed %+v (%v)", got, err)
	}
	job := uuid.New()
	c.exec(`INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status, created_at) VALUES ($1, 'discover', 3, 'related', '', '{}', 'running', now())`, job)
	if err := d.TargetStarted(ctx, SourceRelated, "11", job); err != nil {
		t.Fatal(err)
	}
	c.exec(`UPDATE discovery_targets SET next_read_at = $1 WHERE source = 'related' AND key = '11'`, c.now)
	if got, _ := d.ClaimTargets(ctx, sources, 5); len(got) != 0 {
		t.Fatalf("claimed %+v: a running read, and the rest pushed back", got)
	}
	if n, _ := d.TargetsRunning(ctx); n != 1 {
		t.Errorf("running %d", n)
	}
	var rel11 DueTarget
	c.exec(`UPDATE jobs SET status = 'succeeded' WHERE job_id = $1`, job)
	if got, _ := d.ClaimTargets(ctx, sources, 5); len(got) != 1 {
		t.Fatalf("claimed %+v", got)
	} else {
		rel11 = got[0]
	}
	if rel11.PostUserID != 1 || rel11.PostUsername != "user1" || rel11.ItemID != 11 {
		t.Errorf("related target %+v", rel11)
	}
	if err := d.TargetUnclaim(ctx, SourceRelated, "11"); err != nil {
		t.Fatal(err)
	}
	if boot, err := d.BootUsernames(ctx, 10); err != nil || len(boot) != 3 {
		t.Errorf("boot usernames %v (%v)", boot, err)
	}
}

// Part C's finds are counted like part B's: by source, target and person,
// with the region TikTok served the page in as evidence (what the score's
// "shown to viewers in Iraq" reads); a new account waits for its entry
// check.
func TestPageFindsCounted(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	d := c.d
	d.Cfg.TT.Discovery.Enabled = true
	d.Cfg.TT.Discovery.Sources = map[string]bool{"hashtag": true}
	d.Cfg.TT.Discovery.Score.AudienceStrong = 2
	c.exec(`INSERT INTO accounts (user_id, sec_uid, username, author_key) VALUES (9, $1, 'found', '\x00')`, secOf(9))
	for _, via := range []string{"zzshared", "zzshared", "erbil"} {
		c.exec(`INSERT INTO discovery_sightings (how, found_in, via, region, user_id, sec_uid, username, post_id, seen_at)
			VALUES ('hashtag', 'hashtag', $1, 'IQ', 9, $2, 'found', $3, $4) ON CONFLICT DO NOTHING`, via, secOf(9),
			int64(len(via)), c.now)
	}
	n, err := d.Count(ctx, 100)
	if err != nil || n != 2 {
		t.Fatalf("counted %d (%v), want 2 (one find is the same post twice)", n, err)
	}
	if n := one[int64](t, c, `SELECT count(*) FROM account_sources WHERE how = 'hashtag'`); n != 2 {
		t.Fatalf("%d hashtag sources, want 2", n)
	}
	var ev json.RawMessage
	if err := c.db.QueryRow(ctx, `SELECT evidence FROM account_sources WHERE how = 'hashtag' AND via = 'zzshared'`).Scan(&ev); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(ev, &m); err != nil || m["region"] != "IQ" || m["found_in"] != "hashtag" {
		t.Fatalf("evidence %s", ev)
	}
	if st, _ := c.state("sec:" + secOf(9)); st != StatePending {
		t.Fatalf("state %s, want pending (its entry check)", st)
	}
}

func one[T any](t *testing.T, c *candidates, q string, args ...any) T {
	t.Helper()
	var v T
	if err := c.db.QueryRow(context.Background(), q, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}
