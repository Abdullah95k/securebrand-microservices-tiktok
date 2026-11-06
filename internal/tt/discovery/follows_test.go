package discovery

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Part D's planning (docs/tiktok/discovery-plan.md §9): every admitted,
// enabled schedule gets its lists, due at once; a hidden following list
// and a private account's lists move on without a call; claims take never
// read lists first, skip a list whose read still runs, and push the claim
// back by retry_after; the day's pages add up over the lists.
func TestFollowLists(t *testing.T) {
	c := newCandidates(t)
	ctx := context.Background()
	d := c.d
	d.Cfg.TT.Discovery.Enabled = true
	if dirs := d.ListDirections(); len(dirs) != 0 {
		t.Fatalf("directions with the sources off: %v", dirs)
	}
	d.Cfg.TT.Discovery.Sources = map[string]bool{"following": true, "follower": true}
	dirs := d.ListDirections()
	if !slices.Equal(dirs, []string{"following", "follower"}) {
		t.Fatalf("directions %v", dirs)
	}
	for _, uid := range []int64{1, 2, 3, 4, 5} {
		c.exec(`INSERT INTO accounts (user_id, sec_uid, username, author_key) VALUES ($1, $2, 'user', '\x00')`, uid, secOf(uid))
	}
	c.exec(`UPDATE accounts SET following_visibility = 2 WHERE user_id = 2`)
	c.exec(`UPDATE accounts SET status = 'private' WHERE user_id = 3`)
	for _, uid := range []int64{1, 2, 3, 4} {
		c.exec(`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at)
			VALUES ($1, $2, 9900, now(), now())`, "id:"+strconv.FormatInt(uid, 10), uid)
	}
	// Waiting for admission, and one disabled: no lists yet.
	c.exec(`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at) VALUES ('id:5', 5, 9900, now())`)
	c.exec(`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, admitted_at, enabled)
		VALUES ('u:off', 6, 9900, now(), now(), false)`)

	n, err := d.SeedLists(ctx, dirs, 100)
	if err != nil || n != 8 {
		t.Fatalf("seeded %d lists (%v), want 8", n, err)
	}
	if n, _ := d.SeedLists(ctx, dirs, 100); n != 0 {
		t.Fatalf("seeded %d lists again", n)
	}
	skipped, err := d.SkipLists(ctx, dirs)
	if err != nil || skipped["hidden"] != 1 || skipped["gone"] != 2 {
		t.Fatalf("skipped %v (%v): want 1 hidden (2's following), 2 gone (3's)", skipped, err)
	}

	// 1's lists, 2's followers and 4's lists are due; one read of 4's
	// followers ran before.
	c.exec(`UPDATE follow_lists SET first_read_at = $1, people_seen = 30, deep_cursor = '30' WHERE user_id = 4 AND direction = 'follower'`,
		c.now.Add(-48*time.Hour))
	got, err := d.ClaimLists(ctx, dirs, 4)
	if err != nil || len(got) != 4 {
		t.Fatalf("claimed %v (%v)", got, err)
	}
	for _, l := range got {
		if l.UserID == 4 && l.Direction == "follower" {
			t.Fatalf("a list read before was claimed before the never read ones: %v", got)
		}
		if l.SecUID != secOf(l.UserID) || l.ReadBefore {
			t.Fatalf("claimed %+v", l)
		}
	}
	job := uuid.Must(uuid.NewV7())
	c.exec(`INSERT INTO jobs (job_id, kind, lane, mode, account_ref, user_id, params, status) VALUES ($1, 'follows', 3, 'followers', 'id:4', 4, '{}', 'running')`, job)
	c.exec(`UPDATE follow_lists SET next_read_at = $1 WHERE user_id = 4 AND direction = 'follower'`, c.now.Add(-time.Minute))
	if err := d.ListStarted(ctx, 4, "follower", job); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.ClaimLists(ctx, dirs, 10); len(got) != 0 {
		t.Fatalf("claimed %v while the read of 4's followers runs", got)
	}
	if n, _ := d.ListsRunning(ctx); n != 1 {
		t.Fatalf("%d reads running, want 1", n)
	}
	c.exec(`UPDATE jobs SET status = 'succeeded' WHERE job_id = $1`, job)
	got, _ = d.ClaimLists(ctx, dirs, 10)
	if len(got) != 1 || got[0].UserID != 4 || !got[0].ReadBefore || got[0].DeepCursor != "30" {
		t.Fatalf("claimed %+v, want 4's followers, read before, deep at 30", got)
	}
	var next time.Time
	if err := c.db.QueryRow(ctx, `SELECT next_read_at FROM follow_lists WHERE user_id = 4 AND direction = 'follower'`).Scan(&next); err != nil ||
		!next.Equal(c.now.Add(d.Cfg.TT.Discovery.Follows.RetryAfter)) {
		t.Fatalf("claimed list due again at %v (%v)", next, err)
	}
	if err := d.ListUnclaim(ctx, 4, "follower"); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.ClaimLists(ctx, dirs, 10); len(got) != 1 {
		t.Fatalf("unclaimed list not claimable: %v", got)
	}

	c.exec(`UPDATE follow_lists SET day = $1::date, day_pages = 7 WHERE user_id IN (1, 4)`, c.now)
	c.exec(`UPDATE follow_lists SET day = $1::date, day_pages = 50 WHERE user_id = 2`, c.now.Add(-24*time.Hour))
	if n, err := d.ListPagesToday(ctx); err != nil || n != 28 {
		t.Fatalf("%d pages today (%v), want 28", n, err)
	}
	states, due, err := ListCounts(ctx, c.db, c.now)
	if err != nil || states[[2]string{"following", "hidden"}] != 1 || states[[2]string{"follower", "gone"}] != 1 || due["follower"] != 0 {
		t.Fatalf("counts %v due %v (%v)", states, due, err)
	}
}
