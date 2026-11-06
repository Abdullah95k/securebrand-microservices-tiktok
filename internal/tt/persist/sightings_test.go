package persist

import (
	"context"
	"testing"
	"time"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// Discovery part B (docs/tiktok/discovery-plan.md §7): the persister
// records the people a monitored account's new posts, bio and comments
// bring in, only for rows it writes for the first time, only for accounts
// on the schedule (by reference before the first job stamps the id), and
// only for the sources switched on.
func TestSightings(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	on := map[string]bool{"mention": true, "reply": true, "commenter": true}
	w.Sightings = func(h string) bool { return on[h] }
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	if _, err := ttstore.EnsurePartitions(ctx, w.DB, post(1, t0, 1, nil).CreatedAt.AsTime(), storeWindow); err != nil {
		t.Fatal(err)
	}
	list := ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST
	sightings := func(q string, args ...any) int64 {
		t.Helper()
		return one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings WHERE `+q, args...)
	}

	// Not monitored (an entry check's read): nothing.
	a := account(t0, list, "first", 100, false)
	a.Bio = "follow @friend_x"
	if err := w.Accounts(ctx, []*ttv1.Account{a}); err != nil {
		t.Fatal(err)
	}
	if err := w.Posts(ctx, []*ttv1.Post{post(1, t0, 1, nil)}); err != nil {
		t.Fatal(err)
	}
	if n := sightings(`true`); n != 0 {
		t.Fatalf("%d sightings of an account not monitored", n)
	}

	// Registered by username; its schedule has no user id yet.
	if _, err := w.DB.Exec(ctx, `INSERT INTO account_schedule (account_ref, interval_seconds, next_run_at) VALUES ('u:first', 3600, now())`); err != nil {
		t.Fatal(err)
	}
	t1 := t0.Add(time.Minute)
	if err := w.Posts(ctx, []*ttv1.Post{post(1, t1, 2, nil), post(2, t1, 1, nil)}); err != nil {
		t.Fatal(err)
	}
	if n := sightings(`how = 'mention' AND found_in = 'caption' AND user_id = 8001 AND post_id = 2 AND via_user_id = 7001`); n != 1 {
		t.Errorf("new post: %d caption sightings, want 1 (the post already stored brings none)", n)
	}
	// A re-read, an edit, a redelivery: nothing again.
	p2 := post(2, t1.Add(time.Minute), 5, nil)
	p2.Caption = "edited"
	if err := w.Posts(ctx, []*ttv1.Post{p2, post(2, t1, 1, nil)}); err != nil {
		t.Fatal(err)
	}
	if n := sightings(`found_in = 'caption'`); n != 1 {
		t.Errorf("after a re-read: %d caption sightings, want 1", n)
	}

	// Comments: the commenter, the one the owner answers, and the ones a
	// comment names; the owner's own comment brings no one in.
	c := comment(10, 0, t1, "hi", 9001)
	c.TextRanges = []*ttv1.TextRange{{Type: "mention", Person: personOf(9002)}, {Type: "mention", Person: personOf(7001)}}
	reply := comment(11, 10, t1, "thanks", 7001)
	own := comment(12, 0, t1, "mine", 7001)
	if err := w.Comments(ctx, []*ttv1.Comment{c, reply, own}); err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]int64{
		`how = 'commenter' AND user_id = 9001 AND comment_id = 10`:                  1,
		`how = 'mention' AND found_in = 'comment' AND user_id = 9002`:               1,
		`how = 'reply' AND user_id = 9001 AND comment_id = 11 AND username = 'uhi'`: 1,
		`user_id = 7001`:       0,
		`found_in = 'comment'`: 3,
	} {
		if n := sightings(q); n != want {
			t.Errorf("%s: %d, want %d", q, n, want)
		}
	}
	if err := w.Comments(ctx, []*ttv1.Comment{comment(10, 0, t1.Add(time.Minute), "hi, edited", 9001), reply}); err != nil {
		t.Fatal(err)
	}
	if n := sightings(`found_in = 'comment'`); n != 3 {
		t.Errorf("after an edit and a redelivery: %d comment sightings, want 3", n)
	}

	// The bio: once when it changes, by username only, never the account's own.
	b := account(t1, list, "first", 100, false)
	b.Bio = "follow @friend_x and @First"
	if err := w.Accounts(ctx, []*ttv1.Account{b}); err != nil {
		t.Fatal(err)
	}
	b2 := account(t1.Add(time.Minute), list, "first", 120, false)
	b2.Bio = b.Bio
	if err := w.Accounts(ctx, []*ttv1.Account{b2}); err != nil {
		t.Fatal(err)
	}
	if n := sightings(`found_in = 'bio' AND username = 'friend_x' AND user_id IS NULL`); n != 1 {
		t.Errorf("%d bio sightings, want 1", n)
	}
	if n := sightings(`found_in = 'bio'`); n != 1 {
		t.Errorf("%d bio sightings in all, want 1", n)
	}

	// A source switched off records nothing.
	on["mention"] = false
	if err := w.Posts(ctx, []*ttv1.Post{post(3, t1, 1, nil)}); err != nil {
		t.Fatal(err)
	}
	if n := sightings(`found_in = 'caption'`); n != 1 {
		t.Errorf("mention off: %d caption sightings, want 1", n)
	}
}

func personOf(id int64) *ttv1.Person {
	p := comment(0, 0, time.Time{}, "", id).Author
	p.Username = ""
	return p
}
