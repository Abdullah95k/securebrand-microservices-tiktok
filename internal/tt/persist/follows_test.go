package persist

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
)

func follow(at time.Time, dir ttv1.FollowDirection, person int64, private bool, videos int64) *ttv1.Follow {
	return &ttv1.Follow{Obs: obs(at, "f"), UserId: 7001, Direction: dir,
		Person:  &ttv1.Person{AuthorKey: emit.AuthorKey(secret, person), UserId: proto.Int64(person), SecUid: "sec-p", Username: "person"},
		Private: proto.Bool(private), VideoCount: proto.Int64(videos), FollowerCount: proto.Int64(3)}
}

// Part D (docs/tiktok/discovery-plan.md §9): every link read is kept with
// its first and last time and the person; a link new to a monitored
// account's list records one sighting with what the list showed (the
// pre-checks), a re-read or a redelivery none; the account itself, a
// record without a direction or a person without an id are refused; a
// source switched off records nothing.
func TestFollows(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	on := map[string]bool{"following": true, "follower": true}
	w.Sightings = func(h string) bool { return on[h] }
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	if err := w.Accounts(ctx, []*ttv1.Account{account(t0, ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE, "first", 100, true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.DB.Exec(ctx, `INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at) VALUES ('u:first', 7001, 3600, now())`); err != nil {
		t.Fatal(err)
	}
	fol, fwr := ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWING, ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER
	self := follow(t0, fwr, 7001, false, 1)
	noDir := follow(t0, ttv1.FollowDirection_FOLLOW_DIRECTION_UNSPECIFIED, 8003, false, 1)
	noID := follow(t0, fwr, 0, false, 1)
	noID.Person.UserId = nil
	batch := []*ttv1.Follow{follow(t0, fol, 8001, false, 4), follow(t0, fwr, 8001, false, 4), follow(t0, fwr, 8002, true, 0),
		follow(t0, fwr, 8002, true, 0), self, noDir, noID}
	if err := w.Follows(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM follows`); n != 3 {
		t.Fatalf("%d links, want 3", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM people WHERE user_id IN (8001, 8002)`); n != 2 {
		t.Fatalf("%d people, want 2", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings WHERE how = 'following' AND found_in = 'following'
		AND via_user_id = 7001 AND user_id = 8001 AND NOT private AND videos = 4`); n != 1 {
		t.Errorf("%d following sightings of 8001, want 1", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings WHERE how = 'follower' AND found_in = 'followers'
		AND user_id = 8002 AND private AND videos = 0`); n != 1 {
		t.Errorf("%d follower sightings of 8002, want 1", n)
	}

	// A re-read later: last seen moves, first stays, nothing is recorded
	// again; an older redelivery changes nothing.
	t1 := t0.Add(24 * time.Hour)
	later := follow(t1, fwr, 8002, false, 2)
	if err := w.Follows(ctx, []*ttv1.Follow{later}); err != nil {
		t.Fatal(err)
	}
	if err := w.Follows(ctx, []*ttv1.Follow{follow(t0.Add(-time.Hour), fwr, 8002, true, 9)}); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings`); n != 3 {
		t.Errorf("%d sightings after a re-read, want 3", n)
	}
	var first, last time.Time
	var private bool
	var videos int64
	if err := w.DB.QueryRow(ctx, `SELECT first_seen_at, last_seen_at, person_private, person_videos FROM follows
		WHERE user_id = 7001 AND direction = 'follower' AND person_user_id = 8002`).Scan(&first, &last, &private, &videos); err != nil {
		t.Fatal(err)
	}
	if !first.Equal(t0.Add(-time.Hour)) || !last.Equal(t1) || private || videos != 2 {
		t.Errorf("link: first %v last %v private %v videos %d", first, last, private, videos)
	}

	// Another account's list, not monitored: kept, no sighting.
	other := follow(t1, fwr, 8004, false, 1)
	other.UserId = 7999
	// A source switched off: kept, no sighting.
	on["following"] = false
	if err := w.Follows(ctx, []*ttv1.Follow{other, follow(t1, fol, 8005, false, 1)}); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM follows`); n != 5 {
		t.Errorf("%d links, want 5", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings`); n != 3 {
		t.Errorf("%d sightings, want 3 (not monitored, source off)", n)
	}

	// following_visibility from the profile page is kept when a list read
	// carries none.
	a := account(t1, ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE, "first", 101, true)
	a.FollowingVisibility = proto.Int32(2)
	if err := w.Accounts(ctx, []*ttv1.Account{a}); err != nil {
		t.Fatal(err)
	}
	if err := w.Accounts(ctx, []*ttv1.Account{account(t1.Add(time.Minute), ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST, "first", 101, false)}); err != nil {
		t.Fatal(err)
	}
	if v := one[int16](t, w.DB, `SELECT following_visibility FROM accounts WHERE user_id = 7001`); v != 2 {
		t.Errorf("following_visibility %d, want 2", v)
	}
}
