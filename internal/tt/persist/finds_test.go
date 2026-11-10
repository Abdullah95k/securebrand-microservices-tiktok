package persist

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
)

// found is a post discovery part C found, by user 7001 (not monitored).
func found(id int64, at time.Time, source, via string) *ttv1.Post {
	p := post(id, at, 1, nil)
	p.TextRanges = nil // no part B mentions here
	p.DiscoveredVia = ttv1.DiscoveredVia_DISCOVERED_VIA_HASHTAG
	p.Find = &ttv1.PostFind{Source: source, Via: via, Region: "IQ"}
	return p
}

// Part C (docs/tiktok/discovery-plan.md §8): the author of every post
// found is recorded once per source, target and post, with the region and
// the stored account's secUid and username, whether tks monitors the
// author or not; the same post found by another target is another find; a
// re-read, a redelivery and a source switched off record nothing; the post
// keeps the way it was first found.
func TestFinds(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	on := map[string]bool{"hashtag": true, "search": true}
	w.Sightings = func(h string) bool { return on[h] }
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	if err := w.Accounts(ctx, []*ttv1.Account{account(t0, ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST, "author", 10, false)}); err != nil {
		t.Fatal(err)
	}
	batch := []*ttv1.Post{found(1, t0, "hashtag", "iraqtag"), found(1, t0, "hashtag", "basra"), found(2, t0, "hashtag", "iraqtag"),
		found(3, t0, "sound", "77")}
	if err := w.Posts(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings WHERE how = 'hashtag' AND found_in = 'hashtag'
		AND user_id = 7001 AND sec_uid = 'sec-7001' AND username = 'author' AND region = 'IQ' AND via_user_id IS NULL`); n != 3 {
		t.Fatalf("%d hashtag finds, want 3 (post 1 by two hashtags, post 2)", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings WHERE how = 'sound'`); n != 0 {
		t.Errorf("%d sound finds with the source off", n)
	}
	if v := one[string](t, w.DB, `SELECT discovered_via FROM posts WHERE post_id = 1`); v != "hashtag" {
		t.Errorf("post 1 discovered_via %q", v)
	}
	// A re-read and a redelivery record nothing; a search find of the same
	// post is a find of its own; the account's own list read later keeps
	// discovered_via.
	again := found(1, t0.Add(time.Minute), "hashtag", "iraqtag")
	s := found(2, t0.Add(time.Minute), "search", "12")
	s.DiscoveredVia = ttv1.DiscoveredVia_DISCOVERED_VIA_SEARCH
	list := post(2, t0.Add(2*time.Minute), 1, proto.Bool(false))
	list.TextRanges = nil
	list.DiscoveredVia = ttv1.DiscoveredVia_DISCOVERED_VIA_LIST
	for _, b := range [][]*ttv1.Post{{again}, batch, {s}, {list}} {
		if err := w.Posts(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings WHERE via IS NOT NULL`); n != 4 {
		t.Errorf("%d finds after a re-read, a redelivery and a search, want 4", n)
	}
	if v := one[string](t, w.DB, `SELECT discovered_via FROM posts WHERE post_id = 2`); v != "hashtag" {
		t.Errorf("post 2 discovered_via %q after its list read", v)
	}
}

// A reposts tab's posts (source repost, via the reposting account's id)
// are finds like part C's, and every repost is kept in tt.reposts (owner
// D-047): first seen once, last seen on each read; an account's own post
// on its tab is no repost; nothing with the source off.
func TestReposts(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	on := map[string]bool{"repost": true}
	w.Sightings = func(h string) bool { return on[h] }
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	reposted := func(id int64, at time.Time, by string) *ttv1.Post {
		p := found(id, at, "repost", by)
		p.DiscoveredVia = ttv1.DiscoveredVia_DISCOVERED_VIA_REPOST
		p.Find.Region = ""
		p.Obs.JobIds = []string{"00000000-0000-0000-0000-00000000000" + by[len(by)-1:]}
		return p
	}
	if err := w.Posts(ctx, []*ttv1.Post{reposted(1, t0, "5"), reposted(2, t0, "5"), reposted(1, t0, "6"), reposted(3, t0, "7001")}); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM reposts`); n != 3 {
		t.Fatalf("%d reposts, want 3 (posts 1 and 2 by account 5, post 1 by account 6; the author's own post is none)", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM reposts WHERE author_user_id = 7001 AND first_seen_at = last_seen_at`); n != 3 {
		t.Errorf("%d reposts with their author and one sighting time", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings WHERE how = 'repost' AND found_in = 'repost' AND user_id = 7001`); n != 4 {
		t.Errorf("%d repost finds, want 4 (one per account and post)", n)
	}
	if v := one[string](t, w.DB, `SELECT discovered_via FROM posts WHERE post_id = 2`); v != "repost" {
		t.Errorf("post 2 discovered_via %q", v)
	}
	// A later read: last seen moves, first seen stays, no new find.
	if err := w.Posts(ctx, []*ttv1.Post{reposted(1, t0.Add(time.Hour), "5")}); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM reposts WHERE user_id = 5 AND post_id = 1
		AND first_seen_at = $1 AND last_seen_at = $2 AND last_job_id IS NOT NULL`, t0, t0.Add(time.Hour)); n != 1 {
		t.Errorf("the re-read repost's times are not first %v, last %v", t0, t0.Add(time.Hour))
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM discovery_sightings WHERE how = 'repost'`); n != 4 {
		t.Errorf("%d repost finds after a re-read", n)
	}
	// The source off: nothing kept.
	on["repost"] = false
	if err := w.Posts(ctx, []*ttv1.Post{reposted(9, t0, "8")}); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM reposts WHERE user_id = 8`); n != 0 {
		t.Errorf("%d reposts kept with the source off", n)
	}
}
