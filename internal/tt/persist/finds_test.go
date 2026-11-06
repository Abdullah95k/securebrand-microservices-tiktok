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
