package stage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// targetState is a discovery_targets row.
type targetState struct {
	state, stop  string
	code, itemID int64
	pages, posts int64
	next         *time.Time
}

func readTarget(t *testing.T, h *harness, source, key string) targetState {
	t.Helper()
	var s targetState
	var code, item *int64
	if err := h.d.DB.QueryRow(context.Background(), `SELECT state, coalesce(last_stop, ''), status_code, item_id, pages_read, posts_seen,
		next_read_at FROM discovery_targets WHERE source = $1 AND key = $2`, source, key).Scan(&s.state, &s.stop, &code, &item,
		&s.pages, &s.posts, &s.next); err != nil {
		t.Fatal(err)
	}
	if code != nil {
		s.code = *code
	}
	if item != nil {
		s.itemID = *item
	}
	return s
}

// discover runs one discovery page read as a job of its own, on a target
// row it creates.
func discover(t *testing.T, h *harness, job string, sc *tkstaskv1.DiscoverScope) jobs.Status {
	t.Helper()
	ctx := context.Background()
	origin := map[string]string{DiscoverSearch: "keyword", DiscoverHashtag: "keyword", DiscoverSound: "monitored",
		DiscoverRelated: "post", DiscoverFeed: "feed"}[sc.Source]
	if _, err := h.d.DB.Exec(ctx, `INSERT INTO discovery_targets (source, key, name, origin) VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING`, sc.Source, sc.Key, sc.Name, origin); err != nil {
		t.Fatal(err)
	}
	if err := h.d.spawn(ctx, task.NewDiscover(job, sc)); err != nil {
		t.Fatal(err)
	}
	return h.drain(job)
}

// A hashtag's read resolves the hashtag's id by its detail call, reads its
// posts page after page to the end, and publishes every post with where it
// was found and every author once a page; a read capped at one page stops
// there. The target records what the read did and its next read.
func TestDiscoverHashtag(t *testing.T) {
	h := newHarness(t, tttest.Options{})
	w := h.env.World
	boot := w.Accounts()[0]
	var tagged []*fakett.Account
	for i := range 8 {
		a := w.AddAccount(fmt.Sprintf("tagger%d", i), "", 5, day, day)
		tagged = append(tagged, a)
		w.Mutate(func() {
			for _, p := range a.Posts {
				p.Hashtags = append(p.Hashtags, "iraqtag")
			}
		})
	}
	st := discover(t, h, "tag-1", &tkstaskv1.DiscoverScope{Source: DiscoverHashtag, Key: "iraqtag", Name: "iraqtag", MaxPages: 5,
		BootUsername: boot.Username})
	if st.Counters["discover_pages"] != 2 || st.Counters["discover_stop_end"] != 1 || st.Counters["posts"] != 40 || st.Counters["tasks_failed"] != 0 {
		t.Fatalf("counters %v", st.Counters)
	}
	ps := posts(h, "tag-1")
	if len(ps) != 40 {
		t.Fatalf("%d posts published, want 40", len(ps))
	}
	for _, p := range ps {
		f := p.GetFind()
		if p.DiscoveredVia != ttv1.DiscoveredVia_DISCOVERED_VIA_HASHTAG || f.GetSource() != DiscoverHashtag || f.GetVia() != "iraqtag" ||
			f.GetRegion() == "" {
			t.Fatalf("post %v", p)
		}
	}
	authors := map[int64]int{}
	for _, a := range accounts(h, "tag-1") {
		authors[a.UserId]++
	}
	if len(authors) != 8 || authors[tagged[0].UID] != 2 { // once a page
		t.Errorf("authors published %v, want 8, each once on each of 2 pages", authors)
	}
	tg := readTarget(t, h, DiscoverHashtag, "iraqtag")
	if tg.state != "ok" || tg.stop != "end" || tg.itemID != fakett.ChallengeID("iraqtag") || tg.pages != 2 || tg.posts != 40 ||
		tg.next == nil || !tg.next.Equal(w.Clock().Add(h.d.Cfg.TT.Discovery.Hashtags.Every)) {
		t.Fatalf("target %+v", tg)
	}

	// Capped at one page, with the id known: no detail call.
	st = discover(t, h, "tag-2", &tkstaskv1.DiscoverScope{Source: DiscoverHashtag, Key: "iraqtag", Name: "iraqtag", MaxPages: 1,
		ItemId: fakett.ChallengeID("iraqtag"), BootUsername: boot.Username})
	if st.Counters["discover_pages"] != 1 || st.Counters["discover_stop_cap"] != 1 || st.Counters["requests"] != 1 {
		t.Fatalf("capped read: counters %v", st.Counters)
	}
	if tg := readTarget(t, h, DiscoverHashtag, "iraqtag"); tg.pages != 3 || tg.posts != 70 {
		t.Fatalf("target after two reads %+v", tg)
	}
}

// A search walks TikTok's offsets with its search id, through its tier; a
// login wall (2483 from US exits, T37) ends the read as refused after one
// more try, and the target records the code; a search without a tier
// never runs.
func TestDiscoverSearch(t *testing.T) {
	h := newHarness(t, tttest.Options{Faults: fakett.Faults{LoginAfter: 24}})
	w := h.env.World
	boot := w.Accounts()[0]
	for i := range 6 {
		a := w.AddAccount(fmt.Sprintf("searched%d", i), "", 5, day, day)
		w.Mutate(func() {
			for _, p := range a.Posts {
				p.Desc += " baghdad"
			}
		})
	}
	st := discover(t, h, "search-1", &tkstaskv1.DiscoverScope{Source: DiscoverSearch, Key: "7", Name: "baghdad", MaxPages: 5,
		Tier: "direct", BootUsername: boot.Username})
	// Pages at offsets 0 and 12, then the wall at 24.
	if st.Counters["discover_pages"] != 2 || st.Counters["posts"] != 24 || st.Counters["discover_stop_refused"] != 1 ||
		st.Counters["soft_errors"] != 1 {
		t.Fatalf("counters %v", st.Counters)
	}
	for _, p := range posts(h, "search-1") {
		if f := p.GetFind(); p.DiscoveredVia != ttv1.DiscoveredVia_DISCOVERED_VIA_SEARCH || f.GetVia() != "7" {
			t.Fatalf("post %v", p)
		}
	}
	if tg := readTarget(t, h, DiscoverSearch, "7"); tg.state != "refused" || tg.code != fakett.LoginCode || tg.stop != "refused" {
		t.Fatalf("target %+v", tg)
	}
	var reqs int
	for _, r := range h.env.Requests("/api/search/general/full/") {
		if strings.Contains(r.URL, "search_id=") {
			reqs++
		}
	}
	if reqs == 0 {
		t.Error("no later search page carried the search id")
	}

	st = discover(t, h, "search-2", &tkstaskv1.DiscoverScope{Source: DiscoverSearch, Key: "8", Name: "baghdad", MaxPages: 5,
		BootUsername: boot.Username})
	if st.Counters["requests"] != 0 || st.Counters["tasks_failed"] != 1 {
		t.Fatalf("search without a tier: counters %v", st.Counters)
	}
}

// A post's related posts are one page, read once (no next read); the feed
// is read its number of times, each read bringing new posts, counted by
// day; a sound TikTok has no posts for ends empty.
func TestDiscoverRelatedFeedSound(t *testing.T) {
	h := newHarness(t, tttest.Options{})
	w := h.env.World
	boot := w.Accounts()[0]
	for i := range 4 {
		a := w.AddAccount(fmt.Sprintf("rel%d", i), "", 3, day, day)
		w.Mutate(func() {
			for _, p := range a.Posts {
				p.Hashtags = append(p.Hashtags, "shared")
			}
		})
	}
	w.Mutate(func() { boot.Posts[0].Hashtags = append(boot.Posts[0].Hashtags, "shared") })
	post := boot.Posts[0]
	key := strconv.FormatInt(post.ID, 10)
	st := discover(t, h, "related-1", &tkstaskv1.DiscoverScope{Source: DiscoverRelated, Key: key, MaxPages: 1, ItemId: post.ID,
		PostUserId: boot.UID, BootUsername: boot.Username})
	if st.Counters["discover_pages"] != 1 || st.Counters["discover_stop_end"] != 1 || st.Counters["posts"] < 12 || st.Counters["posts"] > 16 {
		t.Fatalf("related: counters %v", st.Counters)
	}
	if tg := readTarget(t, h, DiscoverRelated, key); tg.state != "ok" || tg.next != nil {
		t.Fatalf("related target %+v", tg)
	}

	st = discover(t, h, "feed-1", &tkstaskv1.DiscoverScope{Source: DiscoverFeed, Key: "feed", MaxPages: 2, BootUsername: boot.Username})
	if st.Counters["discover_pages"] != 2 || st.Counters["discover_stop_cap"] != 1 || st.Counters["posts"] != 24 {
		t.Fatalf("feed: counters %v", st.Counters)
	}
	day := w.Clock().Format(time.DateOnly)
	for _, p := range posts(h, "feed-1") {
		if f := p.GetFind(); f.GetVia() != day || f.GetSource() != DiscoverFeed {
			t.Fatalf("feed post %v", p)
		}
	}

	st = discover(t, h, "sound-1", &tkstaskv1.DiscoverScope{Source: DiscoverSound, Key: "1", ItemId: 1, MaxPages: 3, BootUsername: boot.Username})
	if st.Counters["discover_stop_empty"] != 1 || st.Counters["posts"] != 0 {
		t.Fatalf("sound: counters %v", st.Counters)
	}
}
