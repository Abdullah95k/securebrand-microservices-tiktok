package e2e

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	ttapi "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/api"
)

const refresh = commonv1.CrawlMode_CRAWL_MODE_REFRESH

// TestPostsNoLongerPublic runs the crawler, the persister and the API
// against fakett (docs/tiktok/discovery-plan.md §17): after a backfill, a
// post made private is found by a metrics walk, confirmed on its page and
// stored as unavailable with its code and its content kept; the API lists
// it under ?status=unavailable; when it comes back it is visible again, and
// both changes stay in its history. A post deleted outright (its page's
// 10204 is unconfirmed: T34) keeps its status.
func TestPostsNoLongerPublic(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 31, Accounts: 2, PostsPerAccount: 40, MaxComments: 6, MaxReplies: 2,
		PostSpacing: 20 * time.Hour}})
	w := s.World
	accs := w.Accounts()
	a := accs[0]
	refs := []string{"u:" + accs[0].Username, "u:" + accs[1].Username}
	ctx := context.Background()
	srv := &ttapi.Server{Cfg: s.Cfg, DB: s.DB, Redis: s.Redis, Now: w.Clock}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	key, _, err := ttapi.CreateClient(ctx, s.DB, "reader", []string{ttapi.ScopeDataRead}, 0)
	if err != nil {
		t.Fatal(err)
	}
	api := &client{t: t, url: hs.URL, key: key}
	listed := func(status string) []map[string]any {
		var out struct {
			Posts []map[string]any `json:"posts"`
		}
		api.want(200, "GET", "/accounts/id:"+strconv.FormatInt(a.UID, 10)+"/posts?limit=100&status="+status, nil, &out)
		return out.Posts
	}

	round(t, s, "backfill", full, refs...)
	for _, acc := range accs {
		sameStore(t, s, acc, truth{})
	}

	w.Advance(2 * time.Hour)
	hidden, deleted := a.Posts[17], a.Posts[25]
	w.HidePost(hidden, 10216)
	w.DeletePost(deleted)
	round(t, s, "walk", refresh, refs...)
	// The deleted post is stored as it was, still visible (T34): the truth
	// keeps it so for the comparison.
	w.Mutate(func() { a.Hidden = append(a.Hidden, deleted) })
	sameStore(t, s, a, truth{})
	sameStore(t, s, accs[1], truth{})
	w.Mutate(func() { a.Hidden = a.Hidden[:len(a.Hidden)-1] })
	gone := listed("unavailable")
	if len(gone) != 1 || gone[0]["post_id"] != strconv.FormatInt(hidden.ID, 10) || gone[0]["status_code"] != float64(10216) ||
		gone[0]["status_region"] != "IQ" || gone[0]["unavailable_since"] == nil || gone[0]["caption"] != hidden.Desc {
		t.Fatalf("API, unavailable posts: %v", gone)
	}
	if n := len(listed("visible")); n != len(a.Posts)+1 { // the deleted one, still visible
		t.Errorf("API: %d visible posts, want %d", n, len(a.Posts)+1)
	}
	api.want(400, "GET", "/accounts/id:"+strconv.FormatInt(a.UID, 10)+"/posts?status=deleted", nil, nil)

	w.Advance(2 * time.Hour)
	w.ShowPost(hidden)
	round(t, s, "back", refresh, refs[0])
	if gone := listed("unavailable"); len(gone) != 0 {
		t.Fatalf("API: %d unavailable posts after it came back", len(gone))
	}
	if n := count(t, s, `SELECT count(*) FROM post_status_changes WHERE post_id = $1`, hidden.ID); n != 2 {
		t.Errorf("%d status changes kept, want 2 (unavailable, then visible)", n)
	}
	if n := count(t, s, `SELECT count(*) FROM post_status_changes WHERE post_id <> $1`, hidden.ID); n != 0 {
		t.Errorf("%d status changes of other posts", n)
	}
}
