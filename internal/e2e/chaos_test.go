package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
)

// TestChaosRestartsAndFaults crawls several pages while Facebook rate
// limits and the crawler (abandoning its in-flight tasks, like a killed
// pod), the persister and the finalizer (a scheduler leader change) are
// restarted at random. Every job must still complete, and the store must
// hold exactly the fakefb world: every post and comment, nothing extra, and
// no comment marked deleted. A second round of full jobs over the same pages
// then finds no change.
func TestChaosRestartsAndFaults(t *testing.T) {
	s := Start(t, Options{
		Spec: fakefb.Spec{Seed: 31, Pages: 3, PostsPerPage: 8, PostSpacing: 20 * time.Hour, MaxComments: 50,
			ReplyProbability: 0.3, MaxReplies: 8, Depth2Prob: 0.2, AttachmentProb: 0.1},
		Faults:       fakefb.Faults{RateLimitEvery: 11, Latency: 20 * time.Millisecond},
		CrawlerDrain: 20 * time.Millisecond,
		// Few requests per second per page, so the jobs run through the
		// restarts on any machine.
		Config: func(c *config.Config) { c.FB.PageRPS, c.FB.PageRPSP0 = 5, 5 },
	})
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	pages := s.Fake.World.Pages()
	rng := rand.New(rand.NewPCG(31, 7))

	round := func(name string) []jobView {
		ids := make([]string, len(pages))
		for i, p := range pages {
			j := decodeBody[jobView](t, s.Do(t, key, "POST", "/v1/jobs", []byte(fmt.Sprintf(`{"page":%q,"mode":"full"}`, p.Vanity))),
				http.StatusAccepted)
			ids[i] = j.ID
		}
		// Chaos while the jobs run: a bounded number of restarts, then the
		// pipeline must settle on its own.
		restarts := map[string]int{}
		budget := map[string]int{"crawler": 6, "persister": 3, "finalizer": 3}
		views := make([]jobView, len(ids))
		finished := func() int {
			done := 0
			for i, id := range ids {
				v := decodeBody[jobView](t, s.Do(t, key, "GET", "/v1/jobs/"+id, nil), http.StatusOK)
				views[i] = v
				switch v.Status {
				case "completed", "partial", "failed", "cancelled":
					done++
				}
			}
			return done
		}
		for finished() < len(ids) && restarts["crawler"]+restarts["persister"]+restarts["finalizer"] < 12 {
			time.Sleep(time.Duration(800+rng.IntN(1200)) * time.Millisecond)
			var what string
			for what == "" || restarts[what] >= budget[what] {
				what = []string{"crawler", "crawler", "persister", "finalizer"}[rng.IntN(4)]
			}
			switch what {
			case "crawler":
				s.RestartCrawler(t)
			case "persister":
				s.RestartPersister(t)
			case "finalizer":
				s.RestartFinalizer()
			}
			restarts[what]++
		}
		if restarts["crawler"] < 3 {
			t.Fatalf("%s: the jobs finished after %v restarts; the test needs longer jobs", name, restarts)
		}
		deadline := time.Now().Add(3 * time.Minute)
		for finished() < len(ids) {
			if time.Now().After(deadline) {
				for _, v := range views {
					st, _ := s.tracker.Status(context.Background(), v.ID)
					queued, _ := s.Redis.SIsMember(context.Background(), jobs.FinalizeSet, v.ID).Result()
					t.Logf("job %s %s: pending %d, spawned %d, done %d, queued for finalising %v, counters %v",
						v.ID, v.Status, st.Pending, st.Spawned, st.Done, queued, st.Counters)
				}
				t.Fatalf("%s: jobs did not finish 3 minutes after the chaos ended; restarts %v", name, restarts)
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Logf("%s: restarts %v", name, restarts)
		for i, v := range views {
			t.Logf("%s: job %s %s, counters %v", name, v.ID, v.Status, v.Counters)
			if v.Status != "completed" {
				t.Fatalf("%s: job %s ended %s", name, v.ID, v.Status)
			}
			// Counters survive retries and redeliveries (a page read again
			// after a crash may count twice, never zero times).
			truth, _ := s.Fake.World.TruthFor(pages[i].ID, time.Time{})
			top, all := 0, 0
			for _, p := range truth.Posts {
				top += p.TopLevel
				all += p.TotalComments
			}
			if c := v.Counters; c["comments"] < int64(top) || c["comments"]+c["replies"] < int64(all) || c["posts"] < int64(len(truth.Posts)) {
				t.Fatalf("%s: job %s counted %d posts, %d comments, %d replies; the page has %d posts, %d comments, %d with replies",
					name, v.ID, c["posts"], c["comments"], c["replies"], len(truth.Posts), top, all)
			}
		}
		return views
	}

	exact := func(name string) {
		t.Helper()
		ctx := context.Background()
		for _, p := range pages {
			truth, _ := s.Fake.World.TruthFor(p.ID, time.Time{})
			var wantPosts, wantComments []int64
			for _, pt := range truth.Posts {
				wantPosts = append(wantPosts, pt.PostID)
				wantComments = append(wantComments, pt.CommentIDs...)
			}
			gotPosts := ids(t, s, ctx, `SELECT post_id FROM posts WHERE page_id = $1`, p.ID)
			gotComments := ids(t, s, ctx, `SELECT comment_id FROM comments WHERE page_id = $1`, p.ID)
			if d := diff(wantPosts, gotPosts); d != "" {
				t.Fatalf("%s: page %d posts: %s", name, p.ID, d)
			}
			if d := diff(wantComments, gotComments); d != "" {
				t.Fatalf("%s: page %d comments: %s", name, p.ID, d)
			}
		}
		if n := count(t, s, `SELECT count(*) FROM comments WHERE status <> 'visible' OR edit_count > 0`); n != 0 {
			t.Fatalf("%s: %d comments marked deleted or edited", name, n)
		}
		if n := count(t, s, `SELECT count(*) FROM posts WHERE completeness < 1`); n != 0 {
			t.Fatalf("%s: %d posts below full completeness", name, n)
		}
	}

	round("first round")
	exact("first round")
	for _, v := range round("second round") {
		if v.Counters["comments_deleted"] != 0 {
			t.Fatalf("second round: job %s marked %d comments deleted", v.ID, v.Counters["comments_deleted"])
		}
	}
	exact("second round")
}

func ids(t *testing.T, s *Stack, ctx context.Context, q string, args ...any) []int64 {
	t.Helper()
	rows, err := s.DB.Query(ctx, q, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// diff describes how got differs from want as sets (and repeats in got).
func diff(want, got []int64) string {
	w := map[int64]bool{}
	for _, id := range want {
		w[id] = true
	}
	g := map[int64]int{}
	for _, id := range got {
		g[id]++
	}
	var missing, extra, dup []int64
	for id := range w {
		if g[id] == 0 {
			missing = append(missing, id)
		}
	}
	for id, n := range g {
		if !w[id] {
			extra = append(extra, id)
		}
		if n > 1 {
			dup = append(dup, id)
		}
	}
	if len(missing)+len(extra)+len(dup) == 0 {
		return ""
	}
	for _, l := range [][]int64{missing, extra, dup} {
		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	}
	return fmt.Sprintf("%d missing %v, %d extra %v, %d repeated %v (want %d, got %d)",
		len(missing), head(missing), len(extra), head(extra), len(dup), head(dup), len(w), len(got))
}

func head(l []int64) []int64 { return l[:min(len(l), 5)] }
