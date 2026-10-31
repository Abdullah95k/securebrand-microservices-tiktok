package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// TestSoak runs the scheduled pipeline for IGS_SOAK_DURATION (default 20m)
// while the fakefb world keeps changing (new posts and comments, edits,
// deletions) and a component restarts every IGS_SOAK_RESTART_EVERY (default
// 90s; 0 = never). Short refresh windows and recrawl ages make post
// refreshes and recrawls run along the way. It fails on a stuck or failed
// job and on goroutine growth. At the end, a full job per page must leave
// the store exactly like the world: every comment visible with its current
// text, and every deleted one marked deleted. Opt-in: IGS_SOAK=1
// (make soak); IGS_SOAK_PAGES (default 6); IGS_SOAK_OUT writes the report;
// IGS_SOAK_HEAPPROFILE=prefix writes heap profiles after 2 minutes and at
// the end (prefix-base.pprof, prefix-end.pprof).
func TestSoak(t *testing.T) {
	if os.Getenv("IGS_SOAK") == "" {
		t.Skip("set IGS_SOAK=1 (make soak)")
	}
	duration := envDuration("IGS_SOAK_DURATION", 20*time.Minute)
	restartEvery := envDuration("IGS_SOAK_RESTART_EVERY", 90*time.Second)
	nPages := envInt("IGS_SOAK_PAGES", 6)
	s := Start(t, Options{
		Spec: fakefb.Spec{Seed: 41, Pages: nPages, PostsPerPage: 10, PostSpacing: 20 * time.Hour, MaxComments: 40,
			ReplyProbability: 0.25, MaxReplies: 6, Depth2Prob: 0.15, AttachmentProb: 0.1},
		Faults:       fakefb.Faults{RateLimitEvery: 23, Latency: 10 * time.Millisecond},
		CrawlerDrain: 50 * time.Millisecond,
		Config: func(c *config.Config) {
			c.Scheduler.PostRefresh.Windows = []config.RefreshWindow{{MaxAge: 2000 * time.Hour, Interval: 90 * time.Second}}
			c.Scheduler.PostRefresh.RecrawlAt = []time.Duration{60 * time.Hour, 120 * time.Hour, 170 * time.Hour}
			c.Scheduler.PostRefresh.RecrawlMinComments = 5
		},
	})
	ctx := context.Background()
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead, api.ScopePagesWrite)
	pages := s.Fake.World.Pages()
	for _, p := range pages {
		body := fmt.Sprintf(`{"page":%q,"schedule":{"interval_hours":1}}`, p.Vanity)
		resp := s.Do(t, key, "POST", "/v1/pages", []byte(body))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("register %s: %d", p.Vanity, resp.StatusCode)
		}
	}
	// Re-scrape every 30s instead of hourly.
	if _, err := s.DB.Exec(ctx, `UPDATE page_schedule SET interval_seconds = 30, adaptive = false,
		next_run_at = now() + random() * interval '30 seconds'`); err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewPCG(41, 1))
	deleted := map[int64]bool{} // deleted comments and their replies
	edited := map[int64]string{}
	mutations := map[string]int{}
	restarts := map[string]int{}
	var maxGoroutines, baseGoroutines int
	var maxHeap, baseLive uint64
	// liveHeap is the heap left after a full collection; heap profiles are
	// written after one too, so they show what is retained.
	liveHeap := func(profile string) uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if prefix := os.Getenv("IGS_SOAK_HEAPPROFILE"); prefix != "" {
			f, err := os.Create(prefix + "-" + profile + ".pprof")
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}
		return m.HeapAlloc
	}
	var report strings.Builder
	fmt.Fprintf(&report, "# Soak: %d pages, %s, restart every %s\n\n| t | goroutines | heap in use MB | redis keys | jobs done | running | oldest running |\n|---|---|---|---|---|---|---|\n",
		nPages, duration, restartEvery)

	mutate := func() {
		p := pages[rng.IntN(len(pages))]
		post := p.Posts[rng.IntN(len(p.Posts))]
		switch n := rng.IntN(10); {
		case n < 3:
			if err := s.Fake.World.Mutate(p.ID, rng.IntN(2), 1+rng.IntN(4)); err != nil {
				t.Fatal(err)
			}
			mutations["new posts and comments"]++
		case n < 6:
			if err := s.Fake.World.AddComments(post.ID, 1+rng.IntN(4)); err != nil {
				t.Fatal(err)
			}
			mutations["comments on older posts"]++
		case n < 8:
			if len(post.Comments) == 0 {
				return
			}
			c := post.Comments[rng.IntN(len(post.Comments))]
			text := fmt.Sprintf("edited %d: %s", mutations["edits"]+1, c.Text)
			if err := s.Fake.World.EditComment(c.ID, text); err != nil {
				t.Fatal(err)
			}
			edited[c.ID] = text
			mutations["edits"]++
		default:
			// Keep at least two comments so the post is still read in full.
			if len(post.Comments) < 3 {
				return
			}
			c := post.Comments[rng.IntN(len(post.Comments))]
			var walk func(c *fakefb.Comment)
			walk = func(c *fakefb.Comment) {
				deleted[c.ID] = true
				delete(edited, c.ID)
				for _, r := range c.Replies {
					walk(r)
				}
			}
			walk(c)
			if err := s.Fake.World.DeleteComment(c.ID); err != nil {
				t.Fatal(err)
			}
			mutations["deletions"]++
		}
	}
	sampleHealth := func(elapsed time.Duration) {
		var running, done int
		var oldest time.Duration
		rows, err := s.DB.Query(ctx, `SELECT job_id::text, kind, status, now() - created_at, coalesce(counters::text, ''),
			coalesce(completeness::text, ''), coalesce(error::text, '') FROM jobs`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id, kind, st, counters, comp, jerr string
			var age time.Duration
			if err := rows.Scan(&id, &kind, &st, &age, &counters, &comp, &jerr); err != nil {
				t.Fatal(err)
			}
			switch st {
			case "running", "finalizing", "cancelling":
				running++
				oldest = max(oldest, age)
			case "failed", "partial", "cancelled":
				rows.Close()
				t.Fatalf("%s job %s ended %s (after %s): counters %s, completeness %s, error %s", kind, id, st, elapsed, counters, comp, jerr)
			default:
				done++
			}
		}
		rows.Close()
		if oldest > 10*time.Minute {
			t.Fatalf("a job has been running for %s", oldest)
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		g := runtime.NumGoroutine()
		maxGoroutines, maxHeap = max(maxGoroutines, g), max(maxHeap, m.HeapInuse)
		if baseGoroutines == 0 && elapsed > 2*time.Minute {
			baseGoroutines, baseLive = g, liveHeap("base")
		}
		keys, _ := s.Redis.DBSize(ctx).Result()
		fmt.Fprintf(&report, "| %s | %d | %.0f | %d | %d | %d | %s |\n", elapsed.Round(time.Second), g, float64(m.HeapInuse)/1e6,
			keys, done, running, oldest.Round(time.Second))
	}

	start := time.Now()
	nextMutation, nextRestart, nextSample := start, start.Add(restartEvery), start
	for time.Since(start) < duration {
		if err := s.Planner.Tick(ctx); err != nil {
			t.Logf("planner: %v", err)
		}
		now := time.Now()
		if now.After(nextMutation) {
			mutate()
			nextMutation = now.Add(time.Duration(1+rng.IntN(4)) * time.Second)
		}
		if restartEvery > 0 && now.After(nextRestart) {
			switch rng.IntN(4) {
			case 0, 1:
				s.RestartCrawler(t)
				restarts["crawler"]++
			case 2:
				s.RestartPersister(t)
				restarts["persister"]++
			default:
				s.RestartFinalizer()
				restarts["finalizer"]++
			}
			nextRestart = now.Add(restartEvery)
		}
		if now.After(nextSample) {
			sampleHealth(now.Sub(start))
			nextSample = now.Add(30 * time.Second)
		}
		time.Sleep(time.Second)
	}

	// Settle: no more schedules; let running jobs finish.
	if _, err := s.DB.Exec(ctx, `UPDATE page_schedule SET enabled = false`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "running jobs to finish", 10*time.Minute, func() bool {
		return count(t, s, `SELECT count(*) FROM jobs WHERE status IN ('running','finalizing','cancelling')`) == 0
	})
	sampleHealth(time.Since(start))
	// One full read of every page reconciles what the scheduled jobs could
	// not see (comments on posts outside the refresh windows).
	for _, p := range pages {
		if j := s.runJob(t, key, p.Vanity, "full"); j.Status != "completed" {
			t.Fatalf("final full job of %s: %+v", p.Vanity, j)
		}
	}
	sampleHealth(time.Since(start))

	// The store is the world.
	var stored, visible, markedDeleted, counted int
	for _, p := range pages {
		want := map[int64]string{}
		var walk func(cs []*fakefb.Comment)
		walk = func(cs []*fakefb.Comment) {
			for _, c := range cs {
				want[c.ID] = c.Text
				walk(c.Replies)
			}
		}
		for _, post := range p.Posts {
			walk(post.Comments)
		}
		rows, err := s.DB.Query(ctx, `SELECT comment_id, coalesce(text, ''), status, edit_count FROM comments WHERE page_id = $1`, p.ID)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[int64]bool{}
		for rows.Next() {
			var id int64
			var text, status string
			var edits int
			if err := rows.Scan(&id, &text, &status, &edits); err != nil {
				t.Fatal(err)
			}
			seen[id] = true
			stored++
			wantText, live := want[id]
			switch {
			case live && status != "visible":
				t.Fatalf("comment %d is %s but still on Facebook", id, status)
			case live && text != wantText:
				t.Fatalf("comment %d text %q, want %q", id, text, wantText)
			case !live && (!deleted[id] || status != "deleted"):
				t.Fatalf("comment %d is gone from Facebook (deleted by the test: %v) but %s in the store", id, deleted[id], status)
			}
			if live {
				visible++
			} else {
				markedDeleted++
			}
			if _, ok := edited[id]; ok && edits > 0 {
				counted++
			}
		}
		rows.Close()
		for id := range want {
			if !seen[id] {
				t.Fatalf("page %s: comment %d was never stored", p.Vanity, id)
			}
		}
	}
	// Edits of comments stored before the edit are counted; an edit before
	// the comment was first stored is not an edit for us.
	if counted == 0 && len(edited) > 0 {
		t.Fatalf("none of %d edited comments has an edit counted", len(edited))
	}
	if n, _ := s.Redis.SCard(ctx, jobs.FinalizeSet).Result(); n != 0 {
		t.Fatalf("%d jobs left in the finalize set", n)
	}
	if left, _ := s.Redis.Keys(ctx, redisx.KeyPrefix+"{job:*}:seen:*").Result(); len(left) != 0 {
		t.Fatalf("%d seen lists left behind, e.g. %s", len(left), left[0])
	}
	// Goroutines must not grow with the restarts.
	endLive := liveHeap("end")
	g := runtime.NumGoroutine()
	if baseGoroutines > 0 && g > baseGoroutines*3/2+50 {
		t.Fatalf("goroutines grew from %d to %d", baseGoroutines, g)
	}
	jobsRun := count(t, s, `SELECT count(*) FROM jobs`)
	fmt.Fprintf(&report, "\n- jobs: %d; comments stored %d (%d visible, %d marked deleted)\n- mutations: %v\n- restarts: %v\n"+
		"- edited comments: %d, with an edit counted: %d\n- goroutines: %d after 2 minutes, %d max, %d at the end; heap in use at most %.0f MB\n"+
		"- live heap after a collection: %.0f MB after 2 minutes, %.0f MB at the end\n",
		jobsRun, stored, visible, markedDeleted, mutations, restarts, len(edited), counted, baseGoroutines, maxGoroutines, g, float64(maxHeap)/1e6,
		float64(baseLive)/1e6, float64(endLive)/1e6)
	t.Log("\n" + report.String())
	if out := os.Getenv("IGS_SOAK_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(report.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
