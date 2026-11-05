package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scheduler"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// TestSoak runs tks's scheduled pipeline (planner, crawler, persister,
// finalizer) for TKS_SOAK_DURATION (default 20m) while fakett's world
// keeps changing and its clock runs TKS_SOAK_SPEED (default 240) times
// faster than the wall's, so every account is checked every 2 h 45 min of
// the world's time, its details refreshed daily and its metrics walked:
// new posts with comments, comments and replies on recent posts, deleted
// comments, changed counts, renamed accounts, posts hidden and shown
// again. fakett rate limits, errs on lists and refuses a signed call now
// and then, and a component restarts every TKS_SOAK_RESTART_EVERY
// (default 90s; 0 = never): the crawler (killed, its in-flight tasks
// abandoned), the persister or the finalizer. It fails on a job that does
// not complete, one running for over 10 minutes, and on goroutine growth.
// At the end, a walk of every account back a year must leave the store
// exactly like the world (A6): every post with its counts and status, every
// comment and reply, and every deleted comment marked deleted.
//
// Opt-in: TKS_SOAK=1 (make soak); TKS_SOAK_ACCOUNTS (default 6);
// TKS_SOAK_OUT writes the report; TKS_SOAK_HEAPPROFILE=prefix writes heap
// profiles after 2 minutes and at the end (prefix-base.pprof,
// prefix-end.pprof).
func TestSoak(t *testing.T) {
	if os.Getenv("TKS_SOAK") == "" {
		t.Skip("set TKS_SOAK=1 (make soak)")
	}
	duration := envDuration("TKS_SOAK_DURATION", 20*time.Minute)
	restartEvery := envDuration("TKS_SOAK_RESTART_EVERY", 90*time.Second)
	speed := envInt("TKS_SOAK_SPEED", 240)
	nAccounts := envInt("TKS_SOAK_ACCOUNTS", 6)
	s := Start(t, Options{
		Spec:         fakett.Spec{Seed: 41, Accounts: nAccounts, PostsPerAccount: 30, MaxComments: 30, MaxReplies: 5, PostSpacing: 30 * time.Hour},
		Faults:       fakett.Faults{RateLimitEvery: 23, ListErrorEvery: 17, SignerRefuseEvery: 29},
		Latency:      5 * time.Millisecond,
		CrawlerDrain: 50 * time.Millisecond,
	})
	ctx := context.Background()
	w := s.World
	accs := w.Accounts()
	sc := newSched(s)
	sc.register(t, accs...)

	rng := rand.New(rand.NewPCG(41, 1))
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
		if prefix := os.Getenv("TKS_SOAK_HEAPPROFILE"); prefix != "" {
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
	fmt.Fprintf(&report, "# Soak: %d accounts, %s (%d× clock), restart every %s\n\n"+
		"| t | world time | goroutines | heap in use MB | redis keys | jobs done | running | longest running |\n|---|---|---|---|---|---|---|---|\n",
		nAccounts, duration, speed, restartEvery)

	// touched posts changed after they were made; only untouched posts
	// without comments, already stored, are hidden, so a hidden post's
	// truth is what tks last read of it (its page then serves nothing).
	touched := map[int64]bool{}
	recent := func(a *fakett.Account) *fakett.Post {
		var p *fakett.Post
		w.Mutate(func() { p = a.Posts[rng.IntN(min(len(a.Posts), 5))] })
		touched[p.ID] = true
		return p
	}
	renames := 0
	mutate := func() {
		a := accs[rng.IntN(len(accs))]
		switch n := rng.IntN(20); {
		case n < 4:
			touched[w.AddPost(a, rng.IntN(8)).ID] = true
			mutations["new posts"]++
		case n < 8:
			w.AddComments(recent(a), 1+rng.IntN(4))
			mutations["comments on recent posts"]++
		case n < 10:
			p := recent(a)
			var c *fakett.Comment
			w.Mutate(func() {
				if len(p.Comments) > 0 {
					c = p.Comments[rng.IntN(len(p.Comments))]
				}
			})
			if c != nil {
				w.AddReplies(c, 1+rng.IntN(3))
				mutations["replies"]++
			}
		case n < 12:
			// A deletion, then more comments: the count still goes up,
			// so the post is read whole and the deletion is seen.
			p := recent(a)
			var c *fakett.Comment
			w.Mutate(func() {
				if len(p.Comments) > 2 {
					c = p.Comments[rng.IntN(len(p.Comments))]
				}
			})
			if c != nil {
				w.DeleteComment(p, c)
				w.AddComments(p, 2+rng.IntN(3))
				mutations["comment deletions"]++
			}
		case n < 16:
			w.Mutate(func() {
				for _, p := range a.Posts[:min(len(a.Posts), 10)] {
					touched[p.ID] = true
					p.Plays += int64(rng.IntN(5000))
					p.Likes += int64(rng.IntN(300))
					p.Shares += int64(rng.IntN(20))
				}
				a.Followers += int64(rng.IntN(100))
			})
			mutations["counts"]++
		case n < 17:
			renames++
			w.Rename(a, fmt.Sprintf("%s_r%d", a.Username, renames))
			mutations["renames"]++
		case n < 18:
			var hide *fakett.Post
			w.Mutate(func() {
				if len(a.Posts) > 3 && len(a.Hidden) == 0 {
					hide = a.Posts[1+rng.IntN(len(a.Posts)-1)]
				}
			})
			if hide != nil && !touched[hide.ID] && len(hide.Comments) == 0 &&
				count(t, s, `SELECT count(*) FROM posts WHERE post_id = $1`, hide.ID) == 1 {
				w.HidePost(hide, 10216)
				mutations["posts hidden"]++
			}
		case n < 19:
			var show *fakett.Post
			w.Mutate(func() {
				if len(a.Hidden) > 0 {
					show = a.Hidden[0]
				}
			})
			if show != nil {
				w.ShowPost(show)
				mutations["posts shown again"]++
			}
		default:
			w.Mutate(func() { a.Bio = fmt.Sprintf("bio %d", rng.IntN(1000)) })
			mutations["bios"]++
		}
	}

	// Jobs by when they were first seen running, on the wall's clock (the
	// jobs' own times follow the world's).
	firstSeen := map[uuid.UUID]time.Time{}
	worldStart := w.Clock()
	sampleHealth := func(elapsed time.Duration) {
		var running, done int
		var longest time.Duration
		rows, err := s.DB.Query(ctx, `SELECT job_id, kind, mode, status, coalesce(counters::text, ''), coalesce(completeness::text, '') FROM jobs`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id uuid.UUID
			var kind, mode, st, counters, comp string
			if err := rows.Scan(&id, &kind, &mode, &st, &counters, &comp); err != nil {
				t.Fatal(err)
			}
			switch st {
			case "running", "finalizing", "cancelling":
				running++
				if _, ok := firstSeen[id]; !ok {
					firstSeen[id] = time.Now()
				}
				longest = max(longest, time.Since(firstSeen[id]))
			case "completed":
				done++
			default:
				rows.Close()
				t.Fatalf("%s %s job %s ended %s (after %s): counters %s, completeness %s", kind, mode, id, st, elapsed, counters, comp)
			}
		}
		rows.Close()
		if longest > 10*time.Minute {
			t.Fatalf("a job has been running for %s", longest)
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		g := runtime.NumGoroutine()
		maxGoroutines, maxHeap = max(maxGoroutines, g), max(maxHeap, m.HeapInuse)
		if baseGoroutines == 0 && elapsed > 2*time.Minute {
			baseGoroutines, baseLive = g, liveHeap("base")
		}
		keys, _ := s.Redis.DBSize(ctx).Result()
		fmt.Fprintf(&report, "| %s | %s | %d | %.0f | %d | %d | %d | %s |\n", elapsed.Round(time.Second),
			w.Clock().Sub(worldStart).Round(time.Minute), g, float64(m.HeapInuse)/1e6, keys, done, running, longest.Round(time.Second))
	}
	finalize := func() {
		if err := sc.fin.Tick(ctx); err != nil {
			t.Logf("finalizer: %v", err)
		}
	}
	// The leader's partition upkeep, on the world's clock: daily metrics
	// partitions ahead of it (a capture without one is skipped).
	partitions := func() {
		if _, err := ttstore.EnsurePartitions(ctx, s.DB, w.Clock(),
			store.PartitionWindow{MonthsBack: 14, MonthsAhead: 1, DaysBack: 1, DaysAhead: 14}); err != nil {
			t.Fatal(err)
		}
	}

	began := time.Now()
	last := began
	nextMutation, nextRestart, nextSample := began, began.Add(restartEvery), began
	for time.Since(began) < duration {
		now := time.Now()
		w.Advance(time.Duration(speed) * now.Sub(last))
		last = now
		if err := sc.pl.Tick(ctx); err != nil {
			t.Logf("planner: %v", err)
		}
		finalize()
		if now.After(nextMutation) {
			mutate()
			nextMutation = now.Add(time.Duration(500+rng.IntN(2500)) * time.Millisecond)
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
				// A new leader's finalizer: what the old one held is
				// taken from the finalize set again.
				sc.fin = &scheduler.Finalizer{DB: s.DB, Jobs: s.Tracker, Producer: s.Producer, Cfg: s.Cfg, Timeout: time.Minute, Now: w.Clock}
				restarts["finalizer"]++
			}
			nextRestart = now.Add(restartEvery)
		}
		if now.After(nextSample) {
			partitions()
			sampleHealth(now.Sub(began))
			nextSample = now.Add(30 * time.Second)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Settle: no more checks; let running jobs finish.
	if _, err := s.DB.Exec(ctx, `UPDATE account_schedule SET enabled = false`); err != nil {
		t.Fatal(err)
	}
	settle := func(what string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Minute)
		for count(t, s, `SELECT count(*) FROM jobs WHERE status IN ('running','finalizing','cancelling')`) > 0 {
			if time.Now().After(deadline) {
				t.Fatalf("%s: jobs still running after 10 minutes", what)
			}
			finalize()
			time.Sleep(250 * time.Millisecond)
		}
	}
	settle("scheduled checks")
	sampleHealth(time.Since(began))
	// A walk of every account back a year, a week on, reconciles what the
	// checks leave for later by design (ADR 0007 §4): comments re-read
	// only once a post's comment window allows (weekly from a week old,
	// only on a walk past 30 days), posts hidden or shown again below the
	// newest page. A week on, every window's interval has passed and the
	// account's details are due again.
	w.Advance(170 * time.Hour)
	partitions()
	disp := &dispatch.Dispatcher{DB: s.DB, Jobs: s.Tracker, Producer: s.Producer, Now: w.Clock}
	for _, a := range accs {
		lim := task.DefaultLimits(s.Cfg.Limits, s.Cfg.TT, false)
		lim.MaxAgeSeconds = 365 * 86400
		if _, _, err := disp.Create(ctx, dispatch.Request{Kind: dispatch.KindOnDemand, Lane: tkstaskv1.Lane_LANE_P0, Mode: dispatch.ModeRefresh,
			AccountRef: "u:" + a.Username, Limits: lim}, 0); err != nil {
			t.Fatal(err)
		}
	}
	settle("final walks")
	sampleHealth(time.Since(began))

	// The store is the world.
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}
	if t.Failed() {
		explain(t, s, accs)
	}
	deleted := count(t, s, `SELECT count(*) FROM comments WHERE status = 'deleted'`)
	if mutations["comment deletions"] > 0 && deleted == 0 {
		t.Error("no comment marked deleted")
	}
	if n, _ := s.Redis.SCard(ctx, jobs.FinalizeSet).Result(); n != 0 {
		t.Errorf("%d jobs left in the finalize set", n)
	}
	if left, _ := s.Redis.Keys(ctx, redisx.KeyPrefix+"{job:*}:seen:*").Result(); len(left) != 0 {
		t.Errorf("%d seen lists left behind, e.g. %s", len(left), left[0])
	}
	// Goroutines must not grow with the restarts.
	endLive := liveHeap("end")
	g := runtime.NumGoroutine()
	if baseGoroutines > 0 && g > baseGoroutines*3/2+50 {
		t.Errorf("goroutines grew from %d to %d", baseGoroutines, g)
	}
	var byKind strings.Builder
	rows, err := s.DB.Query(ctx, `SELECT kind || '/' || mode, count(*) FROM jobs GROUP BY 1 ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&byKind, "%s %d, ", k, n)
	}
	rows.Close()
	fmt.Fprintf(&report, "\n- world time: %s\n- jobs: %s\n- stored: %d posts (%d unavailable), %d comments and replies (%d marked deleted), %d metrics rows\n"+
		"- TikTok requests (fakett): %d, %d signed\n- mutations: %v\n- restarts: %v\n"+
		"- goroutines: %d after 2 minutes, %d max, %d at the end; heap in use at most %.0f MB\n"+
		"- live heap after a collection: %.0f MB after 2 minutes, %.0f MB at the end\n",
		w.Clock().Sub(worldStart).Round(time.Minute), strings.TrimSuffix(byKind.String(), ", "),
		count(t, s, `SELECT count(*) FROM posts`), count(t, s, `SELECT count(*) FROM posts WHERE status = 'unavailable'`),
		count(t, s, `SELECT count(*) FROM comments`), deleted, count(t, s, `SELECT count(*) FROM post_metrics`),
		s.Requests.Load(), s.Signer.Calls.Load(), mutations, restarts, baseGoroutines, maxGoroutines, g, float64(maxHeap)/1e6,
		float64(baseLive)/1e6, float64(endLive)/1e6)
	t.Log("\n" + report.String())
	if out := os.Getenv("TKS_SOAK_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(report.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// explain logs, for each post whose visible comments differ from the
// world's, what tks knows of it: the stored counts and comment state, the
// crawler's post cache (the count its last read was planned on, when, and
// whether its threads were complete) and the incomplete mark.
func explain(t *testing.T, s *Stack, accs []*fakett.Account) {
	t.Helper()
	ctx := context.Background()
	ck := checkpoint.New(s.Redis)
	for _, a := range accs {
		for _, p := range a.Posts {
			want := 0
			for _, c := range p.Comments {
				want += 1 + len(c.Replies)
			}
			got := count(t, s, `SELECT count(*) FROM comments WHERE post_id = $1 AND status = 'visible'`, p.ID)
			if got == int64(want) {
				continue
			}
			var stored int64
			var state string
			_ = s.DB.QueryRow(ctx, `SELECT comment_count, coalesce(comments_state, '') FROM posts WHERE post_id = $1`, p.ID).Scan(&stored, &state)
			pc, read := ck.GetPost(ctx, p.ID)
			t.Logf("post %d (%s, made %s): %d comments visible, world %d (count %d); stored count %d, state %q; cache %+v (read %v, at %s); incomplete %q",
				p.ID, a.Username, p.Created.Format(time.RFC3339), got, want, p.CommentCount(), stored, state, pc, read,
				time.Unix(pc.ReadAt, 0).UTC().Format(time.RFC3339), ck.Incomplete(ctx, p.ID))
		}
	}
	t.Logf("world now %s", s.World.Clock().Format(time.RFC3339))
}
