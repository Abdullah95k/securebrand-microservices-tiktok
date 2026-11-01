package stage

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
)

// Only a saved checkpoint that moved counts as progress.
func TestAdvanced(t *testing.T) {
	at := checkpoint.State{Cursor: "c1", Pages: 2}
	for _, tc := range []struct {
		after checkpoint.State
		want  bool
	}{
		{at, false},
		{checkpoint.State{Cursor: "c2", Pages: 3}, true},
		{checkpoint.State{Cursor: "c1", Pages: 3}, true},
		{checkpoint.State{Cursor: "c2", Pages: 2}, true},
	} {
		if got := advanced(at, tc.after); got != tc.want {
			t.Errorf("advanced(%+v, %+v) = %v", at, tc.after, got)
		}
	}
	if advanced(checkpoint.State{}, checkpoint.State{}) {
		t.Error("a stream without a checkpoint never advances")
	}
}

// Stalls (retries without progress) hit max_attempts; a task that keeps
// advancing still stops at the total cap.
func TestExhausted(t *testing.T) {
	w := &Worker{MaxAttempts: 5}
	for _, tc := range []struct {
		attempt, stalls uint32
		want            string
	}{
		{4, 4, ""},
		{5, 5, "max_attempts"},
		{30, 0, ""}, // advancing: retries go on
		{49, 4, ""}, // under both caps
		{50, 0, "max_total_attempts"},
	} {
		if got := w.exhausted(&taskv1.Task{Attempt: tc.attempt, Stalls: tc.stalls}); got != tc.want {
			t.Errorf("attempt %d stalls %d: %q, want %q", tc.attempt, tc.stalls, got, tc.want)
		}
	}
	w.MaxTotalAttempts = 8
	if got := w.exhausted(&taskv1.Task{Attempt: 8}); got != "max_total_attempts" {
		t.Errorf("configured total cap: %q", got)
	}
	// Waits for proxy exits have their own cap, apart from attempts; unset,
	// it is defaultMaxWaits.
	if got := w.exhausted(&taskv1.Task{Attempt: 2, Waits: defaultMaxWaits}); got != "max_waits" {
		t.Errorf("default waits cap: %q", got)
	}
	w.MaxWaits = 480
	if got := w.exhausted(&taskv1.Task{Attempt: 2, Waits: 479}); got != "" {
		t.Errorf("waits under their cap: %q", got)
	}
	if got := w.exhausted(&taskv1.Task{Attempt: 2, Waits: 480}); got != "max_waits" {
		t.Errorf("waits at their cap: %q", got)
	}
}

// A retried attempt adds what it cost to its job, not what it collected
// (the retry collects that again).
func TestCostCounters(t *testing.T) {
	got := costCounters(map[string]int64{"requests": 3, "wire_bytes": 9000, "blocked": 1, "posts": 10, "comments": 40, "drift": 0})
	if len(got) != 3 || got["requests"] != 3 || got["wire_bytes"] != 9000 || got["blocked"] != 1 {
		t.Fatalf("cost counters: %v", got)
	}
	if len(costCounters(nil)) != 0 {
		t.Fatal("nil counters")
	}
}

// Only the work after the last saved checkpoint is added on a retry: the
// saved counters resume into the retry and are reported when it finishes.
func TestUnsaved(t *testing.T) {
	got := unsaved(map[string]int64{"requests": 8, "wire_bytes": 9000, "blocked": 1}, map[string]int64{"requests": 6, "wire_bytes": 7000, "posts": 30})
	if len(got) != 3 || got["requests"] != 2 || got["wire_bytes"] != 2000 || got["blocked"] != 1 {
		t.Fatalf("unsaved: %v", got)
	}
	if len(unsaved(map[string]int64{"requests": 6}, map[string]int64{"requests": 6})) != 0 {
		t.Fatal("nothing new")
	}
}

func TestProfileFresh(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	cfg := &config.Config{}
	cfg.Crawler.ProfileRefresh = 24 * time.Hour
	d := &Deps{Cfg: cfg, Now: func() time.Time { return now }}
	at := func(ago time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(-ago)) }
	for name, c := range map[string]struct {
		sc   *taskv1.PageScope
		want bool
	}{
		"read an hour ago":  {&taskv1.PageScope{PageId: 7, ProfileSeenAt: at(time.Hour)}, true},
		"read two days ago": {&taskv1.PageScope{PageId: 7, ProfileSeenAt: at(48 * time.Hour)}, false},
		"never read":        {&taskv1.PageScope{PageId: 7}, false},
		"page id not known": {&taskv1.PageScope{ProfileSeenAt: at(time.Hour)}, false},
	} {
		if got := d.profileFresh(c.sc); got != c.want {
			t.Errorf("%s: %v", name, got)
		}
	}
	cfg.Crawler.ProfileRefresh = 0
	if d.profileFresh(&taskv1.PageScope{PageId: 7, ProfileSeenAt: at(time.Minute)}) {
		t.Error("profile_refresh 0 reads the HTML on every run")
	}
}

// A task that got no identity because every exit is cooling down waits 5
// minutes, and the wait does not count toward its stalls; other failures to
// get an identity retry soon and do count.
func TestNoIdentityWaitsForExits(t *testing.T) {
	tk := &taskv1.Task{TaskId: "t", Attempt: 2, Stalls: 3}
	o := noIdentity(tk, proxy.ErrTierOpen, map[string]int64{})
	if o.Result != Retry || o.Delay != "5m" || !o.Waiting || o.Next.Attempt != 3 {
		t.Fatalf("every exit cooling down: %+v", o)
	}
	if got := stallsAfter(tk.Stalls, false, o.Waiting); got != 3 {
		t.Fatalf("a wait counted as a stall: %d", got)
	}
	o = noIdentity(tk, errors.New("dial"), map[string]int64{})
	if o.Delay != "30s" || o.Waiting {
		t.Fatalf("other errors: %+v", o)
	}
	if got := stallsAfter(tk.Stalls, false, o.Waiting); got != 4 {
		t.Fatalf("a stall not counted: %d", got)
	}
	o = noIdentity(tk, proxy.ErrExitsPaced, map[string]int64{})
	if o.Delay != "30s" || !o.Waiting {
		t.Fatalf("exits pacing their page loads: %+v", o)
	}
	if got := stallsAfter(tk.Stalls, true, false); got != 0 {
		t.Fatalf("progress resets the stalls: %d", got)
	}
}

// pagePlan: the HTML loads when the identity needs tokens or the page id
// is unknown, and for a stale profile on the first segment before the
// timeline started. (The exit's page-load gate is Loader.Load's.)
func TestPagePlan(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	cfg := &config.Config{}
	cfg.Crawler.ProfileRefresh = 24 * time.Hour
	d := &Deps{Cfg: cfg, Now: func() time.Time { return now }}
	withTokens := &session.Identity{}
	withTokens.SetTokens(session.Tokens{"lsd": "x"})
	noTokens := &session.Identity{}
	fresh := &taskv1.PageScope{PageId: 7, ProfileSeenAt: timestamppb.New(now.Add(-time.Hour))}
	stale := &taskv1.PageScope{PageId: 7, ProfileSeenAt: timestamppb.New(now.Add(-48 * time.Hour))}
	unknown := &taskv1.PageScope{PageRef: "v:somebrand"}
	first, later := &taskv1.Task{Seq: 0}, &taskv1.Task{Seq: 1}
	start, progressed := &checkpoint.State{}, &checkpoint.State{Cursor: "c1", Pages: 2}
	c := map[string]int64{}
	for name, tc := range map[string]struct {
		tk         *taskv1.Task
		sc         *taskv1.PageScope
		st         *checkpoint.State
		id         *session.Identity
		load, prof bool
	}{
		"later segment":           {later, stale, start, withTokens, false, false},
		"fresh profile":           {first, fresh, start, withTokens, false, false},
		"retry after progress":    {first, stale, progressed, withTokens, false, false},
		"stale profile":           {first, stale, start, withTokens, true, true},
		"unknown page id":         {later, unknown, progressed, withTokens, true, true},
		"identity without tokens": {later, stale, progressed, noTokens, true, false},
		"fresh, without tokens":   {first, fresh, start, noTokens, true, false},
	} {
		load, prof := d.pagePlan(tc.tk, tc.sc, tc.st, tc.id, c)
		if load != tc.load || prof != tc.prof {
			t.Errorf("%s: load=%v profile=%v, want %v %v", name, load, prof, tc.load, tc.prof)
		}
	}
	if c["profile_skips"] != 2 {
		t.Fatalf("two fresh profiles skipped: %v", c)
	}
	// A load the task needs waits 30s while its exit paces page loads, 5m
	// while it backs off after a walled one.
	tk := &taskv1.Task{TaskId: "t"}
	if o := pacedRetry(tk, "", session.ErrPaced, map[string]int64{}); o.Delay != "30s" || !o.Waiting {
		t.Fatalf("paced: %+v", o)
	}
	if o := pacedRetry(tk, "", session.ErrExitPaused, map[string]int64{}); o.Delay != "5m" || !o.Waiting {
		t.Fatalf("backing off: %+v", o)
	}
}

// A task escalates one tier per stall; waits for exits are not stalls, so a
// waiting task stays on the cheap tier. Tiers whose exits are all cooling
// down are skipped.
func TestTierForEscalatesByStalls(t *testing.T) {
	pool, err := proxy.New(config.Proxy{
		Tiers: []config.ProxyTier{
			{Name: "isp", Kind: "static_list", Endpoints: []string{"http://isp.proxy.example:10001"}},
			{Name: "res", Kind: "gateway", Gateway: "http://gw.example:7777", Country: "us"},
		},
		Routing: map[string][]string{"default": {"isp", "res"}},
		Breaker: breaker.Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := &Deps{Proxies: pool}
	if got := d.tierFor("comments", 0); got != "isp" {
		t.Fatalf("no stalls: %s", got)
	}
	if got := d.tierFor("comments", 1); got != "res" {
		t.Fatalf("one stall: %s", got)
	}
	if got := d.tierFor("comments", 9); got != "res" {
		t.Fatalf("past the last tier: %s", got)
	}
	ep := proxy.Endpoint{Tier: "isp", Bucket: "isp|isp.proxy.example:10001"}
	pool.Observe(ep, detect.RateLimited)
	pool.Observe(ep, detect.RateLimited)
	if got := d.tierFor("comments", 0); got != "res" {
		t.Fatalf("isp cooling down: %s", got)
	}
}

// Only a task the job never spawned is let through; any other failure to
// record a task done is retried, or the job would hang.
func TestIgnoreUnspawned(t *testing.T) {
	if err := ignoreUnspawned(fmt.Errorf("%w: task x", jobs.ErrNeverSpawned)); err != nil {
		t.Fatalf("never spawned: %v", err)
	}
	if err := ignoreUnspawned(errors.New("dial tcp: connection refused")); err == nil {
		t.Fatal("a Redis error was swallowed")
	}
	if ignoreUnspawned(nil) != nil {
		t.Fatal("nil")
	}
}

func TestPassNewest(t *testing.T) {
	st := checkpoint.State{NewestSeenUnix: 1700000000}
	inc, full := fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL, fbv1.CrawlMode_CRAWL_MODE_FULL
	if got := passNewest(st, inc, false); got.Unix() != 1700000000 {
		t.Errorf("a whole incremental pass: %v", got)
	}
	if got := passNewest(st, inc, true); !got.IsZero() {
		t.Errorf("an incremental pass cut short must leave the watermark: %v", got)
	}
	if got := passNewest(st, full, true); got.Unix() != 1700000000 {
		t.Errorf("a full pass cut short still records what it saw: %v", got)
	}
	if got := passNewest(checkpoint.State{}, full, false); !got.IsZero() {
		t.Errorf("nothing seen: %v", got)
	}
}
