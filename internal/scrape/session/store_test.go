package session

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
)

var storeExits = []string{"http://u:secret@203.0.113.10:8000", "http://u:secret@198.51.100.10:8000", "http://u:secret@192.0.2.10:8000"}

func storePool(t *testing.T, store Store, endpoints ...string) (*Pool, *proxy.Pool) {
	t.Helper()
	proxies, err := proxy.New(config.Proxy{
		Tiers:     []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: endpoints}},
		PageLoads: config.PageLoads{Interval: 2 * time.Minute, Backoff: 15 * time.Minute, MaxBackoff: 2 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.facebook.com", Profile: "chrome_146", IdentityRPS: 1, IdentityBurst: 1,
		MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 8, Store: store}, proxies,
		fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	return pool, proxies
}

// leave stores a stopped crawler's snapshot, free to take.
func leave(t *testing.T, store *RedisStore, owner string, snap Snapshot) {
	t.Helper()
	ctx := context.Background()
	snap.Owner = owner
	if err := store.Checkpoint(ctx, owner, snap, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(ctx, owner); err != nil {
		t.Fatal(err)
	}
}

// idleByID finds a resumed identity.
func idleByID(p *Pool, tier, id string) *Identity {
	for _, i := range p.idle[tier] {
		if i.ID == id {
			return i
		}
	}
	return nil
}

// A crawler that stops saves its sessions (idle or in use) and its exits'
// page-load pauses; the next one resumes them: the same session on the same
// exit, with its tokens and cookies, and no page load. Retired sessions and
// sessions without tokens are not saved, and the proxy's credentials never
// are.
func TestSessionsResumeAfterARestart(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	base, _ := url.Parse("https://www.facebook.com")

	before, _ := storePool(t, store, storeExits...)
	good, _ := before.Acquire(ctx, "isp")
	walled, _ := before.Acquire(ctx, "isp")
	busy, err := before.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	tokens := Tokens{"lsd": "lsd-1", "fb_dtsg": "dtsg-1"}
	good.SetTokens(tokens)
	good.Doer.SetCookie(base, "datr", "datr-1")
	good.NextReq()
	Outcome(good, detect.OK)
	walled.SetTokens(Tokens{"lsd": "lsd-2"})
	PageOutcome(walled, detect.LoginWall)  // retired, and its exit pauses page loads
	busy.SetTokens(Tokens{"lsd": "lsd-3"}) // still in use when the crawler stops
	before.Release(good)
	before.Release(walled)
	if len(before.all) != 2 {
		t.Fatalf("a retired identity is still tracked: %d", len(before.all))
	}
	if n, err := before.Save(ctx); err != nil || n != 2 {
		t.Fatalf("saved %d: %v", n, err)
	}
	if before.live["isp"] != 0 || len(before.idle["isp"]) != 0 || len(before.all) != 0 {
		t.Fatalf("saved identities stay in the pool: live=%d idle=%d", before.live["isp"], len(before.idle["isp"]))
	}
	if strings.Contains(mr.Dump(), "secret") {
		t.Fatal("the proxy's credentials are saved")
	}

	after, proxies := storePool(t, store, storeExits...)
	if n, err := after.Resume(ctx); err != nil || n != 2 {
		t.Fatalf("resumed %d: %v", n, err)
	}
	if mr.Exists(store.snapKey(before.owner)) {
		t.Fatal("a taken snapshot (cookies, tokens) stays in Redis")
	}
	got := idleByID(after, "isp", good.ID)
	if got == nil || idleByID(after, "isp", busy.ID) == nil || after.live["isp"] != 2 || len(after.all) != 2 {
		t.Fatalf("resumed %d sessions, live %d", len(after.idle["isp"]), after.live["isp"])
	}
	if got.Endpoint != good.Endpoint || !maps.Equal(got.Tokens(), tokens) || got.SessionParam() != good.SessionParam() {
		t.Fatalf("resumed on %s with %+v, want %s", got.Endpoint.Exit, got.Tokens(), good.Endpoint.Exit)
	}
	if c := got.Doer.Cookie(base, "datr"); c != "datr-1" {
		t.Fatalf("cookie %q", c)
	}
	if got.NextReq() != "2" || got.requests != 1 || got.maxRequests != good.maxRequests {
		t.Fatalf("the session's counters restart: requests=%d max=%d", got.requests, got.maxRequests)
	}
	if got.TakeReservation() {
		t.Fatal("a resumed session holds a page load")
	}
	if ok, why := proxies.PageLoad(walled.Endpoint); ok || why != "backoff" {
		t.Fatalf("the walled exit loads a page after a restart: %v %q", ok, why)
	}
	if ok, why := proxies.PageLoad(busy.Endpoint); ok || why != "paced" {
		t.Fatalf("an exit that just loaded a page loads another after a restart: %v %q", ok, why)
	}
	// Its outcomes reach its exit like any identity's.
	PageOutcome(got, detect.LoginWall)
	if ok, why := proxies.PageLoad(got.Endpoint); ok || why != "backoff" {
		t.Fatalf("a resumed session's walled page load leaves its exit loading pages: %v %q", ok, why)
	}

	// Each snapshot is taken once, its pacing too: a later start never
	// re-applies it over what page loads showed since.
	again, againProxies := storePool(t, store, storeExits...)
	if n, _ := again.Resume(ctx); n != 0 {
		t.Fatalf("a saved session resumed twice: %d", n)
	}
	if ok, _ := againProxies.PageLoad(walled.Endpoint); !ok {
		t.Fatal("the exits' saved pacing is restored twice")
	}

	// A session whose exit is no longer configured is dropped.
	leave(t, store, "old", Snapshot{SavedAt: time.Now(), Sessions: []Saved{{ID: good.ID, Tier: "isp", Exit: good.Endpoint.Exit,
		Profile: "chrome_146", Tokens: tokens, SavedAt: time.Now()}}})
	other, _ := storePool(t, store, "http://u:secret@192.0.2.99:8000")
	if n, err := other.Resume(ctx); err != nil || n != 0 || other.live["isp"] != 0 {
		t.Fatalf("resumed %d on another exit: %v", n, err)
	}
}

// A running crawler checkpoints its sessions, in use or idle, under a
// lease: no other crawler takes them while it runs, nor does it take its
// own. One that dies without saving leaves its last checkpoint, free once
// its lease ends.
func TestCheckpointsOutliveACrawlerThatDies(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	dies, _ := storePool(t, store, storeExits...)
	inUse, err := dies.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	inUse.SetTokens(Tokens{"lsd": "lsd-1"})
	if n, err := dies.Checkpoint(ctx); err != nil || n != 1 {
		t.Fatalf("checkpointed %d: %v", n, err)
	}
	next, _ := storePool(t, store, storeExits...)
	if n, _ := next.Resume(ctx); n != 0 {
		t.Fatal("a running crawler's sessions are taken")
	}
	mr.FastForward(checkpointLease + time.Second)
	if n, _ := dies.Resume(ctx); n != 0 {
		t.Fatal("a crawler takes its own checkpoint")
	}
	if n, err := next.Resume(ctx); err != nil || n != 1 || idleByID(next, "isp", inUse.ID) == nil {
		t.Fatalf("resumed %d from a crawler that died: %v", n, err)
	}

	// RunCheckpoint keeps checkpointing until it stops.
	runs, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- next.RunCheckpoint(runs, 10*time.Millisecond) }()
	key := store.snapKey(next.owner)
	for deadline := time.Now().Add(5 * time.Second); !mr.Exists(key) && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if !mr.Exists(key) || !mr.Exists(store.leaseKey(next.owner)) {
		t.Fatal("no checkpoint under a lease")
	}
}

// Sessions resume up to the tier's cap, and only for the fingerprint they
// were made with.
func TestResumeKeepsToTheCapAndTheProfile(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	var saved []Saved
	for i, exit := range []string{"isp|203.0.113.10:8000", "isp|198.51.100.10:8000", "isp|192.0.2.10:8000"} {
		saved = append(saved, Saved{ID: fmt.Sprint("id-", i), Tier: "isp", Exit: exit, Profile: "chrome_146",
			Tokens: Tokens{"lsd": "x"}, SavedAt: time.Now()})
	}
	saved[0].Profile = "firefox"
	leave(t, store, "old", Snapshot{SavedAt: time.Now(), Sessions: saved})
	pool, _ := storePool(t, store, storeExits...)
	pool.cfg.MaxPerTier = 1
	if n, err := pool.Resume(ctx); err != nil || n != 1 || pool.live["isp"] != 1 || pool.idle["isp"][0].ID != "id-1" {
		t.Fatalf("resumed %d (live %d): %v", n, pool.live["isp"], err)
	}
}

// Sessions of a snapshot past SavedSessionTTL are not resumed (its pauses
// still count), nor are a gateway's (its sticky session expires).
func TestSavedSessionsThatCannotResume(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	s := Saved{ID: "id-1", Tier: "isp", Exit: "isp|203.0.113.10:8000", Profile: "chrome_146", Tokens: Tokens{"lsd": "x"}}
	leave(t, store, "old", Snapshot{SavedAt: time.Now().Add(-SavedSessionTTL - time.Minute), Sessions: []Saved{s},
		Exits: map[string]proxy.ExitState{s.Exit: {PausedUntil: time.Now().Add(time.Hour)}}})
	got, err := store.TakeOrphans(ctx, "me")
	if err != nil || len(got) != 1 || len(got[0].Sessions) != 0 || len(got[0].Exits) != 1 {
		t.Fatalf("%+v %v", got, err)
	}

	base, _ := url.Parse("https://www.facebook.com")
	gw := newIdentity("g", proxy.Endpoint{Tier: "res", URL: "http://u:p@gw.example:7777", SessionID: "s"}, Fingerprint{}, transport.NewStd(time.Second), base, 1, 1, 0)
	gw.SetTokens(Tokens{"lsd": "x"})
	if _, ok := gw.saved(); ok {
		t.Fatal("a gateway session is saved")
	}
	direct := newIdentity("d", proxy.Endpoint{Tier: "direct"}, Fingerprint{}, transport.NewStd(time.Second), base, 1, 1, 0)
	if _, ok := direct.saved(); ok {
		t.Fatal("a session without tokens is saved")
	}
	direct.SetTokens(Tokens{"lsd": "x"})
	if _, ok := direct.saved(); !ok {
		t.Fatal("a direct session is not saved")
	}
	direct.markRetired("unhealthy")
	if _, ok := direct.saved(); ok {
		t.Fatal("a retired session is saved")
	}
}

// A crawler resumes the sessions of one that stops after it started (a
// rolling update starts the new pod first), and keeps a snapshot past the
// longest pause it holds.
func TestSessionsSavedLaterResume(t *testing.T) {
	mr := miniredis.RunT(t)
	store := &RedisStore{R: redis.NewClient(&redis.Options{Addr: mr.Addr()}), NS: "t."}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, _ := storePool(t, store, storeExits...)
	if n, err := pool.Resume(ctx); err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	go func() { _ = pool.RunResume(ctx, 10*time.Millisecond) }()
	s := Saved{ID: "id-1", Tier: "isp", Exit: "isp|203.0.113.10:8000", Profile: "chrome_146", Tokens: Tokens{"lsd": "x"}, SavedAt: time.Now()}
	if err := store.Checkpoint(ctx, "old", Snapshot{Owner: "old", SavedAt: time.Now(), Sessions: []Saved{s},
		Exits: map[string]proxy.ExitState{s.Exit: {PausedUntil: time.Now().Add(48 * time.Hour)}}}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("{t.tks:sessions}:snap:old"); ttl < 48*time.Hour {
		t.Fatalf("the snapshot expires before its pause ends: %s", ttl)
	}
	if err := store.Release(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	id, err := pool.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); id.ID != "id-1" && time.Now().Before(deadline); {
		pool.Release(id)
		time.Sleep(20 * time.Millisecond)
		id, _ = pool.Acquire(ctx, "isp")
	}
	if id.ID != "id-1" {
		t.Fatal("a session saved after the start is not resumed")
	}
}
