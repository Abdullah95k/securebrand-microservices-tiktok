package e2e

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/canary"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// variants returns each operation's variants by status.
func variants(t *testing.T, s *Stack, name string) map[string]string {
	t.Helper()
	qs, err := registry.ListFrom(context.Background(), s.DB, ttstore.Registry)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, q := range qs {
		if q.Name == name {
			out[q.DocID] = q.Status
		}
	}
	return out
}

func canaryRun(t *testing.T, c *app.Canary, want string) canary.Report {
	t.Helper()
	ctx := context.Background()
	if err := c.Registry.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	rep, err := c.Run(ctx)
	if err != nil {
		t.Fatalf("canary run: %v (%+v)", err, rep)
	}
	if rep.Status != want {
		js, _ := json.MarshalIndent(rep, "", "  ")
		t.Fatalf("canary run %s, want %s:\n%s", rep.Status, want, js)
	}
	return rep
}

func promoted(rep canary.Report, op, to, why string) bool {
	return slices.ContainsFunc(rep.Promoted, func(p canary.Promotion) bool {
		return p.Operation == op && p.To == to && p.Why == why
	})
}

// TestCanaryHeals runs tks-canary as the binary wires it against fakett
// while TikTok changes what it accepts (build brief M8: "a stale operation
// heals itself"), and checks after each change that the crawler, reloading
// its registry, stores what the world holds:
//  1. nothing changed: every operation healthy, twins added, the working
//     twin of the list kept as a fallback, the refused twin of comments not;
//  2. a signing change: TikTok refuses the creator list unsigned; the
//     crawler's check still succeeds through the fallback, and the canary
//     promotes the signed list;
//  3. the signing change undone: the canary promotes the unsigned list
//     again, which needs no browser time;
//  4. an endpoint moved: comments fail with nothing to promote (degraded,
//     the health gauge 0, the alert's condition) until an operator adds a
//     candidate on the new path, which the next run promotes.
func TestCanaryHeals(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 11, Accounts: 3, PostsPerAccount: 12, MaxComments: 30, MaxReplies: 5,
		PostSpacing: 20 * time.Hour}})
	ctx := context.Background()
	accs := s.World.Accounts()
	refs := make([]string, len(accs))
	for i, a := range accs {
		refs[i] = "u:" + a.Username
		s.Cfg.TT.Canary.Accounts = append(s.Cfg.TT.Canary.Accounts, a.Username)
	}
	c, err := app.NewCanary(ctx, s.Cfg, s.DB, app.CanaryOptions{Signer: s.Signer,
		DoerFactory: func(transport.Options) (transport.Doer, error) { return transport.NewStd(10 * time.Second), nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	reload := func() {
		if err := s.Crawler.Registry.Reload(ctx); err != nil {
			t.Fatal(err)
		}
	}
	base := fakett.Faults{RequireSignature: true, SignedPaths: []string{"/api/comment/list/", "/api/comment/list/reply/"}}

	// 1. Nothing changed.
	rep := canaryRun(t, c, canary.StatusOK)
	if len(rep.Promoted) != 0 {
		t.Errorf("promoted %+v with nothing changed", rep.Promoted)
	}
	for _, tw := range []string{"creator_list/creator-signed", "comments/web", "replies/web"} {
		if !slices.Contains(rep.Twins, tw) {
			t.Errorf("twin %s not added: %v", tw, rep.Twins)
		}
	}
	if got := variants(t, s, "creator_list"); got["creator"] != registry.StatusActive || got["creator-signed"] != registry.StatusFallback {
		t.Errorf("creator_list variants %v, want creator active, creator-signed a fallback", got)
	}
	if got := variants(t, s, "comments"); got["web-signed"] != registry.StatusActive || got["web"] != registry.StatusCandidate {
		t.Errorf("comments variants %v, want web-signed active, web (refused) a candidate", got)
	}
	for _, op := range []string{"creator_list", "comments", "replies"} {
		if v := gauge(metrics.CanaryHealthy.WithLabelValues(op)); v != 1 {
			t.Errorf("%s healthy gauge %v, want 1", op, v)
		}
	}
	if v := gauge(metrics.CanarySigner); v != 1 {
		t.Errorf("signer gauge %v, want 1", v)
	}
	reload() // the crawler learns the spare
	round(t, s, "before", full, refs...)
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}

	// 2. TikTok starts signing the creator list.
	f := base
	f.SignedPaths = append(slices.Clone(base.SignedPaths), "/api/creator/item_list/")
	s.Fake.SetFaults(f)
	s.World.Advance(time.Hour)
	np := s.World.AddPost(accs[0], 5)
	signedBefore := s.Signer.Calls.Load()
	// The crawler falls back to the spare at once.
	round(t, s, "signed-fallback", incremental, refs...)
	if s.Signer.Calls.Load() == signedBefore {
		t.Error("no list call signed after TikTok began to sign the list")
	}
	if n := count(t, s, `SELECT count(*) FROM posts WHERE post_id = $1`, np.ID); n != 1 {
		t.Errorf("the new post is stored %d times, want once", n)
	}
	rep = canaryRun(t, c, canary.StatusOK)
	if !promoted(rep, "creator_list", "creator-signed", canary.WhyFailed) {
		t.Fatalf("creator-signed not promoted: %+v", rep.Promoted)
	}
	if got := variants(t, s, "creator_list"); got["creator-signed"] != registry.StatusActive || got["creator"] != registry.StatusFallback {
		t.Errorf("creator_list variants %v after the promotion", got)
	}
	reload()
	s.World.Advance(time.Hour)
	p1 := s.World.AddPost(accs[1], 3)
	round(t, s, "signed", incremental, refs...)
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}

	// 3. TikTok stops signing the list again: the unsigned list is
	// promoted back, as it costs no signature.
	s.Fake.SetFaults(base)
	rep = canaryRun(t, c, canary.StatusOK)
	if !promoted(rep, "creator_list", "creator", canary.WhyUnsigned) {
		t.Fatalf("creator not promoted back: %+v", rep.Promoted)
	}
	reload()
	s.World.Advance(time.Hour)
	p2 := s.World.AddPost(accs[2], 4)
	round(t, s, "unsigned", incremental, refs...)
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}

	// 4. The comment endpoint moves: nothing to promote until an operator
	// adds the new path.
	moved := base
	moved.MovedPaths = map[string]string{"/api/comment/list/": "/api/comment/list/v2/"}
	s.Fake.SetFaults(moved)
	rep = canaryRun(t, c, canary.StatusDegraded)
	if !slices.Contains(rep.Failing, "comments") || len(rep.Promoted) != 0 {
		t.Fatalf("failing %v, promoted %+v; want comments failing, nothing promoted", rep.Failing, rep.Promoted)
	}
	if v := gauge(metrics.CanaryHealthy.WithLabelValues("comments")); v != 0 {
		t.Errorf("comments healthy gauge %v, want 0", v)
	}
	var tmpl registry.Query
	qs, err := registry.ListFrom(ctx, s.DB, ttstore.Registry)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range qs {
		if q.Name == "comments" && q.DocID == "web-signed" {
			tmpl = q
		}
	}
	tmpl.DocID, tmpl.Endpoint, tmpl.Status, tmpl.Source = "v2-signed", "/api/comment/list/v2/", registry.StatusCandidate, "manual"
	if err := registry.UpsertTo(ctx, s.DB, ttstore.Registry, tmpl); err != nil {
		t.Fatal(err)
	}
	rep = canaryRun(t, c, canary.StatusOK)
	if !promoted(rep, "comments", "v2-signed", canary.WhyFailed) {
		t.Fatalf("v2-signed not promoted: %+v", rep.Promoted)
	}
	reload()
	s.World.Advance(7 * time.Hour) // past the new posts' first comment window
	for _, p := range []*fakett.Post{np, p1, p2} {
		s.World.AddComments(p, 3)
	}
	round(t, s, "moved", incremental, refs...)
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}

	// Runs are recorded for the admin route and the gauges.
	if n := count(t, s, `SELECT count(*) FROM canary_runs`); n != 5 {
		t.Errorf("%d canary runs recorded, want 5", n)
	}
	if v := gauge(metrics.CanaryLastSuccess); v == 0 {
		t.Error("no last-success time")
	}
}
