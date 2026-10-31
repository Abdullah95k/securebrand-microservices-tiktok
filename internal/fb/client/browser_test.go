package client_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	fbclient "github.com/abdullah95k/securebrand-microservices-igs/internal/fb/client"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/fbtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// These browser tests need fakefb and Facebook's hooks, so they live on the
// Facebook side of the shared browser package.

func chromium(t *testing.T) string {
	t.Helper()
	bin, err := browser.FindBinary("")
	if err != nil {
		t.Skip(err)
	}
	return bin
}

const timelineDoc = "7316000000000001"

func fake(t *testing.T, f fakefb.Faults) (*fakefb.Server, string) {
	t.Helper()
	srv := fakefb.NewServer(fakefb.NewWorld(fakefb.DefaultSpec()), fakefb.Options{
		Docs: map[string][]string{"ProfileCometTimelineFeedRefetchQuery": {timelineDoc}}, Faults: f})
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return srv, hs.URL
}

func render(t *testing.T, opts browser.Options) (*browser.Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return browser.Render(ctx, chromium(t), opts)
}

// The render returns the DOM after scripts ran and the GraphQL requests
// the page made itself, with their doc_id and response.
func TestRenderCapturesThePagesOwnGraphQL(t *testing.T) {
	srv, base := fake(t, fakefb.Faults{JSOnlyTokens: true})
	page := srv.World.Pages()[0]
	raw, _ := http.Get(base + "/" + page.Vanity)
	html, _ := io.ReadAll(raw.Body)
	_ = raw.Body.Close()
	if htmlx.Valid(htmlx.ExtractTokens(html)) {
		t.Fatal("JSOnlyTokens: the served markup must not carry the tokens")
	}

	res, err := render(t, browser.Options{URL: base + "/" + page.Vanity, UserAgent: "Mozilla/5.0 fbs-test", AcceptLanguage: "en-US,en;q=0.9"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rendered %s (%d) in %s: %d bytes, %d captured", res.URL, res.Status, res.Elapsed, len(res.HTML), len(res.Captured))
	if res.Status != 200 || !strings.Contains(string(res.HTML), page.Name) || !htmlx.Valid(htmlx.ExtractTokens(res.HTML)) {
		t.Fatalf("status %d, tokens %+v", res.Status, htmlx.ExtractTokens(res.HTML))
	}
	if len(res.Captured) != 1 {
		t.Fatalf("captured %+v", res.Captured)
	}
	x := res.Captured[0]
	if x.FriendlyName != "ProfileCometTimelineFeedRefetchQuery" || x.DocID != timelineDoc || x.Status != 200 ||
		!strings.Contains(string(x.Body), "timeline") || !strings.Contains(string(x.Variables), `"count"`) {
		t.Fatalf("exchange %+v body %.200s", x, x.Body)
	}
}

// A page whose tokens need JavaScript: the HTTP bootstrap fails, the
// browser fallback bootstraps the identity, and GraphQL works with it.
func TestBootstrapFallback(t *testing.T) {
	chromium(t)
	env := fbtest.New(t, fbtest.Options{Faults: fakefb.Faults{JSOnlyTokens: true}})
	ctx := context.Background()
	p := env.World.Pages()[0]
	id, _ := env.Pool.Acquire(ctx, "direct")
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); !errors.Is(err, session.ErrBootstrap) {
		t.Fatalf("without the browser the bootstrap must fail: %v", err)
	}
	env.Pool.Release(id)

	b, err := browser.NewBootstrapper(config.Browser{MaxConcurrent: 1, MaxShare: 0.02, Timeout: time.Minute}, "en-US,en;q=0.9", fbclient.Hooks("en-US,en;q=0.9"))
	if err != nil {
		t.Fatal(err)
	}
	env.Loader.Browser = b
	id, _ = env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	pl, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity)
	if err != nil || !id.Bootstrapped() || pl.Class != detect.OK {
		t.Fatalf("browser bootstrap: %v %+v", err, pl)
	}
	resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"count": 3, "page_id": p.ID}})
	if err != nil || resp.Class != detect.OK {
		t.Fatalf("timeline with browser tokens: %v %v %s", err, resp.Class, resp.Reason)
	}
}
