package sign_test

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

func TestAgentHeaders(t *testing.T) {
	hdr := [][2]string{{"sec-ch-ua-platform", `"Windows"`}, {"user-agent", "Mozilla/5.0 Chrome"}, {"sec-ch-ua", `"Chrome";v="140"`},
		{"sec-ch-ua-mobile", "?0"}, {"accept", "*/*"}}
	lp := sign.Agent{UserAgent: "Lightpanda/1.0"}
	got := lp.Headers(hdr)
	if len(got) != 2 || got[0] != [2]string{"user-agent", "Lightpanda/1.0"} || got[1][0] != "accept" {
		t.Fatalf("Lightpanda without hints: %v", got)
	}
	hs := sign.Agent{UserAgent: "Mozilla/5.0 HeadlessChrome/153", SecChUa: `"HeadlessChrome";v="153"`, SecChUaMobile: "?0", SecChUaPlatform: `"Linux"`}
	got = hs.Headers(hdr)
	if got[0][1] != `"Linux"` || got[1][1] != hs.UserAgent || got[2][1] != hs.SecChUa {
		t.Fatalf("headless shell: %v", got)
	}
	if hdr[1][1] != "Mozilla/5.0 Chrome" {
		t.Fatal("Headers changed its argument")
	}
}

// engines are the browsers installed here: Chromium (and the old headless
// build, $TKS_HEADLESS_SHELL_BINARY) and Lightpanda ($TKS_LIGHTPANDA_BINARY).
func engines(t *testing.T) map[string]sign.EngineConfig {
	t.Helper()
	out := map[string]sign.EngineConfig{}
	if bin, err := browser.FindBinary(""); err == nil {
		out["chromium"] = sign.EngineConfig{Binary: bin, NoSandbox: true}
	}
	if bin := os.Getenv("TKS_HEADLESS_SHELL_BINARY"); bin != "" {
		out["headless-shell"] = sign.EngineConfig{Binary: bin, NoSandbox: true}
	}
	if bin := os.Getenv("TKS_LIGHTPANDA_BINARY"); bin != "" {
		out["lightpanda"] = sign.EngineConfig{Binary: bin, Engine: browser.EngineLightpanda}
	}
	if len(out) == 0 {
		t.Skip("no browser installed")
	}
	return out
}

// End to end against fakett, per browser: a page that presents itself as
// itself signs the comment call with its own values and navigator's, the
// identity sends it with the browser's own headers, and fakett answers.
func TestEngineSignerAgainstFakett(t *testing.T) {
	for name, ec := range engines(t) {
		t.Run(name, func(t *testing.T) {
			env := tttest.New(t, tttest.Options{Faults: fakett.Faults{RequireSignature: true,
				SignedPaths: []string{"/api/comment/list/", "/api/comment/list/reply/"}}})
			acc := env.World.Accounts()[0]
			var post *fakett.Post
			for _, p := range acc.Posts {
				if len(p.Comments) > 0 {
					post = p
					break
				}
			}
			id := env.Identity(t, acc.Username)
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			ref := env.HTTP.URL + "/@" + acc.Username
			ec.AcceptLanguage, ec.CallTimeout = tttest.AcceptLanguage, 30*time.Second
			var agents sign.Agents
			bc := config.Signer{Pages: 1, SignTimeout: 30 * time.Second, PageTimeout: 45 * time.Second}
			pool := sign.New(bc, agents.Opener(sign.EngineOpener(ec)))
			defer pool.Close()
			env.Client.Signer = sign.AsAgent{Signer: ttclient.SignerFor(config.SignerBrowser, pool), Agents: &agents}
			req := graphql.Request{Query: "comments", Vars: map[string]any{"item_id": post.ID, "count": 20, "cursor": 0}, Referer: ref}
			resp, err := env.Client.Do(ctx, id, req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.Class != detect.OK || !resp.Parts[0].Get("comments").IsArray() {
				t.Fatalf("signed comments: %v (%s)", resp.Class, resp.Reason)
			}
			a, ok := agents.Agent()
			if !ok || a.UserAgent == "" || a.UserAgent == id.FP.UserAgent {
				t.Fatalf("agent %+v (identity %q)", a, id.FP.UserAgent)
			}
			if name == "lightpanda" && !strings.Contains(a.UserAgent, "Lightpanda") {
				t.Fatalf("Lightpanda said %q", a.UserAgent)
			}
			if name != "lightpanda" && !strings.Contains(a.UserAgent, "HeadlessChrome") {
				t.Fatalf("headless Chromium said %q", a.UserAgent)
			}
			sent := env.Requests("/api/comment/list/")
			r := sent[len(sent)-1]
			q := mustQuery(t, r.URL)
			if q.Get("X-Gnarly") == "" || q.Get("browser_version") != a.AppVersion || q.Get("device_id") == id.Tokens()["device_id"] {
				t.Fatalf("sent %s", r.URL)
			}
			for _, h := range r.Header {
				if h[0] == "user-agent" && h[1] != a.UserAgent {
					t.Fatalf("sent as %q, signed by %q", h[1], a.UserAgent)
				}
			}
			// The page can send the call itself, as itself.
			var page interface {
				PageFetch(ctx context.Context, rawURL string) (*browser.Fetched, error)
				ProxyBytes() int64
				Close()
			}
			postID := strconv.FormatInt(post.ID, 10)
			pg, err := sign.EngineOpener(ec)(ctx, id, ref+"/video/"+postID)
			if err != nil {
				t.Fatal(err)
			}
			page = pg.(*sign.EnginePage)
			defer page.Close()
			var cq registry.Query
			for _, q := range env.Seed.Queries {
				if q.Name == "comments" && ttclient.Signed(q) {
					cq = q
				}
			}
			built := ttclient.Builder{AcceptLanguage: tttest.AcceptLanguage}.Build(id, cq, []byte(`{"item_id":"`+postID+`","count":20,"cursor":0}`), env.Seed.ClientParams, ref)
			f, err := page.PageFetch(ctx, built.Path+"?"+built.Query)
			if err != nil || f.Status != 200 || !strings.Contains(string(f.Body), `"comments"`) {
				t.Fatalf("page fetch: %+v %v", f, err)
			}
			if page.ProxyBytes() == 0 {
				t.Fatal("the page's requests did not go through its forwarder")
			}
		})
	}
}

func mustQuery(t *testing.T, raw string) url.Values {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Query()
}
