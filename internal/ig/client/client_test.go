package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// recorder answers every request with an NDJSON data line and keeps it; its
// cookie jar holds only what the test sets.
type recorder struct {
	transport.Doer
	reqs    *[]*transport.Request
	cookies map[string]string
}

func (r recorder) Do(_ context.Context, req *transport.Request) (*transport.Response, error) {
	*r.reqs = append(*r.reqs, req)
	return &transport.Response{Status: 200, Header: http.Header{"Content-Type": {"text/javascript; charset=utf-8"}},
		Body: []byte(`{"data":{"xig_user_by_username":{"pk":"1"}},"extensions":{"is_final":true}}`)}, nil
}
func (r recorder) Cookie(_ *url.URL, name string) string { return r.cookies[name] }
func (recorder) Close()                                  {}

func testIdentity(t *testing.T, reqs *[]*transport.Request, cookies map[string]string) *session.Identity {
	t.Helper()
	proxies, err := proxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: BaseURL, Profile: "chrome_146",
		IdentityRPS: 1000, IdentityBurst: 1000, MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 1}, proxies, fps,
		func(transport.Options) (transport.Doer, error) { return recorder{reqs: reqs, cookies: cookies}, nil })
	if err != nil {
		t.Fatal(err)
	}
	id, err := pool.Acquire(context.Background(), "direct")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// The seed holds the five recorded operations; only highlights has its own
// endpoint and header.
func TestSeed(t *testing.T) {
	s, err := Seed()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"profile": "27981003384861049", "grid_first": "29483846714553007", "grid": "28904005159232689",
		"comments": "27837656105907150", "highlights": "39683663864580775"}
	if len(s.Queries) != len(want) {
		t.Fatalf("%d queries", len(s.Queries))
	}
	for _, q := range s.Queries {
		if want[q.Name] != q.DocID || registry.IsPlaceholderDocID(q.DocID) {
			t.Errorf("%s: doc_id %s", q.Name, q.DocID)
		}
		if (q.Name == "highlights") != (q.Endpoint != "") {
			t.Errorf("%s: endpoint %q", q.Name, q.Endpoint)
		}
	}
	if s.ClientParams["__comet_req"] != "7" || s.ClientParams["__d"] != "www" || s.ClientParams["x_asbd_id"] != "359341" {
		t.Fatalf("client params %v", s.ClientParams)
	}
}

// The form in the recorded order with the page's tokens, Instagram's
// headers, and highlights on /graphql/query with its root field header.
func TestRequests(t *testing.T) {
	var reqs []*transport.Request
	id := testIdentity(t, &reqs, map[string]string{"csrftoken": "cookieCSRF"})
	id.SetTokens(session.Tokens{"lsd": "L1", "csrf_token": "pageCSRF", "__spin_r": "101", "__spin_b": "trunk", "__spin_t": "1700",
		"__rev": "101", "__hs": "20.HYP", "__hsi": "55", "__comet_req": "7", "jazoest": "2211"})
	seed, err := Seed()
	if err != nil {
		t.Fatal(err)
	}
	c := &graphql.Client{Registry: registry.NewStatic(seed), Rules: igdetect.Rules(), Builder: Builder{AcceptLanguage: "en-US,en;q=0.9"}}
	ctx := context.Background()
	for _, r := range []graphql.Request{
		{Query: "grid", Vars: map[string]any{"user_id": "1", "cursor": "CUR"}, Referer: BaseURL + "/brand/"},
		{Query: "highlights", Vars: map[string]any{"reel_ids": []string{"highlight:9"}, "initial_reel_id": "highlight:9"}},
	} {
		if resp, err := c.Do(ctx, id, r); err != nil || resp.Class != detect.OK {
			t.Fatalf("%s: %v %+v", r.Query, err, resp)
		}
	}
	grid, hl := reqs[0], reqs[1]
	if grid.URL != BaseURL+"/api/graphql" || hl.URL != BaseURL+"/graphql/query" {
		t.Fatalf("urls %s %s", grid.URL, hl.URL)
	}
	body := regexp.MustCompile(`__s=[^&]*`).ReplaceAllString(string(grid.Body), "__s=<S>")
	want := `av=0&__d=www&__user=0&__a=1&__req=1&__hs=20.HYP&dpr=1&__ccg=EXCELLENT&__rev=101&__s=<S>&__hsi=55&__comet_req=7` +
		`&lsd=L1&jazoest=2211&__spin_r=101&__spin_b=trunk&__spin_t=1700&fb_api_caller_class=RelayModern` +
		`&fb_api_req_friendly_name=PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery_connection` +
		`&variables=%7B%22__relay_internal__pv__PolarisSEOIsCrawlerrelayprovider%22%3Afalse%2C%22after%22%3A%22CUR%22%2C%22first%22%3A12%2C%22id%22%3A%221%22%7D` +
		`&server_timestamps=true&doc_id=28904005159232689`
	if body != want {
		t.Fatalf("grid body:\n%s\nwant\n%s", body, want)
	}
	h := map[string]string{}
	var names []string
	for _, kv := range grid.Header {
		h[kv[0]] = kv[1]
		names = append(names, kv[0])
	}
	if h["x-csrftoken"] != "pageCSRF" || h["x-ig-app-id"] != "936619743392459" || h["x-fb-lsd"] != "L1" ||
		h["x-asbd-id"] != "359341" || h["x-fb-friendly-name"] != "PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery_connection" ||
		h["origin"] != BaseURL || h["referer"] != BaseURL+"/brand/" || h["x-ig-max-touch-points"] != "0" {
		t.Fatalf("headers %v", h)
	}
	if last := hl.Header[len(hl.Header)-1]; last != [2]string{"x-root-field-name", "xdt_api__v1__feed__reels_media__connection"} {
		t.Fatalf("highlights header %v", last)
	}
	b, _ := json.Marshal(names)
	if len(names) != 20 {
		t.Fatalf("header set %s", b)
	}

	// Without a page csrf_token, the csrftoken cookie stands in.
	id.SetTokens(session.Tokens{"lsd": "L1"})
	if _, err := c.Do(ctx, id, graphql.Request{Query: "profile", Vars: map[string]any{"username": "brand"}}); err != nil {
		t.Fatal(err)
	}
	for _, kv := range reqs[2].Header {
		if kv[0] == "x-csrftoken" && kv[1] != "cookieCSRF" {
			t.Fatalf("x-csrftoken = %q", kv[1])
		}
	}
}

func TestHooks(t *testing.T) {
	h := Hooks()
	if !h.Wall("/accounts/login/") || !h.Wall("/challenge/action/") || h.Wall("/brand/") {
		t.Fatal("walls")
	}
	if _, ok := h.Extract([]byte(`<html>no tokens</html>`)); ok {
		t.Fatal("a page without an LSD bootstrapped")
	}
	if tok, ok := h.Extract([]byte(`["LSD",[],{"token":"L"},1]`)); !ok || tok["lsd"] != "L" {
		t.Fatalf("%v %v", tok, ok)
	}
}
