package client

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// recorder answers every request with an empty data object and keeps it.
type recorder struct {
	transport.Doer
	reqs *[]*transport.Request
}

func (r recorder) Do(_ context.Context, req *transport.Request) (*transport.Response, error) {
	*r.reqs = append(*r.reqs, req)
	return &transport.Response{Status: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: []byte(`{"data":{"node":{"id":"1"}}}`)}, nil
}

func (recorder) Close() {}

// Facebook's endpoint and header order, with a query's registry endpoint and
// headers on top: an override keeps the header's position, a new header
// goes last.
func TestQueryEndpointAndHeaders(t *testing.T) {
	var reqs []*transport.Request
	id := testIdentity(t, &reqs)
	vars := json.RawMessage(`{"id":"{{id}}"}`)
	c := &graphql.Client{Rules: detect.DefaultRules(), Builder: Builder{AcceptLanguage: "en-US"}, Registry: registry.NewStatic(registry.Seed{
		ClientParams: map[string]string{"x_asbd_id": "359341"},
		Queries: []registry.Query{
			{Name: "plain", DocID: "1", FriendlyName: "PlainQuery", Variables: vars, Status: registry.StatusActive},
			{Name: "routed", DocID: "2", FriendlyName: "RoutedQuery", Variables: vars, Status: registry.StatusActive,
				Endpoint: "/graphql/query", Headers: []registry.Header{
					{Name: "x-root-field-name", Value: "xdt_root"},
					{Name: "accept", Value: "application/json"},
				}},
		}})}
	for _, q := range []string{"plain", "routed"} {
		resp, err := c.Do(context.Background(), id, graphql.Request{Query: q, Vars: map[string]any{"id": "7"}})
		if err != nil || resp.Class != detect.OK {
			t.Fatalf("%s: %v %+v", q, err, resp)
		}
	}
	if len(reqs) != 2 {
		t.Fatalf("%d requests", len(reqs))
	}
	plain, routed := reqs[0], reqs[1]
	if plain.URL != "https://www.example.com/api/graphql/" || routed.URL != "https://www.example.com/graphql/query" {
		t.Fatalf("urls %s %s", plain.URL, routed.URL)
	}
	names := func(r *transport.Request) []string {
		var out []string
		for _, h := range r.Header {
			out = append(out, h[0])
		}
		return out
	}
	want := []string{"sec-ch-ua-platform", "x-fb-lsd", "sec-ch-ua", "sec-ch-ua-mobile", "x-fb-friendly-name",
		"x-asbd-id", "user-agent", "content-type", "accept", "origin", "sec-fetch-site", "sec-fetch-mode",
		"sec-fetch-dest", "referer", "accept-encoding", "accept-language", "priority"}
	if got := names(plain); len(got) != len(want) || fmtList(got) != fmtList(want) {
		t.Fatalf("default header order changed:\n%v\n%v", got, want)
	}
	// The override keeps accept's position; the new header goes last.
	if got := names(routed); fmtList(got) != fmtList(append(append([]string{}, want...), "x-root-field-name")) {
		t.Fatalf("routed header order: %v", got)
	}
	for _, h := range routed.Header {
		if h[0] == "accept" && h[1] != "application/json" {
			t.Fatalf("accept = %q", h[1])
		}
		if h[0] == "x-root-field-name" && h[1] != "xdt_root" {
			t.Fatalf("x-root-field-name = %q", h[1])
		}
	}
	for _, h := range plain.Header {
		if h[0] == "accept" && h[1] != "*/*" {
			t.Fatalf("a query's headers leaked into another: accept = %q", h[1])
		}
	}
}

// testIdentity is an identity on https://www.example.com whose requests
// are recorded in reqs.
func testIdentity(t *testing.T, reqs *[]*transport.Request) *session.Identity {
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
	pool, err := session.NewPool(session.PoolConfig{BaseURL: "https://www.example.com", Profile: "chrome_146",
		IdentityRPS: 1000, IdentityBurst: 1000, MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 1}, proxies, fps,
		func(transport.Options) (transport.Doer, error) { return recorder{reqs: reqs}, nil })
	if err != nil {
		t.Fatal(err)
	}
	id, err := pool.Acquire(context.Background(), "direct")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// The Facebook form, byte for byte as the client sent it before requests
// were built by a RequestBuilder (the session id __s is random).
func TestFacebookRequestBytes(t *testing.T) {
	var reqs []*transport.Request
	id := testIdentity(t, &reqs)
	seedBytes, _ := configs.FS.ReadFile("queries.seed.yaml")
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	c := &graphql.Client{Rules: detect.DefaultRules(), Registry: registry.NewStatic(seed),
		Builder: Builder{AcceptLanguage: "en-US,en;q=0.9", Locale: "en_US"}}
	for _, dtsg := range []string{"", "DTSG9"} {
		id.SetTokens(htmlx.ExtractTokens([]byte(`["LSD",[],{"token":"L5d"}] ["DTSGInitialData",[],{"token":"` + dtsg + `"}]` +
			` "__spin_r":101,"__spin_b":"trunk","__spin_t":1700,"hsi":"55","server_revision":101,"haste_session":"20.HYP","__comet_req":15`)))
		if _, err := c.Do(context.Background(), id, graphql.Request{Query: "comments", Referer: "https://www.example.com/p/1",
			Vars: map[string]any{"feedback_id": "Zm=", "cursor": "C1"}}); err != nil {
			t.Fatal(err)
		}
	}
	const vars = `variables=%7B%22commentsAfterCount%22%3A-1%2C%22commentsAfterCursor%22%3A%22C1%22%2C%22commentsBeforeCount%22%3Anull%2C` +
		`%22commentsBeforeCursor%22%3Anull%2C%22commentsIntentToken%22%3A%22RANKED_UNFILTERED_CHRONOLOGICAL_REPLIES_INTENT_V1%22%2C` +
		`%22feedLocation%22%3A%22DEDICATED_COMMENTING_SURFACE%22%2C%22focusCommentID%22%3Anull%2C%22id%22%3A%22Zm%3D%22%2C%22scale%22%3A1%2C` +
		`%22useDefaultActor%22%3Afalse%7D`
	want := []string{
		`av=0&__aaid=0&__user=0&__a=1&__req=1&__hs=20.HYP&dpr=1&__ccg=EXCELLENT&__rev=101&__s=<S>&__hsi=55&__comet_req=15&locale=en_US` +
			`&lsd=L5d&jazoest=2229&__spin_r=101&__spin_b=trunk&__spin_t=1700&fb_api_caller_class=RelayModern` +
			`&fb_api_req_friendly_name=CommentsListComponentsPaginationQuery&` + vars + `&server_timestamps=true&doc_id=9000000000000002`,
		`av=0&__aaid=0&__user=0&__a=1&__req=2&__hs=20.HYP&dpr=1&__ccg=EXCELLENT&__rev=101&__s=<S>&__hsi=55&__comet_req=15&locale=en_US` +
			`&fb_dtsg=DTSG9&lsd=L5d&jazoest=2363&__spin_r=101&__spin_b=trunk&__spin_t=1700&fb_api_caller_class=RelayModern` +
			`&fb_api_req_friendly_name=CommentsListComponentsPaginationQuery&` + vars + `&server_timestamps=true&doc_id=9000000000000002`,
	}
	sess := regexp.MustCompile(`__s=[^&]*`)
	for i, r := range reqs {
		if got := sess.ReplaceAllString(string(r.Body), "__s=<S>"); got != want[i] {
			t.Fatalf("request %d body:\n%s\nwant\n%s", i, got, want[i])
		}
		if r.URL != "https://www.example.com/api/graphql/" || r.Method != "POST" {
			t.Fatalf("request %d: %s %s", i, r.Method, r.URL)
		}
	}
}

func fmtList(s []string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
