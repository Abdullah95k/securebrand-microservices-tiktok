package graphql

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
)

// webDoer answers like TikTok's web API: an empty 200 to a request
// without the "signed" parameter, a JSON object with statusCode otherwise.
type webDoer struct {
	transport.Doer
	reqs *[]*transport.Request
}

func (d webDoer) Do(_ context.Context, req *transport.Request) (*transport.Response, error) {
	*d.reqs = append(*d.reqs, req)
	u, _ := url.Parse(req.URL)
	h := http.Header{"Content-Type": {"application/json"}}
	switch {
	case u.Query().Get("signed") == "":
		return &transport.Response{Status: 200, Header: h}, nil
	case u.Query().Get("secUid") == "private":
		return &transport.Response{Status: 200, Header: h, Body: []byte(`{"statusCode":10222,"status_msg":"private"}`)}, nil
	default:
		return &transport.Response{Status: 200, Header: h, Body: []byte(`{"statusCode":0,"itemList":[{"id":"1"}],"hasMore":true}`)}, nil
	}
}

func (webDoer) Close() {}

// getBuilder writes a GET with the variables as a sorted query string, and
// adds signed=1 for the "signed" variant.
type getBuilder struct{}

func (getBuilder) Build(_ *session.Identity, q registry.Query, vars []byte, _ map[string]string, _ string) Built {
	v := url.Values{}
	gjson.ParseBytes(vars).ForEach(func(k, val gjson.Result) bool { v.Set(k.String(), val.String()); return true })
	if q.DocID == "signed" {
		v.Set("signed", "1")
	}
	return Built{Method: q.Method, Path: "/api/post/item_list/", Query: v.Encode(), Header: [][2]string{{"accept", "*/*"}}}
}

// statusDecoder reads TikTok's envelope: one JSON object whose statusCode
// is an error when it is not 0.
func statusDecoder(body []byte) ([]gjson.Result, []detect.GQLError, bool) {
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil, nil, false
	}
	r := gjson.ParseBytes(body)
	code := r.Get("statusCode").Int()
	if code != 0 {
		return []gjson.Result{r}, []detect.GQLError{{Code: code, Message: r.Get("status_msg").String()}}, false
	}
	return []gjson.Result{r}, nil, true
}

func webIdentity(t *testing.T, reqs *[]*transport.Request) *session.Identity {
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
		func(transport.Options) (transport.Doer, error) { return webDoer{reqs: reqs}, nil })
	if err != nil {
		t.Fatal(err)
	}
	id, err := pool.Acquire(context.Background(), "direct")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// A GET operation goes out with its query string and no body; an empty
// answer under EmptyIsStale falls back to the next variant; the decoder's
// status codes reach the rules.
func TestWebAPIGetDecoderAndEmptyFallback(t *testing.T) {
	var reqs []*transport.Request
	id := webIdentity(t, &reqs)
	vars := []byte(`{"secUid":"{{sec}}","count":"16"}`)
	c := &Client{
		Rules:   detect.Rules{EmptyIsStale: true, UnavailableCodes: []int64{10222}},
		Builder: getBuilder{}, Decode: statusDecoder,
		Registry: registry.NewStatic(registry.Seed{Queries: []registry.Query{
			{Name: "posts", DocID: "unsigned", Method: "GET", Variables: vars, Status: registry.StatusActive, Priority: 10},
			{Name: "posts", DocID: "signed", Method: "GET", Variables: vars, Status: registry.StatusFallback, Priority: 5},
		}}),
	}
	resp, err := c.Do(context.Background(), id, Request{Query: "posts", Vars: map[string]any{"sec": "abc"}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Class != detect.OK || resp.Query.DocID != "signed" || len(resp.Parts) != 1 {
		t.Fatalf("want OK from the signed variant, got %v (%s) from %q", resp.Class, resp.Reason, resp.Query.DocID)
	}
	if len(reqs) != 2 {
		t.Fatalf("want the unsigned try then the signed one, got %d requests", len(reqs))
	}
	for _, r := range reqs {
		u, _ := url.Parse(r.URL)
		if r.Method != "GET" || r.Body != nil || u.Path != "/api/post/item_list/" || u.Query().Get("secUid") != "abc" || u.Query().Get("count") != "16" {
			t.Fatalf("bad request %s %s body=%q", r.Method, r.URL, r.Body)
		}
	}
	resp, err = c.Do(context.Background(), id, Request{Query: "posts", Vars: map[string]any{"sec": "private"}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Class != detect.ContentUnavailable {
		t.Fatalf("statusCode 10222: want content_unavailable, got %v (%s)", resp.Class, resp.Reason)
	}
}

// Without EmptyIsStale an empty answer stays a soft error, as for fbs.
func TestEmptyAnswerDefaultsToSoftError(t *testing.T) {
	var reqs []*transport.Request
	id := webIdentity(t, &reqs)
	c := &Client{Rules: detect.DefaultRules(), Builder: getBuilder{}, Decode: statusDecoder,
		Registry: registry.NewStatic(registry.Seed{Queries: []registry.Query{
			{Name: "posts", DocID: "unsigned", Method: "GET", Variables: []byte(`{}`), Status: registry.StatusActive},
		}})}
	resp, err := c.Do(context.Background(), id, Request{Query: "posts"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Class != detect.SoftError {
		t.Fatalf("got %v (%s)", resp.Class, resp.Reason)
	}
}

// urlSigner adds signed=1 to the URL (the doer then answers), or fails.
type urlSigner struct {
	fail  bool
	calls int
}

func (s *urlSigner) Sign(_ context.Context, _ *session.Identity, q registry.Query, raw string, hdr [][2]string) (string, [][2]string, error) {
	s.calls++
	if s.fail {
		return "", nil, errors.New("no page")
	}
	return raw + "&signed=1", append(hdr, [2]string{"x-signed-by", q.Name}), nil
}

// The signer gets the built request after pacing and its URL and headers go
// out; a signer's failure sends nothing and is ErrSign, not a transport
// error, so the identity is not blamed.
func TestSignerSignsBeforeSending(t *testing.T) {
	var reqs []*transport.Request
	id := webIdentity(t, &reqs)
	sg := &urlSigner{}
	c := &Client{Rules: detect.Rules{EmptyIsStale: true}, Builder: getBuilder{}, Decode: statusDecoder, Signer: sg,
		Registry: registry.NewStatic(registry.Seed{Queries: []registry.Query{
			{Name: "posts", DocID: "unsigned", Method: "GET", Variables: []byte(`{"secUid":"abc"}`), Status: registry.StatusActive},
		}})}
	resp, err := c.Do(context.Background(), id, Request{Query: "posts"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Class != detect.OK || len(reqs) != 1 || sg.calls != 1 {
		t.Fatalf("got %v (%s), %d requests, %d signs", resp.Class, resp.Reason, len(reqs), sg.calls)
	}
	u, _ := url.Parse(reqs[0].URL)
	if u.Query().Get("signed") != "1" || u.Query().Get("secUid") != "abc" || reqs[0].Header[len(reqs[0].Header)-1][1] != "posts" {
		t.Fatalf("sent %s %v", reqs[0].URL, reqs[0].Header)
	}
	sg.fail = true
	_, err = c.Do(context.Background(), id, Request{Query: "posts"})
	if !errors.Is(err, ErrSign) || errors.Is(err, ErrTransport) || len(reqs) != 1 {
		t.Fatalf("failed sign: err %v, %d requests", err, len(reqs))
	}
}
