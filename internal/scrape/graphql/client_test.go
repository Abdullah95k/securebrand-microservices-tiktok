package graphql

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
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

// stubBuilder writes a fixed request, so the client's own part shows.
type stubBuilder struct{}

func (stubBuilder) Build(_ *session.Identity, q registry.Query, vars []byte, _ map[string]string, referer string) Built {
	return Built{Path: "/gql", Body: EncodeForm([]Field{{K: "doc_id", V: q.DocID}, {K: "variables", V: string(vars)}, {K: "empty"}}),
		Header: [][2]string{{"x-a", "1"}, {"accept", "*/*"}, {"referer", referer}}}
}

// The builder's request reaches the wire, under the query's registry
// endpoint and headers when it has them: a header the builder sent takes
// the query's value in place, a new one goes last.
func TestQueryEndpointAndHeaders(t *testing.T) {
	var reqs []*transport.Request
	id := testIdentity(t, &reqs)
	vars := json.RawMessage(`{"id":"{{id}}"}`)
	c := &Client{Rules: detect.DefaultRules(), Builder: stubBuilder{}, Registry: registry.NewStatic(registry.Seed{
		Queries: []registry.Query{
			{Name: "plain", DocID: "1", Variables: vars, Status: registry.StatusActive},
			{Name: "routed", DocID: "2", Variables: vars, Status: registry.StatusActive,
				Endpoint: "/graphql/query", Headers: []registry.Header{
					{Name: "x-root-field-name", Value: "xdt_root"},
					{Name: "accept", Value: "application/json"},
				}},
		}})}
	for _, q := range []string{"plain", "routed"} {
		resp, err := c.Do(context.Background(), id, Request{Query: q, Vars: map[string]any{"id": "7"}})
		if err != nil || resp.Class != detect.OK {
			t.Fatalf("%s: %v %+v", q, err, resp)
		}
	}
	if len(reqs) != 2 {
		t.Fatalf("%d requests", len(reqs))
	}
	plain, routed := reqs[0], reqs[1]
	if plain.URL != "https://www.example.com/gql" || routed.URL != "https://www.example.com/graphql/query" {
		t.Fatalf("urls %s %s", plain.URL, routed.URL)
	}
	if string(plain.Body) != `doc_id=1&variables=%7B%22id%22%3A%227%22%7D` || plain.Method != "POST" {
		t.Fatalf("plain: %s %s", plain.Method, plain.Body)
	}
	want := `[["x-a","1"],["accept","*/*"],["referer","https://www.example.com"]]`
	if got := fmtHeader(plain.Header); got != want {
		t.Fatalf("plain headers %s", got)
	}
	want = `[["x-a","1"],["accept","application/json"],["referer","https://www.example.com"],["x-root-field-name","xdt_root"]]`
	if got := fmtHeader(routed.Header); got != want {
		t.Fatalf("routed headers %s", got)
	}
}

func fmtHeader(h [][2]string) string {
	b, _ := json.Marshal(h)
	return string(b)
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
