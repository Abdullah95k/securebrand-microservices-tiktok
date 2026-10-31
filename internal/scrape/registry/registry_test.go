package registry

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
)

func TestSeedAndRender(t *testing.T) {
	b, _ := configs.FS.ReadFile("queries.seed.yaml")
	seed, err := ParseSeed(b)
	if err != nil {
		t.Fatal(err)
	}
	reg := NewStatic(seed)
	for _, name := range []string{"timeline", "comments", "comments_newest", "replies"} {
		if _, err := reg.Get(name); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	q, _ := reg.Get("comments")
	out, err := Render(q.Variables, map[string]any{"feedback_id": "ZmVlZA==", "cursor": "C1"})
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	_ = json.Unmarshal(out, &v)
	if v["id"] != "ZmVlZA==" || v["commentsAfterCursor"] != "C1" || v["commentsAfterCount"] != float64(-1) {
		t.Fatalf("rendered %s", out)
	}
	out, _ = Render(q.Variables, map[string]any{"feedback_id": "x"})
	_ = json.Unmarshal(out, &v)
	if v["commentsAfterCursor"] != nil {
		t.Fatalf("missing placeholder must render null, got %v", v["commentsAfterCursor"])
	}
}

func TestFallbackOrdering(t *testing.T) {
	reg := NewStatic(Seed{Queries: []Query{
		{Name: "q", DocID: "old", Status: StatusFallback, Priority: 50, Variables: json.RawMessage(`{}`)},
		{Name: "q", DocID: "new", Status: StatusActive, Priority: 1, Variables: json.RawMessage(`{}`)},
		{Name: "q", DocID: "cand", Status: StatusCandidate, Priority: 99, Variables: json.RawMessage(`{}`)},
	}})
	q, _ := reg.Get("q")
	if q.DocID != "new" {
		t.Fatalf("active entry must win, got %s", q.DocID)
	}
	fb, ok := reg.Fallback("q", "new")
	if !ok || fb.DocID != "old" {
		t.Fatalf("fallback = %v %v", fb.DocID, ok)
	}
}

func TestSeedRoundTripAndPlaceholders(t *testing.T) {
	b, err := configs.FS.ReadFile("queries.seed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s, err := ParseSeed(b)
	if err != nil {
		t.Fatal(err)
	}
	if got := PlaceholderQueries(s); len(got) != len(s.Queries) {
		t.Fatalf("every embedded seed query is a placeholder, got %v", got)
	}
	s.Queries[0].DocID = "24681357902468135"
	out, err := MarshalSeed(s)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseSeed(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Queries) != len(s.Queries) || back.Queries[0].DocID != "24681357902468135" {
		t.Fatalf("round trip lost data: %+v", back.Queries)
	}
	if got := PlaceholderQueries(back); len(got) != len(s.Queries)-1 {
		t.Fatalf("placeholders after update: %v", got)
	}
	for i := range s.Queries {
		var a, b map[string]any
		_ = json.Unmarshal(s.Queries[i].Variables, &a)
		_ = json.Unmarshal(back.Queries[i].Variables, &b)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatalf("%s variables changed:\n%v\n%v", s.Queries[i].Name, a, b)
		}
	}
	if fmt.Sprint(back.ClientParams) != fmt.Sprint(s.ClientParams) {
		t.Fatal("client params changed")
	}
}

// A query's endpoint and headers come from the seed, with lower-case header
// names, survive a round trip in send order, and are checked: no host,
// query string or path tricks, no transport headers, no duplicates.
func TestSeedEndpointAndHeaders(t *testing.T) {
	const doc = `
queries:
  - name: highlights
    friendly_name: HighlightsQuery
    doc_id: "1"
    parser: highlights
    endpoint: /graphql/query
    headers:
      - {name: X-Root-Field-Name, value: xdt_root}
      - {name: x-bloks-version-id, value: abc123}
    variables: {id: "{{id}}"}
  - name: plain
    friendly_name: PlainQuery
    doc_id: "2"
    parser: plain
    variables: {}
`
	s, err := ParseSeed([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	q := s.Queries[0]
	want := []Header{{"x-root-field-name", "xdt_root"}, {"x-bloks-version-id", "abc123"}}
	if q.Endpoint != "/graphql/query" || fmt.Sprint(q.Headers) != fmt.Sprint(want) {
		t.Fatalf("parsed %q %v", q.Endpoint, q.Headers)
	}
	if p := s.Queries[1]; p.Endpoint != "" || p.Headers != nil {
		t.Fatalf("a query without them gets none: %q %v", p.Endpoint, p.Headers)
	}
	out, err := MarshalSeed(s)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseSeed(out)
	if err != nil {
		t.Fatal(err)
	}
	if b := back.Queries[0]; b.Endpoint != q.Endpoint || fmt.Sprint(b.Headers) != fmt.Sprint(want) {
		t.Fatalf("round trip: %q %v\n%s", b.Endpoint, b.Headers, out)
	}
	if b := back.Queries[1]; b.Endpoint != "" || b.Headers != nil {
		t.Fatalf("round trip added a route: %q %v", b.Endpoint, b.Headers)
	}

	// The embedded Facebook seed names no endpoint or header: fbs's
	// client defaults apply.
	fb, _ := configs.FS.ReadFile("queries.seed.yaml")
	seed, err := ParseSeed(fb)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range seed.Queries {
		if q.Endpoint != "" || q.Headers != nil {
			t.Fatalf("%s: %q %v", q.Name, q.Endpoint, q.Headers)
		}
	}

	for _, e := range []string{"/api/graphql", "/api/graphql/", "/graphql/query"} {
		if _, err := normalizeRoute("q", e, nil); err != nil {
			t.Fatalf("%s: %v", e, err)
		}
	}
	for _, e := range []string{"api/graphql", "https://example.com/api/graphql", "//example.com/api/graphql",
		"/api/../graphql", "/api//graphql", "/api/graphql?x=1", "/api/graphql#x", "/api/%2e%2e/x", "/api/graphql\n"} {
		if _, err := normalizeRoute("q", e, nil); err == nil {
			t.Fatalf("endpoint %q accepted", e)
		}
	}
	for _, h := range [][]Header{
		{{"Cookie", "a=b"}},
		{{"host", "example.com"}},
		{{"content-length", "1"}},
		{{"x-a", "1"}, {"X-A", "2"}},
		{{"x a", "1"}},
		{{"", "1"}},
		{{"x-a", "1\r\nx-b: 2"}},
	} {
		if _, err := normalizeRoute("q", "", h); err == nil {
			t.Fatalf("headers %v accepted", h)
		}
	}
}
