package replay

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/rawsink"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/fbtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

// response fetches one real fakefb response of query.
func response(t *testing.T, env *fbtest.Env, query string, vars map[string]any) *graphql.Response {
	t.Helper()
	ctx := context.Background()
	id, _ := env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: query, Vars: vars})
	if err != nil || resp.Class != detect.OK {
		t.Fatalf("%s: %v %v", query, err, resp)
	}
	return resp
}

func TestReplay(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	s3, err := s3x.Open(ctx, u.Host, "k", "secret-key", "us-east-1", false, "fbs-raw")
	if err != nil {
		t.Fatal(err)
	}
	sink := rawsink.New(s3, "fbs-raw")

	// A timeline response whose shape drifted (a required field renamed),
	// captured the way the client captures drift, and a healthy comments
	// response.
	drifted := fbtest.New(t, fbtest.Options{Faults: fakefb.Faults{DriftRequired: true}})
	page := drifted.World.Pages()[0]
	tl := response(t, drifted, "timeline", map[string]any{"page_id": page.ID, "count": 3})
	if _, rep := parse.Timeline(tl.Parts, page.ID, drifted.HTTP.URL); rep.OK() {
		t.Fatal("the fault must make the timeline drift")
	}
	drifted.GQL.Raw = sink
	drifted.GQL.CaptureAll = true // drift still goes to raw-failures/
	drifted.GQL.Parsed(tl, false)

	healthy := fbtest.New(t, fbtest.Options{})
	post := healthy.World.Pages()[0].Posts[0]
	vars := map[string]any{"feedback_id": parse.FeedbackID(post.ID)}
	cm := response(t, healthy, "comments", vars)
	sink.Capture(ctx, "comments", detect.SchemaDrift, cm.Body, vars) // say it drifted then, but parses now
	sink.Capture(ctx, "comments", detect.RateLimited, []byte(`{"error":1675004}`), vars)
	// Two queries sharing the timeline parser drift alike.
	sink.Capture(ctx, "timeline_alt", detect.SchemaDrift, tl.Body, tl.Vars)
	sink.FlushAll(ctx)

	dir := t.TempDir()
	res, err := Run(ctx, s3, Options{Bucket: "fbs-raw", Prefix: "raw-failures",
		Parsers:     map[string]string{"timeline": "timeline", "timeline_alt": "timeline", "comments": "comments", "replies": "replies"},
		FixturesDir: dir, Name: "brand", Samples: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v timeline=%+v comments=%+v", res, res.Queries["timeline"], res.Queries["comments"])
	tlr, cmr := res.Queries["timeline"], res.Queries["comments"]
	if tlr.Drift != 1 || tlr.Missing["story.creation_time"] == 0 || len(tlr.Fixtures) != 1 || !res.Drifting() {
		t.Fatalf("timeline replay: %+v", tlr)
	}
	if cmr.OK != 1 || cmr.Skipped != 1 || cmr.Drift != 0 || cmr.Classes["rate_limited"] != 1 {
		t.Fatalf("comments replay: %+v", cmr)
	}
	// The fixture is in the parser test layout and scrubbed.
	b, err := os.ReadFile(filepath.Join(tlr.Fixtures[0], "response.ndjson"))
	if err != nil || len(b) == 0 {
		t.Fatalf("fixture: %v", err)
	}
	// The golden tests read the parse context from meta.json.
	mb, err := os.ReadFile(filepath.Join(tlr.Fixtures[0], "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		PageID int64 `json:"page_id"`
	}
	if json.Unmarshal(mb, &meta) != nil || meta.PageID != page.ID {
		t.Fatalf("meta %s", mb)
	}
	// Queries sharing a parser, and a second run, never overwrite fixtures.
	alt := res.Queries["timeline_alt"]
	if len(alt.Fixtures) != 1 || alt.Fixtures[0] == tlr.Fixtures[0] {
		t.Fatalf("shared parser fixtures: %v %v", tlr.Fixtures, alt.Fixtures)
	}
	again, err := Run(ctx, s3, Options{Bucket: "fbs-raw", Prefix: "raw-failures", Parsers: map[string]string{"timeline": "timeline"},
		Queries: []string{"timeline"}, FixturesDir: dir, Name: "brand", Samples: 1})
	if err != nil || len(again.Queries["timeline"].Fixtures) != 1 || again.Queries["timeline"].Fixtures[0] == tlr.Fixtures[0] {
		t.Fatalf("a second run overwrote a fixture: %v %+v", err, again.Queries["timeline"])
	}
}

func TestParseContextFromVars(t *testing.T) {
	cc := parseContext(map[string]any{"page_id": "42", "feedback_id": "ZmVlZGJhY2s6MTJfMzQ"}) // feedback:12_34, unpadded
	if cc.PageID != 42 || cc.PostID != 12 || cc.ParentCommentID != 34 {
		t.Fatalf("%+v", cc)
	}
}
