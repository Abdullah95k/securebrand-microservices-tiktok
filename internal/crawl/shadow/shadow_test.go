package shadow

import (
	"context"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/fbtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

type sink struct{ vars []map[string]any }

func (s *sink) Capture(_ context.Context, _ string, _ detect.Class, _ []byte, vars map[string]any) {
	s.vars = append(s.vars, vars)
}

func timeline(t *testing.T) (*graphql.Response, parse.TimelineResult, int64) {
	t.Helper()
	env := fbtest.New(t, fbtest.Options{})
	ctx := context.Background()
	id, _ := env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"page_id": p.ID, "count": 3}})
	if err != nil {
		t.Fatal(err)
	}
	tl, rep := parse.Timeline(resp.Parts, p.ID, env.HTTP.URL)
	if !rep.OK() || len(tl.Posts) == 0 {
		t.Fatal(rep)
	}
	return resp, tl, p.ID
}

func TestShadowOutcomes(t *testing.T) {
	resp, _, pageID := timeline(t)
	raw := &sink{}
	always := func() float64 { return 0 }
	base := "http://fakefb"

	// Compare with the base URL the candidates are given.
	active, _ := parse.Timeline(resp.Parts, pageID, base)
	same := &Set{Rate: 1, sample: always, Raw: raw, Timeline: parse.Timeline}
	if got := same.CompareTimeline("timeline", resp, active, pageID, base); got != Same {
		t.Fatalf("identical parser: %s", got)
	}

	dropsOne := &Set{Rate: 1, sample: always, Raw: raw, Timeline: func(parts []gjson.Result, id int64, b string) (parse.TimelineResult, parse.Report) {
		r, rep := parse.Timeline(parts, id, b)
		r.Posts = r.Posts[1:]
		return r, rep
	}}
	if got := dropsOne.CompareTimeline("timeline", resp, active, pageID, base); got != Differs {
		t.Fatalf("a candidate that loses a post: %s", got)
	}
	drifts := &Set{Rate: 1, sample: always, Raw: raw, Timeline: func(parts []gjson.Result, id int64, b string) (parse.TimelineResult, parse.Report) {
		return parse.TimelineResult{}, parse.Report{Missing: []string{"story.post_id"}}
	}}
	if got := drifts.CompareTimeline("timeline", resp, active, pageID, base); got != Drift {
		t.Fatalf("a drifting candidate: %s", got)
	}
	// Differences are captured with the request's vars for replay.
	if len(raw.vars) != 2 || raw.vars[0]["shadow"] != Differs || raw.vars[1]["shadow_missing"] == nil || raw.vars[0]["page_id"] == nil {
		t.Fatalf("captures %+v", raw.vars)
	}

	// Not sampled, nil, or without a candidate: nothing runs.
	never := &Set{Rate: 0.5, sample: func() float64 { return 0.9 }, Timeline: parse.Timeline}
	var none *Set
	if never.CompareTimeline("timeline", resp, active, pageID, base) != "" || none.CompareTimeline("timeline", resp, active, pageID, base) != "" {
		t.Fatal("ran without being sampled")
	}
	if Configured(0.05, nil) != nil {
		t.Fatal("no candidate registered: Configured must be nil")
	}
}
