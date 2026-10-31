package spike

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

// flagged serves the timeline with two provider flags on, as Facebook's
// real template has.
type flagged struct {
	registry.Registry
	timeline registry.Query
}

func (f flagged) Get(name string) (registry.Query, error) {
	if name == "timeline" {
		return f.timeline, nil
	}
	return f.Registry.Get(name)
}

func TestTrimAgainstFakeFacebook(t *testing.T) {
	r, env, out := runner(t, fakefb.Faults{})
	q, err := env.GQL.Registry.Get("timeline")
	if err != nil {
		t.Fatal(err)
	}
	q.Variables, err = setVariables(q.Variables, map[string]any{providerPrefix + "Heavy": true, providerPrefix + "Light": true, providerPrefix + "Off": false})
	if err != nil {
		t.Fatal(err)
	}
	c := *env.GQL
	c.Registry = flagged{Registry: env.GQL.Registry, timeline: q}
	r.GQL = &c

	recs := r.Trim(context.Background(), []string{env.World.Pages()[0].Vanity},
		TrimOptions{Each: true, TimelineCount: 10, TimelineCounts: []int{3, 10, 25}, CommentCounts: []int{25}})
	var got []string
	for _, rec := range recs {
		got = append(got, rec.Query+"/"+rec.Variant)
		if rec.Class != "ok" {
			t.Errorf("%s/%s: %s %s", rec.Query, rec.Variant, rec.Class, rec.Error)
		}
	}
	want := "timeline/baseline timeline/flags_off timeline/off:Heavy timeline/off:Light timeline/count=3 timeline/count=25 " +
		"comments/baseline comments/after_count=25 comments_newest/baseline comments_newest/after_count=25"
	if strings.Join(got, " ") != want {
		t.Fatalf("requests:\n got %s\nwant %s", strings.Join(got, " "), want)
	}

	// The records are counts and field names only, and read back.
	back, err := readTrim(bytes.NewReader(out.Bytes()))
	if err != nil || len(back) != len(recs) {
		t.Fatalf("read back %d of %d: %v", len(back), len(recs), err)
	}
	allowed := map[string]bool{"type": true, "at": true, "tier": true, "page": true, "query": true, "variant": true, "class": true,
		"status": true, "bytes": true, "wire_bytes": true, "items": true, "missing": true, "optional": true, "error": true}
	for _, line := range bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n")) {
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatal(err)
		}
		for k := range m {
			if !allowed[k] {
				t.Fatalf("record carries %q: %s", k, line)
			}
		}
	}

	rows := TrimSummary(recs)
	text := FormatTrim(rows)
	t.Log(text)
	base, _ := findRow(rows, "timeline", BaselineVariant)
	small, _ := findRow(rows, "timeline", "count=3")
	big, _ := findRow(rows, "timeline", "count=25")
	// A smaller page is the same data over more requests: judged on cost
	// per item, not on items per request.
	if base.MedianItems != 10 || small.MedianItems != 3 || !small.Usable(base) || !big.Usable(base) || big.MedianItems <= base.MedianItems {
		t.Fatalf("page sizes: base %+v small %+v big %+v", base, small, big)
	}
	if strings.Contains(text, "NOT usable") {
		t.Fatalf("summary:\n%s", text)
	}
}

func TestTrimSummary(t *testing.T) {
	recs := []TrimRecord{
		{Type: "trim", Query: "timeline", Variant: BaselineVariant, Class: "ok", Wire: 1000, Items: 10, Optional: []string{"story.views"}},
		{Type: "trim", Query: "timeline", Variant: BaselineVariant, Class: "ok", Wire: 1200, Items: 10},
		{Type: "trim", Query: "timeline", Variant: "flags_off", Class: "ok", Wire: 400, Items: 10, Optional: []string{"story.reactions"}},
		{Type: "trim", Query: "timeline", Variant: "flags_off", Class: "ok", Wire: 500, Items: 10},
		{Type: "trim", Query: "timeline", Variant: "off:Broken", Class: "ok", Wire: 300, Items: 10, Missing: []string{"story.creation_time"}},
		{Type: "trim", Query: "timeline", Variant: "off:Refused", Class: "soft_error", Wire: 100},
		{Type: "trim", Query: "timeline", Variant: "off:Fewer", Class: "ok", Wire: 300, Items: 6},
	}
	rows := TrimSummary(recs)
	if rows[0].Variant != BaselineVariant {
		t.Fatalf("baseline first: %+v", rows)
	}
	base := rows[0]
	off, _ := findRow(rows, "timeline", "flags_off")
	if base.WirePerItem != 110 || off.WirePerItem != 45 || off.Delta > -0.59 || off.Delta < -0.60 {
		t.Fatalf("cost per item: base %+v off %+v", base, off)
	}
	// Lost optional fields are those the baseline had.
	if len(off.LostOptional) != 1 || off.LostOptional[0] != "story.reactions" || !off.Usable(base) {
		t.Fatalf("flags_off: %+v", off)
	}
	broken, _ := findRow(rows, "timeline", "off:Broken")
	refused, _ := findRow(rows, "timeline", "off:Refused")
	fewer, _ := findRow(rows, "timeline", "off:Fewer")
	if broken.Usable(base) || refused.Usable(base) || refused.OK != 0 || fewer.Usable(base) {
		t.Fatalf("unusable variants: %+v %+v %+v", broken, refused, fewer)
	}
	text := FormatTrim(rows)
	for _, want := range []string{"missing story.creation_time", "1 of 1 requests not ok", "NOT usable: fewer items", "loses optional story.reactions", "-59%"} {
		if !strings.Contains(text, want) {
			t.Errorf("summary lacks %q:\n%s", want, text)
		}
	}
}
