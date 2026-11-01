package task

import (
	"testing"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
)

func TestNormalizeRef(t *testing.T) {
	for in, want := range map[string]string{
		"Some.User":   "u:some.user",
		"@some_user":  "u:some_user",
		"u:Some_User": "u:some_user",
		"https://www.tiktok.com/@some.user?lang=en": "u:some.user",
		"tiktok.com/@some.user/video/123":           "u:some.user",
		"sec:MS4wLjABAAAA-x_Y":                      "sec:MS4wLjABAAAA-x_Y",
		"id:0042":                                   "id:42",
	} {
		got, err := NormalizeRef(in)
		if err != nil || got != want {
			t.Errorf("%q: got %q %v, want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "...", "u:", "id:-1", "id:x", "sec:", "sec:a/b", "some user", "../etc", "user%2f"} {
		if got, err := NormalizeRef(in); err == nil {
			t.Errorf("%q: accepted as %q", in, got)
		}
	}
}

func TestScopeFromRef(t *testing.T) {
	if s := ScopeFromRef("u:name"); s.Username != "name" || s.AccountRef != "u:name" {
		t.Errorf("u: %v", s)
	}
	if s := ScopeFromRef("sec:abc"); s.SecUid != "abc" {
		t.Errorf("sec: %v", s)
	}
	if s := ScopeFromRef("id:7"); s.UserId != 7 {
		t.Errorf("id: %v", s)
	}
}

// Ids are deterministic per job, scope, mode and segment; an account's
// stream key moves from its reference to its user id once known, and a
// continuation keeps the lineage.
func TestIDs(t *testing.T) {
	lim := &tkstaskv1.Limits{}
	a := NewAccount("job", tkstaskv1.Lane_LANE_P1, commonv1.CrawlMode_CRAWL_MODE_FULL, ScopeFromRef("u:name"), lim)
	b := NewAccount("job", tkstaskv1.Lane_LANE_P1, commonv1.CrawlMode_CRAWL_MODE_FULL, ScopeFromRef("u:name"), lim)
	if a.TaskId != b.TaskId || a.LineageId != b.LineageId || ScopeKey(a) != "accountref:u:name" {
		t.Fatalf("ids differ: %s %s, key %s", a.TaskId, b.TaskId, ScopeKey(a))
	}
	a.GetAccount().UserId = 9
	next := Continuation(a, "123")
	if next.LineageId != a.LineageId || next.Seq != 1 || next.TaskId == a.TaskId || ScopeKey(next) != "account:9" || next.Cursor != "123" {
		t.Fatalf("continuation %v", next)
	}
	if r := Retry(next, ""); r.TaskId != next.TaskId || r.Attempt != 1 || r.Cursor != "123" {
		t.Fatalf("retry %v", r)
	}
	p := NewPost(a, commonv1.CrawlMode_CRAWL_MODE_FULL, &tkstaskv1.PostScope{PostId: 5})
	if p.Kind != tkstaskv1.Kind_KIND_POST || ScopeKey(p) != "post:5" || p.JobId != "job" {
		t.Fatalf("post task %v", p)
	}
}

// Media downloads follow tt.media.include, off by default (D5), whatever
// the shared limits say; the age window is tt.freshness.backfill_days.
func TestDefaultLimits(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Limits.IncludeMedia, cfg.Limits.IncludeCommentMedia = true, true
	l := DefaultLimits(cfg.Limits, cfg.TT, true)
	if l.IncludeMedia || l.IncludeCommentMedia {
		t.Errorf("media on by default: %v", l)
	}
	if l.MaxAgeSeconds != 365*86400 || l.MaxCommentsPerPost != cfg.TT.Comments.MaxPerPost || !l.IncludeReplies {
		t.Errorf("limits %v", l)
	}
	cfg.TT.Media.Include = true
	if l := DefaultLimits(cfg.Limits, cfg.TT, false); !l.IncludeMedia || !l.IncludeCommentMedia {
		t.Errorf("the switch did not turn media on: %v", l)
	}
}
