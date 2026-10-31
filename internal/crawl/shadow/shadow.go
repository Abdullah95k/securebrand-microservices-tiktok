// Package shadow runs a candidate parser next to the active one on a sample
// of live responses, so a new parser version is proven on production
// traffic before it becomes active. Outcomes are counted per query
// (igs_parse_shadow_total: same, differs, drift, error); a response the two
// parse differently is captured raw under raw-failures/shadow-<query>/,
// apart from real drift, for `fbctl replay --query shadow-<query>` and
// fixtures. The active parser's result is what the stage emits either way,
// and a candidate that panics only counts as an error.
package shadow

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

// TimelineParser has parse.Timeline's signature.
type TimelineParser func(parts []gjson.Result, pageID int64, baseURL string) (parse.TimelineResult, parse.Report)

// CommentsParser has parse.Comments' signature.
type CommentsParser func(parts []gjson.Result, cc parse.CommentContext) (parse.CommentsResult, parse.Report)

// Set is the candidate parsers and the share of responses they also see.
// A nil Set, or one without parsers, does nothing.
type Set struct {
	Rate     float64
	Timeline TimelineParser
	Comments CommentsParser // comments and replies
	Raw      graphql.RawSink
	sample   func() float64
}

func (s *Set) take() bool {
	if s == nil || s.Rate <= 0 {
		return false
	}
	f := rand.Float64
	if s.sample != nil {
		f = s.sample
	}
	return f() < s.Rate
}

// Result outcomes.
const (
	Same    = "same"
	Differs = "differs"
	Drift   = "drift" // the candidate misses required fields
	Error   = "error" // the candidate panicked
)

// CompareTimeline parses resp with the candidate when sampled and compares
// its posts with the active parser's.
func (s *Set) CompareTimeline(query string, resp *graphql.Response, active parse.TimelineResult, pageID int64, baseURL string) string {
	if s == nil || s.Timeline == nil || !s.take() {
		return ""
	}
	var cand parse.TimelineResult
	var rep parse.Report
	if err := safely(func() { cand, rep = s.Timeline(resp.Parts, pageID, baseURL) }); err != nil {
		return s.record(query, resp, Error, err.Error())
	}
	if !rep.OK() {
		return s.record(query, resp, Drift, fmt.Sprint(rep.Missing))
	}
	a, b := postKeys(active), postKeys(cand)
	return s.record(query, resp, diff(a, b), "")
}

// safely runs a candidate, turning a panic into an error: a candidate must
// never take the crawler down.
func safely(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("candidate parser panicked: %v", r)
		}
	}()
	fn()
	return nil
}

// CompareComments is CompareTimeline for comments and replies.
func (s *Set) CompareComments(query string, resp *graphql.Response, active parse.CommentsResult, cc parse.CommentContext) string {
	if s == nil || s.Comments == nil || !s.take() {
		return ""
	}
	var cand parse.CommentsResult
	var rep parse.Report
	if err := safely(func() { cand, rep = s.Comments(resp.Parts, cc) }); err != nil {
		return s.record(query, resp, Error, err.Error())
	}
	if !rep.OK() {
		return s.record(query, resp, Drift, fmt.Sprint(rep.Missing))
	}
	return s.record(query, resp, diff(commentKeys(active), commentKeys(cand)), "")
}

func (s *Set) record(query string, resp *graphql.Response, outcome, why string) string {
	metrics.ParseShadow.WithLabelValues(query, outcome).Inc()
	if outcome != Same && s.Raw != nil {
		vars := map[string]any{"shadow": outcome}
		for k, v := range resp.Vars {
			vars[k] = v
		}
		if why != "" {
			vars["shadow_missing"] = why
		}
		s.Raw.Capture(context.Background(), "shadow-"+query, detect.SchemaDrift, resp.Body, vars)
	}
	return outcome
}

// postKeys renders each post as its wire encoding, keyed by id.
func postKeys(r parse.TimelineResult) map[string][]byte {
	out := map[string][]byte{}
	for _, b := range r.Posts {
		buf, _ := proto.MarshalOptions{Deterministic: true}.Marshal(b.Post)
		for _, m := range b.Media {
			mb, _ := proto.MarshalOptions{Deterministic: true}.Marshal(m)
			buf = append(buf, mb...)
		}
		out[fmt.Sprint(b.Post.GetPostId())] = buf
	}
	return out
}

func commentKeys(r parse.CommentsResult) map[string][]byte {
	out := map[string][]byte{}
	for _, b := range r.Comments {
		buf, _ := proto.MarshalOptions{Deterministic: true}.Marshal(b.Comment)
		out[fmt.Sprint(b.Comment.GetCommentId())] = buf
	}
	keys := make([]string, 0, len(r.Threads))
	for _, t := range r.Threads {
		keys = append(keys, fmt.Sprintf("%d:%s:%d", t.CommentID, t.FeedbackID, t.ReplyCount))
	}
	sort.Strings(keys)
	out["threads"] = []byte(fmt.Sprint(keys))
	return out
}

func diff(a, b map[string][]byte) string {
	if len(a) != len(b) {
		return Differs
	}
	for k, v := range a {
		if string(b[k]) != string(v) {
			return Differs
		}
	}
	return Same
}

// Candidates are the parsers under evaluation. A new parser version
// registers itself here (from its file's init) and, once its outcomes stay
// `same`, replaces the active parser and is removed from here.
var Candidates struct {
	Timeline TimelineParser
	Comments CommentsParser
}

// Configured returns the Set of the registered candidates at rate, or nil
// when none is registered or rate is 0.
func Configured(rate float64, raw graphql.RawSink) *Set {
	if rate <= 0 || (Candidates.Timeline == nil && Candidates.Comments == nil) {
		return nil
	}
	return &Set{Rate: rate, Timeline: Candidates.Timeline, Comments: Candidates.Comments, Raw: raw}
}
