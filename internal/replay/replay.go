// Package replay parses captured raw Facebook responses (rawsink objects in
// the raw bucket) again with the current parsers. After a parser fix it
// shows, on the very responses that drifted in production, whether they
// parse now, before anything is deployed; it also turns drifted responses
// into scrubbed parser fixtures. It never contacts Facebook.
package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/rawsink"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fixtures"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

// ShadowPrefix prefixes the query of shadow-parser captures.
const ShadowPrefix = "shadow-"

// Options selects what to replay.
type Options struct {
	Bucket string
	// Prefix is "raw-failures" (drifted and failed responses) or "raw"
	// (everything, with fb.capture_raw: all).
	Prefix string
	// Parsers maps query names to their parser (timeline, comments,
	// replies); queries not listed are skipped. A shadow parser's captures
	// are under "shadow-<query>" and replay with that query's parser.
	Parsers map[string]string
	// Queries restricts the replay; empty = every query in Parsers.
	Queries []string
	// Since skips objects written before it.
	Since time.Time
	// MaxObjects caps the objects read (newest last); 0 = no cap.
	MaxObjects int
	// FixturesDir, when set, receives up to Samples still-drifting
	// responses per query as scrubbed fixtures named Name.
	FixturesDir string
	Name        string
	Samples     int
}

// QueryResult is the outcome for one query.
type QueryResult struct {
	Lines int `json:"lines"`
	OK    int `json:"ok"`    // parse now with every required field
	Drift int `json:"drift"` // still miss required fields
	// Skipped responses were not parseable answers (rate limits, walls,
	// transport errors captured as failures).
	Skipped  int            `json:"skipped"`
	Classes  map[string]int `json:"classes"`           // class when captured
	Missing  map[string]int `json:"missing,omitempty"` // required fields still missing
	Fixtures []string       `json:"fixtures,omitempty"`
}

// Result is a whole replay.
type Result struct {
	Objects int                     `json:"objects"`
	Lines   int                     `json:"lines"`
	Queries map[string]*QueryResult `json:"queries"`
}

// Drifting reports whether any replayed response still drifts.
func (r Result) Drifting() bool {
	for _, q := range r.Queries {
		if q.Drift > 0 {
			return true
		}
	}
	return false
}

// Run replays the selected objects.
func Run(ctx context.Context, s3 *minio.Client, o Options) (Result, error) {
	res := Result{Queries: map[string]*QueryResult{}}
	queries := o.Queries
	if len(queries) == 0 {
		for q := range o.Parsers {
			queries = append(queries, q)
		}
	}
	sort.Strings(queries)
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return res, err
	}
	defer dec.Close()
	for _, q := range queries {
		parser, ok := o.Parsers[strings.TrimPrefix(q, ShadowPrefix)]
		if !ok {
			return res, fmt.Errorf("replay: unknown query %q", q)
		}
		var keys []string
		for obj := range s3.ListObjects(ctx, o.Bucket, minio.ListObjectsOptions{Prefix: o.Prefix + "/" + q + "/", Recursive: true}) {
			if obj.Err != nil {
				return res, fmt.Errorf("replay: list: %w", obj.Err)
			}
			if !o.Since.IsZero() && obj.LastModified.Before(o.Since) {
				continue
			}
			keys = append(keys, obj.Key)
		}
		sort.Strings(keys) // keys sort by hour, then pod and sequence
		if o.MaxObjects > 0 && len(keys) > o.MaxObjects {
			keys = keys[len(keys)-o.MaxObjects:]
		}
		qr := &QueryResult{Classes: map[string]int{}, Missing: map[string]int{}}
		res.Queries[q] = qr
		for _, k := range keys {
			if err := replayObject(ctx, s3, dec, o, k, q, parser, qr); err != nil {
				return res, err
			}
			res.Objects++
		}
		res.Lines += qr.Lines
	}
	return res, nil
}

func replayObject(ctx context.Context, s3 *minio.Client, dec *zstd.Decoder, o Options, key, query, parser string, qr *QueryResult) error {
	obj, err := s3.GetObject(ctx, o.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("replay: %s: %w", key, err)
	}
	defer func() { _ = obj.Close() }()
	if err := dec.Reset(obj); err != nil {
		return fmt.Errorf("replay: %s: %w", key, err)
	}
	sc := bufio.NewScanner(dec)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		var l rawsink.Line
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return fmt.Errorf("replay: %s: %w", key, err)
		}
		qr.Lines++
		qr.Classes[l.Class]++
		if l.Class != detect.OK.String() && l.Class != detect.SchemaDrift.String() {
			qr.Skipped++
			continue
		}
		body := []byte(l.Body)
		if len(body) == 0 {
			body = []byte(l.Text)
		}
		rep := Parse(parser, body, l.Vars)
		if rep.OK() {
			qr.OK++
			continue
		}
		qr.Drift++
		for _, m := range rep.Missing {
			qr.Missing[m]++
		}
		if o.FixturesDir != "" && len(qr.Fixtures) < max(o.Samples, 1) {
			d, err := writeFixture(o, query, parser, key, l, body, rep)
			if err != nil {
				return err
			}
			qr.Fixtures = append(qr.Fixtures, d)
		}
	}
	return sc.Err()
}

// writeFixture stores a still-drifting response in the parser test layout:
// <dir>/<parser>/<date>_<name>-<query>-<n>, never over an existing one, with
// the meta keys the golden tests read for context.
func writeFixture(o Options, query, parser, key string, l rawsink.Line, body []byte, rep parse.Report) (string, error) {
	name := o.Name
	if name == "" {
		name = "replay"
	}
	ctx := parseContext(l.Vars)
	var keep []string
	if ctx.PageID != 0 {
		keep = append(keep, strconv.FormatInt(ctx.PageID, 10))
	}
	base := fmt.Sprintf("%s_%s-%s", l.At.Format("2006-01-02"), name, query)
	caseName := base + "-1"
	for n := 2; ; n++ {
		if _, err := os.Stat(filepath.Join(o.FixturesDir, parser, caseName)); os.IsNotExist(err) {
			break
		}
		caseName = fmt.Sprintf("%s-%d", base, n)
	}
	return fixtures.Write(o.FixturesDir, parser, caseName, "response.ndjson", body, map[string]any{
		"page_id": ctx.PageID, "post_id": ctx.PostID, "parent_comment_id": ctx.ParentCommentID,
		"query": query, "class": l.Class, "missing": rep.Missing, "replayed_from": key, "vars": l.Vars}, keep)
}

// parseContext derives the parse context from the request's template values.
func parseContext(vars map[string]any) parse.CommentContext {
	post, comment := parse.FeedbackTarget(stringVar(vars, "feedback_id"))
	return parse.CommentContext{PageID: intVar(vars, "page_id"), PostID: post, ParentCommentID: comment}
}

// Parse runs parser on a raw response with the request's template values
// as context and returns the parser's report.
func Parse(parser string, body []byte, vars map[string]any) parse.Report {
	parts := graphql.SplitParts(body)
	cc := parseContext(vars)
	switch parser {
	case "timeline":
		_, rep := parse.Timeline(parts, cc.PageID, "https://www.facebook.com")
		return rep
	case "comments":
		_, rep := parse.Comments(parts, parse.CommentContext{PostID: cc.PostID, PageID: cc.PageID})
		return rep
	case "replies":
		_, rep := parse.Comments(parts, parse.CommentContext{PageID: cc.PageID, PostID: cc.PostID,
			ParentCommentID: cc.ParentCommentID, BaseDepth: 1, Replies: true})
		return rep
	}
	return parse.Report{Missing: []string{"unknown parser " + parser}}
}

func stringVar(vars map[string]any, k string) string {
	s, _ := vars[k].(string)
	return s
}

func intVar(vars map[string]any, k string) int64 {
	switch v := vars[k].(type) {
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	}
	return 0
}
