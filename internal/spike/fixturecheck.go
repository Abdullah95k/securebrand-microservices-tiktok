package spike

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

// FixtureCheck is the parse result of one captured fixture.
type FixtureCheck struct {
	Query    string   `json:"query"`
	Dir      string   `json:"dir"`
	Items    int      `json:"items"` // pages, posts or comments parsed
	Missing  []string `json:"missing_required,omitempty"`
	Optional []string `json:"missing_optional,omitempty"`
	Err      string   `json:"error,omitempty"`
}

// OK reports whether the parser understood the fixture: no required field
// is missing and something was parsed. An unrecognised shape usually parses
// to nothing rather than to missing fields.
func (c FixtureCheck) OK() bool { return c.Err == "" && len(c.Missing) == 0 && c.Items > 0 }

type fixtureMeta struct {
	PageID          int64 `json:"page_id"`
	PostID          int64 `json:"post_id"`
	ParentCommentID int64 `json:"parent_comment_id"`
	// IDFromRef marks a page whose HTML names no id: it was captured by
	// its numeric id, which stands in for it (as the crawler's does).
	IDFromRef bool `json:"id_from_ref"`
}

// CheckFixtures runs the parsers over every fixture under root
// (root/<query>/<case>/), the same way the golden tests do, so drift is
// visible without a Go toolchain.
func CheckFixtures(root string) ([]FixtureCheck, error) {
	queries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var out []FixtureCheck
	for _, q := range queries {
		if !q.IsDir() {
			continue
		}
		cases, err := os.ReadDir(filepath.Join(root, q.Name()))
		if err != nil {
			return nil, err
		}
		for _, c := range cases {
			if c.IsDir() {
				out = append(out, checkFixture(q.Name(), filepath.Join(root, q.Name(), c.Name())))
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Dir < out[j].Dir })
	return out, nil
}

func checkFixture(query, dir string) FixtureCheck {
	res := FixtureCheck{Query: query, Dir: dir}
	var m fixtureMeta
	if b, err := os.ReadFile(filepath.Join(dir, "meta.json")); err == nil {
		_ = json.Unmarshal(b, &m)
	}
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			res.Err = err.Error()
		}
		return b
	}
	var rep parse.Report
	switch query {
	case "page_html":
		html := read("response.html")
		if res.Err != "" {
			return res
		}
		var requested int64
		if m.IDFromRef {
			requested = m.PageID
		}
		pg, r := parse.Page(html, 0, requested)
		if rep = r; pg != nil {
			res.Items = 1
		}
	case "timeline":
		body := read("response.ndjson")
		if res.Err != "" {
			return res
		}
		var tl parse.TimelineResult
		tl, rep = parse.Timeline(graphql.SplitParts(body), m.PageID, "https://www.facebook.com")
		res.Items = len(tl.Posts)
		if tl.NoFeed {
			res.Err = "no feed: the page id names something without a timeline (a new-style page's older Page id?)"
		}
	case "comments", "replies":
		body := read("response.ndjson")
		if res.Err != "" {
			return res
		}
		cc := parse.CommentContext{PostID: m.PostID, PageID: m.PageID}
		if query == "replies" {
			cc.Replies, cc.BaseDepth, cc.ParentCommentID, cc.RootCommentID = true, 1, m.ParentCommentID, m.ParentCommentID
		}
		var cr parse.CommentsResult
		cr, rep = parse.Comments(graphql.SplitParts(body), cc)
		res.Items = len(cr.Comments)
	default:
		res.Err = "no parser for query " + query
		return res
	}
	res.Missing, res.Optional = rep.Missing, rep.Optional
	return res
}

// FormatFixtureChecks renders the checks, one line per fixture.
func FormatFixtureChecks(cs []FixtureCheck) string {
	var b strings.Builder
	for _, c := range cs {
		status := StatusPass
		if !c.OK() {
			status = StatusFail
		}
		fmt.Fprintf(&b, "%-4s %-9s %s: %d parsed", status, c.Query, c.Dir, c.Items)
		if len(c.Missing) > 0 {
			fmt.Fprintf(&b, ", missing required %v", c.Missing)
		}
		if len(c.Optional) > 0 {
			fmt.Fprintf(&b, ", missing optional %v", c.Optional)
		}
		if c.Err != "" {
			b.WriteString(", " + c.Err)
		}
		b.WriteString("\n")
	}
	return b.String()
}
