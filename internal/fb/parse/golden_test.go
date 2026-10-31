package parse_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

var update = flag.Bool("update", false, "rewrite expected.json golden files")

type meta struct {
	PageID          int64 `json:"page_id"`
	PostID          int64 `json:"post_id"`
	ParentCommentID int64 `json:"parent_comment_id"`
	Scrubbed        bool  `json:"__scrubbed"`
}

func pj(m proto.Message) json.RawMessage {
	b, _ := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	var out bytes.Buffer
	_ = json.Compact(&out, b)
	return out.Bytes()
}

// TestGolden runs every parser over every fixture in testdata/fb and
// compares the normalised output with expected.json. Every parser version
// must pass every fixture of its query, old shapes included.
func TestGolden(t *testing.T) {
	root := "../../../testdata/fb"
	queries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, q := range queries {
		cases, _ := os.ReadDir(filepath.Join(root, q.Name()))
		for _, c := range cases {
			dir := filepath.Join(root, q.Name(), c.Name())
			t.Run(q.Name()+"/"+c.Name(), func(t *testing.T) {
				var m meta
				mb, err := os.ReadFile(filepath.Join(dir, "meta.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(mb, &m); err != nil {
					t.Fatal(err)
				}
				if !m.Scrubbed {
					t.Fatal("fixture is not marked __scrubbed; run fbctl fixtures scrub")
				}
				got := run(t, q.Name(), dir, m)
				gb, _ := json.MarshalIndent(got, "", "  ")
				exp := filepath.Join(dir, "expected.json")
				if *update {
					if err := os.WriteFile(exp, append(gb, '\n'), 0o644); err != nil {
						t.Fatal(err)
					}
					return
				}
				want, err := os.ReadFile(exp)
				if err != nil {
					t.Fatalf("missing expected.json (run go test -update): %v", err)
				}
				if !bytes.Equal(bytes.TrimSpace(want), bytes.TrimSpace(gb)) {
					t.Fatalf("output differs from %s; run go test ./internal/fb/parse -update and review the diff\n got: %s", exp, gb)
				}
			})
			n++
		}
	}
	if n == 0 {
		t.Fatal("no fixtures found")
	}
}

func run(t *testing.T, query, dir string, m meta) any {
	switch query {
	case "page_html":
		html, _ := os.ReadFile(filepath.Join(dir, "response.html"))
		p, rep := parse.Page(html, 0, 0)
		if !rep.OK() {
			t.Fatalf("required fields missing: %v", rep.Missing)
		}
		return map[string]any{"page": pj(p), "optional_missing": rep.Optional}
	case "timeline":
		body, _ := os.ReadFile(filepath.Join(dir, "response.ndjson"))
		res, rep := parse.Timeline(graphql.SplitParts(body), m.PageID, "https://www.facebook.com")
		if !rep.OK() {
			t.Fatalf("required fields missing: %v", rep.Missing)
		}
		var posts []any
		for _, b := range res.Posts {
			var media []json.RawMessage
			for _, x := range b.Media {
				media = append(media, pj(x))
			}
			posts = append(posts, map[string]any{"post": pj(b.Post), "media": media})
		}
		return map[string]any{"posts": posts, "page_info": res.PageInfo, "optional_missing": rep.Optional}
	case "comments", "replies":
		body, _ := os.ReadFile(filepath.Join(dir, "response.ndjson"))
		cc := parse.CommentContext{PostID: m.PostID, PageID: m.PageID}
		if query == "replies" {
			cc.Replies, cc.BaseDepth, cc.ParentCommentID, cc.RootCommentID = true, 1, m.ParentCommentID, m.ParentCommentID
		}
		res, rep := parse.Comments(graphql.SplitParts(body), cc)
		if !rep.OK() {
			t.Fatalf("required fields missing: %v", rep.Missing)
		}
		var comments []json.RawMessage
		for _, b := range res.Comments {
			comments = append(comments, pj(b.Comment))
		}
		return map[string]any{"comments": comments, "threads": res.Threads, "page_info": res.PageInfo,
			"total_count": res.TotalCount, "optional_missing": rep.Optional}
	}
	t.Fatalf("no parser for fixture query %q", query)
	return nil
}
