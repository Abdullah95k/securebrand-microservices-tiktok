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

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scrub"
)

var update = flag.Bool("update", false, "rewrite expected.json golden files")

const root = "../../../testdata/tt"

func pj(m proto.Message) json.RawMessage {
	if m == nil || !m.ProtoReflect().IsValid() {
		return json.RawMessage("null")
	}
	b, _ := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	var out bytes.Buffer
	_ = json.Compact(&out, b)
	return out.Bytes()
}

func pjs[T proto.Message](ms []T) []json.RawMessage {
	out := []json.RawMessage{}
	for _, m := range ms {
		out = append(out, pj(m))
	}
	return out
}

// key is a fixed author key for the goldens: a hash of the id, so a person
// keeps one key across fixtures.
func key(id int64) []byte { return []byte{byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id)} }

var opts = parse.Options{AuthorKey: key, Query: "golden", Variant: "fixture"}

// TestGolden runs the parser of each fixture's kind over every fixture in
// testdata/tt and compares the output with its expected.json. Every parser
// version must pass every fixture, old shapes included. The fixtures are
// scrubbed real answers (internal/tt/scrub): their ids are pseudonyms that
// keep their time and int64 range.
func TestGolden(t *testing.T) {
	n := 0
	for _, k := range scrub.Kinds {
		cases, _ := os.ReadDir(filepath.Join(root, string(k)))
		for _, c := range cases {
			dir := filepath.Join(root, string(k), c.Name())
			n++
			t.Run(string(k)+"/"+c.Name(), func(t *testing.T) {
				m, err := scrub.ReadMeta(dir)
				if err != nil {
					t.Fatal(err)
				}
				if !m.Synthetic() && (!m.Scrubbed || m.Scrubber < scrub.Version) {
					t.Fatal("fixture not scrubbed by this scrubber version; run tksctl fixtures scrub")
				}
				if m.Kind != k {
					t.Fatalf("meta.json says kind %q, the directory %q", m.Kind, k)
				}
				body, err := os.ReadFile(filepath.Join(dir, k.File()))
				if err != nil {
					t.Fatal(err)
				}
				got, rep := run(k, body)
				got["missing"], got["optional_missing"] = rep.Missing, rep.Optional
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
					t.Fatalf("missing expected.json (run go test ./internal/tt/parse -run Golden -update): %v", err)
				}
				if !bytes.Equal(bytes.TrimSpace(want), bytes.TrimSpace(gb)) {
					t.Fatalf("output differs from %s; run go test ./internal/tt/parse -run Golden -update and review the diff\n got: %s", exp, gb)
				}
			})
		}
	}
	if n == 0 {
		t.Fatal("no fixtures found")
	}
}

func run(k scrub.Kind, body []byte) (map[string]any, parse.Report) {
	switch k {
	case scrub.ProfilePage, scrub.UserDetail:
		var res parse.AccountResult
		var rep parse.Report
		if k == scrub.ProfilePage {
			res, rep = parse.ProfilePage(body, opts)
		} else {
			res, rep = parse.UserDetail(body, opts)
		}
		return map[string]any{"account": pj(res.Account), "media": pjs(res.Media), "status": res.Status.String(),
			"status_code": res.StatusCode, "dropped": res.Dropped}, rep
	case scrub.PostPage:
		res, rep := parse.PostPage(body, opts)
		out := map[string]any{"account": pj(res.Account), "status_code": res.StatusCode, "dropped": res.Dropped}
		if res.Post != nil {
			out["post"] = bundle(*res.Post)
		}
		return out, rep
	case scrub.PostList, scrub.CreatorList:
		res, rep := parse.PostList(body, opts)
		var posts []any
		for _, b := range res.Posts {
			posts = append(posts, bundle(b))
		}
		return map[string]any{"posts": posts, "account": pj(res.Account), "account_media": pjs(res.AccountMedia),
			"has_more": res.HasMore, "cursor": res.Cursor, "filtered": res.Filtered, "status_code": res.StatusCode,
			"dropped": res.Dropped}, rep
	case scrub.Comments, scrub.Replies:
		res, rep := parse.Comments(body, parse.CommentContext{Replies: k == scrub.Replies}, opts)
		var comments []any
		for _, b := range res.Comments {
			comments = append(comments, map[string]any{"comment": pj(b.Comment), "media": pjs(b.Media)})
		}
		return map[string]any{"comments": comments, "threads": res.Threads, "total": res.Total, "has_more": res.HasMore,
			"cursor": res.Cursor, "status_code": res.StatusCode, "dropped": res.Dropped}, rep
	}
	panic("no parser for kind " + string(k))
}

func bundle(b parse.PostBundle) map[string]any {
	return map[string]any{"post": pj(b.Post), "metrics": pj(b.Metrics), "music": pj(b.Music), "media": pjs(b.Media)}
}
