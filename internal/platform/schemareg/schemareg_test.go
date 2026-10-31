package schemareg

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	protosrc "github.com/abdullah95k/securebrand-microservices-igs/api/proto"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

func TestImports(t *testing.T) {
	got := imports("syntax = \"proto3\";\nimport \"google/protobuf/timestamp.proto\";\nimport public \"a/b.proto\";\n  import \"c.proto\" ;\n// import \"x.proto\";\n")
	if strings.Join(got, ",") != "a/b.proto,c.proto" {
		t.Fatalf("%v", got)
	}
}

func registry(t *testing.T) *Client {
	t.Helper()
	u := os.Getenv("IGS_TEST_SCHEMA_REGISTRY")
	if u == "" {
		t.Skip("IGS_TEST_SCHEMA_REGISTRY not set")
	}
	return &Client{URL: u}
}

// copyFS returns the embedded contract as a mutable map.
func copyFS(t *testing.T) fstest.MapFS {
	t.Helper()
	m := fstest.MapFS{}
	err := fs.WalkDir(protosrc.FS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := fs.ReadFile(protosrc.FS, p)
		m[p] = &fstest.MapFile{Data: b}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func edit(t *testing.T, m fstest.MapFS, file, old, new string) {
	t.Helper()
	s := string(m[file].Data)
	if !strings.Contains(s, old) {
		t.Fatalf("%s lacks %q", file, old)
	}
	m[file] = &fstest.MapFile{Data: []byte(strings.Replace(s, old, new, 1))}
}

// cleanup deletes the test's subjects, those referencing others first.
func cleanup(t *testing.T, c *Client, prefix string) {
	t.Helper()
	subjects, _ := c.Subjects(context.Background())
	sort.Slice(subjects, func(i, j int) bool {
		return strings.HasSuffix(subjects[i], "-value") && !strings.HasSuffix(subjects[j], "-value")
	})
	for _, s := range subjects {
		if strings.HasPrefix(s, prefix) {
			if err := c.DeleteSubject(context.Background(), s); err != nil {
				t.Errorf("cleanup %s: %v", s, err)
			}
		}
	}
}

func version(t *testing.T, res []Result, subject string) int {
	t.Helper()
	for _, r := range res {
		if r.Subject == subject {
			return r.Version
		}
	}
	t.Fatalf("no result for %s in %+v", subject, res)
	return 0
}

func TestApplyGatesIncompatibleChanges(t *testing.T) {
	c := registry(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("t%d.", rand.Int64N(1<<40))
	files := copyFS(t)
	t.Cleanup(func() { cleanup(t, c, prefix) })
	res, err := Apply(ctx, c, files, prefix, topics.ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]int{}
	for _, r := range res {
		ids[r.Subject] = r.ID
		if r.Version != 1 {
			t.Fatalf("%s: version %d", r.Subject, r.Version)
		}
	}
	if len(ids) != len(topics.ValueSchemas)+1 { // every topic, and common.proto
		t.Fatalf("subjects %v", ids)
	}
	if lvl, err := c.Compatibility(ctx, prefix+topics.Comment+"-value"); err != nil || lvl != BackwardTransitive {
		t.Fatalf("compatibility %q %v", lvl, err)
	}

	// Applying the same contract again changes nothing.
	again, err := Apply(ctx, c, files, prefix, topics.ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range again {
		if r.ID != ids[r.Subject] || r.Version != 1 {
			t.Fatalf("not idempotent: %+v", r)
		}
	}

	// Changing a field's type breaks consumers: the check reports it and
	// apply refuses it.
	broken := copyFS(t)
	edit(t, broken, "securebrand/fb/v1/comment.proto", "string text = 11;", "int64 text = 11;")
	if _, err := Apply(ctx, c, broken, prefix, topics.ValueSchemas, true); err == nil {
		t.Fatal("check accepted a type change")
	}
	if _, err := Apply(ctx, c, broken, prefix, topics.ValueSchemas, false); err == nil || !IsIncompatible(err) {
		t.Fatalf("apply must refuse the type change: %v", err)
	}

	// Adding a field is compatible: a new version.
	grown := copyFS(t)
	edit(t, grown, "securebrand/fb/v1/comment.proto", "string extra_json = 22;", "string extra_json = 22;\n  string language = 1000;")
	if _, err := Apply(ctx, c, grown, prefix, topics.ValueSchemas, true); err != nil {
		t.Fatalf("check refused an added field: %v", err)
	}
	res, err = Apply(ctx, c, grown, prefix, topics.ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	if v := version(t, res, prefix+topics.Comment+"-value"); v != 2 {
		t.Fatalf("comment schema version %d, want 2", v)
	}

	// A breaking change hidden behind a new import is still caught: the
	// check stages the new file under a temporary subject.
	hidden := copyFS(t)
	hidden["securebrand/fb/v1/lang.proto"] = &fstest.MapFile{Data: []byte("syntax = \"proto3\";\npackage securebrand.fb.v1;\nmessage Lang { string code = 1; }\n")}
	edit(t, hidden, "securebrand/fb/v1/comment.proto", `import "securebrand/fb/v1/common.proto";`,
		"import \"securebrand/fb/v1/common.proto\";\nimport \"securebrand/fb/v1/lang.proto\";")
	edit(t, hidden, "securebrand/fb/v1/comment.proto", "string text = 11;", "int64 text = 11;\n  Lang lang = 24;")
	edit(t, hidden, "securebrand/fb/v1/comment.proto", "string extra_json = 22;", "string extra_json = 22;\n  string language = 1000;")
	if _, err := Apply(ctx, c, hidden, prefix, topics.ValueSchemas, true); err == nil {
		t.Fatal("check passed a type change made alongside a new import")
	}

	// A compatible change to the shared import that a dependent uses: the
	// check resolves the dependent against the new import, then apply
	// registers both.
	shared := copyFS(t)
	edit(t, shared, "securebrand/fb/v1/comment.proto", "string extra_json = 22;", "string extra_json = 22;\n  string language = 1000;")
	shared["securebrand/fb/v1/common.proto"] = &fstest.MapFile{Data: append(shared["securebrand/fb/v1/common.proto"].Data,
		[]byte("\nmessage Geo {\n  double lat = 1;\n  double lng = 2;\n}\n")...)}
	edit(t, shared, "securebrand/fb/v1/post.proto", "message Post {", "message Post {\n  Geo geo = 99;")
	if res, err := Apply(ctx, c, shared, prefix, topics.ValueSchemas, true); err != nil {
		t.Fatalf("check refused a compatible change to the shared import: %v %+v", err, res)
	}
	// Nothing is registered when one subject fails, even after compatible
	// ones (the shared import here) passed.
	mixed := copyFS(t)
	for k, v := range shared {
		mixed[k] = v
	}
	edit(t, mixed, "securebrand/fb/v1/job.proto", "message JobEvent {", "message JobEventRenamed {")
	if _, err := Apply(ctx, c, mixed, prefix, topics.ValueSchemas, false); err == nil {
		t.Fatal("apply accepted a removed message")
	}
	if v, _ := c.Lookup(ctx, prefix+"securebrand/fb/v1/common.proto", Schema{Schema: string(shared["securebrand/fb/v1/common.proto"].Data), SchemaType: "PROTOBUF"}); v != 0 {
		t.Fatalf("a failed apply registered the shared import (version %d)", v)
	}
	// An operator's own level is kept.
	if err := c.SetCompatibility(ctx, prefix+topics.Job+"-value", "NONE"); err != nil {
		t.Fatal(err)
	}
	res, err = Apply(ctx, c, shared, prefix, topics.ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	if version(t, res, prefix+"securebrand/fb/v1/common.proto") != 2 || version(t, res, prefix+topics.Post+"-value") != 2 {
		t.Fatalf("shared change not registered: %+v", res)
	}
	if lvl, _ := c.Compatibility(ctx, prefix+topics.Job+"-value"); lvl != "NONE" {
		t.Fatalf("operator level overwritten: %s", lvl)
	}
	// The checks' temporary subjects are gone.
	subjects, _ := c.Subjects(ctx)
	for _, s := range subjects {
		if strings.HasPrefix(s, prefix+"igs-check-") {
			t.Fatalf("temporary subject left: %s", s)
		}
	}
}

// A connection the registry drops is retried once.
func TestRetriesDroppedConnection(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		_, _ = w.Write([]byte(`["a"]`))
	}))
	defer srv.Close()
	c := &Client{URL: srv.URL}
	subjects, err := c.Subjects(context.Background())
	if err != nil || len(subjects) != 1 || calls.Load() != 2 {
		t.Fatalf("%v %v after %d calls", subjects, err, calls.Load())
	}
}

// Every request is marked replayable, so net/http itself retries a POST on
// a kept-alive connection the registry closed while it sat idle.
func TestRequestsAreReplayable(t *testing.T) {
	var key string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key = r.Header.Get("Idempotency-Key")
		_, _ = w.Write([]byte(`{"is_compatible":true}`))
	}))
	defer srv.Close()
	c := &Client{URL: srv.URL}
	if err := c.do(context.Background(), http.MethodPost, "/compatibility/subjects/x/versions/latest", map[string]string{"schema": "s"}, nil); err != nil {
		t.Fatal(err)
	}
	if key == "" {
		t.Fatal("POST without Idempotency-Key: net/http will not replay it")
	}
}
