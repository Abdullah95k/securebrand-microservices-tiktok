package spike

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/fbtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// A page whose HTML names no id is measured when it was asked for by its
// numeric id, and ends no_page_id when asked for by its vanity.
func TestSpikePageWithoutAnIDInItsHTML(t *testing.T) {
	r, env, out := runner(t, fakefb.Faults{NoPageID: true})
	p := env.World.Pages()[0]
	byID := "id:" + strconv.FormatInt(p.ID, 10)
	r.Opt.Concurrency = 1
	r.Run(context.Background(), []PageRef{{Ref: p.Vanity}, {Ref: byID}})
	ends := map[string]Record{}
	for _, line := range bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n")) {
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Type == "timeline" {
			ends[rec.Page] = rec
		}
	}
	if v := ends["v:"+strings.ToLower(p.Vanity)]; v.EndReason != "no_page_id" {
		t.Fatalf("by vanity: %+v", v)
	}
	if n := ends[byID]; n.EndReason == "no_page_id" || n.Posts == 0 {
		t.Fatalf("by id: %+v", n)
	}
}

// Capture keeps a page whose id it cannot find, with what its HTML holds
// (counts only), and captures the same page asked for by its numeric id.
func TestCaptureKeepsAPageWithoutAnID(t *testing.T) {
	r, env, _ := runner(t, fakefb.Faults{NoPageID: true})
	r.FailDir = t.TempDir()
	p := env.World.Pages()[0]

	dirs, err := r.Capture(context.Background(), p.Vanity, t.TempDir(), "noid")
	if err == nil || !strings.Contains(err.Error(), "page id not found") || len(dirs) != 1 {
		t.Fatalf("dirs %v, err %v", dirs, err)
	}
	if want := filepath.Join(r.FailDir, "page_html", ""); !strings.HasPrefix(dirs[0], want) || !strings.Contains(err.Error(), dirs[0]) {
		t.Fatalf("kept under %s, err %v", dirs[0], err)
	}
	if _, err := os.Stat(filepath.Join(dirs[0], "response.html")); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dirs[0], "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		Class     string         `json:"class"`
		FinalPath string         `json:"final_path"`
		Hints     map[string]int `json:"id_hints"`
		Tokens    map[string]any `json:"tokens_found"`
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Class != "ok" || !strings.EqualFold(meta.FinalPath, "/"+p.Vanity) || len(meta.Hints) == 0 || meta.Tokens["lsd"] != true {
		t.Fatalf("meta %s", b)
	}
	for k, n := range meta.Hints {
		if n != 0 {
			t.Errorf("hint %s = %d on a page with no id", k, n)
		}
	}

	// Without a fail dir, nothing is kept and the error says only that.
	r.FailDir = ""
	if dirs, err := r.Capture(context.Background(), p.Vanity, t.TempDir(), "noid"); err == nil || len(dirs) != 0 {
		t.Fatalf("dirs %v, err %v", dirs, err)
	}

	// The same page by its numeric id is captured, marked as such, and the
	// fixture check reads it with that id.
	dir := t.TempDir()
	dirs, err = r.Capture(context.Background(), strconv.FormatInt(p.ID, 10), dir, "byid")
	if err != nil || len(dirs) == 0 || !strings.HasPrefix(dirs[0], filepath.Join(dir, "page_html")) {
		t.Fatalf("by id: dirs %v, err %v", dirs, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dirs[0], "meta.json")); !bytes.Contains(b, []byte(`"id_from_ref": true`)) {
		t.Fatalf("page meta %s", b)
	}
	checks, err := CheckFixtures(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range checks {
		if c.Query == "page_html" && !c.OK() {
			t.Fatalf("fixture check: %+v", c)
		}
	}
}

// dead is an exit that stopped answering.
type dead struct {
	transport.Doer
	calls *atomic.Int32
}

func (d dead) Do(context.Context, *transport.Request) (*transport.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("net/http: request canceled (Client.Timeout exceeded while awaiting headers)")
}

// A page load that fails in transport retires its identity and is tried on
// a fresh one, so a dead exit loses no pages and gets no more of them.
func TestSpikeRetriesAPageLoadOnAFreshIdentity(t *testing.T) {
	var created, calls atomic.Int32
	env := fbtest.New(t, fbtest.Options{Doers: func(transport.Options) (transport.Doer, error) {
		std := transport.NewStd(10 * time.Second)
		if created.Add(1) == 1 {
			return dead{Doer: std, calls: &calls}, nil
		}
		return std, nil
	}})
	var out bytes.Buffer
	r := &Runner{Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, Out: &out,
		Opt: Options{Tier: "direct", MaxTimelinePages: 2, PostsPerPage: 1, MaxCommentPages: 2, ThreadsPerPost: 2,
			Orderings: []string{"comments"}, Concurrency: 1, PageTimeout: time.Minute}}
	var pages []PageRef
	for _, p := range env.World.Pages() {
		pages = append(pages, PageRef{Ref: p.Vanity})
	}
	r.Run(context.Background(), pages)

	var boots []Record
	for _, line := range bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n")) {
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Type == "bootstrap" {
			boots = append(boots, rec)
		}
	}
	// Every attempt is a bootstrap record: the dead exit's counts as failed.
	if len(boots) != len(pages)+1 || len(pages) < 2 {
		t.Fatalf("bootstrap records: %+v", boots)
	}
	for i, b := range boots {
		ok := b.OK != nil && *b.OK
		if ok != (i > 0) || (i == 0 && !strings.Contains(b.Reason, "transport")) || (i == 1 && b.Page != boots[0].Page) {
			t.Errorf("record %d: %+v", i, b)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("the dead exit was tried %d times", calls.Load())
	}
	s, err := Report(bytes.NewReader(out.Bytes()), DefaultThresholds)
	if err != nil || s.BootstrapSuccess > 0.7 {
		t.Fatalf("bootstrap success %.2f, err %v", s.BootstrapSuccess, err)
	}
}

// A page that loads as a wall is never taken for the page, even when it was
// asked for by its numeric id.
func TestCaptureRefusesAWallLoadedByID(t *testing.T) {
	r, env, _ := runner(t, fakefb.Faults{NoPageID: true})
	r.FailDir = t.TempDir()
	p := env.World.Pages()[0]
	// A bootstrapped identity whose next page load is a wall.
	id, err := r.Pool.Acquire(context.Background(), r.Opt.Tier)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := id.Base.Parse("/" + p.Vanity)
	if _, err := r.Loader.Bootstrap(context.Background(), id, u.String()); err != nil {
		t.Fatal(err)
	}
	r.Pool.Release(id)
	dirs, err := r.Capture(context.Background(), "id:1", t.TempDir(), "gone")
	if !errors.Is(err, session.ErrBootstrap) || len(dirs) != 1 || !strings.HasPrefix(dirs[0], r.FailDir) {
		t.Fatalf("bootstrapped identity: dirs %v, err %v", dirs, err)
	}
	// On a fresh identity too.
	r2, _, _ := runner(t, fakefb.Faults{})
	r2.FailDir = t.TempDir()
	dirs, err = r2.Capture(context.Background(), "id:1", t.TempDir(), "gone")
	if !errors.Is(err, session.ErrBootstrap) || len(dirs) != 1 || !strings.HasPrefix(dirs[0], r2.FailDir) {
		t.Fatalf("fresh identity: dirs %v, err %v", dirs, err)
	}
	b, _ := os.ReadFile(filepath.Join(dirs[0], "meta.json"))
	if !bytes.Contains(b, []byte(`"class": "content_unavailable"`)) {
		t.Fatalf("meta %s", b)
	}
}

// The trim experiment's page load is retried on a fresh identity too.
func TestTrimRetriesAPageLoadOnAFreshIdentity(t *testing.T) {
	var created atomic.Int32
	var calls atomic.Int32
	env := fbtest.New(t, fbtest.Options{Doers: func(transport.Options) (transport.Doer, error) {
		std := transport.NewStd(10 * time.Second)
		if created.Add(1) == 1 {
			return dead{Doer: std, calls: &calls}, nil
		}
		return std, nil
	}})
	var out bytes.Buffer
	r := &Runner{Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, Out: &out, Opt: Options{Tier: "direct"}}
	recs := r.Trim(context.Background(), []string{env.World.Pages()[0].Vanity}, TrimOptions{TimelineCount: 3, CommentCounts: []int{5}})
	if len(recs) == 0 || calls.Load() != 1 {
		t.Fatalf("records %d, dead exit tried %d times", len(recs), calls.Load())
	}
	for _, rec := range recs {
		if rec.Query == "page" || rec.Class != "ok" {
			t.Fatalf("record %+v", rec)
		}
	}
}
