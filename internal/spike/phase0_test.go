package spike

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	fbclient "github.com/abdullah95k/securebrand-microservices-igs/internal/fb/client"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/fbtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

const (
	testPassword = "s3cr3t-proxy-pass"
	testUser     = "proxyuser-xyz"
)

func embeddedSeed(t *testing.T) registry.Seed {
	t.Helper()
	b, err := configs.FS.ReadFile("queries.seed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s, err := registry.ParseSeed(b)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testKit(t *testing.T, o fbtest.Options) (*Kit, *fbtest.Env, *bytes.Buffer) {
	t.Helper()
	env := fbtest.New(t, o)
	proxies, err := proxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"ip":"203.0.113.7"}`)
	}))
	t.Cleanup(echo.Close)
	rules := detect.DefaultRules()
	var pages []PageRef
	for i, p := range env.World.Pages() {
		region := "us"
		if i%2 == 1 {
			region = "eu"
		}
		pages = append(pages, PageRef{Ref: p.Vanity, Labels: map[string]string{"region": region, "size": "small"}})
	}
	var log bytes.Buffer
	k := &Kit{
		Stack: Stack{BaseURL: env.HTTP.URL, Proxies: proxies, Pool: env.Pool, Loader: env.Loader, HTTP: http.DefaultClient,
			NewGQL: func(s registry.Seed) *graphql.Client {
				return &graphql.Client{Registry: registry.NewStatic(s), Rules: rules, Builder: fbclient.Builder{AcceptLanguage: "en-US,en;q=0.9"}}
			}},
		Log: &log,
		Cfg: KitConfig{Out: t.TempDir(), Tiers: []string{"direct"}, Pages: pages, BaseSeed: embeddedSeed(t),
			Options: Options{MaxTimelinePages: 50, PostsPerPage: 2, MaxCommentPages: 50, ThreadsPerPost: 200,
				Orderings: []string{"comments", "comments_relevant"}, MediaSamples: 1, Concurrency: 2,
				PageTimeout: time.Minute, RPS: 0.3, Version: "test"},
			CostPerGB: map[string]float64{"direct": 4}, IPEchoURL: echo.URL, Secrets: []string{testPassword, testUser},
			Report: ReportOptions{Thresholds: Thresholds{MinMedianCompleteness: 0.7, MinBootstrapSuccess: 0.9, MaxBlockRate: 0.05, MinMedianDays: 1}}},
	}
	return k, env, &log
}

func bundleFiles(t *testing.T, path string) map[string][]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	out := map[string][]byte{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(tr)
		out[h.Name] = body
	}
	return out
}

// TestPhase0AgainstFakeFacebook runs every step against a fakefb whose
// doc_ids have rotated away from the embedded placeholders, so the run only
// works if discovery finds the new ones.
func TestPhase0AgainstFakeFacebook(t *testing.T) {
	k, _, log := testKit(t, fbtest.Options{RotateDocIDs: true})
	res, err := k.Run(context.Background())
	if err != nil {
		t.Fatalf("phase0: %v\n%s", err, log)
	}
	t.Log("\n" + log.String())
	if !res.Go || res.Recommended != "direct" || len(res.Tiers) != 1 {
		t.Fatalf("expected GO on direct: %+v", res)
	}
	s := res.Tiers[0]
	if s.Pages != len(k.Cfg.Pages) || s.Completeness["comments"].Median != 1 || s.ProjectedMonthlyUSD <= 0 {
		t.Fatalf("summary: %+v", s)
	}
	if len(s.Strata["region"]) != 2 {
		t.Fatalf("strata: %+v", s.Strata)
	}

	reg, err := ReadRegistry(filepath.Join(k.Cfg.Out, FileRegistry))
	if err != nil {
		t.Fatal(err)
	}
	if q := registry.PlaceholderQueries(reg); len(q) != 0 {
		t.Fatalf("registry still has placeholders: %v", q)
	}
	for _, q := range reg.Queries {
		if !strings.HasPrefix(q.DocID, "7316") {
			t.Fatalf("%s: doc_id %s is not the rotated one", q.Name, q.DocID)
		}
	}

	files := bundleFiles(t, filepath.Join(k.Cfg.Out, FileBundle))
	for _, want := range []string{FilePreflight, FileDocIDs, FileRegistry, FileFixtures, FileReport, FileReportTxt, FileMeta,
		SmokeFile("direct"), RunFile("direct")} {
		if _, ok := files["phase0/"+want]; !ok {
			t.Errorf("bundle misses %s", want)
		}
	}
	fixtures := 0
	for name, body := range files {
		if strings.HasPrefix(name, "phase0/fixtures/") {
			fixtures++
		}
		if bytes.Contains(body, []byte(testPassword)) || bytes.Contains(body, []byte(testUser)) {
			t.Errorf("%s contains a credential", name)
		}
	}
	if fixtures < 8 { // meta.json + response for page_html, timeline, comments, replies
		t.Errorf("bundle has %d fixture files", fixtures)
	}

	// Resume: nothing left to measure, results unchanged.
	before, _ := os.ReadFile(filepath.Join(k.Cfg.Out, RunFile("direct")))
	k.Cfg.From = StepRun
	if _, err := k.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(k.Cfg.Out, RunFile("direct")))
	if !bytes.Equal(before, after) {
		t.Fatalf("resume measured pages again (%d -> %d bytes)", len(before), len(after))
	}
}

func TestPhase0StopsWhenDocIDsAreMissing(t *testing.T) {
	k, _, log := testKit(t, fbtest.Options{RotateDocIDs: true})
	k.Cfg.BaseSeed.Queries = append(k.Cfg.BaseSeed.Queries, registry.Query{Name: "extra", FriendlyName: "NoSuchQuery",
		DocID: "9000000000000099", Parser: "comments", Variables: []byte(`{}`)})
	_, err := k.Run(context.Background())
	var se *StepError
	if !errors.As(err, &se) || se.Step != StepDocIDs || !se.NeedsAction || !strings.Contains(err.Error(), "NoSuchQuery") {
		t.Fatalf("want a docids StepError, got %v\n%s", err, log)
	}
	files := bundleFiles(t, filepath.Join(k.Cfg.Out, FileBundle))
	if _, ok := files["phase0/"+FileDocIDs]; !ok {
		t.Fatal("bundle must carry docids.json")
	}
}

func TestPhase0RefusesPlaceholdersAgainstFacebook(t *testing.T) {
	k, _, _ := testKit(t, fbtest.Options{})
	k.Stack.BaseURL = "https://www.facebook.com"
	path := filepath.Join(t.TempDir(), "seed.yaml")
	if err := WriteRegistry(path, embeddedSeed(t), "test"); err != nil {
		t.Fatal(err)
	}
	k.Cfg.Registry, k.Cfg.From = path, StepCapture
	_, err := k.Run(context.Background())
	if !errors.Is(err, ErrPlaceholderDocIDs) {
		t.Fatalf("want ErrPlaceholderDocIDs, got %v", err)
	}
}

func TestPhase0PreflightFailureStops(t *testing.T) {
	k, _, _ := testKit(t, fbtest.Options{})
	k.Cfg.Tiers = []string{"nonexistent"}
	_, err := k.Run(context.Background())
	var se *StepError
	if !errors.As(err, &se) || se.Step != StepPreflight || se.NeedsAction {
		t.Fatalf("want a preflight StepError, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(k.Cfg.Out, FilePreflight)); err != nil {
		t.Fatal("preflight.json must be written")
	}
}

func TestPhase0SmokeStopsWhenBlocked(t *testing.T) {
	k, env, log := testKit(t, fbtest.Options{})
	k.Cfg.From = StepSmoke
	reg := filepath.Join(t.TempDir(), "seed.yaml")
	if err := WriteRegistry(reg, embeddedSeed(t), "test"); err != nil {
		t.Fatal(err)
	}
	k.Cfg.Registry = reg
	env.Fake.SetFaults(fakefb.Faults{RateLimitEvery: 1})
	_, err := k.Run(context.Background())
	var se *StepError
	if !errors.As(err, &se) || se.Step != StepSmoke || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("want a smoke StepError about blocks, got %v\n%s", err, log)
	}
}

// A capture whose GraphQL responses are refused keeps them, scrubbed and
// with what came back, in the bundle: the error alone does not say why.
func TestPhase0CaptureKeepsRefusedResponses(t *testing.T) {
	k, env, log := testKit(t, fbtest.Options{})
	k.Cfg.From = StepCapture
	reg := filepath.Join(t.TempDir(), "seed.yaml")
	if err := WriteRegistry(reg, embeddedSeed(t), "test"); err != nil {
		t.Fatal(err)
	}
	k.Cfg.Registry = reg
	env.Fake.SetFaults(fakefb.Faults{RateLimitEvery: 1})
	_, err := k.Run(context.Background())
	var se *StepError
	if !errors.As(err, &se) || se.Step != StepCapture || !strings.Contains(err.Error(), DirFailures) {
		t.Fatalf("want a capture StepError naming %s, got %v\n%s", DirFailures, err, log)
	}
	if !strings.Contains(log.String(), `rate_limited fb error 1675004; http 200, text/html; charset="utf-8"`) {
		t.Fatalf("the console must say what came back:\n%s", log)
	}
	files := bundleFiles(t, filepath.Join(k.Cfg.Out, FileBundle))
	var metas int
	for name, body := range files {
		if !strings.HasPrefix(name, "phase0/"+DirFailures+"/timeline/") || !strings.HasSuffix(name, "/meta.json") {
			continue
		}
		metas++
		var m struct {
			PageID      int64           `json:"page_id"`
			Status      int             `json:"status"`
			ContentType string          `json:"content_type"`
			Class       string          `json:"class"`
			Tokens      map[string]bool `json:"tokens_found"`
			Cookies     []string        `json:"cookies"`
			Scrubbed    bool            `json:"__scrubbed"`
		}
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatal(err)
		}
		if m.PageID == 0 || m.Status != 200 || !strings.HasPrefix(m.ContentType, "text/html") || m.Class != "rate_limited" ||
			!m.Tokens["lsd"] || !slices.Contains(m.Cookies, "datr") || !m.Scrubbed {
			t.Errorf("%s: %+v", name, m)
		}
		if _, ok := files[strings.TrimSuffix(name, "meta.json")+"response.ndjson"]; !ok {
			t.Errorf("%s: the response itself is missing", name)
		}
	}
	if metas != len(refs(k.Cfg.Pages, k.Cfg.CapturePages)) {
		t.Fatalf("%d refused timelines kept, want one per captured page; bundle: %v", metas, slices.Sorted(maps.Keys(files)))
	}

	// A capture that works again leaves no failures of the earlier one.
	env.Fake.SetFaults(fakefb.Faults{})
	k.Cfg.From = StepCapture
	if _, err := k.Run(context.Background()); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	for name := range bundleFiles(t, filepath.Join(k.Cfg.Out, FileBundle)) {
		if strings.HasPrefix(name, "phase0/"+DirFailures+"/") {
			t.Fatalf("stale failure bundled: %s", name)
		}
	}
}

func TestBundleRefusesSecrets(t *testing.T) {
	k, _, _ := testKit(t, fbtest.Options{})
	if err := os.WriteFile(filepath.Join(k.Cfg.Out, RunFile("direct")), []byte(`{"reason":"`+testPassword+`"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := k.bundle(); !errors.Is(err, ErrSecretInBundle) {
		t.Fatalf("want ErrSecretInBundle, got %v", err)
	}
	if err := os.WriteFile(filepath.Join(k.Cfg.Out, RunFile("direct")), []byte(`{"reason":"proxyconnect http://u:p@1.2.3.4:80"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := k.bundle(); !errors.Is(err, ErrSecretInBundle) {
		t.Fatalf("want ErrSecretInBundle for a URL with credentials, got %v", err)
	}
	_ = os.WriteFile(filepath.Join(k.Cfg.Out, RunFile("direct")), []byte("{}\n"), 0o644)
	// A response kept under failures/ is Facebook content, which can hold
	// such URLs; its meta.json is ours and is still checked.
	kept := filepath.Join(k.Cfg.Out, DirFailures, "timeline", "20261030_page")
	if err := os.MkdirAll(kept, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(kept, "response.html"), []byte(`<a href="https://www.facebook.com?next=a@b">x</a>`), 0o644)
	_ = os.WriteFile(filepath.Join(kept, "meta.json"), []byte(`{"reason":"redirect http://u:p@1.2.3.4:80"}`), 0o644)
	if err := k.bundle(); !errors.Is(err, ErrSecretInBundle) || !strings.Contains(err.Error(), "meta.json") {
		t.Fatalf("want ErrSecretInBundle for the kept meta.json, got %v", err)
	}
	_ = os.WriteFile(filepath.Join(kept, "meta.json"), []byte(`{"source":"capture"}`), 0o644)
	if err := k.bundle(); err != nil {
		t.Fatalf("a kept Facebook response must not stop the bundle: %v", err)
	}
	// Unknown files never enter the bundle.
	_ = os.WriteFile(filepath.Join(k.Cfg.Out, "proxies.yaml"), []byte("password: "+testPassword), 0o600)
	if err := k.bundle(); err != nil {
		t.Fatal(err)
	}
	if _, ok := bundleFiles(t, filepath.Join(k.Cfg.Out, FileBundle))["phase0/proxies.yaml"]; ok {
		t.Fatal("proxies.yaml must not be bundled")
	}
}

// A fixture left raw (by an older kit or an interrupted write) is scrubbed
// before it is packed.
func TestBundleScrubsRawFixtures(t *testing.T) {
	k, _, _ := testKit(t, fbtest.Options{})
	d := filepath.Join(k.Cfg.Out, DirFixtures, "page_html", "20261030_har_session")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	raw := `{"author":{"__typename":"User","id":"100012345","name":"Jane Real"}}`
	if err := os.WriteFile(filepath.Join(d, "response.html"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := k.bundle(); err != nil {
		t.Fatal(err)
	}
	got := string(bundleFiles(t, filepath.Join(k.Cfg.Out, FileBundle))["phase0/fixtures/page_html/20261030_har_session/response.html"])
	if got == "" || strings.Contains(got, "Jane Real") || strings.Contains(got, "100012345") {
		t.Fatalf("bundled fixture: %q", got)
	}
}

func TestEstimateRun(t *testing.T) {
	recs := []Record{
		{Type: "bootstrap", Page: "a", Wire: 1000},
		{Type: "request", Page: "a", Query: "timeline", Wire: 1000},
		{Type: "request", Page: "a", Query: "timeline", Wire: 1000},
		{Type: "timeline", Page: "a", EndReason: "end"},
		{Type: "request", Page: "a", Query: "comments", Wire: 1000},
		{Type: "request", Page: "a", Query: "replies", Wire: 1000},
		{Type: "request", Page: "a", Query: "replies", Wire: 1000},
		{Type: "comments", Page: "a"},
	}
	full := Options{MaxTimelinePages: 60, PostsPerPage: 4, Orderings: []string{"a", "b"}, RPS: 0.5, Concurrency: 2}
	e := EstimateRun(recs, full, 100, 2)
	// 1 bootstrap + 2 timeline + 2 portability + 4*2 measurements * 3 requests.
	if e.RequestsPerPage != 29 {
		t.Fatalf("requests/page %v", e.RequestsPerPage)
	}
	if want := 100 * 29 / 1.0 / 3600; e.Hours < want*0.99 || e.Hours > want*1.01 {
		t.Fatalf("hours %v, want %v", e.Hours, want)
	}
	if e.WireBytes != 100*29*1000 || e.CostUSD != e.WireBytes/1e9*2 {
		t.Fatalf("traffic %+v", e)
	}
	recs[3].EndReason = "max_pages"
	if e := EstimateRun(recs, full, 1, 0); e.RequestsPerPage != 1+60+2+24 {
		t.Fatalf("capped timeline must use the full limit: %v", e.RequestsPerPage)
	}
	// Comparison orderings on one post: 4 + 1 measurements.
	full.ComparePosts = 1
	if e := EstimateRun(recs, full, 1, 0); e.RequestsPerPage != 1+60+2+15 {
		t.Fatalf("compare posts: %v", e.RequestsPerPage)
	}
}
