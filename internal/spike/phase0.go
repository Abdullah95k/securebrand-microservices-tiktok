package spike

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fixtures"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Phase 0 steps, in order. Each can also be run on its own with fbs-spike.
const (
	StepPreflight = "preflight"
	StepDocIDs    = "docids"
	StepCapture   = "capture"
	StepSmoke     = "smoke"
	StepRun       = "run"
	StepReport    = "report"
	StepBundle    = "bundle"
)

// Steps lists the phase0 steps in order.
var Steps = []string{StepPreflight, StepDocIDs, StepCapture, StepSmoke, StepRun, StepReport, StepBundle}

// Files written to the output directory.
const (
	FilePreflight = "preflight.json"
	FileDocIDs    = "docids.json"
	FileRegistry  = "registry.yaml"
	DirFixtures   = "fixtures"
	FileFixtures  = "fixtures.json"
	DirFailures   = "failures" // scrubbed responses capture could not use
	FileReportTxt = "report.txt"
	FileReport    = "report.json"
	FileMeta      = "meta.json"
	FileBundle    = "phase0-bundle.tar.gz"
)

// SmokeFile and RunFile name the per-tier results files.
func SmokeFile(tier string) string { return "smoke-" + tier + ".jsonl" }
func RunFile(tier string) string   { return "run-" + tier + ".jsonl" }

// StepError stops the orchestrator. NeedsAction means the step worked but
// found something only a person (or a parser change) can resolve; the
// bundle is written so the findings can be sent back.
type StepError struct {
	Step        string
	NeedsAction bool
	Err         error
}

func (e *StepError) Error() string { return e.Step + ": " + e.Err.Error() }
func (e *StepError) Unwrap() error { return e.Err }

// Stack holds the clients the kit builds runners from. cmd/fbs-spike wires
// the real ones; tests wire fakefb.
type Stack struct {
	BaseURL string
	Proxies *proxy.Pool
	Pool    *session.Pool
	Loader  *session.Loader
	HTTP    *http.Client // direct egress: JS bundles and media
	// NewGQL returns a GraphQL client for a registry.
	NewGQL func(registry.Seed) *graphql.Client
	// EchoClient overrides the preflight exit IP client (tests).
	EchoClient func(proxy.Endpoint) *http.Client
}

// KitConfig configures a Phase 0 run.
type KitConfig struct {
	Out      string
	Tiers    []string
	Pages    []PageRef
	BaseSeed registry.Seed // template for docids (the embedded seed)
	// Registry is a registry file to use instead of the docids step (for
	// example one written by fbs-spike har).
	Registry string
	From     string // first step to run (default preflight)
	Options  Options
	// CostPerGB is the proxy price per tier, from the tier config.
	CostPerGB  map[string]float64
	ConfigPath string
	IPEchoURL  string
	// Secrets must never appear in the bundle (proxy passwords and user names).
	Secrets []string
	Report  ReportOptions

	DocIDPages   int // default 3
	CapturePages int // default 2
	SmokePages   int // default 3
	MaxBundles   int // default 150
}

// Kit runs Phase 0 end to end: preflight, docids, capture, smoke, run,
// report, bundle.
type Kit struct {
	Stack Stack
	Cfg   KitConfig
	Log   io.Writer

	seed    registry.Seed
	started time.Time
	logMu   sync.Mutex
}

func (k *Kit) logf(format string, args ...any) {
	if k.Log != nil {
		k.logMu.Lock()
		_, _ = fmt.Fprintf(k.Log, format+"\n", args...)
		k.logMu.Unlock()
	}
}

func (k *Kit) path(name string) string { return filepath.Join(k.Cfg.Out, name) }

func (k *Kit) runner(seed registry.Seed, opt Options, out io.Writer) *Runner {
	return &Runner{Pool: k.Stack.Pool, Loader: k.Stack.Loader, GQL: k.Stack.NewGQL(seed), HTTP: k.Stack.HTTP, Out: out, Opt: opt}
}

func refs(pages []PageRef, n int) []string {
	var out []string
	for i := 0; i < len(pages) && i < n; i++ {
		out = append(out, pages[i].Ref)
	}
	return out
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// Run executes the steps from Cfg.From to the end. It returns the report
// (when the report step ran) and a *StepError when a step stopped the run.
func (k *Kit) Run(ctx context.Context) (Result, error) {
	c := &k.Cfg
	if c.DocIDPages <= 0 {
		c.DocIDPages = 3
	}
	if c.CapturePages <= 0 {
		c.CapturePages = 2
	}
	if c.SmokePages <= 0 {
		c.SmokePages = 3
	}
	if c.MaxBundles <= 0 {
		c.MaxBundles = 150
	}
	if c.From == "" {
		c.From = StepPreflight
	}
	start := -1
	for i, s := range Steps {
		if s == c.From {
			start = i
		}
	}
	if start < 0 {
		return Result{}, fmt.Errorf("spike: unknown step %q (steps: %s)", c.From, strings.Join(Steps, ", "))
	}
	if len(c.Tiers) == 0 || len(c.Pages) == 0 {
		return Result{}, fmt.Errorf("spike: phase0 needs --tiers and --pages")
	}
	if err := os.MkdirAll(c.Out, 0o755); err != nil {
		return Result{}, err
	}
	k.started = time.Now().UTC()
	if c.Options.RPS > 0.33 {
		k.logf("warning: --rps %.2f is above the 0.33 requests/second per identity the pipeline uses; blocks measured at this pace overstate production", c.Options.RPS)
	}
	var res Result
	for _, step := range Steps[start:] {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		k.logf("\n== %s ==", step)
		var err error
		switch step {
		case StepPreflight:
			err = k.preflight(ctx)
		case StepDocIDs:
			err = k.docids(ctx)
		case StepCapture:
			err = k.capture(ctx)
		case StepSmoke:
			err = k.smoke(ctx)
		case StepRun:
			err = k.run(ctx)
		case StepReport:
			res, err = k.report()
		case StepBundle:
			err = k.bundle()
		}
		if err != nil {
			var se *StepError
			if errors.As(err, &se) && se.NeedsAction {
				if berr := k.bundle(); berr != nil {
					k.logf("bundle: %v", berr)
				}
			}
			return res, err
		}
	}
	return res, nil
}

// registry returns the registry the measuring steps use.
func (k *Kit) registry() (registry.Seed, error) {
	if len(k.seed.Queries) > 0 {
		return k.seed, nil
	}
	p := k.Cfg.Registry
	if p == "" {
		p = k.path(FileRegistry)
	}
	s, err := ReadRegistry(p)
	if err != nil {
		return s, &StepError{Step: StepDocIDs, Err: fmt.Errorf("no registry (%w); run the docids step or pass --registry", err)}
	}
	if err := CheckRegistry(s, k.Stack.BaseURL); err != nil {
		return s, &StepError{Step: StepDocIDs, NeedsAction: true, Err: err}
	}
	k.seed = s
	return s, nil
}

func (k *Kit) preflight(ctx context.Context) error {
	seed := k.Cfg.BaseSeed
	if k.Cfg.Registry != "" {
		s, err := ReadRegistry(k.Cfg.Registry)
		if err != nil {
			return &StepError{Step: StepPreflight, Err: err}
		}
		seed = s
	}
	res := Preflight(ctx, PreflightDeps{Proxies: k.Stack.Proxies, Pool: k.Stack.Pool, Loader: k.Stack.Loader, EchoClient: k.Stack.EchoClient},
		PreflightOptions{Tiers: k.Cfg.Tiers, Page: k.Cfg.Pages[0].Ref, IPEchoURL: k.Cfg.IPEchoURL, ConfigPath: k.Cfg.ConfigPath,
			Seed: seed, BaseURL: k.Stack.BaseURL})
	k.logf("%s", strings.TrimRight(FormatPreflight(res), "\n"))
	if err := writeJSON(k.path(FilePreflight), res); err != nil {
		return err
	}
	if !res.OK {
		return &StepError{Step: StepPreflight, Err: errors.New("preflight failed; fix the FAIL lines and run again")}
	}
	return nil
}

func (k *Kit) docids(ctx context.Context) error {
	if k.Cfg.Registry != "" {
		k.logf("using --registry %s; skipping discovery", k.Cfg.Registry)
		_, err := k.registry()
		return err
	}
	opt := k.Cfg.Options
	opt.Tier = k.Cfg.Tiers[0]
	r := k.runner(k.Cfg.BaseSeed, opt, io.Discard)
	pages := refs(k.Cfg.Pages, k.Cfg.DocIDPages)
	found, n, err := r.DocIDs(ctx, pages, k.Cfg.MaxBundles)
	if err != nil {
		return &StepError{Step: StepDocIDs, Err: err}
	}
	merged, dr := MergeDocIDs(k.Cfg.BaseSeed, found)
	k.logf("scanned %d bundles from %d pages: %d operations", n, len(pages), len(found))
	for _, name := range sortedKeys(dr.Found) {
		k.logf("FOUND   %s = %s", name, dr.Found[name])
	}
	for _, name := range dr.Missing {
		k.logf("MISSING %s", name)
	}
	if err := writeJSON(k.path(FileDocIDs), dr); err != nil {
		return err
	}
	if len(dr.Missing) > 0 {
		return &StepError{Step: StepDocIDs, NeedsAction: true, Err: fmt.Errorf("doc_ids not found for %s: record a HAR in a logged-out browser "+
			"(docs/runbooks/phase0-spike.md, \"HAR fallback\"), then run fbs-spike har --in session.har --write-registry %s "+
			"and rerun with --registry %s --from capture", strings.Join(dr.Missing, ", "), k.path(FileRegistry), k.path(FileRegistry))}
	}
	if err := WriteRegistry(k.path(FileRegistry), merged, "fbs-spike docids ("+strings.Join(pages, ", ")+")"); err != nil {
		return err
	}
	k.logf("wrote %s", k.path(FileRegistry))
	_, err = k.registry()
	return err
}

var unsafeName = regexp.MustCompile(`[^a-z0-9_]+`)

func caseName(ref string) string {
	ref = strings.TrimPrefix(strings.TrimPrefix(ref, "v:"), "id:")
	n := strings.Trim(unsafeName.ReplaceAllString(strings.ToLower(ref), "_"), "_")
	if n == "" {
		n = "page"
	}
	return n
}

func (k *Kit) capture(ctx context.Context) error {
	seed, err := k.registry()
	if err != nil {
		return err
	}
	opt := k.Cfg.Options
	opt.Tier = k.Cfg.Tiers[0]
	r := k.runner(seed, opt, io.Discard)
	// failures/ describes this capture only.
	r.FailDir = k.path(DirFailures)
	if err := os.RemoveAll(r.FailDir); err != nil {
		return err
	}
	root := k.path(DirFixtures)
	kept := 0
	for _, p := range refs(k.Cfg.Pages, k.Cfg.CapturePages) {
		ref, err := task.NormalizeRef(p)
		if err != nil {
			k.logf("capture %s: %v", p, err)
			continue
		}
		dirs, err := r.Capture(ctx, p, root, caseName(ref))
		for _, d := range dirs {
			k.logf("wrote %s", d)
			if strings.HasPrefix(d, r.FailDir+string(filepath.Separator)) {
				kept++
			}
		}
		if err != nil {
			k.logf("capture %s: %s", p, Redact(err.Error()))
		}
	}
	checks, err := CheckFixtures(root)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	k.logf("%s", strings.TrimRight(FormatFixtureChecks(checks), "\n"))
	if err := writeJSON(k.path(FileFixtures), checks); err != nil {
		return err
	}
	kinds := map[string]bool{}
	var bad []string
	for _, c := range checks {
		if c.OK() {
			kinds[c.Query] = true
		} else {
			bad = append(bad, c.Dir)
		}
	}
	switch {
	case len(bad) > 0:
		return &StepError{Step: StepCapture, NeedsAction: true, Err: fmt.Errorf("the parsers do not understand %d captured fixtures: "+
			"Facebook's response shapes differ from the modelled ones; send %s so the parsers can be updated", len(bad), k.path(FileBundle))}
	case !kinds["timeline"]:
		where := "see the lines above"
		if kept > 0 {
			where += "; the responses are under " + r.FailDir
		}
		return &StepError{Step: StepCapture, NeedsAction: true, Err: fmt.Errorf("no timeline fixture could be captured (%s); send %s", where, k.path(FileBundle))}
	}
	return nil
}

func smokeOptions(full Options, pages int) Options {
	o := full
	o.MaxTimelinePages = min(full.MaxTimelinePages, 10)
	o.PostsPerPage = min(full.PostsPerPage, 1)
	if len(full.Orderings) > 0 {
		o.Orderings = full.Orderings[:1]
	}
	o.MediaSamples = min(full.MediaSamples, 1)
	o.Concurrency = max(1, min(full.Concurrency, pages))
	return o
}

// Estimate projects a full run from a smoke run.
type Estimate struct {
	RequestsPerPage float64
	Hours           float64
	WireBytes       float64
	CostUSD         float64
}

// EstimateRun extrapolates the full limits from smoke records: timeline
// requests as observed (or the full limit when the smoke run hit its cap),
// plus comment and reply requests per (post, ordering) measurement times the
// measurements of the full run.
func EstimateRun(smoke []Record, full Options, pages int, costPerGB float64) Estimate {
	var timelineReqs, commentReqs, wire, proxied float64
	var timelines, measurements, capped int
	pageSet := map[string]bool{}
	for _, r := range smoke {
		switch r.Type {
		case "request":
			proxied++
			wire += float64(r.Wire)
			switch r.Query {
			case "timeline":
				timelineReqs++
			case "replies":
				commentReqs++
			default:
				if strings.HasPrefix(r.Query, "comments") {
					commentReqs++
				}
			}
			pageSet[r.Page] = true
		case "bootstrap":
			proxied++
			wire += float64(r.Wire)
		case "timeline":
			timelines++
			if r.EndReason == "max_pages" {
				capped++
			}
		case "comments":
			measurements++
		}
	}
	var e Estimate
	if timelines == 0 || proxied == 0 {
		return e
	}
	t := timelineReqs / float64(timelines)
	if capped > 0 {
		t = float64(full.MaxTimelinePages)
	}
	perMeasurement := 0.0
	if measurements > 0 {
		perMeasurement = commentReqs / float64(measurements)
	}
	compare := full.PostsPerPage
	if full.ComparePosts > 0 {
		compare = min(full.ComparePosts, full.PostsPerPage)
	}
	measurementsPerPage := full.PostsPerPage + max(0, len(full.Orderings)-1)*compare
	e.RequestsPerPage = 1 + t + 2 + float64(measurementsPerPage)*perMeasurement
	rate := full.RPS * float64(max(1, full.Concurrency))
	if rate > 0 {
		e.Hours = float64(pages) * e.RequestsPerPage / rate / 3600
	}
	e.WireBytes = float64(pages) * e.RequestsPerPage * wire / proxied
	e.CostUSD = e.WireBytes / 1e9 * costPerGB
	return e
}

func readRecordsFile(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return readRecords(f)
}

func (k *Kit) smoke(ctx context.Context) error {
	seed, err := k.registry()
	if err != nil {
		return err
	}
	pages := k.Cfg.Pages[:min(k.Cfg.SmokePages, len(k.Cfg.Pages))]
	var problems []string
	for _, tier := range k.Cfg.Tiers {
		opt := smokeOptions(k.Cfg.Options, len(pages))
		opt.Tier, opt.CostPerGBUSD = tier, k.Cfg.CostPerGB[tier]
		path := k.path(SmokeFile(tier))
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		k.runner(seed, opt, f).Run(ctx, pages)
		_ = f.Close()
		if err := ctx.Err(); err != nil {
			return err
		}
		recs, err := readRecordsFile(path)
		if err != nil {
			return err
		}
		s := summarize(tier, recs, k.Cfg.Report)
		k.logf("%s: bootstrap %.0f%%, block rate %.1f%%, median %.0f posts, timeline ends %v, %.0f requests/page at smoke limits",
			tier, 100*s.BootstrapSuccess, 100*s.BlockRate, s.MedianPosts, s.TimelineEnd, s.RequestsPerPage)
		switch {
		case s.BootstrapSuccess == 0:
			problems = append(problems, tier+": no page bootstrapped (see the bootstrap records in "+path+")")
		case s.BlockRate >= 0.5:
			problems = append(problems, fmt.Sprintf("%s: blocked: %.0f%% of requests were rate limited, walled or checkpointed; %s",
				tier, 100*s.BlockRate, Advice[CheckBlockRate]))
		case s.TimelineEnd["drift"] > 0:
			problems = append(problems, tier+": the timeline parser reported missing required fields (drift); send the bundle")
		case s.MedianPosts == 0:
			problems = append(problems, fmt.Sprintf("%s: the timeline parser found no posts (timeline ends %v); send the bundle", tier, s.TimelineEnd))
		}
		e := EstimateRun(recs, k.Cfg.Options, len(k.Cfg.Pages), k.Cfg.CostPerGB[tier])
		msg := fmt.Sprintf("%s: full run estimate for %d pages: ~%.0f requests/page, ~%.1f hours, ~%s through the proxy",
			tier, len(k.Cfg.Pages), e.RequestsPerPage, e.Hours, human(e.WireBytes))
		if e.CostUSD > 0 {
			msg += fmt.Sprintf(", ~$%.2f", e.CostUSD)
		}
		k.logf("%s", msg)
	}
	if len(problems) > 0 {
		return &StepError{Step: StepSmoke, NeedsAction: true, Err: fmt.Errorf("%s", strings.Join(problems, "; "))}
	}
	return nil
}

func (k *Kit) run(ctx context.Context) error {
	seed, err := k.registry()
	if err != nil {
		return err
	}
	for _, tier := range k.Cfg.Tiers {
		path := k.path(RunFile(tier))
		done, err := DoneFile(path)
		if err != nil {
			return err
		}
		todo := Remaining(k.Cfg.Pages, done)
		k.logf("%s: %d pages to measure (%d already done) -> %s", tier, len(todo), len(k.Cfg.Pages)-len(todo), path)
		if len(todo) == 0 {
			continue
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		opt := k.Cfg.Options
		opt.Tier, opt.CostPerGBUSD = tier, k.Cfg.CostPerGB[tier]
		r := k.runner(seed, opt, f)
		r.Progress = func(done, total int, page string) {
			k.logf("%s: %d/%d %s", tier, done, total, page)
		}
		r.Run(ctx, todo)
		_ = f.Close()
		if err := ctx.Err(); err != nil {
			k.logf("interrupted: rerun with --from run to resume")
			return err
		}
	}
	return nil
}

func (k *Kit) report() (Result, error) {
	var readers []io.Reader
	for _, tier := range k.Cfg.Tiers {
		b, err := os.ReadFile(k.path(RunFile(tier)))
		if errors.Is(err, os.ErrNotExist) {
			k.logf("%s: no results yet", tier)
			continue
		}
		if err != nil {
			return Result{}, err
		}
		readers = append(readers, bytes.NewReader(b))
	}
	res, err := ReportAll(k.Cfg.Report, readers...)
	if err != nil {
		return res, err
	}
	txt := FormatAll(res)
	k.logf("%s", strings.TrimRight(txt, "\n"))
	if err := os.WriteFile(k.path(FileReportTxt), []byte(txt), 0o644); err != nil {
		return res, err
	}
	return res, writeJSON(k.path(FileReport), res)
}

// bundleFile reports whether a path under the output directory belongs in
// the bundle. The config file never lives there, and anything unknown is
// left out.
func bundleFile(rel string) bool {
	switch rel {
	case FilePreflight, FileDocIDs, FileRegistry, FileFixtures, FileReportTxt, FileReport, FileMeta:
		return true
	}
	if strings.HasPrefix(rel, DirFixtures+"/") || strings.HasPrefix(rel, DirFailures+"/") {
		return true
	}
	return (strings.HasPrefix(rel, "smoke-") || strings.HasPrefix(rel, "run-")) && strings.HasSuffix(rel, ".jsonl")
}

// scrubFixtures scrubs every fixture case under root that this scrubber
// version has not scrubbed. Fixtures are written scrubbed; one left raw by
// an older kit or an interrupted write, or scrubbed by an older version, is
// scrubbed here before it can be packed.
func scrubFixtures(root string) error {
	queries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, q := range queries {
		if !q.IsDir() {
			continue
		}
		cases, err := os.ReadDir(filepath.Join(root, q.Name()))
		if err != nil {
			return err
		}
		for _, c := range cases {
			if !c.IsDir() {
				continue
			}
			if err := fixtures.ScrubDir(filepath.Join(root, q.Name(), c.Name()), nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// ErrSecretInBundle stops the bundle when a configured credential shows up
// in a file.
var ErrSecretInBundle = errors.New("spike: bundle would contain a proxy credential")

func (k *Kit) bundle() error {
	meta := map[string]any{
		"version": k.Cfg.Options.Version, "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH,
		"started_at": k.started, "written_at": time.Now().UTC(), "base_url": k.Stack.BaseURL, "tiers": k.Cfg.Tiers,
		"pages": len(k.Cfg.Pages), "options": k.Cfg.Options, "cost_per_gb_usd": k.Cfg.CostPerGB,
		"thresholds": k.Cfg.Report.Thresholds, "registry_flag": k.Cfg.Registry != "",
	}
	if err := writeJSON(k.path(FileMeta), meta); err != nil {
		return err
	}
	for _, d := range []string{DirFixtures, DirFailures} {
		if err := scrubFixtures(k.path(d)); err != nil {
			return err
		}
	}
	var files []string
	err := filepath.WalkDir(k.Cfg.Out, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(k.Cfg.Out, p)
		rel = filepath.ToSlash(rel)
		if bundleFile(rel) {
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(files)
	var secrets []string
	for _, s := range k.Cfg.Secrets {
		if len(s) >= 6 {
			secrets = append(secrets, s)
		}
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, rel := range files {
		b, err := os.ReadFile(filepath.Join(k.Cfg.Out, rel))
		if err != nil {
			return err
		}
		for _, s := range secrets {
			if bytes.Contains(b, []byte(s)) {
				return fmt.Errorf("%w: %s (remove the file or redact it, then run --from bundle)", ErrSecretInBundle, rel)
			}
		}
		// The spike's own outputs are redacted; a URL with credentials in
		// them means redaction missed a path. Fixtures, and the responses
		// kept under failures/, are Facebook content.
		facebook := strings.HasPrefix(rel, DirFixtures+"/") || (strings.HasPrefix(rel, DirFailures+"/") && path.Base(rel) != "meta.json")
		if loc := userinfo.FindIndex(b); loc != nil && !facebook {
			return fmt.Errorf("%w: %s contains a URL with credentials (%s)", ErrSecretInBundle, rel, Redact(string(b[loc[0]:loc[1]])))
		}
		if err := tw.WriteHeader(&tar.Header{Name: "phase0/" + rel, Mode: 0o644, Size: int64(len(b)), ModTime: time.Now()}); err != nil {
			return err
		}
		if _, err := tw.Write(b); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if err := os.WriteFile(k.path(FileBundle), buf.Bytes(), 0o644); err != nil {
		return err
	}
	k.logf("wrote %s (%d files, %s); it contains no proxy credentials. Send it back to get the parsers and doc_ids fixed.",
		k.path(FileBundle), len(files), human(float64(buf.Len())))
	return nil
}
