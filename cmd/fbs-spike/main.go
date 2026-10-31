// Command fbs-spike is the Phase 0 harness: it measures what logged-out
// Facebook serves before the pipeline is scaled up. It needs no Kafka,
// Postgres or Redis. See docs/runbooks/phase0-spike.md.
//
//	fbs-spike phase0         --config phase0/proxies.yaml --pages phase0/pages.csv --tiers isp,residential
//	fbs-spike preflight      --tiers isp,residential --page <page>
//	fbs-spike docids         --page <page>[,<page>...] [--write-registry my-seed.yaml]
//	fbs-spike har            --in session.har --write-registry my-seed.yaml [--fixtures-dir DIR --name brand_x]
//	fbs-spike render         --page <page> [--tier T] [--write-registry my-seed.yaml]   (needs Chromium)
//	fbs-spike capture        --registry my-seed.yaml --page <page> --name brand_x [--dir testdata/fb] [--fail-dir DIR]
//	fbs-spike check-fixtures --dir testdata/fb
//	fbs-spike run            --registry my-seed.yaml --pages pages.csv --tier residential --out results.jsonl
//	fbs-spike report         --in a.jsonl[,b.jsonl] [--json]
//	fbs-spike trim           --registry my-seed.yaml --pages pages.csv --tier isp --out trim.jsonl [--each]
//	fbs-spike trim-report    --in trim.jsonl [--json]
//
// Exit codes: 0 ok (GO), 1 error, 2 usage, 3 NO-GO, 4 needs action (missing
// doc_ids, parser drift), 130 interrupted.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	fbclient "github.com/abdullah95k/securebrand-microservices-igs/internal/fb/client"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/media"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/spike"
)

var version = "dev"

const (
	exitError       = 1
	exitUsage       = 2
	exitNoGo        = 3
	exitNeedsAction = 4
	exitInterrupted = 130
)

type flags struct {
	cfgPath, registryFile, tier, tiers, pagesFile, out, outDir, in, page, name, dir, fixturesDir string
	writeRegistry, orderings, from, ipEcho, timelineCounts, commentCounts, failDir               string
	asJSON, resume, each                                                                         bool
	rps, reqPerDay, minDays                                                                      float64
	timelinePages, postsPerPage, commentPages, threads, mediaSamples, conc, maxBundles, compare  int
	trimPages, timelineCount                                                                     int
	maxPostComments                                                                              int64
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "-h", "--help", "help":
		usage()
	case "version", "--version":
		fmt.Println(version)
		return
	}
	var f flags
	fs := flag.NewFlagSet("fbs-spike "+cmd, flag.ExitOnError)
	fs.StringVar(&f.cfgPath, "config", os.Getenv("IGS_CONFIG"), "config files, comma separated (fb.proxy tiers, fb.base_url, ...)")
	fs.StringVar(&f.registryFile, "registry", "", "registry YAML to use instead of the embedded seed (from docids or har)")
	fs.StringVar(&f.tier, "tier", "direct", "proxy tier (run, capture, docids)")
	fs.StringVar(&f.tiers, "tiers", "", "proxy tiers, comma separated (phase0, preflight; default --tier)")
	fs.StringVar(&f.pagesFile, "pages", "", "pages file: one page per line, or CSV with a page,size,category,region,language header")
	fs.StringVar(&f.out, "out", "spike-results.jsonl", "results file (run)")
	fs.StringVar(&f.outDir, "out-dir", "phase0/out", "output directory (phase0)")
	fs.StringVar(&f.in, "in", "spike-results.jsonl", "results files, comma separated (report); HAR file (har)")
	fs.BoolVar(&f.asJSON, "json", false, "print JSON (report, preflight, har, check-fixtures)")
	fs.StringVar(&f.page, "page", "", "page(s), comma separated (docids, capture, preflight)")
	fs.StringVar(&f.name, "name", "sample", "fixture case name (capture, har)")
	fs.StringVar(&f.dir, "dir", "testdata/fb", "fixture root (capture, check-fixtures)")
	fs.StringVar(&f.fixturesDir, "fixtures-dir", "", "write HAR response bodies as fixtures under this root (har)")
	fs.StringVar(&f.failDir, "fail-dir", "", "keep responses that were not ok, and a page whose id was not found, under this root (capture)")
	fs.StringVar(&f.writeRegistry, "write-registry", "", "write the registry with the discovered doc_ids to this file (docids, har)")
	fs.StringVar(&f.from, "from", spike.StepPreflight, "first step: "+strings.Join(spike.Steps, ", ")+" (phase0)")
	fs.StringVar(&f.ipEcho, "ip-echo-url", spike.DefaultIPEchoURL, "service returning the caller's IP, used to check proxy exits; empty skips (preflight, phase0)")
	fs.BoolVar(&f.resume, "resume", true, "skip pages already finished in --out (run)")
	fs.Float64Var(&f.rps, "rps", 0.3, "per-identity requests per second")
	fs.Float64Var(&f.reqPerDay, "req-per-day", spike.DefaultRequestsPerDay, "production requests per day for the cost projection (report, phase0)")
	fs.Float64Var(&f.minDays, "min-days", spike.DefaultThresholds.MinMedianDays, "go/no-go: timeline depth in days (the backfill window you need)")
	fs.IntVar(&f.timelinePages, "timeline-pages", 60, "max timeline pages per page")
	fs.IntVar(&f.postsPerPage, "posts-per-page", 3, "posts sampled per page for comment completeness")
	fs.Int64Var(&f.maxPostComments, "max-post-comments", 300, "prefer posts with at most this many comments, so measurements finish (0 = busiest posts)")
	fs.IntVar(&f.commentPages, "comment-pages", 40, "max comment pages per post and ordering")
	fs.IntVar(&f.threads, "threads-per-post", 100, "reply threads followed per post and ordering")
	fs.StringVar(&f.orderings, "orderings", "comments,comments_newest,comments_relevant", "comment orderings; the first is measured on every sampled post")
	fs.IntVar(&f.compare, "compare-posts", 1, "sampled posts per page that also get the other orderings (0 = all)")
	fs.IntVar(&f.mediaSamples, "media-samples", 3, "CDN downloads per page")
	fs.IntVar(&f.conc, "concurrency", 4, "pages in parallel")
	fs.IntVar(&f.maxBundles, "max-bundles", 150, "JS bundles to scan (docids)")
	fs.IntVar(&f.trimPages, "trim-pages", 3, "pages measured, the first ones of --pages (trim)")
	fs.BoolVar(&f.each, "each", false, "also switch each provider flag off on its own (trim)")
	fs.IntVar(&f.timelineCount, "timeline-count", 0, "the baseline's timeline page size (trim; default crawler.timeline_page_size)")
	fs.StringVar(&f.timelineCounts, "timeline-counts", "3,25", "timeline page sizes measured besides the baseline (trim)")
	fs.StringVar(&f.commentCounts, "comment-counts", "10,25,50", "commentsAfterCount values measured besides the template's (trim)")
	_ = fs.Parse(args)

	cfg, err := config.Load(f.cfgPath)
	if err != nil {
		fail(err)
	}
	plog.Setup("fbs-spike", cfg.Log.Level, "text")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "report":
		os.Exit(report(f))
	case "check-fixtures":
		os.Exit(checkFixtures(f))
	case "har":
		os.Exit(har(f))
	case "trim-report":
		os.Exit(trimReport(f))
	}

	seed, err := loadSeed(f.registryFile)
	if err != nil {
		fail(err)
	}
	st, err := newStack(cfg, f)
	if err != nil {
		fail(err)
	}
	opt := options(f)
	opt.Tier = f.tier
	runner := func(out io.Writer) *spike.Runner {
		return &spike.Runner{Pool: st.Pool, Loader: st.Loader, GQL: st.NewGQL(seed), HTTP: st.HTTP, Out: out, Opt: opt}
	}
	switch cmd {
	case "phase0":
		os.Exit(phase0(ctx, cfg, f, st, opt))
	case "preflight":
		page := firstPage(f)
		res := spike.Preflight(ctx, spike.PreflightDeps{Proxies: st.Proxies, Pool: st.Pool, Loader: st.Loader},
			spike.PreflightOptions{Tiers: tierList(f), Page: page, IPEchoURL: f.ipEcho, ConfigPath: f.cfgPath, Seed: seed, BaseURL: cfg.FB.BaseURL})
		if f.asJSON {
			printJSON(res)
		} else {
			fmt.Print(spike.FormatPreflight(res))
		}
		if !res.OK {
			os.Exit(exitError)
		}
	case "run":
		if err := spike.CheckRegistry(seed, cfg.FB.BaseURL); err != nil {
			fail(err)
		}
		pages, err := spike.ReadPagesFile(f.pagesFile)
		if err != nil {
			fail(err)
		}
		if f.resume {
			done, err := spike.DoneFile(f.out)
			if err != nil {
				fail(err)
			}
			if todo := spike.Remaining(pages, done); len(todo) < len(pages) {
				fmt.Fprintf(os.Stderr, "resuming: %d of %d pages already done in %s\n", len(pages)-len(todo), len(pages), f.out)
				pages = todo
			}
		}
		out, err := os.OpenFile(f.out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			fail(err)
		}
		defer func() { _ = out.Close() }()
		opt.CostPerGBUSD = costs(cfg)[f.tier]
		r := runner(out)
		r.Progress = func(done, total int, page string) { fmt.Fprintf(os.Stderr, "%d/%d %s\n", done, total, page) }
		r.Run(ctx, pages)
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "interrupted; run the same command again to resume")
			os.Exit(exitInterrupted)
		}
		fmt.Printf("results appended to %s; run: fbs-spike report --in %s\n", f.out, f.out)
	case "docids":
		pages := splitList(f.page)
		if len(pages) == 0 && f.pagesFile != "" {
			ps, err := spike.ReadPagesFile(f.pagesFile)
			if err != nil {
				fail(err)
			}
			for i := 0; i < len(ps) && i < 3; i++ {
				pages = append(pages, ps[i].Ref)
			}
		}
		if len(pages) == 0 {
			fail(errors.New("docids needs --page or --pages"))
		}
		ids, n, err := runner(io.Discard).DocIDs(ctx, pages, f.maxBundles)
		if err != nil {
			fail(err)
		}
		fmt.Fprintf(os.Stderr, "scanned %d bundles, found %d operations\n", n, len(ids))
		merged, dr := spike.MergeDocIDs(seed, ids)
		b, _ := yaml.Marshal(map[string]any{"registry_queries_found": dr.Found, "missing": dr.Missing, "all_operations": len(ids)})
		fmt.Print(string(b))
		if f.writeRegistry != "" {
			if err := spike.WriteRegistry(f.writeRegistry, merged, "fbs-spike docids ("+strings.Join(pages, ", ")+")"); err != nil {
				fail(err)
			}
			fmt.Fprintf(os.Stderr, "wrote %s\n", f.writeRegistry)
		}
		if len(dr.Missing) > 0 {
			fmt.Fprintln(os.Stderr, "some doc_ids were not found: use the HAR fallback (docs/runbooks/phase0-spike.md)")
			os.Exit(exitNeedsAction)
		}
	case "render":
		os.Exit(render(ctx, cfg, f, st, seed))
	case "trim":
		os.Exit(trim(ctx, cfg, f, runner, seed))
	case "capture":
		if err := spike.CheckRegistry(seed, cfg.FB.BaseURL); err != nil {
			fail(err)
		}
		r := runner(io.Discard)
		r.FailDir = f.failDir
		dirs, err := r.Capture(ctx, firstPage(f), f.dir, f.name)
		for _, d := range dirs {
			fmt.Println("wrote", d)
		}
		if err != nil {
			fail(err)
		}
		fmt.Printf("review the scrubbed files, then: fbs-spike check-fixtures --dir %s (or go test ./internal/fb/parse -update)\n", f.dir)
	default:
		usage()
	}
}

func options(f flags) spike.Options {
	return spike.Options{MaxTimelinePages: f.timelinePages, PostsPerPage: f.postsPerPage, MaxCommentPages: f.commentPages,
		ThreadsPerPost: f.threads, Orderings: splitList(f.orderings), MediaSamples: f.mediaSamples, Concurrency: f.conc,
		MaxPostComments: f.maxPostComments, ComparePosts: f.compare,
		PageTimeout: 30 * time.Minute, RPS: f.rps, Version: version}
}

func reportOptions(f flags) spike.ReportOptions {
	th := spike.DefaultThresholds
	th.MinMedianDays = f.minDays
	return spike.ReportOptions{Thresholds: th, RequestsPerDay: f.reqPerDay}
}

func phase0(ctx context.Context, cfg *config.Config, f flags, st spike.Stack, opt spike.Options) int {
	pages, err := spike.ReadPagesFile(f.pagesFile)
	if err != nil {
		fail(err)
	}
	base, err := loadSeed("")
	if err != nil {
		fail(err)
	}
	k := &spike.Kit{Stack: st, Log: os.Stdout, Cfg: spike.KitConfig{
		Out: f.outDir, Tiers: tierList(f), Pages: pages, BaseSeed: base, Registry: f.registryFile, From: f.from,
		Options: opt, CostPerGB: costs(cfg), ConfigPath: f.cfgPath, IPEchoURL: f.ipEcho, Secrets: secrets(cfg),
		Report: reportOptions(f), MaxBundles: f.maxBundles,
	}}
	res, err := k.Run(ctx)
	var se *spike.StepError
	switch {
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(os.Stderr, "interrupted")
		return exitInterrupted
	case errors.As(err, &se) && se.NeedsAction:
		fmt.Fprintln(os.Stderr, "\nACTION NEEDED:", spike.Redact(err.Error()))
		return exitNeedsAction
	case err != nil:
		fmt.Fprintln(os.Stderr, "\nfbs-spike:", spike.Redact(err.Error()))
		return exitError
	case !res.Go:
		return exitNoGo
	}
	return 0
}

func report(f flags) int {
	var readers []io.Reader
	for _, p := range splitList(f.in) {
		b, err := os.ReadFile(p)
		if err != nil {
			fail(err)
		}
		readers = append(readers, bytes.NewReader(b))
	}
	res, err := spike.ReportAll(reportOptions(f), readers...)
	if err != nil {
		fail(err)
	}
	if f.asJSON {
		printJSON(res)
	} else {
		fmt.Print(spike.FormatAll(res))
	}
	if !res.Go {
		return exitNoGo
	}
	return 0
}

func checkFixtures(f flags) int {
	cs, err := spike.CheckFixtures(f.dir)
	if err != nil {
		fail(err)
	}
	if f.asJSON {
		printJSON(cs)
	} else {
		fmt.Print(spike.FormatFixtureChecks(cs))
	}
	for _, c := range cs {
		if !c.OK() {
			return exitNeedsAction
		}
	}
	return 0
}

func har(f flags) int {
	seed, err := loadSeed(f.registryFile)
	if err != nil {
		fail(err)
	}
	in, err := os.Open(f.in)
	if err != nil {
		fail(err)
	}
	c, err := spike.ReadHAR(in)
	_ = in.Close()
	if err != nil {
		fail(err)
	}
	// A fixture failure still leaves the doc_ids, the point of the import,
	// to print and write; it fails the command at the end.
	res, fixturesErr := spike.ImportHAR(c, seed, f.fixturesDir, f.name)
	if f.asJSON {
		printJSON(res)
	} else {
		fmt.Printf("%d GraphQL requests and %d page loads in %s\n", len(c.Operations), len(c.Pages), f.in)
		for _, q := range res.Queries {
			if q.Kept != "" {
				fmt.Printf("KEPT    %-18s %s = %s (not in the HAR; the registry's doc_id stays)\n", q.Query, q.FriendlyName, q.Kept)
				continue
			}
			if q.DocID == "" {
				fmt.Printf("MISSING %-18s %s: not in the HAR\n", q.Query, q.FriendlyName)
				continue
			}
			fmt.Printf("FOUND   %-18s %s = %s (%d requests)\n", q.Query, q.FriendlyName, q.DocID, q.Requests)
			if len(q.Added) > 0 {
				fmt.Printf("        variables added:   %s\n", strings.Join(q.Added, ", "))
			}
			if len(q.Removed) > 0 {
				fmt.Printf("        variables removed: %s\n", strings.Join(q.Removed, ", "))
			}
			if len(q.Set) > 0 {
				set := make([]string, 0, len(q.Set))
				for k, v := range q.Set {
					b, _ := json.Marshal(v)
					set = append(set, k+"="+string(b))
				}
				sort.Strings(set)
				fmt.Printf("        variables set in the registry: %s\n", strings.Join(set, ", "))
			}
			if len(q.Unset) > 0 {
				fmt.Printf("        variables not set (they differ between requests or are not settings): %s\n", strings.Join(q.Unset, ", "))
			}
			if len(q.IntentTokens) > 0 {
				fmt.Printf("        comment orderings: %s\n", strings.Join(q.IntentTokens, ", "))
			}
			if q.IntentMissing != "" {
				fmt.Printf("        no request used this query's ordering %s; the lines above describe the others\n", q.IntentMissing)
			}
		}
		if len(res.Other) > 0 {
			fmt.Printf("%d other operations (not used by the registry)\n", len(res.Other))
		}
		for _, d := range res.Fixtures {
			fmt.Println("wrote", d)
		}
	}
	if f.writeRegistry != "" {
		if err := spike.WriteRegistry(f.writeRegistry, res.Seed, "fbs-spike har ("+f.in+")"); err != nil {
			fail(err)
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", f.writeRegistry)
	}
	if len(res.DocIDs.Missing) > 0 {
		fmt.Fprintln(os.Stderr, "the HAR misses some queries: record again and open a post, choose \"All comments\" and expand replies")
	}
	if fixturesErr != nil {
		fmt.Fprintln(os.Stderr, "fbs-spike: fixtures:", spike.Redact(fixturesErr.Error()))
		return exitError
	}
	if len(res.DocIDs.Missing) > 0 {
		return exitNeedsAction
	}
	return 0
}

func newStack(cfg *config.Config, f flags) (spike.Stack, error) {
	fpBytes, err := configs.FS.ReadFile("fingerprints.yaml")
	if err != nil {
		return spike.Stack{}, err
	}
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return spike.Stack{}, err
	}
	proxies, err := proxy.New(cfg.FB.Proxy)
	if err != nil {
		return spike.Stack{}, err
	}
	// Each page holds an identity and briefly a second one (portability).
	pool, err := session.NewPool(session.PoolConfig{BaseURL: cfg.FB.BaseURL, Profile: cfg.FB.FingerprintProfile,
		Timeout: cfg.FB.RequestTimeout, IdentityRPS: f.rps, IdentityBurst: 1, MaxRequestsMin: 100000, MaxRequestsMax: 100001,
		MaxPerTier: f.conc*2 + 2}, proxies, fps, nil)
	if err != nil {
		return spike.Stack{}, err
	}
	rules := detect.DefaultRules()
	return spike.Stack{
		BaseURL: cfg.FB.BaseURL, Proxies: proxies, Pool: pool,
		Loader: &session.Loader{Rules: rules, AcceptLanguage: cfg.FB.AcceptLanguage, Hooks: fbclient.Hooks(cfg.FB.AcceptLanguage)},
		HTTP:   media.NewHTTPClient(),
		NewGQL: func(s registry.Seed) *graphql.Client {
			return &graphql.Client{Registry: registry.NewStatic(s), Rules: rules,
				Builder: fbclient.Builder{AcceptLanguage: cfg.FB.AcceptLanguage, Locale: cfg.FB.Locale}}
		},
	}, nil
}

func loadSeed(path string) (registry.Seed, error) {
	if path != "" {
		return spike.ReadRegistry(path)
	}
	b, err := configs.FS.ReadFile("queries.seed.yaml")
	if err != nil {
		return registry.Seed{}, err
	}
	return registry.ParseSeed(b)
}

func costs(cfg *config.Config) map[string]float64 {
	out := map[string]float64{}
	for _, t := range cfg.FB.Proxy.Tiers {
		out[t.Name] = t.CostPerGBUSD
	}
	return out
}

// secrets lists the proxy credentials that must never reach the bundle.
func secrets(cfg *config.Config) []string {
	var out []string
	for _, t := range cfg.FB.Proxy.Tiers {
		out = append(out, t.Username, t.Password)
		for _, raw := range append([]string{t.Gateway}, t.Endpoints...) {
			if u, err := url.Parse(raw); err == nil && u.User != nil {
				p, _ := u.User.Password()
				out = append(out, u.User.Username(), p)
			}
		}
	}
	return out
}

func tierList(f flags) []string {
	if t := splitList(f.tiers); len(t) > 0 {
		return t
	}
	return []string{f.tier}
}

func firstPage(f flags) string {
	if p := splitList(f.page); len(p) > 0 {
		return p[0]
	}
	if f.pagesFile != "" {
		if ps, err := spike.ReadPagesFile(f.pagesFile); err == nil {
			return ps[0].Ref
		}
	}
	return ""
}

// trim runs the payload experiment and prints its summary.
func trim(ctx context.Context, cfg *config.Config, f flags, runner func(io.Writer) *spike.Runner, seed registry.Seed) int {
	if err := spike.CheckRegistry(seed, cfg.FB.BaseURL); err != nil {
		fail(err)
	}
	pages, err := spike.ReadPagesFile(f.pagesFile)
	if err != nil {
		fail(err)
	}
	var refs []string
	for i := 0; i < len(pages) && i < f.trimPages; i++ {
		refs = append(refs, pages[i].Ref)
	}
	if len(refs) == 0 {
		fail(errors.New("trim needs --pages"))
	}
	tc, err := intList(f.timelineCounts)
	if err != nil {
		fail(err)
	}
	cc, err := intList(f.commentCounts)
	if err != nil {
		fail(err)
	}
	out, err := os.OpenFile(f.out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fail(err)
	}
	defer func() { _ = out.Close() }()
	count := f.timelineCount
	if count <= 0 {
		count = cfg.Crawler.TimelinePageSize
	}
	recs := runner(out).Trim(ctx, refs, spike.TrimOptions{Each: f.each, TimelineCount: count, TimelineCounts: tc, CommentCounts: cc})
	fmt.Print(spike.FormatTrim(spike.TrimSummary(recs)))
	fmt.Printf("records appended to %s; again: fbs-spike trim-report --in %s\n", f.out, f.out)
	if ctx.Err() != nil {
		return exitInterrupted
	}
	return 0
}

func trimReport(f flags) int {
	var recs []spike.TrimRecord
	for _, p := range splitList(f.in) {
		r, err := spike.ReadTrim(p)
		if err != nil {
			fail(err)
		}
		recs = append(recs, r...)
	}
	rows := spike.TrimSummary(recs)
	if f.asJSON {
		printJSON(rows)
	} else {
		fmt.Print(spike.FormatTrim(rows))
	}
	return 0
}

func intList(s string) ([]int, error) {
	var out []int
	for _, v := range splitList(s) {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", v)
		}
		out = append(out, n)
	}
	return out, nil
}

func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// render loads a page in headless Chromium through the tier and lists the
// GraphQL operations the page itself requested: the automated version of
// recording a HAR by hand.
func render(ctx context.Context, cfg *config.Config, f flags, st spike.Stack, seed registry.Seed) int {
	page := firstPage(f)
	ref, err := task.NormalizeRef(page)
	if err != nil {
		fail(err)
	}
	b, err := browser.NewBootstrapper(cfg.FB.Browser, cfg.FB.AcceptLanguage, fbclient.Hooks(cfg.FB.AcceptLanguage))
	if err != nil {
		fail(err)
	}
	id, err := st.Pool.Acquire(ctx, f.tier)
	if err != nil {
		fail(err)
	}
	defer st.Pool.Release(id)
	u, _ := id.Base.Parse(task.RefPath(ref))
	xs, err := b.Capture(ctx, id, u.String())
	if err != nil {
		fail(err)
	}
	found := map[string]string{}
	for _, x := range xs {
		var keys []string
		var vars map[string]any
		if json.Unmarshal(x.Variables, &vars) == nil {
			for k := range vars {
				keys = append(keys, k)
			}
			sort.Strings(keys)
		}
		fmt.Printf("%-48s doc_id %-20s status %d  variables %s\n", x.FriendlyName, x.DocID, x.Status, strings.Join(keys, ","))
		if x.FriendlyName != "" && x.DocID != "" {
			found[x.FriendlyName] = x.DocID
		}
	}
	merged, dr := spike.MergeDocIDs(seed, found)
	out, _ := yaml.Marshal(map[string]any{"registry_queries_found": dr.Found, "missing": dr.Missing, "requests_captured": len(xs)})
	fmt.Print(string(out))
	if f.writeRegistry != "" {
		if err := spike.WriteRegistry(f.writeRegistry, merged, "fbs-spike render ("+page+")"); err != nil {
			fail(err)
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", f.writeRegistry)
	}
	if len(dr.Missing) > 0 {
		// A page load only requests the first timeline page; comment and
		// reply queries need the HAR fallback (clicks) or the bundles.
		fmt.Fprintln(os.Stderr, "the page did not request every registry query: use docids or the HAR fallback for the rest")
		return exitNeedsAction
	}
	return 0
}

func usage() {
	cmds := []string{"phase0", "preflight", "docids", "har", "render", "capture", "check-fixtures", "run", "report", "trim", "trim-report", "version"}
	sort.Strings(cmds)
	fmt.Fprintf(os.Stderr, "usage: fbs-spike <%s> [flags]   (fbs-spike <cmd> -h for flags)\nsee docs/runbooks/phase0-spike.md\n", strings.Join(cmds, "|"))
	os.Exit(exitUsage)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "fbs-spike:", spike.Redact(err.Error()))
	os.Exit(exitError)
}
