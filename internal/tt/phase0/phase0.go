// Package phase0 is tks's Phase 0 kit (docs/tiktok build brief §6, M7;
// checkpoint C4): the measurements the owner runs on the Phase 0 droplet
// before the budget, the sizing and the alert thresholds are set (D9).
//
// It does what the crawler does for an account in its first days, with the
// crawler's own client, parsers, operation registry and browser signer: the
// profile page (an identity's bootstrap), the creator list's check and its
// backfill back to the window, whole comment reads with their replies (every
// page signed by TikTok's own script in a browser page, as in production),
// and a video file. Every request becomes a record: its operation, class,
// wire bytes, time, exit and, when signed, the browser time of its signature.
// On top of that it asks Phase 0's questions: how many calls one IP takes
// before TikTok pushes back (a rate ladder, T20), how long a signed URL
// stays good (T27), whether a page signs for other accounts' posts (T32),
// and whether video hosts serve the downloader's own IP (T33). The report
// turns the records into GO or NO-GO and a cost per account per day at
// 1k, 10k, 100k and 300k accounts.
//
// Steps, in order (each can be resumed with --from): preflight, variants,
// smoke, run, ladder, report, bundle. The results hold no username, name, id,
// link or text: an account is its line in the accounts file, an exit a label
// (build brief §7), so the bundle can be sent back.
package phase0

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/htmlx"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// Steps.
const (
	StepPreflight = "preflight"
	StepVariants  = "variants"
	StepSmoke     = "smoke"
	StepRun       = "run"
	StepLadder    = "ladder"
	StepReport    = "report"
	StepBundle    = "bundle"
)

// Steps lists the steps in order.
var Steps = []string{StepPreflight, StepVariants, StepSmoke, StepRun, StepLadder, StepReport, StepBundle}

// Files written to the output directory.
const (
	FilePreflight   = "preflight.json"
	FileVariants    = "variants.json"
	FileVariantsLog = "variants.jsonl"
	FileRegistry    = "registry.yaml"
	FileReport      = "report.json"
	FileReportTxt   = "report.txt"
	FileMeta        = "meta.json"
	FileBundle      = "tks-phase0-bundle.tar.gz"
)

// SmokeFile, RunFile and LadderFile name a tier's results files.
func SmokeFile(tier string) string  { return "smoke-" + tier + ".jsonl" }
func RunFile(tier string) string    { return "run-" + tier + ".jsonl" }
func LadderFile(tier string) string { return "ladder-" + tier + ".jsonl" }

// StepError stops the kit. NeedsAction means the step worked but found
// something only a person (or a code change) can resolve; the bundle is
// written so the findings can be sent back.
type StepError struct {
	Step        string
	NeedsAction bool
	Err         error
}

func (e *StepError) Error() string { return e.Step + ": " + e.Err.Error() }
func (e *StepError) Unwrap() error { return e.Err }

// Stack holds the collaborators. cmd/tks-spike wires the real ones; tests
// wire fakett.
type Stack struct {
	BaseURL string
	Pool    *session.Pool
	Loader  *session.Loader
	// HTTP is this machine's own egress (the direct video way, T33).
	HTTP  *http.Client
	Rules detect.Rules
	// Opener opens signer pages (sign.BrowserOpener); nil leaves every call
	// unsigned, so comments are refused.
	Opener sign.Opener
	Signer config.Signer
	// LadderPools returns identity pools for up to n exit IPs of a tier,
	// one IP each; nil skips the ladder.
	LadderPools func(tier string, n int, rps float64) ([]LadderIP, error)
}

// Config configures a run.
type Config struct {
	Out   string
	Tiers []string
	// Accounts are the usernames, in the order chosen; records name them by
	// their position (1-based). They are only held in memory.
	Accounts []string
	// Seed is the operation registry the variants step starts from.
	Seed registry.Seed
	From string

	Run, Smoke Limits
	// SmokeAccounts and VariantAccounts are how many accounts those steps
	// use, from the first.
	SmokeAccounts, VariantAccounts int
	// Parallel is the accounts measured at once, each on its own identity.
	Parallel int
	// MaxCaptchas stops the run after this many captchas, so a bad exit
	// list does not burn IPs.
	MaxCaptchas int
	SoftPause   time.Duration
	// ReplayAges are the ages at which a signed URL is sent again;
	// ReplayWait is how long the run waits, once its accounts are done,
	// for the replays still due.
	ReplayAges []time.Duration
	ReplayWait time.Duration
	// IdentityRPS is the pace of one identity (tt.identity_rps), for the
	// smoke step's estimate.
	IdentityRPS    float64
	AcceptLanguage string
	// CookieDomains get the video cookie (tt.media.cookie_domains).
	CookieDomains []string
	Ladder        LadderConfig
	Model         Model
	Thresholds    Thresholds
	// CostPerGB is the proxy price per GB per tier (fb.proxy tiers).
	CostPerGB  map[string]float64
	ConfigPath string
	// Secrets must never appear in the bundle (proxy passwords and user
	// names).
	Secrets []string
	Version string
	Now     func() time.Time
}

// Kit runs Phase 0.
type Kit struct {
	Stack Stack
	Cfg   Config
	Log   io.Writer

	exits        Exits
	signer       *sign.Pool
	registrySeed *registry.Seed
	started      time.Time
	logMu        sync.Mutex
	resolvedMu   sync.Mutex
	resolved     map[int]string // account -> secUid, for the ladder
}

func (k *Kit) logf(format string, args ...any) {
	if k.Log != nil {
		k.logMu.Lock()
		_, _ = fmt.Fprintf(k.Log, format+"\n", args...)
		k.logMu.Unlock()
	}
}

func (k *Kit) now() time.Time {
	if k.Cfg.Now != nil {
		return k.Cfg.Now()
	}
	return time.Now()
}

func (k *Kit) path(name string) string { return filepath.Join(k.Cfg.Out, name) }

// line is an account's label in the records: its position, 1-based.
func (k *Kit) line(i int) int { return i + 1 }

func (k *Kit) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// cookieDomains are the hosts that get the video cookie: the configured
// ones and the base URL's own (fakett serves media from it).
func (k *Kit) cookieDomains() []string {
	d := slices.Clone(k.Cfg.CookieDomains)
	if u, err := url.Parse(k.Stack.BaseURL); err == nil && u.Hostname() != "" {
		d = append(d, u.Hostname())
	}
	return d
}

// client is the crawler's client on a registry, with the measured signer.
func (k *Kit) client(reg registry.Registry) *graphql.Client {
	c := &graphql.Client{Registry: reg, Rules: k.Stack.Rules, Builder: ttclient.Builder{AcceptLanguage: k.Cfg.AcceptLanguage}, Decode: ttclient.Decode}
	if k.signer != nil {
		c.Signer = ttclient.SignerFor(config.SignerBrowser, measuredSigner{k.signer})
	}
	return c
}

func (k *Kit) newWork(tier string, rec *Recorder, lim Limits) *work {
	reg := registry.Registry(registry.NewStatic(k.Cfg.Seed))
	if k.registrySeed != nil {
		reg = registry.NewStatic(*k.registrySeed)
	}
	return &work{k: k, client: k.client(reg), rec: rec, tier: tier, lim: lim, resolved: map[int]string{}}
}

func challenged(html []byte) bool { return htmlx.IsChallenge(html) }

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

func (k *Kit) defaults() {
	c := &k.Cfg
	if c.SmokeAccounts <= 0 {
		c.SmokeAccounts = 3
	}
	if c.VariantAccounts <= 0 {
		c.VariantAccounts = 3
	}
	c.Parallel = max(c.Parallel, 1)
	if c.MaxCaptchas <= 0 {
		c.MaxCaptchas = 10
	}
	if c.SoftPause == 0 {
		c.SoftPause = 2 * time.Second
	}
	if c.IdentityRPS <= 0 {
		c.IdentityRPS = 0.33
	}
	if c.From == "" {
		c.From = StepPreflight
	}
	c.Model.defaults()
	c.Thresholds.defaults()
	c.Ladder.defaults()
}

// Run executes the steps from Cfg.From to the end. It returns the report
// (when the report step ran) and a *StepError when a step stopped the run.
func (k *Kit) Run(ctx context.Context) (Report, error) {
	k.defaults()
	c := &k.Cfg
	start := slices.Index(Steps, c.From)
	if start < 0 {
		return Report{}, fmt.Errorf("phase0: unknown step %q (steps: %s)", c.From, strings.Join(Steps, ", "))
	}
	// The report and the bundle read only the results files, so a report
	// can be priced again from a bundle without the accounts.
	if len(c.Tiers) == 0 || (len(c.Accounts) == 0 && start < slices.Index(Steps, StepReport)) {
		return Report{}, errors.New("phase0: needs tiers and accounts")
	}
	if err := os.MkdirAll(c.Out, 0o700); err != nil {
		return Report{}, err
	}
	k.started = k.now().UTC()
	k.resolved = map[int]string{}
	if k.Stack.Opener != nil && start < slices.Index(Steps, StepReport) {
		k.signer = sign.New(k.Stack.Signer, measuredOpener(k.Stack.Opener))
		defer k.signer.Close()
	}
	var rep Report
	for _, step := range Steps[start:] {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		k.logf("\n== %s ==", step)
		var err error
		switch step {
		case StepPreflight:
			err = k.preflight(ctx)
		case StepVariants:
			err = k.variants(ctx)
		case StepSmoke:
			err = k.loadRegistry()
			if err == nil {
				err = k.smoke(ctx)
			}
		case StepRun:
			err = k.loadRegistry()
			if err == nil {
				err = k.run(ctx)
			}
		case StepLadder:
			err = k.loadRegistry()
			if err == nil {
				err = k.ladder(ctx)
			}
		case StepReport:
			rep, err = k.report()
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
			return rep, err
		}
	}
	return rep, nil
}

// loadRegistry reads the variants step's registry.
func (k *Kit) loadRegistry() error {
	if k.registrySeed != nil {
		return nil
	}
	b, err := os.ReadFile(k.path(FileRegistry))
	if err != nil {
		return &StepError{Step: StepVariants, Err: fmt.Errorf("no registry (%w): run the variants step (--from variants)", err)}
	}
	s, err := registry.ParseSeed(b)
	if err != nil {
		return &StepError{Step: StepVariants, Err: err}
	}
	k.registrySeed = &s
	return nil
}

// PreflightResult is what the preflight step checked.
type PreflightResult struct {
	OK     bool              `json:"ok"`
	Lines  []string          `json:"lines"`
	Tiers  map[string]string `json:"tiers"` // tier -> the first page's class
	Signer string            `json:"signer"`
}

func (k *Kit) preflight(ctx context.Context) error {
	res := PreflightResult{OK: true, Tiers: map[string]string{}}
	line := func(ok bool, format string, args ...any) {
		s := "PASS  "
		if !ok {
			s, res.OK = "FAIL  ", false
		}
		res.Lines = append(res.Lines, s+fmt.Sprintf(format, args...))
	}
	if p := k.Cfg.ConfigPath; p != "" {
		if st, err := os.Stat(p); err != nil {
			line(false, "config %s: %v", p, err)
		} else {
			line(st.Mode().Perm()&0o077 == 0, "config %s is private (mode %v; chmod 600 it if not)", p, st.Mode().Perm())
		}
	}
	rec := NewRecorder(io.Discard, StepPreflight, "")
	for _, tier := range k.Cfg.Tiers {
		w := k.newWork(tier, rec, Limits{})
		id, err := k.Stack.Pool.Acquire(ctx, tier)
		if err != nil {
			line(false, "tier %s: no identity: %v", tier, Clean(err.Error()))
			continue
		}
		start := time.Now()
		sec, why := w.page(ctx, id, k.exits.Label(id), 1, k.Cfg.Accounts[0])
		res.Tiers[tier] = first(why, "ok")
		line(sec != "", "tier %s: the first account's profile page in %s (%s)", tier, time.Since(start).Round(time.Millisecond), first(why, "ok"))
		if sec != "" && k.Stack.Opener != nil && tier == k.Cfg.Tiers[0] {
			start := time.Now()
			pg, err := k.Stack.Opener(ctx, id, profileURL(id, k.Cfg.Accounts[0]))
			if err == nil {
				pg.Close()
				res.Signer = "ok"
				line(true, "signer: a browser page opened on the first account in %s", time.Since(start).Round(time.Millisecond))
			} else {
				res.Signer = Clean(err.Error())
				line(false, "signer: no browser page: %s (run: make tks-browser-check)", res.Signer)
			}
		}
		k.Stack.Pool.Release(id)
	}
	if k.Stack.Opener == nil {
		res.Signer = "none"
		line(false, "signer: none; comments and replies need TikTok's signature (run with --signer browser)")
	}
	for _, l := range res.Lines {
		k.logf("%s", l)
	}
	if err := writeJSON(k.path(FilePreflight), res); err != nil {
		return err
	}
	if !res.OK {
		return &StepError{Step: StepPreflight, Err: errors.New("preflight failed; fix the FAIL lines and run again")}
	}
	return nil
}

// measure runs a step's accounts on a tier, Parallel at a time, skipping
// those already done; it stops handing out accounts at the captcha cap.
func (k *Kit) measure(ctx context.Context, w *work, accounts []int) {
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	done := 0
	for range k.Cfg.Parallel {
		wg.Go(func() {
			for i := range jobs {
				if ctx.Err() != nil || w.captchas.Load() >= int64(k.Cfg.MaxCaptchas) {
					continue
				}
				w.account(ctx, k.line(i), k.Cfg.Accounts[i])
				mu.Lock()
				done++
				k.logf("%s %s: %d/%d accounts", w.rec.Step, w.tier, done, len(accounts))
				mu.Unlock()
			}
		})
	}
	for _, i := range accounts {
		select {
		case jobs <- i:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()
	k.resolvedMu.Lock()
	maps.Copy(k.resolved, w.resolved)
	k.resolvedMu.Unlock()
}

func (k *Kit) smoke(ctx context.Context) error {
	n := min(k.Cfg.SmokeAccounts, len(k.Cfg.Accounts))
	var problems []string
	for _, tier := range k.Cfg.Tiers {
		f, err := os.OpenFile(k.path(SmokeFile(tier)), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		rec := NewRecorder(f, StepSmoke, tier)
		w := k.newWork(tier, rec, k.Cfg.Smoke)
		k.measure(ctx, w, seq(n))
		_ = f.Close()
		if err := ctx.Err(); err != nil {
			return err
		}
		recs, err := ReadFile(k.path(SmokeFile(tier)))
		if err != nil {
			return err
		}
		s := Summarize(recs)
		k.logf("%s: pages %s, lists %s, comments %s, captchas %d, drift %d, %s per request",
			tier, s.okShare(OpPage), s.okShare(OpListCheck), s.okShare(OpComments), s.Captchas, s.Drift, human(s.wirePerRequest()))
		switch {
		case s.Ops[OpPage].OK == 0:
			problems = append(problems, tier+": no profile page loaded (see "+k.path(SmokeFile(tier))+")")
		case s.Requests > 0 && float64(s.Blocks) >= 0.5*float64(s.Requests):
			problems = append(problems, fmt.Sprintf("%s: blocked: %d of %d requests refused, walled or challenged", tier, s.Blocks, s.Requests))
		case s.Drift > 0:
			problems = append(problems, tier+": a parser reported missing required fields (drift); send the bundle")
		case s.Ops[OpListCheck].OK == 0:
			problems = append(problems, tier+": no list answered; send the bundle")
		case s.Ops[OpComments].N > 0 && s.Ops[OpComments].OK == 0:
			problems = append(problems, tier+": no comment page answered; send the bundle")
		}
		perAccount := k.Cfg.Run.maxRequests()
		hours := float64(len(k.Cfg.Accounts)) * float64(perAccount) / (k.Cfg.IdentityRPS * float64(k.Cfg.Parallel)) / 3600
		k.logf("%s: the run takes at most ~%.1f h for %d accounts (%d at a time; at most %d requests an account); the ladder at most %s more",
			tier, hours, len(k.Cfg.Accounts), k.Cfg.Parallel, perAccount, k.Cfg.Ladder.maxDuration())
	}
	if len(problems) > 0 {
		return &StepError{Step: StepSmoke, NeedsAction: true, Err: errors.New(strings.Join(problems, "; "))}
	}
	return nil
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// maxRequests is the most requests one account can cost under these
// limits: the page, the list, the comment reads with their threads, and a
// video both ways.
func (l Limits) maxRequests() int {
	return 1 + 1 + l.MaxListPages + l.CommentPosts*(l.MaxCommentPages+l.ReplyThreads*l.MaxReplyPages) + 2
}

func (k *Kit) run(ctx context.Context) error {
	for _, tier := range k.Cfg.Tiers {
		path := k.path(RunFile(tier))
		recs, err := ReadFile(path)
		if err != nil {
			return err
		}
		done := map[int]bool{}
		for _, r := range recs {
			if r.Op == OpAccount {
				done[r.Account] = true
			}
		}
		var todo []int
		for i := range k.Cfg.Accounts {
			if !done[k.line(i)] {
				todo = append(todo, i)
			}
		}
		k.logf("%s: %d accounts to measure (%d done) -> %s", tier, len(todo), len(k.Cfg.Accounts)-len(todo), path)
		if len(todo) == 0 {
			continue
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // the kit's own output directory
		if err != nil {
			return err
		}
		rec := NewRecorder(f, StepRun, tier)
		w := k.newWork(tier, rec, k.Cfg.Run)
		w.replays = newReplayer(ctx, w, k.Cfg.ReplayAges, k.Cfg.Run.Replays)
		k.measure(ctx, w, todo)
		if w.replays.pending() && ctx.Err() == nil {
			k.logf("%s: waiting up to %s for the signed URLs' replays still due (T27)", tier, k.Cfg.ReplayWait)
		}
		w.replays.finish(k.Cfg.ReplayWait)
		_ = f.Close()
		if err := ctx.Err(); err != nil {
			k.logf("interrupted: rerun with --from run to resume")
			return err
		}
		if err := rec.Err(); err != nil {
			return err
		}
		if n := w.captchas.Load(); n >= int64(k.Cfg.MaxCaptchas) {
			return &StepError{Step: StepRun, NeedsAction: true, Err: fmt.Errorf("%s: stopped after %d captchas, so the exits are not burnt; send %s", tier, n, k.path(FileBundle))}
		}
	}
	return nil
}

func (k *Kit) report() (Report, error) {
	in := map[string][]Record{}
	for _, tier := range k.Cfg.Tiers {
		for _, f := range []string{RunFile(tier), LadderFile(tier)} {
			recs, err := ReadFile(k.path(f))
			if err != nil {
				return Report{}, err
			}
			in[tier] = append(in[tier], recs...)
		}
	}
	rep := BuildReport(in, ReportOptions{Model: k.Cfg.Model, Thresholds: k.Cfg.Thresholds, CostPerGB: k.Cfg.CostPerGB,
		Ladder: k.Cfg.Ladder, Limits: k.Cfg.Run, Accounts: len(k.Cfg.Accounts)})
	txt := FormatReport(rep)
	k.logf("%s", strings.TrimRight(txt, "\n"))
	if err := os.WriteFile(k.path(FileReportTxt), []byte(txt), 0o600); err != nil {
		return rep, err
	}
	return rep, writeJSON(k.path(FileReport), rep)
}

// bundleFile reports whether a file of the output directory belongs in the
// bundle: the kit's own outputs, nothing else.
func bundleFile(rel string) bool {
	switch rel {
	case FilePreflight, FileVariants, FileVariantsLog, FileRegistry, FileReport, FileReportTxt, FileMeta:
		return true
	}
	for _, p := range []string{"smoke-", "run-", "ladder-"} {
		if strings.HasPrefix(rel, p) && strings.HasSuffix(rel, ".jsonl") && !strings.Contains(rel, "/") {
			return true
		}
	}
	return false
}

// ErrUnsafeBundle stops the bundle when a file holds a proxy credential or
// an account's username.
var ErrUnsafeBundle = errors.New("phase0: bundle would contain a credential or a username")

func (k *Kit) bundle() error {
	meta := map[string]any{
		"version": k.Cfg.Version, "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH,
		"started_at": k.started, "written_at": k.now().UTC(), "tiers": k.Cfg.Tiers, "accounts": len(k.Cfg.Accounts),
		"parallel": k.Cfg.Parallel, "run_limits": k.Cfg.Run, "smoke_limits": k.Cfg.Smoke, "replay_ages": k.Cfg.ReplayAges,
		"ladder": k.Cfg.Ladder, "model": k.Cfg.Model, "thresholds": k.Cfg.Thresholds, "cost_per_gb_usd": k.Cfg.CostPerGB,
		"signer": k.Stack.Signer, "signer_on": k.Stack.Opener != nil,
	}
	if err := writeJSON(k.path(FileMeta), meta); err != nil {
		return err
	}
	entries, err := os.ReadDir(k.Cfg.Out)
	if err != nil {
		return err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && bundleFile(e.Name()) {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	var secrets []string
	for _, s := range k.Cfg.Secrets {
		if len(s) >= 6 {
			secrets = append(secrets, s)
		}
	}
	names := usernames(k.Cfg.Accounts)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, rel := range files {
		b, err := os.ReadFile(k.path(rel))
		if err != nil {
			return err
		}
		for _, s := range secrets {
			if bytes.Contains(b, []byte(s)) {
				return fmt.Errorf("%w: %s holds a proxy credential", ErrUnsafeBundle, rel)
			}
		}
		if names != nil && names.Match(b) {
			return fmt.Errorf("%w: %s holds an account's username (rerun with --from report after removing it)", ErrUnsafeBundle, rel)
		}
		if loc := credentials.FindIndex(b); loc != nil {
			return fmt.Errorf("%w: %s holds a URL with credentials", ErrUnsafeBundle, rel)
		}
		if err := tw.WriteHeader(&tar.Header{Name: "tks-phase0/" + rel, Mode: 0o600, Size: int64(len(b)), ModTime: k.now()}); err != nil {
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
	if err := os.WriteFile(k.path(FileBundle), buf.Bytes(), 0o600); err != nil {
		return err
	}
	k.logf("wrote %s (%d files, %s): no username, id, link, text or proxy credential in it", k.path(FileBundle), len(files), human(float64(buf.Len())))
	return nil
}

// usernames matches any of the accounts' usernames as a whole word.
// Usernames shorter than 5 characters are left out: they would match the
// kit's own words.
func usernames(accounts []string) *regexp.Regexp {
	var alts []string
	for _, a := range accounts {
		if len(a) >= 5 {
			alts = append(alts, regexp.QuoteMeta(strings.ToLower(a)))
		}
	}
	if len(alts) == 0 {
		return nil
	}
	return regexp.MustCompile(`(?i)(^|[^a-z0-9_.])(` + strings.Join(alts, "|") + `)($|[^a-z0-9_.])`)
}

func human(b float64) string {
	switch {
	case b >= 1e9:
		return fmt.Sprintf("%.2f GB", b/1e9)
	case b >= 1e6:
		return fmt.Sprintf("%.1f MB", b/1e6)
	case b >= 1e3:
		return fmt.Sprintf("%.1f KB", b/1e3)
	}
	return fmt.Sprintf("%.0f B", b)
}
