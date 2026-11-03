package phase0

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
)

// The signers comparison (tks-spike signers): which browser can sign
// comment calls for less memory than Chrome, which froze the pilot server
// (2026-11-03). Each browser presents itself as itself (sign.EngineOpener:
// its own user agent and client hints, its automation flag as it sets it)
// and does the crawler's work for a few accounts, one after the other,
// while its memory and CPU are sampled and a guard stops it before the
// machine runs short. Its page then stays open, idle, as Chrome's grew
// while idle, and signs once more.
const (
	StepSigners     = "signers"
	StepSignersIdle = "signers_idle"
	// OpPageFetch is a comment call the signer page sent itself.
	OpPageFetch = "page_fetch"

	FileSigners       = "signers.json"
	FileSignersTxt    = "signers.txt"
	FileSignersBundle = "tks-signers-bundle.tar.gz"
)

// SignersFile and SignersMemFile name an engine's records and samples.
func SignersFile(engine string) string    { return "signers-" + engine + ".jsonl" }
func SignersMemFile(engine string) string { return "signers-" + engine + "-mem.jsonl" }

// SignerEngine is one browser the comparison tries.
type SignerEngine struct {
	Name   string
	Opener sign.Opener
	// Tune changes the engine's pool configuration (open_on, idle_close,
	// max_page_mb) from the comparison's.
	Tune func(*config.Signer)
}

// SignersConfig configures the comparison.
type SignersConfig struct {
	Engines []SignerEngine
	// Accounts is how many accounts each engine works, from the first.
	Accounts int
	Limits   Limits
	// Idle is how long the page stays open, unused, after the accounts.
	Idle time.Duration
	// FetchEvery has the page also send the call itself on its first
	// signature and every this many after (0: never).
	FetchEvery  int
	SampleEvery time.Duration
	Guard       MemGuard
	// Signer is the pool's configuration (one page).
	Signer config.Signer
	// Pause is the wait between engines, for memory to settle.
	Pause time.Duration
}

func (c *SignersConfig) defaults() {
	if c.Accounts <= 0 {
		c.Accounts = 5
	}
	if c.SampleEvery <= 0 {
		c.SampleEvery = 2 * time.Second
	}
	c.Signer.Pages = max(c.Signer.Pages, 1)
	if c.Signer.SignTimeout <= 0 {
		c.Signer.SignTimeout = 30 * time.Second
	}
	if c.Signer.PageTimeout <= 0 {
		c.Signer.PageTimeout = 60 * time.Second
	}
}

// EngineResult is one browser's outcome.
type EngineResult struct {
	Engine string      `json:"engine"`
	Agent  *sign.Agent `json:"agent,omitempty"`
	// Stopped is why the engine stopped before its end, if it did.
	Stopped      string         `json:"stopped,omitempty"`
	Accounts     int            `json:"accounts"`
	AccountStops map[string]int `json:"account_stops"`
	PagesOpened  int            `json:"pages_opened"`
	PageOpenMs   Dist           `json:"page_open_ms"`
	// Signed are the comment and reply calls the page signed and the
	// identity sent; SignErrors the calls the page failed to sign (no page,
	// no signature).
	Signed        int            `json:"signed"`
	SignedOK      int            `json:"signed_ok"`
	SignErrors    int            `json:"sign_errors"`
	SignedClasses map[string]int `json:"signed_classes"`
	SignMs        Dist           `json:"sign_ms"`
	// ByBlock is the signed calls answered and sent in each 5 minutes from
	// the first ("answered/sent"): whether a page stays good (C4's went bad
	// within minutes).
	ByBlock []string `json:"by_block"`
	// PageFetches are the calls the page sent itself.
	PageFetches      int            `json:"page_fetches"`
	PageFetchOK      int            `json:"page_fetch_ok"`
	PageFetchClasses map[string]int `json:"page_fetch_classes"`
	// AfterIdle is the class of the first signed call after the idle
	// wait ("not run" when none was made).
	AfterIdle string `json:"after_idle"`
	// Memory (PSS of the browsers' processes, MB) and CPU (% of one core).
	PeakMB          float64 `json:"peak_mb"`
	WorkMeanMB      float64 `json:"work_mean_mb"`
	IdleStartMB     float64 `json:"idle_start_mb"`
	IdleEndMB       float64 `json:"idle_end_mb"`
	IdleGrowthMBMin float64 `json:"idle_growth_mb_per_min"`
	PeakProcs       int     `json:"peak_procs"`
	CPUMeanPct      float64 `json:"cpu_mean_pct"`
	CPUPeakPct      float64 `json:"cpu_peak_pct"`
	MinHostMB       float64 `json:"min_host_avail_mb"`
	// BrowserProxyBytes is what the browser moved through the exit;
	// PlainBytesPerSigned the identity's bytes per signed call it sent.
	BrowserProxyBytes   int64   `json:"browser_proxy_bytes"`
	PlainBytesPerSigned float64 `json:"plain_bytes_per_signed"`
	Verdict             string  `json:"verdict"`
}

// SignersReport is the comparison's outcome.
type SignersReport struct {
	At       time.Time      `json:"at"`
	Tier     string         `json:"tier"`
	Accounts int            `json:"accounts"`
	Idle     string         `json:"idle"`
	Guard    MemGuard       `json:"guard"`
	Engines  []EngineResult `json:"engines"`
}

// Signers runs the comparison: each engine in turn, then the report and
// the bundle.
func (k *Kit) Signers(ctx context.Context, sc SignersConfig) (SignersReport, error) {
	k.defaults()
	sc.defaults()
	c := &k.Cfg
	if len(c.Tiers) == 0 || len(c.Accounts) == 0 || len(sc.Engines) == 0 {
		return SignersReport{}, errors.New("signers: needs a tier, accounts and engines")
	}
	if err := os.MkdirAll(c.Out, 0o700); err != nil {
		return SignersReport{}, err
	}
	k.started = k.now().UTC()
	k.resolved = map[int]string{}
	if c.Registry != nil {
		k.registrySeed = c.Registry
	}
	if k.registrySeed == nil {
		if err := k.loadRegistry(); err != nil {
			k.logf("signers: no variants registry in %s; using the seed's variants", c.Out)
		}
	}
	tier := c.Tiers[0]
	accounts := c.Accounts[:min(sc.Accounts, len(c.Accounts))]
	rep := SignersReport{At: k.started, Tier: tier, Accounts: len(accounts), Idle: sc.Idle.String(), Guard: sc.Guard}
	for i, e := range sc.Engines {
		if i > 0 && !k.sleep(ctx, sc.Pause) {
			break
		}
		k.logf("\n== signers: %s ==", e.Name)
		res, err := k.signersEngine(ctx, e, tier, accounts, sc)
		if err != nil {
			return rep, err
		}
		k.logf("%s: %s", e.Name, res.Verdict)
		rep.Engines = append(rep.Engines, res)
		if ctx.Err() != nil {
			break
		}
	}
	if err := writeJSON(k.path(FileSigners), rep); err != nil {
		return rep, err
	}
	txt := FormatSigners(rep)
	if err := os.WriteFile(k.path(FileSignersTxt), []byte(txt), 0o600); err != nil {
		return rep, err
	}
	k.logf("\n%s", txt)
	if err := k.writeBundle(FileSignersBundle); err != nil {
		return rep, err
	}
	return rep, ctx.Err()
}

// enginePage is a signer page in the comparison: it sends some calls
// itself as well (FetchEvery) and counts its proxy bytes when closed.
type enginePage struct {
	sign.Page           // measured
	raw       sign.Page // the engine's own (sign.EnginePage)
	run       *engineRun
	signs     int
}

type engineRun struct {
	k          *Kit
	rec        *Recorder
	every      int
	opened     atomic.Int64
	proxyBytes atomic.Int64
}

func (p *enginePage) Sign(ctx context.Context, rawURL string) (string, error) {
	out, err := p.Page.Sign(ctx, rawURL)
	if err != nil {
		return out, err
	}
	p.signs++
	if r := p.run; r.every > 0 && (p.signs-1)%r.every == 0 {
		if f, ok := p.raw.(interface {
			PageFetch(context.Context, string) (*browser.Fetched, error)
		}); ok {
			r.pageFetch(ctx, f, rawURL)
		}
	}
	return out, nil
}

func (p *enginePage) Close() {
	if b, ok := p.raw.(interface{ ProxyBytes() int64 }); ok {
		p.run.proxyBytes.Add(b.ProxyBytes())
	}
	p.Page.Close()
}

// pageFetch has the page send the call itself, and records TikTok's
// answer.
func (r *engineRun) pageFetch(ctx context.Context, f interface {
	PageFetch(context.Context, string) (*browser.Fetched, error)
}, rawURL string) {
	rec := Record{Op: OpPageFetch, Signed: true}
	if c := callFrom(ctx); c != nil {
		rec.Account = c.account
	}
	start := time.Now()
	got, err := f.PageFetch(ctx, rawURL)
	rec.Ms = ms(start)
	switch {
	case err != nil:
		rec.Class, rec.Reason = detect.Transport.String(), err.Error()
	default:
		rec.Status = got.Status
		parts, errs, hasData := ttclient.Decode(got.Body)
		cls, reason := r.k.Stack.Rules.Classify(detect.Input{Status: got.Status, Body: got.Body, Errors: errs, HasData: hasData,
			ContentType: "application/json", Expect: "graphql"})
		if cls == detect.OK && len(parts) == 0 {
			cls, reason = detect.RegistryStale, "empty answer: request refused"
		}
		rec.Class, rec.Reason = cls.String(), reason
	}
	r.rec.Add(rec)
}

func (k *Kit) signersEngine(ctx context.Context, e SignerEngine, tier string, accounts []string, sc SignersConfig) (EngineResult, error) {
	res := EngineResult{Engine: e.Name, AccountStops: map[string]int{}, SignedClasses: map[string]int{}, PageFetchClasses: map[string]int{}}
	ectx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	recF, err := os.OpenFile(k.path(SignersFile(e.Name)), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return res, err
	}
	defer func() { _ = recF.Close() }()
	memF, err := os.OpenFile(k.path(SignersMemFile(e.Name)), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return res, err
	}
	defer func() { _ = memF.Close() }()
	rec := NewRecorder(recF, StepSigners, tier)
	smp := newSampler(e.Name, memF, sc.SampleEvery, sc.Guard, cancel)
	smp.start()

	run := &engineRun{k: k, rec: rec, every: sc.FetchEvery}
	var agents sign.Agents
	open := func(ctx context.Context, id *session.Identity, pageURL string) (sign.Page, error) {
		pg, err := e.Opener(ctx, id, pageURL)
		if err == nil {
			run.opened.Add(1)
		}
		return pg, err
	}
	measured := measuredOpener(agents.Opener(open))
	pc := sc.Signer
	if e.Tune != nil {
		e.Tune(&pc)
	}
	k.signer = sign.New(pc, func(ctx context.Context, id *session.Identity, pageURL string) (sign.Page, error) {
		pg, err := measured(ctx, id, pageURL)
		if err != nil {
			return nil, err
		}
		raw := pg
		if m, ok := pg.(*measuredPage); ok {
			raw = m.Page
		}
		return &enginePage{Page: pg, raw: raw, run: run}, nil
	})
	k.agents = &agents
	defer func() {
		if k.signer != nil {
			k.signer.Close()
		}
		k.signer, k.agents = nil, nil
	}()

	smp.setPhase("work")
	w := k.newWork(tier, rec, sc.Limits)
	for i, name := range accounts {
		if ectx.Err() != nil {
			break
		}
		w.account(ectx, k.line(i), name)
		k.logf("%s: account %d of %d done", e.Name, i+1, len(accounts))
	}
	if ectx.Err() == nil && sc.Idle > 0 {
		k.logf("%s: the page stays open, unused, for %s", e.Name, sc.Idle)
		smp.setPhase("idle")
		k.sleep(ectx, sc.Idle)
	}
	if ectx.Err() == nil {
		// One more signature on the page that idled.
		smp.setPhase("after_idle")
		idle := NewRecorder(recF, StepSignersIdle, tier)
		lim := sc.Limits
		lim.CommentPosts, lim.MaxCommentPages, lim.ReplyThreads, lim.MaxListPages = 1, 1, 0, 0
		k.newWork(tier, idle, lim).account(ectx, k.line(0), accounts[0])
	}
	k.signer.Close()
	k.signer = nil
	smp.setPhase("closed")
	smp.finish()
	if err := rec.Err(); err != nil {
		return res, err
	}
	samples, reason, err := smp.result()
	if err != nil {
		return res, err
	}
	if reason != "" {
		res.Stopped = "memory guard: " + reason
	} else if cause := context.Cause(ectx); ctx.Err() != nil && cause != nil {
		res.Stopped = cause.Error()
	}
	if a, ok := agents.Agent(); ok {
		res.Agent = &a
	}
	res.PagesOpened = int(run.opened.Load())
	res.BrowserProxyBytes = run.proxyBytes.Load()
	recs, err := ReadFile(k.path(SignersFile(e.Name)))
	if err != nil {
		return res, err
	}
	engineFigures(&res, recs, samples)
	return res, nil
}

// engineFigures fills a result from the engine's records and samples.
func engineFigures(res *EngineResult, recs []Record, samples []MemSample) {
	var sign, open []float64
	var plain int64
	res.AfterIdle = "not run"
	var first time.Time
	var blocks [][2]int
	for _, r := range recs {
		switch {
		case r.Op == OpAccount && r.Step == StepSigners:
			res.Accounts++
			res.AccountStops[first0(r.Stop, "ok")]++
		case r.Op == OpPageFetch:
			res.PageFetches++
			res.PageFetchClasses[r.Class]++
			if r.Class == detect.OK.String() {
				res.PageFetchOK++
			}
		case (r.Op == OpComments || r.Op == OpReplies) && r.Signed:
			if r.PageOpenMs > 0 {
				open = append(open, float64(r.PageOpenMs))
			}
			if r.Step == StepSignersIdle {
				if res.AfterIdle == "not run" {
					res.AfterIdle = r.Class
				}
				continue
			}
			if r.SignError {
				res.SignErrors++
				continue
			}
			res.Signed++
			res.SignedClasses[r.Class]++
			if first.IsZero() {
				first = r.T
			}
			b := int(r.T.Sub(first) / (5 * time.Minute))
			for len(blocks) <= b {
				blocks = append(blocks, [2]int{})
			}
			blocks[b][1]++
			if r.Class == detect.OK.String() {
				res.SignedOK++
				blocks[b][0]++
			}
			sign = append(sign, float64(r.SignMs))
			plain += r.Wire
		}
	}
	res.SignMs, res.PageOpenMs = dist(sign), dist(open)
	for _, b := range blocks {
		res.ByBlock = append(res.ByBlock, fmt.Sprintf("%d/%d", b[0], b[1]))
	}
	if res.Signed > 0 {
		res.PlainBytesPerSigned = float64(plain) / float64(res.Signed)
	}
	var work, cpu []float64
	var idle []MemSample
	res.MinHostMB = -1
	for _, s := range samples {
		res.PeakMB = max(res.PeakMB, s.BrowserMB)
		res.PeakProcs = max(res.PeakProcs, s.Procs)
		if s.HostAvailMB > 0 && (res.MinHostMB < 0 || s.HostAvailMB < res.MinHostMB) {
			res.MinHostMB = s.HostAvailMB
		}
		switch s.Phase {
		case "work":
			work = append(work, s.BrowserMB)
			cpu = append(cpu, s.CPUPct)
		case "idle":
			idle = append(idle, s)
			cpu = append(cpu, s.CPUPct)
		}
		res.CPUPeakPct = max(res.CPUPeakPct, s.CPUPct)
	}
	res.MinHostMB = max(res.MinHostMB, 0)
	res.WorkMeanMB = dist(work).Mean
	res.CPUMeanPct = dist(cpu).Mean
	if len(idle) > 0 {
		a, b := idle[0], idle[len(idle)-1]
		res.IdleStartMB, res.IdleEndMB = a.BrowserMB, b.BrowserMB
		if mins := b.T.Sub(a.T).Minutes(); mins > 0 {
			res.IdleGrowthMBMin = (b.BrowserMB - a.BrowserMB) / mins
		}
	}
	res.Verdict = verdictOf(*res)
}

func verdictOf(r EngineResult) string {
	var parts []string
	switch {
	case r.Signed == 0 && r.SignErrors > 0:
		parts = append(parts, fmt.Sprintf("could not sign (%d sign errors)", r.SignErrors))
	case r.Signed == 0:
		parts = append(parts, "signed nothing")
	default:
		parts = append(parts, fmt.Sprintf("TikTok answered %s of its signed calls", pct(r.SignedOK, r.Signed)))
	}
	if r.PageFetches > 0 {
		parts = append(parts, fmt.Sprintf("%s of the calls the page sent itself", pct(r.PageFetchOK, r.PageFetches)))
	}
	parts = append(parts, fmt.Sprintf("memory peak %.0f MB", r.PeakMB))
	if r.Stopped != "" {
		parts = append(parts, "STOPPED: "+r.Stopped)
	}
	return strings.Join(parts, "; ")
}

// FormatSigners writes the comparison as a table, a column per browser.
func FormatSigners(rep SignersReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "tks-spike signers, %s: %d account(s) per browser on tier %s, %s idle; guard: browser over %.0f MB, machine under %.0f MB available\n\n",
		rep.At.Format("2006-01-02 15:04 UTC"), rep.Accounts, rep.Tier, rep.Idle, rep.Guard.MaxBrowserMB, rep.Guard.MinHostMB)
	if len(rep.Engines) == 0 {
		b.WriteString("no browser ran\n")
		return b.String()
	}
	row := func(name string, f func(EngineResult) string) {
		fmt.Fprintf(&b, "%-30s", name)
		for _, e := range rep.Engines {
			fmt.Fprintf(&b, " %-30s", f(e))
		}
		b.WriteString("\n")
	}
	row("", func(e EngineResult) string { return e.Engine })
	row("says it is", func(e EngineResult) string {
		if e.Agent == nil {
			return "the identity's Chrome"
		}
		return product(e.Agent.UserAgent)
	})
	row("navigator.webdriver", func(e EngineResult) string {
		if e.Agent == nil {
			return "false (hidden)"
		}
		return fmt.Sprint(e.Agent.Webdriver)
	})
	row("accounts worked", func(e EngineResult) string { return fmt.Sprint(e.Accounts) })
	row("pages opened", func(e EngineResult) string { return fmt.Sprint(e.PagesOpened) })
	row("page open, median", func(e EngineResult) string { return secs(e.PageOpenMs.P50) })
	row("signed calls answered", func(e EngineResult) string { return pct(e.SignedOK, e.Signed) })
	row("  refused as", func(e EngineResult) string { return classes(e.SignedClasses) })
	row("  answered, each 5 minutes", func(e EngineResult) string { return first0(strings.Join(e.ByBlock, " "), "-") })
	row("sign errors", func(e EngineResult) string { return fmt.Sprint(e.SignErrors) })
	row("page sends answered", func(e EngineResult) string { return pct(e.PageFetchOK, e.PageFetches) })
	row("sign time, median / p95", func(e EngineResult) string { return secs(e.SignMs.P50) + " / " + secs(e.SignMs.P95) })
	row("signed after the idle wait", func(e EngineResult) string { return e.AfterIdle })
	row("memory, peak", func(e EngineResult) string { return fmt.Sprintf("%.0f MB", e.PeakMB) })
	row("memory, mean while working", func(e EngineResult) string { return fmt.Sprintf("%.0f MB", e.WorkMeanMB) })
	row("memory, idle start -> end", func(e EngineResult) string { return fmt.Sprintf("%.0f -> %.0f MB", e.IdleStartMB, e.IdleEndMB) })
	row("memory growth while idle", func(e EngineResult) string { return fmt.Sprintf("%+.1f MB/min", e.IdleGrowthMBMin) })
	row("processes, most", func(e EngineResult) string { return fmt.Sprint(e.PeakProcs) })
	row("CPU, mean / peak (1 core)", func(e EngineResult) string { return fmt.Sprintf("%.0f%% / %.0f%%", e.CPUMeanPct, e.CPUPeakPct) })
	row("machine memory, lowest", func(e EngineResult) string { return fmt.Sprintf("%.0f MB", e.MinHostMB) })
	row("browser proxy data", func(e EngineResult) string { return human(float64(e.BrowserProxyBytes)) })
	row("identity data per signed call", func(e EngineResult) string { return human(e.PlainBytesPerSigned) })
	row("stopped early", func(e EngineResult) string { return first0(e.Stopped, "no") })
	b.WriteString("\n")
	for _, e := range rep.Engines {
		fmt.Fprintf(&b, "%s: %s\n", e.Engine, e.Verdict)
	}
	return b.String()
}

// product is a user agent's last product token (Lightpanda/1.0,
// HeadlessChrome/153.0.0.0), or the whole string.
func product(ua string) string {
	fs := strings.Fields(ua)
	for i := len(fs) - 1; i >= 0; i-- {
		if strings.Contains(fs[i], "/") && !strings.HasPrefix(fs[i], "Safari/") {
			return fs[i]
		}
	}
	return ua
}

func secs(msv float64) string {
	switch {
	case msv == 0:
		return "-"
	case msv < 1000:
		return fmt.Sprintf("%.0f ms", msv)
	}
	return fmt.Sprintf("%.1f s", msv/1000)
}

func classes(m map[string]int) string {
	var out []string
	for _, c := range sortedKeys(m) {
		if c != detect.OK.String() {
			out = append(out, fmt.Sprintf("%s %d", c, m[c]))
		}
	}
	if len(out) == 0 {
		return "-"
	}
	return strings.Join(out, ", ")
}

// signersBundleFile reports the comparison's files.
func signersBundleFile(rel string) bool {
	if rel == FileSigners || rel == FileSignersTxt {
		return true
	}
	return strings.HasPrefix(rel, "signers-") && strings.HasSuffix(rel, ".jsonl") && !strings.Contains(rel, "/")
}
