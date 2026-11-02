package phase0

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
)

// Dist describes a sample.
type Dist struct {
	N    int     `json:"n"`
	Mean float64 `json:"mean"`
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	Max  float64 `json:"max"`
}

func dist(xs []float64) Dist {
	if len(xs) == 0 {
		return Dist{}
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	sum := 0.0
	for _, x := range s {
		sum += x
	}
	q := func(p float64) float64 { return s[min(len(s)-1, int(math.Ceil(p*float64(len(s))))-1)] }
	return Dist{N: len(s), Mean: sum / float64(len(s)), P50: q(0.5), P95: q(0.95), Max: s[len(s)-1]}
}

// OpStats are one operation's requests.
type OpStats struct {
	N       int            `json:"n"`
	OK      int            `json:"ok"`
	Classes map[string]int `json:"classes"`
	// Retried are soft errors retried once, as the crawler does: not
	// counted against the operation.
	Retried int `json:"retried,omitempty"`
	// Wire and Ms describe the answered requests.
	Wire Dist `json:"wire_bytes"`
	Ms   Dist `json:"ms"`
	// The signer, over signed requests.
	Signed     int  `json:"signed,omitempty"`
	SignedOK   int  `json:"signed_ok,omitempty"`
	SignErrors int  `json:"sign_errors,omitempty"`
	SignMs     Dist `json:"sign_ms"`
	PageOpenMs Dist `json:"page_open_ms"`
	// Cross are signed requests whose page was opened for another account
	// (T32), and how many of them TikTok answered.
	Cross   int `json:"cross_account,omitempty"`
	CrossOK int `json:"cross_account_ok,omitempty"`

	wire, ms, sign, open []float64
}

// ReplayBucket counts signed URLs sent again within an age bucket (T27).
type ReplayBucket struct {
	N  int `json:"n"`
	OK int `json:"ok"`
}

// LadderStep is one ladder step on one IP.
type LadderStep struct {
	Rate   float64 `json:"rate"`
	Calls  int     `json:"calls"`
	Blocks int     `json:"blocks"`
	Stop   string  `json:"stop,omitempty"`
}

// ExitStats are one exit's requests in the run.
type ExitStats struct {
	Requests int     `json:"requests"`
	Blocks   int     `json:"blocks"`
	PerHour  float64 `json:"per_hour"`
	first    time.Time
	last     time.Time
}

// Summary is a tier's measurements.
type Summary struct {
	Ops        map[string]*OpStats `json:"ops"`
	Requests   int                 `json:"requests"`
	Blocks     int                 `json:"blocks"`
	Captchas   int                 `json:"captchas"`
	Challenges int                 `json:"challenges"`
	Drift      int                 `json:"drift"`
	Hours      float64             `json:"hours"`

	Accounts     int            `json:"accounts"`
	Measured     int            `json:"measured"` // accounts whose list was read
	AccountStops map[string]int `json:"account_stops"`
	// PostsPerDay is each measured account's posting rate within the
	// backfill window.
	PostsPerDay Dist `json:"posts_per_day"`
	// Window totals over measured accounts.
	Posts           int     `json:"posts"`
	VideoPosts      int     `json:"video_posts"`
	CommentCount    int64   `json:"comment_count"`
	CommentPagesEst int64   `json:"comment_pages_est"`
	ListPages       Dist    `json:"list_pages"`
	BackfillHours   float64 `json:"-"`

	// Comment reads: how many, how many whole, what share of the post's
	// count they collected when whole (T15), and the signed pages per
	// ceil(count/20) page estimate (the reads' replies included).
	CommentReads    int     `json:"comment_reads"`
	CommentComplete int     `json:"comment_complete"`
	Served          Dist    `json:"served_ratio"`
	PagesFactor     float64 `json:"pages_factor"`
	PagesFactorFrom string  `json:"pages_factor_from"`

	Replays map[string]*ReplayBucket `json:"replays"`
	Exits   map[string]*ExitStats    `json:"exits"`
	Ladder  map[string][]LadderStep  `json:"ladder,omitempty"`
}

// Replay age buckets.
var replayBuckets = []struct {
	name string
	max  time.Duration
}{{"<=2m", 2 * time.Minute}, {"<=10m", 10 * time.Minute}, {"<=30m", 30 * time.Minute}, {"<=1h", time.Hour}, {">1h", math.MaxInt64}}

func bucketOf(age time.Duration) string {
	for _, b := range replayBuckets {
		if age <= b.max+30*time.Second {
			return b.name
		}
	}
	return ">1h"
}

func isRequest(op string) bool {
	switch op {
	case OpAccount, OpCommentRead, OpLadderStep:
		return false
	}
	return true
}

// Summarize reads a tier's records.
func Summarize(recs []Record) Summary {
	s := Summary{Ops: map[string]*OpStats{}, AccountStops: map[string]int{}, Replays: map[string]*ReplayBucket{},
		Exits: map[string]*ExitStats{}, Ladder: map[string][]LadderStep{}}
	var first, last time.Time
	var ppd, listPages []float64
	var served []float64
	var factorPages, factorEst float64
	var anyPages, anyItems float64
	for _, r := range recs {
		if r.Step == StepLadder && r.Op != OpLadder && r.Op != OpLadderStep {
			continue // the ladder's own bootstraps
		}
		if first.IsZero() || r.T.Before(first) {
			first = r.T
		}
		if r.T.After(last) {
			last = r.T
		}
		if r.Drift != "" {
			s.Drift++
		}
		switch r.Op {
		case OpAccount:
			s.Accounts++
			s.AccountStops[first0(r.Stop, "ok")]++
			if r.Pages > 0 && r.Days > 0 {
				s.Measured++
				ppd = append(ppd, float64(r.Posts)/r.Days)
				listPages = append(listPages, float64(r.Pages))
				s.Posts += r.Posts
				s.VideoPosts += r.VideoPosts
				s.CommentCount += r.CommentCount
				s.CommentPagesEst += r.CommentPagesEst
			}
			continue
		case OpCommentRead:
			s.CommentReads++
			anyPages += float64(r.Pages + r.ReplyPages)
			anyItems += float64(r.Collected)
			if r.Complete {
				s.CommentComplete++
				if r.CommentCount > 0 {
					served = append(served, float64(r.Collected)/float64(r.CommentCount))
					factorPages += float64(r.Pages + r.ReplyPages)
					factorEst += math.Ceil(float64(r.CommentCount) / commentPageSize)
				}
			}
			continue
		case OpLadderStep:
			s.Ladder[r.Exit] = append(s.Ladder[r.Exit], LadderStep{Rate: r.Rate, Calls: r.Calls, Blocks: r.Blocks, Stop: r.Stop})
			continue
		}
		if !isRequest(r.Op) {
			continue
		}
		o := s.Ops[r.Op]
		if o == nil {
			o = &OpStats{Classes: map[string]int{}}
			s.Ops[r.Op] = o
		}
		o.N++
		o.Classes[r.Class]++
		if r.Retried {
			o.Retried++
		}
		if r.Class == detect.OK.String() {
			o.OK++
			o.wire = append(o.wire, float64(r.Wire))
			o.ms = append(o.ms, float64(r.Ms))
		}
		if r.Signed {
			o.Signed++
			if r.Class == detect.OK.String() {
				o.SignedOK++
			}
			if r.SignError || r.Class == classSignError {
				o.SignErrors++
			}
			if !r.SignError && r.Class != classSignError {
				o.sign = append(o.sign, float64(r.SignMs))
			}
			if r.PageOpenMs > 0 {
				o.open = append(o.open, float64(r.PageOpenMs))
			}
			if r.CrossAccount {
				o.Cross++
				if r.Class == detect.OK.String() {
					o.CrossOK++
				}
			}
		}
		if r.Op == OpSigReplay {
			b := bucketOf(time.Duration(r.AgeS) * time.Second)
			if s.Replays[b] == nil {
				s.Replays[b] = &ReplayBucket{}
			}
			s.Replays[b].N++
			if r.Class == detect.OK.String() {
				s.Replays[b].OK++
			}
		}
		if r.Op == OpVideoProxy || r.Op == OpVideoDirect || r.Op == OpLadder {
			continue // media hosts, and the ladder's deliberate overload: not in the run's counts
		}
		s.Requests++
		if blocking(r.Class) {
			s.Blocks++
		}
		switch r.Class {
		case detect.Checkpoint.String():
			s.Captchas++
		case classChallenge:
			s.Challenges++
		}
		if r.Exit != "" {
			e := s.Exits[r.Exit]
			if e == nil {
				e = &ExitStats{first: r.T}
				s.Exits[r.Exit] = e
			}
			e.Requests++
			if blocking(r.Class) {
				e.Blocks++
			}
			if r.T.Before(e.first) {
				e.first = r.T
			}
			if r.T.After(e.last) {
				e.last = r.T
			}
		}
	}
	for _, o := range s.Ops {
		o.Wire, o.Ms, o.SignMs, o.PageOpenMs = dist(o.wire), dist(o.ms), dist(o.sign), dist(o.open)
	}
	for _, e := range s.Exits {
		if h := e.last.Sub(e.first).Hours(); h > 0 {
			e.PerHour = float64(e.Requests) / h
		}
	}
	if !first.IsZero() {
		s.Hours = last.Sub(first).Hours()
	}
	s.PostsPerDay, s.ListPages, s.Served = dist(ppd), dist(listPages), dist(served)
	switch {
	case factorEst > 0:
		s.PagesFactor, s.PagesFactorFrom = factorPages/factorEst, "whole reads"
	case anyItems > 0:
		// No whole read: pages per item collected, against 20 a page.
		s.PagesFactor, s.PagesFactorFrom = anyPages/anyItems*commentPageSize, "partial reads"
	default:
		s.PagesFactor, s.PagesFactorFrom = 1, "assumed (no comment read)"
	}
	return s
}

func first0(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (s Summary) okShare(op string) string {
	o := s.Ops[op]
	if o == nil || o.N == 0 {
		return "none"
	}
	return fmt.Sprintf("%d/%d ok", o.OK, o.N)
}

func (s Summary) wirePerRequest() float64 {
	var sum float64
	var n int
	for op, o := range s.Ops {
		if op == OpVideoProxy || op == OpVideoDirect {
			continue
		}
		sum += o.Wire.Mean * float64(o.Wire.N)
		n += o.Wire.N
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}

// share is the operations' answered calls, of those that count: a soft
// error retried once is the retry's, as in the crawler.
func (s Summary) share(ops ...string) (ok, n int) {
	for _, op := range ops {
		if o := s.Ops[op]; o != nil {
			ok += o.OK
			n += o.N - o.Retried
		}
	}
	return ok, n
}

// Tolerance is the highest ladder rate an IP took with at most okBlock of
// its calls blocked, every lower step too; 0 when its first step failed.
func Tolerance(steps []LadderStep, okBlock float64) (rate float64, reached float64) {
	s := slices.Clone(steps)
	slices.SortFunc(s, func(a, b LadderStep) int {
		switch {
		case a.Rate < b.Rate:
			return -1
		case a.Rate > b.Rate:
			return 1
		}
		return 0
	})
	for _, st := range s {
		reached = st.Rate
		if st.Calls == 0 || float64(st.Blocks) > okBlock*float64(st.Calls) {
			break
		}
		rate = st.Rate
	}
	return rate, reached
}

// Thresholds decide GO or NO-GO.
type Thresholds struct {
	MinPages           float64 `json:"min_pages"`             // profile pages answered
	MinLists           float64 `json:"min_lists"`             // list calls answered
	MaxCaptchasPer1000 float64 `json:"max_captchas_per_1000"` // captchas and challenges per 1000 requests in the run
	MinSigned          float64 `json:"min_signed"`            // signed calls answered
	MinServed          float64 `json:"min_served"`            // median share of a post's count a whole read collects (warning only; A4, T15)
	// MinLadderRPS is the rate per IP the ladder must hold (the crawler's
	// identity pace).
	MinLadderRPS float64 `json:"min_ladder_rps"`
	// BudgetUSD is the owner's budget per account per day at
	// TargetAccounts (D9); 0 = not set yet.
	BudgetUSD      float64 `json:"budget_usd"`
	TargetAccounts int     `json:"target_accounts"`
}

func (t *Thresholds) defaults() {
	if t.MinPages <= 0 {
		t.MinPages = 0.95
	}
	if t.MinLists <= 0 {
		t.MinLists = 0.98
	}
	if t.MaxCaptchasPer1000 <= 0 {
		t.MaxCaptchasPer1000 = 2
	}
	if t.MinSigned <= 0 {
		t.MinSigned = 0.95
	}
	if t.MinServed <= 0 {
		t.MinServed = 0.9
	}
	if t.MinLadderRPS <= 0 {
		t.MinLadderRPS = 0.33
	}
	if t.TargetAccounts <= 0 {
		t.TargetAccounts = 300_000
	}
}

// Check results.
const (
	Pass = "PASS"
	Warn = "WARN"
	Fail = "FAIL"
	Info = "INFO"
)

// Check is one line of the verdict.
type Check struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Value  string `json:"value"`
	Want   string `json:"want,omitempty"`
	Advice string `json:"advice,omitempty"`
}

// ReportOptions configure the report.
type ReportOptions struct {
	Model      Model
	Thresholds Thresholds
	CostPerGB  map[string]float64
	Ladder     LadderConfig
	Limits     Limits
	Accounts   int
}

// TierReport is one tier's verdict, measurements and costs.
type TierReport struct {
	Tier     string   `json:"tier"`
	Verdict  string   `json:"verdict"`
	Checks   []Check  `json:"checks"`
	Summary  Summary  `json:"summary"`
	Estimate Estimate `json:"estimate"`
}

// Report is the whole run's.
type Report struct {
	Verdict string       `json:"verdict"`
	Tiers   []TierReport `json:"tiers"`
}

// BuildReport summarises each tier's records into its checks and costs.
func BuildReport(in map[string][]Record, o ReportOptions) Report {
	o.Model.defaults()
	o.Thresholds.defaults()
	o.Ladder.defaults()
	rep := Report{Verdict: "GO"}
	for _, tier := range sortedKeys(in) {
		s := Summarize(in[tier])
		tr := TierReport{Tier: tier, Summary: s}
		tol, ipsTested := ladderTolerance(s, o.Ladder.OKBlock)
		tr.Estimate = o.Model.Estimate(s, tol, o.CostPerGB[tier])
		tr.Checks = checks(s, tr.Estimate, tol, ipsTested, o)
		tr.Verdict = "GO"
		for _, c := range tr.Checks {
			if c.Status == Fail {
				tr.Verdict = "NO-GO"
			}
		}
		if tr.Verdict != "GO" {
			rep.Verdict = "NO-GO"
		}
		rep.Tiers = append(rep.Tiers, tr)
	}
	if len(rep.Tiers) == 0 {
		rep.Verdict = "NO-GO"
	}
	return rep
}

// ladderTolerance is the lowest tolerated rate over the IPs that climbed
// (the fleet is sized for its weakest IP), and how many climbed.
func ladderTolerance(s Summary, okBlock float64) (float64, int) {
	if len(s.Ladder) == 0 {
		return 0, 0
	}
	low := math.Inf(1)
	for _, steps := range s.Ladder {
		r, _ := Tolerance(steps, okBlock)
		low = math.Min(low, r)
	}
	return low, len(s.Ladder)
}

func pct(ok, n int) string {
	if n == 0 {
		return "none"
	}
	return fmt.Sprintf("%.1f%% (%d/%d)", 100*float64(ok)/float64(n), ok, n)
}

func atLeast(ok, n int, min float64) bool { return n > 0 && float64(ok) >= min*float64(n) }

func checks(s Summary, e Estimate, tol float64, ips int, o ReportOptions) []Check {
	t := o.Thresholds
	var out []Check
	add := func(c Check) { out = append(out, c) }
	status := func(ok bool) string {
		if ok {
			return Pass
		}
		return Fail
	}

	ok, n := s.share(OpPage)
	add(Check{Name: "profile pages", Status: status(atLeast(ok, n, t.MinPages)), Value: pct(ok, n), Want: fmt.Sprintf(">= %.0f%%", 100*t.MinPages),
		Advice: "pages are each identity's bootstrap; failures here are walls or challenges (T28): try another tier"})
	ok, n = s.share(OpListCheck, OpListBackfill)
	add(Check{Name: "list calls", Status: status(atLeast(ok, n, t.MinLists)), Value: pct(ok, n), Want: fmt.Sprintf(">= %.0f%%", 100*t.MinLists),
		Advice: "the creator list is every check; see the classes in report.json"})
	per1000 := 0.0
	if s.Requests > 0 {
		per1000 = 1000 * float64(s.Captchas+s.Challenges) / float64(s.Requests)
	}
	add(Check{Name: "captchas and challenges", Status: status(s.Requests > 0 && per1000 <= t.MaxCaptchasPer1000),
		Value: fmt.Sprintf("%.2f per 1000 requests (%d captchas, %d challenges in %d)", per1000, s.Captchas, s.Challenges, s.Requests),
		Want:  fmt.Sprintf("<= %.1f", t.MaxCaptchasPer1000), Advice: "at the crawler's pace TikTok should rarely ask (T6); a higher rate means the exits are scored badly"})
	add(Check{Name: "parsers", Status: status(s.Drift == 0), Value: fmt.Sprintf("%d answers with missing required fields", s.Drift), Want: "0",
		Advice: "TikTok's answers changed shape: send the bundle"})
	signed, signedOK, signErr := 0, 0, 0
	for _, op := range []string{OpComments, OpReplies, OpListCheck, OpListBackfill} {
		if x := s.Ops[op]; x != nil {
			signed += x.Signed
			signedOK += x.SignedOK
			signErr += x.SignErrors
		}
	}
	add(Check{Name: "signed calls", Status: status(atLeast(signedOK, signed, t.MinSigned)),
		Value: pct(signedOK, signed) + fmt.Sprintf(", %d signer errors", signErr), Want: fmt.Sprintf(">= %.0f%%", 100*t.MinSigned),
		Advice: "comments and replies need TikTok's signature (G1); check the browser (make tks-browser-check) and the signer's errors"})
	served := Check{Name: "comments collected (T15)", Status: Pass, Value: fmt.Sprintf("median %.0f%% of a post's count over %d whole reads", 100*s.Served.P50, s.Served.N),
		Want: fmt.Sprintf(">= %.0f%% (warning only; A4)", 100*t.MinServed)}
	if s.Served.N == 0 {
		served.Status, served.Value = Warn, "no whole read (raise --max-comment-pages)"
	} else if s.Served.P50 < t.MinServed {
		served.Status, served.Advice = Warn, "TikTok's count includes comments it does not serve; A4's 90% target needs the owner's review"
	}
	add(served)
	switch ips {
	case 0:
		add(Check{Name: "per-IP tolerance (T20)", Status: Warn, Value: "not measured (no ladder)", Advice: "run the ladder step (--from ladder); the cost assumes the crawler's pace"})
	default:
		add(Check{Name: "per-IP tolerance (T20)", Status: status(tol >= t.MinLadderRPS),
			Value: fmt.Sprintf("%.2f requests/s on the weakest of %d IPs (about %.0f a day, if it lasts)", tol, ips, tol*86400),
			Want:  fmt.Sprintf(">= %.2f", t.MinLadderRPS), Advice: "below the crawler's own pace the tier cannot run the checks: try another tier or slower identities"})
	}
	if len(s.Replays) > 0 {
		var parts []string
		for _, b := range replayBuckets {
			if r := s.Replays[b.name]; r != nil {
				parts = append(parts, fmt.Sprintf("%s %d/%d", b.name, r.OK, r.N))
			}
		}
		add(Check{Name: "signature lifetime (T27)", Status: Info, Value: "accepted when sent again at " + strings.Join(parts, ", ")})
	}
	if x := s.Ops[OpComments]; x != nil && x.Cross > 0 {
		c := Check{Name: "signing for other accounts (T32)", Status: Info,
			Value: fmt.Sprintf("%s of the comment pages signed on a page opened for another account answered", pct(x.CrossOK, x.Cross))}
		if !atLeast(x.CrossOK, x.Cross, t.MinSigned) {
			c.Status, c.Advice = Warn, "pages may need to sign on the account's own page: more page opens (browser time)"
		}
		add(c)
	}
	if v := s.Ops[OpVideoDirect]; v != nil && v.N > 0 {
		p := s.Ops[OpVideoProxy]
		c := Check{Name: "videos from this machine's IP (T33)", Status: Info,
			Value: fmt.Sprintf("direct %s; through the exit %s", pct(v.OK, v.N), pct(p.OK, p.N))}
		if v.OK < v.N {
			c.Status, c.Advice = Warn, "video files would have to go through the proxy tier: costs its bytes (media is off today, D5)"
		}
		add(c)
	}
	switch {
	case t.BudgetUSD <= 0:
		add(Check{Name: "budget (D9)", Status: Info, Value: e.perAccountAt(t.TargetAccounts), Advice: "the owner sets the budget from this report"})
	case len(e.Unpriced) > 0:
		add(Check{Name: "budget (D9)", Status: Warn, Value: e.perAccountAt(t.TargetAccounts), Want: "<= " + USD(t.BudgetUSD),
			Advice: "prices missing (" + strings.Join(e.Unpriced, ", ") + "): run --from report with them"})
	default:
		c := e.project(t.TargetAccounts)
		add(Check{Name: "budget (D9)", Status: status(c.PerAccountUSD <= t.BudgetUSD), Value: e.perAccountAt(t.TargetAccounts),
			Want: "<= " + USD(t.BudgetUSD) + " per account per day"})
	}
	return out
}

// FormatReport writes the report for a terminal.
func FormatReport(rep Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Phase 0: %s\n", rep.Verdict)
	for _, t := range rep.Tiers {
		s := t.Summary
		fmt.Fprintf(&b, "\n== tier %s: %s ==\n", t.Tier, t.Verdict)
		for _, c := range t.Checks {
			fmt.Fprintf(&b, "%-4s  %-36s %s", c.Status, c.Name, c.Value)
			if c.Want != "" {
				fmt.Fprintf(&b, "  (want %s)", c.Want)
			}
			b.WriteString("\n")
			if c.Advice != "" && (c.Status == Fail || c.Status == Warn) {
				fmt.Fprintf(&b, "      -> %s\n", c.Advice)
			}
		}
		fmt.Fprintf(&b, "\naccounts %d (lists read %d; stops %v), %.1f h, %d requests\n", s.Accounts, s.Measured, s.AccountStops, s.Hours, s.Requests)
		fmt.Fprintf(&b, "posting: median %.2f posts a day, mean %.2f; %d posts in the window, %d videos; comment count %.1f a post\n",
			s.PostsPerDay.P50, s.PostsPerDay.Mean, s.Posts, s.VideoPosts, ratio(float64(s.CommentCount), float64(s.Posts)))
		fmt.Fprintf(&b, "comment reads %d (%d whole); signed pages per estimated page %.2f (%s)\n", s.CommentReads, s.CommentComplete, s.PagesFactor, s.PagesFactorFrom)
		b.WriteString("\noperation       requests  answered  wire (mean / p95)       time p50   signed  sign p50  page opens\n")
		for _, op := range []string{OpPage, OpListCheck, OpListBackfill, OpComments, OpReplies, OpSigReplay, OpVideoProxy, OpVideoDirect, OpLadder} {
			o := s.Ops[op]
			if o == nil {
				continue
			}
			fmt.Fprintf(&b, "%-15s %8d  %8d  %9s / %-10s %7.0fms  %6d  %6.0fms  %d\n", op, o.N, o.OK, human(o.Wire.Mean), human(o.Wire.P95),
				o.Ms.P50, o.Signed, o.SignMs.P50, o.PageOpenMs.N)
		}
		if len(s.Exits) > 0 {
			b.WriteString("\nexits in the run (requests, blocked, per hour):")
			for _, l := range sortedKeys(s.Exits) {
				e := s.Exits[l]
				fmt.Fprintf(&b, " %s %d/%d/%.0f;", l, e.Requests, e.Blocks, e.PerHour)
			}
			b.WriteString("\n")
		}
		for _, ip := range sortedKeys(s.Ladder) {
			fmt.Fprintf(&b, "ladder %s:", ip)
			for _, st := range s.Ladder[ip] {
				fmt.Fprintf(&b, " %.2f/s %d/%d blocked;", st.Rate, st.Blocks, st.Calls)
			}
			b.WriteString("\n")
		}
		b.WriteString(t.Estimate.Format())
	}
	return b.String()
}

func ratio(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}
