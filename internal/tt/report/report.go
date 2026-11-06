// Package report is tks's first-run report and the canary's stop rules
// (M18; C5; docs/runbooks/tks-production.md): over a time window, in Iraq
// time, what tks sent to TikTok and how it was answered (requests by
// operation and outcome, signatures, blocks, proxy data, browser time, pod
// restarts and memory: from Prometheus), what it stored (new posts within
// 3 hours, comment and post completeness: from Postgres), and the cost per
// 1,000 accounts at the owner's prices with projections. The stop rules
// read the same numbers: any STOP means stop the ramp (tksctl stop) and
// look before going on.
package report

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/iraq"
)

// Iraq is Iraq's time (UTC+3, no summer time), the owner's.
var Iraq = iraq.Zone

// Window is the report's time span.
type Window struct{ From, To time.Time }

// Duration is the window's length.
func (w Window) Duration() time.Duration { return w.To.Sub(w.From) }

// Count is one labelled number.
type Count struct {
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

// Traffic is what Prometheus counted in the window; nil without it.
type Traffic struct {
	Requests       []Count  `json:"requests"` // by query and class
	RequestsTotal  float64  `json:"requests_total"`
	Blocks         float64  `json:"blocks"`     // rate_limited, login_wall, checkpoint
	Challenges     float64  `json:"challenges"` // checkpoint: captchas and challenges
	Signs          []Count  `json:"signs"`      // tks-signer's signatures by result
	SignedAnswered float64  `json:"signed_answered"`
	SignedRefused  float64  `json:"signed_refused"`
	ProxyGB        float64  `json:"proxy_gb"`
	BrowserHours   float64  `json:"browser_hours"`
	Restarts       []Count  `json:"restarts"`       // by container (kube-state-metrics)
	OOMKills       []Count  `json:"oom_kills"`      // containers whose last end was an OOM kill
	MemoryPeakMB   []Count  `json:"memory_peak_mb"` // by container (cAdvisor)
	Missing        []string `json:"missing,omitempty"`
}

// Store is what Postgres holds for the window.
type Store struct {
	Accounts       map[string]int64 `json:"accounts"` // by status
	Scheduled      int64            `json:"scheduled"`
	NewPosts       int64            `json:"new_posts"` // first read by a check in the window
	Within3h       float64          `json:"within_3h"` // share
	Within6h       float64          `json:"within_6h"`
	MedianDelayMin float64          `json:"median_delay_min"`
	CommentReads   map[string]int64 `json:"comment_reads"` // comments_state of posts read in the window
	CommentRatio   float64          `json:"comment_ratio"` // median collected ÷ count, complete reads
	PostRatio      float64          `json:"post_ratio"`    // median stored posts ÷ profile count
	DatabaseGB     float64          `json:"database_gb"`
}

// ReadStore reads the window's numbers from Postgres.
func ReadStore(ctx context.Context, db *pgxpool.Pool, w Window) (Store, error) {
	s := Store{Accounts: map[string]int64{}, CommentReads: map[string]int64{}}
	rows, err := db.Query(ctx, `SELECT status, count(*) FROM accounts GROUP BY status`)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return s, err
		}
		s.Accounts[st] = n
	}
	if err := rows.Err(); err != nil {
		return s, err
	}
	var med *float64
	err = db.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM account_schedule WHERE enabled),
		       count(*),
		       coalesce(avg((stored_at - created_at <= interval '3 hours')::int), 0),
		       coalesce(avg((stored_at - created_at <= interval '6 hours')::int), 0),
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM stored_at - created_at) / 60),
		       pg_database_size(current_database()) / 1e9
		FROM posts
		WHERE first_mode IN ('incremental', 'refresh') AND stored_at >= $1 AND stored_at < $2`, w.From, w.To).
		Scan(&s.Scheduled, &s.NewPosts, &s.Within3h, &s.Within6h, &med, &s.DatabaseGB)
	if err != nil {
		return s, fmt.Errorf("report: new posts: %w", err)
	}
	if med != nil {
		s.MedianDelayMin = *med
	}
	rows, err = db.Query(ctx, `SELECT comments_state, count(*) FROM posts
		WHERE comments_synced_at >= $1 AND comments_synced_at < $2 GROUP BY 1`, w.From, w.To)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return s, err
		}
		s.CommentReads[st] = n
	}
	if err := rows.Err(); err != nil {
		return s, err
	}
	var cr, pr *float64
	if err := db.QueryRow(ctx, `
		SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY
		       (coalesce(comments_collected, 0) + coalesce(replies_collected, 0))::float8 / comment_count_read)
		FROM posts WHERE comments_state = 'complete' AND comment_count_read > 0
		  AND comments_synced_at >= $1 AND comments_synced_at < $2`, w.From, w.To).Scan(&cr); err != nil {
		return s, fmt.Errorf("report: comments: %w", err)
	}
	// Posts stored against the profile's count, for accounts whose
	// posts all fall inside the backfill's year (older accounts show less
	// by design: tks reads back 365 days).
	if err := db.QueryRow(ctx, `
		WITH a AS (
			SELECT a.user_id, a.video_count, count(p.post_id) AS stored, min(p.created_at) AS oldest
			FROM accounts a JOIN posts p ON p.user_id = a.user_id
			WHERE a.status = 'active' AND a.video_count > 0
			GROUP BY a.user_id, a.video_count)
		SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY stored::float8 / video_count)
		FROM a WHERE oldest > now() - interval '330 days'`).Scan(&pr); err != nil {
		return s, fmt.Errorf("report: posts: %w", err)
	}
	if cr != nil {
		s.CommentRatio = *cr
	}
	if pr != nil {
		s.PostRatio = *pr
	}
	return s, nil
}

// Rules are the stop rules' thresholds (flags of tksctl report).
type Rules struct {
	MaxBlockRate       float64 // blocks ÷ requests
	MaxChallengesPer1k float64
	MaxSignRefused     float64 // refused ÷ signed calls
	MaxRestarts        float64 // per container in the window
	BudgetPerAccount   float64 // USD a day; 0 = none
	MinWithin3h        float64 // a warning, not a stop: A2 is judged over 7 days
	MinCommentComplete float64 // a warning too (A4)
}

// DefaultRules are the canary's (C5).
var DefaultRules = Rules{MaxBlockRate: 0.05, MaxChallengesPer1k: 5, MaxSignRefused: 0.2, MaxRestarts: 3,
	MinWithin3h: 0.99, MinCommentComplete: 0.98}

// Verdict is one rule's outcome.
type Verdict struct {
	Level string `json:"level"` // STOP or WARN
	Rule  string `json:"rule"`
}

// Check applies the rules. perAccountUSD < 0 means unpriced.
func Check(r Rules, t *Traffic, s Store, perAccountUSD float64) []Verdict {
	var out []Verdict
	add := func(level, f string, a ...any) { out = append(out, Verdict{level, fmt.Sprintf(f, a...)}) }
	if t != nil && t.RequestsTotal > 0 {
		if br := t.Blocks / t.RequestsTotal; br > r.MaxBlockRate {
			add("STOP", "block rate %.1f %% is over %.1f %%", 100*br, 100*r.MaxBlockRate)
		}
		if c := 1000 * t.Challenges / t.RequestsTotal; c > r.MaxChallengesPer1k {
			add("STOP", "%.1f captchas or challenges per 1,000 requests, over %.1f", c, r.MaxChallengesPer1k)
		}
	}
	if t != nil {
		if n := t.SignedAnswered + t.SignedRefused; n > 0 && t.SignedRefused/n > r.MaxSignRefused {
			add("STOP", "TikTok refused %.0f %% of signed calls, over %.0f %%", 100*t.SignedRefused/n, 100*r.MaxSignRefused)
		}
		for _, c := range t.OOMKills {
			if c.Value > 0 {
				add("STOP", "%s was stopped for using too much memory (OOM kill)", c.Labels["container"])
			}
		}
		for _, c := range t.Restarts {
			if c.Value > r.MaxRestarts {
				add("STOP", "%s restarted %.0f times, over %.0f", c.Labels["container"], c.Value, r.MaxRestarts)
			}
		}
	}
	if r.BudgetPerAccount > 0 && perAccountUSD > r.BudgetPerAccount {
		add("STOP", "cost per account $%.5f a day is over the budget $%.5f", perAccountUSD, r.BudgetPerAccount)
	}
	if s.NewPosts > 0 && s.Within3h < r.MinWithin3h {
		add("WARN", "%.1f %% of new posts stored within 3 hours, under %.0f %%", 100*s.Within3h, 100*r.MinWithin3h)
	}
	var reads, complete int64
	for st, n := range s.CommentReads {
		reads += n
		if st == "complete" {
			complete = n
		}
	}
	if reads > 0 && float64(complete)/float64(reads) < r.MinCommentComplete {
		add("WARN", "%.1f %% of comment reads complete, under %.0f %%", 100*float64(complete)/float64(reads), 100*r.MinCommentComplete)
	}
	return out
}

// Projection is a day's cost at a number of accounts.
type Projection struct {
	Accounts int     `json:"accounts"`
	DayUSD   float64 `json:"day_usd"`
	MonthUSD float64 `json:"month_usd"`
}

// Project scales a per-account day cost linearly (IPs, signer pages,
// compute and storage all grow with the accounts at the canary's pace).
func Project(perAccountUSD float64, sizes ...int) []Projection {
	out := make([]Projection, len(sizes))
	for i, n := range sizes {
		d := perAccountUSD * float64(n)
		out[i] = Projection{Accounts: n, DayUSD: d, MonthUSD: 30 * d}
	}
	return out
}

func sorted(cs []Count, keys ...string) []Count {
	sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].Value != cs[j].Value {
			return cs[i].Value > cs[j].Value
		}
		return label(cs[i], keys) < label(cs[j], keys)
	})
	return cs
}

func label(c Count, keys []string) string {
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, c.Labels[k])
	}
	return strings.Join(parts, " ")
}

func pct(a, b float64) string {
	if b == 0 || math.IsNaN(a/b) {
		return "-"
	}
	return fmt.Sprintf("%.1f %%", 100*a/b)
}

// Report is the whole report.
type Report struct {
	Window      Window       `json:"window"`
	Traffic     *Traffic     `json:"traffic,omitempty"`
	Store       Store        `json:"store"`
	PerAccount  float64      `json:"cost_per_account_day_usd"`
	Per1000     float64      `json:"cost_per_1000_accounts_day_usd"`
	CostNote    string       `json:"cost_note,omitempty"`
	Projections []Projection `json:"projections"`
	Verdicts    []Verdict    `json:"verdicts"`
}

// Stop reports whether a rule says stop.
func (r Report) Stop() bool {
	for _, v := range r.Verdicts {
		if v.Level == "STOP" {
			return true
		}
	}
	return false
}

// Write prints the report for people, in Iraq time.
func (r Report) Write(out io.Writer) error {
	w := &strings.Builder{}
	f := "2006-01-02 15:04"
	fmt.Fprintf(w, "tks report, %s to %s Iraq time (UTC+3), %s\n\n", r.Window.From.In(Iraq).Format(f), r.Window.To.In(Iraq).Format(f),
		r.Window.Duration().Round(time.Minute))
	switch {
	case r.Stop():
		fmt.Fprintln(w, "VERDICT: STOP. Run `tksctl stop`, then read the reasons below before going on.")
	default:
		fmt.Fprintln(w, "VERDICT: GO. No stop rule fired.")
	}
	for _, v := range r.Verdicts {
		fmt.Fprintf(w, "  %s: %s\n", v.Level, v.Rule)
	}
	s := r.Store
	fmt.Fprintf(w, "\nAccounts: %d scheduled; by status:", s.Scheduled)
	for _, k := range sortedKeys(s.Accounts) {
		fmt.Fprintf(w, " %s %d", k, s.Accounts[k])
	}
	fmt.Fprintln(w)
	if t := r.Traffic; t != nil {
		fmt.Fprintf(w, "\nRequests to TikTok: %.0f; blocks %.0f (%s); captchas and challenges %.0f (%.1f per 1,000)\n",
			t.RequestsTotal, t.Blocks, pct(t.Blocks, t.RequestsTotal), t.Challenges, 1000*safeDiv(t.Challenges, t.RequestsTotal))
		for _, c := range sorted(t.Requests, "query", "class") {
			fmt.Fprintf(w, "  %-28s %-20s %10.0f\n", c.Labels["query"], c.Labels["class"], c.Value)
		}
		fmt.Fprintf(w, "Signatures (tks-signer):")
		for _, c := range sorted(t.Signs, "result") {
			fmt.Fprintf(w, " %s %.0f", c.Labels["result"], c.Value)
		}
		fmt.Fprintf(w, "; signed calls answered %.0f, refused %.0f (%s)\n", t.SignedAnswered, t.SignedRefused,
			pct(t.SignedRefused, t.SignedAnswered+t.SignedRefused))
		fmt.Fprintf(w, "Proxy data: %.2f GB; signer browser time: %.1f hours\n", t.ProxyGB, t.BrowserHours)
		if len(t.MemoryPeakMB) > 0 || len(t.Restarts) > 0 {
			fmt.Fprintln(w, "Containers (peak memory MB, restarts):")
			rs := map[string]float64{}
			for _, c := range t.Restarts {
				rs[c.Labels["container"]] = c.Value
			}
			for _, c := range sorted(t.MemoryPeakMB, "container") {
				fmt.Fprintf(w, "  %-22s %8.0f MB %4.0f\n", c.Labels["container"], c.Value, rs[c.Labels["container"]])
			}
		}
		if len(t.Missing) > 0 {
			fmt.Fprintf(w, "Not in Prometheus (left out): %s\n", strings.Join(t.Missing, ", "))
		}
	} else {
		fmt.Fprintln(w, "\nRequests, signatures, blocks, proxy data and containers: no Prometheus given (--prometheus URL).")
	}
	fmt.Fprintf(w, "\nNew posts found by checks: %d; stored within 3 h %s, within 6 h %s, median %.0f min\n",
		s.NewPosts, pctShare(s.Within3h, s.NewPosts), pctShare(s.Within6h, s.NewPosts), s.MedianDelayMin)
	fmt.Fprint(w, "Comment reads:")
	for _, k := range sortedKeys(s.CommentReads) {
		fmt.Fprintf(w, " %s %d", k, s.CommentReads[k])
	}
	fmt.Fprintf(w, "; collected ÷ count (complete reads, median) %.2f\n", s.CommentRatio)
	fmt.Fprintf(w, "Posts stored ÷ profile count (accounts younger than the backfill's year, median): %.2f\n", s.PostRatio)
	fmt.Fprintf(w, "Database: %.2f GB\n", s.DatabaseGB)
	fmt.Fprintf(w, "\nCost (last 24 hours, the owner's prices): $%.5f per account a day, $%.2f per 1,000 accounts a day\n", r.PerAccount, r.Per1000)
	if r.CostNote != "" {
		fmt.Fprintf(w, "  %s\n", r.CostNote)
	}
	for _, p := range r.Projections {
		fmt.Fprintf(w, "  %7d accounts: $%9.2f a day, $%10.2f a month\n", p.Accounts, p.DayUSD, p.MonthUSD)
	}
	_, err := io.WriteString(out, w.String())
	return err
}

func pctShare(share float64, n int64) string {
	if n == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f %%", 100*share)
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func sortedKeys(m map[string]int64) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
