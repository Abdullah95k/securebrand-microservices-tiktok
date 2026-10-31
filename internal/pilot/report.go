// Package pilot measures what the pipeline costs in its steady state, from
// the jobs it ran: requests, wire bytes and blocks per page per day by page
// tier, and the comment completeness it reached. Phase 0 measures first
// crawls; this is what production pays for every day.
package pilot

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Row is the jobs of one kind over the measured span, for the pages now in
// one tier (a page's past jobs count in the tier it has at report time).
type Row struct {
	Tier            string `json:"tier"`
	Kind            string `json:"kind"` // backfill | scheduled | on_demand
	Pages           int64  `json:"pages"`
	Jobs            int64  `json:"jobs"`
	Requests        int64  `json:"requests"`
	WireBytes       int64  `json:"wire_bytes"`
	Blocked         int64  `json:"blocked"`
	SoftErrors      int64  `json:"soft_errors"`
	TransportErrors int64  `json:"transport_errors"`
	Drift           int64  `json:"drift"`
	Posts           int64  `json:"posts"`
	Comments        int64  `json:"comments"`
	Replies         int64  `json:"replies"`
}

// Report is the pilot's measurement.
type Report struct {
	Since time.Time `json:"since"`
	Until time.Time `json:"until"`
	Rows  []Row     `json:"rows"`
	// SteadyPages are the distinct pages with scheduled jobs.
	SteadyPages int64 `json:"steady_pages"`
	// SteadySince is when the first scheduled job in the span was created:
	// the steady state is measured from then, not from Since.
	SteadySince time.Time `json:"steady_since"`
	// Completeness of the comments of posts first seen in the span, among
	// those with comments: all of them, and those seen at least a day ago
	// (whose refreshes have had time to run).
	Posts               int64   `json:"posts_with_comments"`
	MeanCompleteness    float64 `json:"mean_completeness"`
	SettledPosts        int64   `json:"settled_posts"`
	SettledCompleteness float64 `json:"settled_mean_completeness"`
}

// Days is the span measured, in days.
func (r Report) Days() float64 { return r.Until.Sub(r.Since).Hours() / 24 }

// SteadyDays is the span the scheduled jobs cover, from the first one: a
// pilot shorter than --since is not averaged over hours without jobs.
func (r Report) SteadyDays() float64 {
	from := r.Since
	if r.SteadySince.After(from) {
		from = r.SteadySince
	}
	return r.Until.Sub(from).Hours() / 24
}

// Steady sums the scheduled jobs (the every-3-hours crawls and the
// comment refreshes riding on them): the traffic production pays daily.
func (r Report) Steady() Row {
	out := Row{Tier: "all", Kind: "scheduled"}
	for _, x := range r.Rows {
		if x.Kind != "scheduled" {
			continue
		}
		out.Pages += x.Pages
		out.Jobs += x.Jobs
		out.Requests += x.Requests
		out.WireBytes += x.WireBytes
		out.Blocked += x.Blocked
		out.SoftErrors += x.SoftErrors
		out.TransportErrors += x.TransportErrors
		out.Drift += x.Drift
		out.Posts += x.Posts
		out.Comments += x.Comments
		out.Replies += x.Replies
	}
	if r.SteadyPages > 0 {
		out.Pages = r.SteadyPages
	}
	return out
}

// Load measures the jobs created from since on.
func Load(ctx context.Context, db *pgxpool.Pool, since time.Time) (Report, error) {
	rep := Report{Since: since, Until: time.Now().UTC()}
	rows, err := db.Query(ctx, `
		SELECT coalesce(s.tier, '?'), j.kind, count(DISTINCT j.page_ref), count(*),
			coalesce(sum((j.counters->>'requests')::bigint), 0),
			coalesce(sum((j.counters->>'wire_bytes')::bigint), 0),
			coalesce(sum((j.counters->>'blocked')::bigint), 0),
			coalesce(sum((j.counters->>'soft_errors')::bigint), 0),
			coalesce(sum((j.counters->>'transport_errors')::bigint), 0),
			coalesce(sum((j.counters->>'drift')::bigint), 0),
			coalesce(sum((j.counters->>'posts')::bigint), 0),
			coalesce(sum((j.counters->>'comments')::bigint), 0),
			coalesce(sum((j.counters->>'replies')::bigint), 0)
		FROM jobs j LEFT JOIN page_schedule s ON s.page_ref = j.page_ref
		WHERE j.created_at >= $1 AND j.status IN ('completed', 'partial', 'failed')
		GROUP BY 1, 2 ORDER BY 2, 1`, since)
	if err != nil {
		return rep, err
	}
	for rows.Next() {
		var x Row
		if err := rows.Scan(&x.Tier, &x.Kind, &x.Pages, &x.Jobs, &x.Requests, &x.WireBytes, &x.Blocked, &x.SoftErrors,
			&x.TransportErrors, &x.Drift, &x.Posts, &x.Comments, &x.Replies); err != nil {
			rows.Close()
			return rep, err
		}
		rep.Rows = append(rep.Rows, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rep, err
	}
	var first *time.Time
	if err := db.QueryRow(ctx, `SELECT count(DISTINCT page_ref), min(created_at) FROM jobs
		WHERE created_at >= $1 AND kind = 'scheduled' AND status IN ('completed', 'partial', 'failed')`, since).Scan(&rep.SteadyPages, &first); err != nil {
		return rep, err
	}
	if first != nil {
		rep.SteadySince = first.UTC()
	}
	err = db.QueryRow(ctx, `
		SELECT count(*), coalesce(avg(least(completeness, 1)), 0),
			count(*) FILTER (WHERE first_seen_at <= now() - interval '24 hours'),
			coalesce(avg(least(completeness, 1)) FILTER (WHERE first_seen_at <= now() - interval '24 hours'), 0)
		FROM posts WHERE first_seen_at >= $1 AND comment_count_total > 0 AND completeness IS NOT NULL`, since).
		Scan(&rep.Posts, &rep.MeanCompleteness, &rep.SettledPosts, &rep.SettledCompleteness)
	return rep, err
}

// Projection scales the steady state to a fleet.
type Projection struct {
	Pages       int     // pages production will crawl
	IdentityRPS float64 // fb.identity_rps: requests per second per IP
	CostPerGB   float64 // a per-GB tier's price, e.g. residential
	Headroom    float64 // IPs beyond the average need, for peaks (0.5 = 50%)
}

// Format renders the report and, with p.Pages, the projection.
func Format(r Report, p Projection) string {
	var b strings.Builder
	days := r.Days()
	fmt.Fprintf(&b, "jobs created %s to %s (%.1f days)\n\n", r.Since.Format(time.RFC3339), r.Until.Format(time.RFC3339), days)
	fmt.Fprintf(&b, "%-9s %-5s %6s %6s %10s %10s %9s %8s %7s %9s  %s\n", "kind", "tier", "pages", "jobs", "requests", "wire",
		"req/page", "blocked", "posts", "comments", "failures (soft/transport/drift)")
	rows := append([]Row(nil), r.Rows...)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Kind != rows[j].Kind {
			return rows[i].Kind < rows[j].Kind
		}
		return rows[i].Tier < rows[j].Tier
	})
	for _, x := range rows {
		fmt.Fprintf(&b, "%-9s %-5s %6d %6d %10d %10s %9.0f %8d %7d %9d  %d/%d/%d\n", x.Kind, x.Tier, x.Pages, x.Jobs, x.Requests,
			human(float64(x.WireBytes)), ratio(x.Requests, x.Pages), x.Blocked, x.Posts, x.Comments+x.Replies, x.SoftErrors, x.TransportErrors, x.Drift)
	}
	st := r.Steady()
	days = r.SteadyDays()
	if st.Pages == 0 || days <= 0 {
		b.WriteString("\nno scheduled jobs yet: the steady state starts after the backfills, with the first 3-hourly runs.\n")
		return b.String()
	}
	reqDay := float64(st.Requests) / float64(st.Pages) / days
	bytesDay := float64(st.WireBytes) / float64(st.Pages) / days
	fmt.Fprintf(&b, "\nsteady state (scheduled jobs over %.1f days): %.0f requests and %s per page per day; block rate %.2f%%\n",
		days, reqDay, human(bytesDay), 100*ratio(st.Blocked, st.Requests))
	if r.Posts > 0 {
		fmt.Fprintf(&b, "comment completeness: %.1f%% over %d posts with comments", 100*r.MeanCompleteness, r.Posts)
		if r.SettledPosts > 0 {
			fmt.Fprintf(&b, "; %.1f%% over the %d first seen a day or more ago", 100*r.SettledCompleteness, r.SettledPosts)
		}
		b.WriteString("\n")
	}
	if p.Pages > 0 {
		n := float64(p.Pages)
		fmt.Fprintf(&b, "\nfor %d pages: %.2fM requests and %s per day", p.Pages, reqDay*n/1e6, human(bytesDay*n))
		if p.IdentityRPS > 0 {
			ips := reqDay * n / 86400 / p.IdentityRPS * (1 + p.Headroom)
			fmt.Fprintf(&b, "; about %.0f IPs at %.2g requests/s each (%.0f%% headroom)", math.Ceil(ips), p.IdentityRPS, 100*p.Headroom)
		}
		if p.CostPerGB > 0 {
			fmt.Fprintf(&b, "; all on a $%.2f/GB tier: $%.0f a day", p.CostPerGB, bytesDay*n/1e9*p.CostPerGB)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
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
