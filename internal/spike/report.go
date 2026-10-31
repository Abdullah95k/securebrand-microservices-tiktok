package spike

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
)

// Thresholds are the Phase 0 go/no-go criteria.
type Thresholds struct {
	MinMedianCompleteness float64 // e.g. 0.70
	MinBootstrapSuccess   float64 // e.g. 0.90
	MaxBlockRate          float64 // e.g. 0.05
	MinMedianDays         float64 // e.g. 365 (or the reduced default you accept)
}

// DefaultThresholds match the plan.
var DefaultThresholds = Thresholds{MinMedianCompleteness: 0.70, MinBootstrapSuccess: 0.90, MaxBlockRate: 0.05, MinMedianDays: 365}

// DefaultRequestsPerDay is the production volume the cost projection uses:
// about 300k pages/day incremental plus 2k pages/day backfilled (README sizing).
const DefaultRequestsPerDay = 16e6

// ReportOptions configure ReportAll.
type ReportOptions struct {
	Thresholds
	// RequestsPerDay is the production request volume for the monthly cost
	// projection (default DefaultRequestsPerDay).
	RequestsPerDay float64
}

// Check keys, used in Summary.Failed and for the advice in Format.
const (
	CheckCompleteness = "completeness"
	CheckBootstrap    = "bootstrap"
	CheckBlockRate    = "block_rate"
	CheckDepth        = "depth"
)

// Advice says what to try when a check fails (docs/runbooks/phase0-spike.md,
// "Reading the report").
var Advice = map[string]string{
	CheckCompleteness: "compare the orderings and their walls above; if even the best ordering is low, logged-out access hides comments: plan the browser fallback or another surface",
	CheckBootstrap:    "check the region table (EU consent wall) and the tokens found; try another fingerprint profile or another tier",
	CheckBlockRate:    "lower --rps, add exits (more static endpoints or gateway sessions) or use another tier",
	CheckDepth:        "timelines end before max_age: set limits.max_age_days to the measured median, or accept partial backfills",
}

// Summary is the aggregated result for one tier (or for every record, from
// Report).
type Summary struct {
	Tier         string  `json:"tier"`
	RPS          float64 `json:"rps,omitempty"`
	CostPerGBUSD float64 `json:"cost_per_gb_usd,omitempty"`

	Pages              int                        `json:"pages"`
	BootstrapSuccess   float64                    `json:"bootstrap_success"`
	ConsentWalls       int                        `json:"consent_walls"`
	TokensFoundRate    map[string]float64         `json:"tokens_found_rate"`
	Requests           int                        `json:"requests"`
	ClassRate          map[string]float64         `json:"class_rate"`
	BlockRate          float64                    `json:"block_rate"`
	TimelineEnd        map[string]int             `json:"timeline_end"`
	MedianPosts        float64                    `json:"median_posts"`
	MedianDays         float64                    `json:"median_days"`
	Completeness       map[string]OrderingSummary `json:"completeness"`
	BestOrdering       string                     `json:"best_ordering,omitempty"`
	BytesByQuery       map[string]ByteStats       `json:"bytes_by_query"`
	WireByQuery        map[string]ByteStats       `json:"wire_bytes_by_query"`
	Portability        float64                    `json:"cursor_portability"`
	MediaSuccess       float64                    `json:"media_success"`
	MedianMediaExpiryH float64                    `json:"median_media_expiry_hours"`

	// Cost. Proxied requests are GraphQL requests plus HTML page loads.
	WireBytes           int64   `json:"wire_bytes"`
	WirePerRequest      float64 `json:"wire_bytes_per_request"`
	RequestsPerPage     float64 `json:"requests_per_page"`
	WirePerPage         float64 `json:"wire_bytes_per_page"`
	RunCostUSD          float64 `json:"run_cost_usd"`
	ProjectedMonthlyUSD float64 `json:"projected_monthly_usd"`
	RequestsPerDay      float64 `json:"projection_requests_per_day"`

	// Strata maps a label dimension (size, category, region, language) to
	// one row per value.
	Strata map[string][]StratumSummary `json:"strata,omitempty"`

	Verdict []string `json:"verdict"`
	Failed  []string `json:"failed,omitempty"`
	Go      bool     `json:"go"`
}

// OrderingSummary is comment completeness for one ordering. Measurements
// cut short by the spike's own limits (Capped) say nothing about what
// Facebook serves, so Median and P10 leave them out whenever at least one
// measurement finished; MedianAll includes them (a lower bound).
type OrderingSummary struct {
	Posts     int     `json:"posts"`
	Median    float64 `json:"median"`
	P10       float64 `json:"p10"`
	MedianAll float64 `json:"median_all"`
	Capped    int     `json:"capped"`
	Walls     int     `json:"walls"`
}

// ByteStats summarises response sizes.
type ByteStats struct {
	N    int     `json:"n"`
	Mean float64 `json:"mean"`
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
}

// StratumSummary is one row of a strata table.
type StratumSummary struct {
	Value              string  `json:"value"`
	Pages              int     `json:"pages"`
	BootstrapSuccess   float64 `json:"bootstrap_success"`
	BlockRate          float64 `json:"block_rate"`
	MedianDays         float64 `json:"median_days"`
	MedianCompleteness float64 `json:"median_completeness"` // best ordering of the tier
}

// Result is the report over one or more results files.
type Result struct {
	Tiers []Summary `json:"tiers"`
	// Go is true when at least one tier passes every check.
	Go bool `json:"go"`
	// Recommended is the passing tier with the lowest projected cost (or the
	// lowest block rate when prices are missing).
	Recommended string `json:"recommended,omitempty"`
}

func quantile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(math.Round(q * float64(len(s)-1)))
	return s[i]
}

func readRecords(ins ...io.Reader) ([]Record, error) {
	var out []Record
	for _, in := range ins {
		sc := bufio.NewScanner(in)
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		for sc.Scan() {
			var r Record
			if json.Unmarshal(sc.Bytes(), &r) == nil && r.Type != "" {
				out = append(out, r)
			}
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Report aggregates every record into one summary, whatever its tier.
func Report(in io.Reader, th Thresholds) (Summary, error) {
	recs, err := readRecords(in)
	if err != nil {
		return Summary{}, err
	}
	return summarize("all", recs, ReportOptions{Thresholds: th}), nil
}

// ReportAll aggregates results files per tier.
func ReportAll(opt ReportOptions, ins ...io.Reader) (Result, error) {
	recs, err := readRecords(ins...)
	if err != nil {
		return Result{}, err
	}
	byTier := map[string][]Record{}
	var tiers []string
	for _, r := range recs {
		if _, ok := byTier[r.Tier]; !ok {
			tiers = append(tiers, r.Tier)
		}
		byTier[r.Tier] = append(byTier[r.Tier], r)
	}
	sort.Strings(tiers)
	var res Result
	for _, t := range tiers {
		res.Tiers = append(res.Tiers, summarize(t, byTier[t], opt))
	}
	var best *Summary
	priced := true
	for i := range res.Tiers {
		if res.Tiers[i].Go && res.Tiers[i].CostPerGBUSD == 0 {
			priced = false
		}
	}
	for i := range res.Tiers {
		s := &res.Tiers[i]
		if !s.Go {
			continue
		}
		res.Go = true
		switch {
		case best == nil:
			best = s
		case priced && s.ProjectedMonthlyUSD < best.ProjectedMonthlyUSD:
			best = s
		case !priced && s.BlockRate < best.BlockRate:
			best = s
		}
	}
	if best != nil {
		res.Recommended = best.Tier
	}
	return res, nil
}

type pageAgg struct {
	labels            map[string]string
	boots, bootOK     int
	requests, blocked int
	days              []float64
	ratiosByOrdering  map[string][]float64
}

func isBlocked(class string) bool {
	return class == "rate_limited" || class == "login_wall" || class == "checkpoint"
}

func summarize(tier string, recs []Record, opt ReportOptions) Summary {
	if opt.RequestsPerDay <= 0 {
		opt.RequestsPerDay = DefaultRequestsPerDay
	}
	s := Summary{Tier: tier, TokensFoundRate: map[string]float64{}, ClassRate: map[string]float64{}, TimelineEnd: map[string]int{},
		Completeness: map[string]OrderingSummary{}, BytesByQuery: map[string]ByteStats{}, WireByQuery: map[string]ByteStats{},
		RequestsPerDay: opt.RequestsPerDay}
	var boots, bootOK, port, portOK, med, medOK int
	tokens := map[string]int{}
	classes := map[string]int{}
	var posts, days, expiry []float64
	ratios := map[string][]float64{}    // finished measurements
	allRatios := map[string][]float64{} // finished and capped
	capped := map[string]int{}
	walls := map[string]int{}
	bytes := map[string][]float64{}
	wire := map[string][]float64{}
	pages := map[string]*pageAgg{}
	agg := func(p string) *pageAgg {
		a := pages[p]
		if a == nil {
			a = &pageAgg{ratiosByOrdering: map[string][]float64{}}
			pages[p] = a
		}
		return a
	}
	for _, r := range recs {
		if r.Type == "run" {
			if r.RPS > 0 {
				s.RPS = r.RPS
			}
			if r.CostPerGBUSD > 0 {
				s.CostPerGBUSD = r.CostPerGBUSD
			}
			continue
		}
		if r.Page == "" {
			continue
		}
		a := agg(r.Page)
		switch r.Type {
		case "page":
			if len(r.Labels) > 0 {
				a.labels = r.Labels
			}
		case "bootstrap":
			boots++
			a.boots++
			if r.OK != nil && *r.OK {
				bootOK++
				a.bootOK++
			}
			if r.Consent {
				s.ConsentWalls++
			}
			for k, v := range r.TokensFound {
				if v {
					tokens[k]++
				}
			}
			s.WireBytes += r.Wire
			if r.Wire > 0 {
				wire["page_html"] = append(wire["page_html"], float64(r.Wire))
			}
		case "request":
			s.Requests++
			a.requests++
			classes[r.Class]++
			if isBlocked(r.Class) {
				a.blocked++
			}
			if r.Bytes > 0 {
				bytes[r.Query] = append(bytes[r.Query], float64(r.Bytes))
			}
			s.WireBytes += r.Wire
			if r.Wire > 0 {
				wire[r.Query] = append(wire[r.Query], float64(r.Wire))
			}
		case "timeline":
			s.TimelineEnd[r.EndReason]++
			posts = append(posts, float64(r.Posts))
			days = append(days, r.DaysCovered)
			a.days = append(a.days, r.DaysCovered)
		case "comments":
			ratio := 1.0
			if r.Reported > 0 {
				ratio = math.Min(1, float64(r.Collected)/float64(r.Reported))
			}
			allRatios[r.Ordering] = append(allRatios[r.Ordering], ratio)
			if r.Capped {
				capped[r.Ordering]++
			} else {
				ratios[r.Ordering] = append(ratios[r.Ordering], ratio)
				a.ratiosByOrdering[r.Ordering] = append(a.ratiosByOrdering[r.Ordering], ratio)
			}
			if r.EndReason == "wall" {
				walls[r.Ordering]++
			}
		case "portability":
			port++
			if r.OK != nil && *r.OK {
				portOK++
			}
		case "media":
			med++
			if r.OK != nil && *r.OK {
				medOK++
			}
			if r.ExpiresInH != 0 {
				expiry = append(expiry, r.ExpiresInH)
			}
		}
	}
	s.Pages = len(pages)
	if boots > 0 {
		s.BootstrapSuccess = float64(bootOK) / float64(boots)
		for k, v := range tokens {
			s.TokensFoundRate[k] = float64(v) / float64(boots)
		}
	}
	for k, v := range classes {
		s.ClassRate[k] = float64(v) / float64(max(1, s.Requests))
	}
	s.BlockRate = s.ClassRate["rate_limited"] + s.ClassRate["login_wall"] + s.ClassRate["checkpoint"]
	s.MedianPosts, s.MedianDays = quantile(posts, 0.5), quantile(days, 0.5)
	for ord, all := range allRatios {
		rs := ratios[ord]
		if len(rs) == 0 {
			rs = all // every measurement hit the limits: the lower bound is all there is
		}
		s.Completeness[ord] = OrderingSummary{Posts: len(all), Median: quantile(rs, 0.5), P10: quantile(rs, 0.1),
			MedianAll: quantile(all, 0.5), Capped: capped[ord], Walls: walls[ord]}
	}
	stats := func(xs []float64) ByteStats {
		sum := 0.0
		for _, b := range xs {
			sum += b
		}
		return ByteStats{N: len(xs), Mean: sum / float64(len(xs)), P50: quantile(xs, 0.5), P95: quantile(xs, 0.95)}
	}
	for q, bs := range bytes {
		s.BytesByQuery[q] = stats(bs)
	}
	for q, bs := range wire {
		s.WireByQuery[q] = stats(bs)
	}
	if port > 0 {
		s.Portability = float64(portOK) / float64(port)
	}
	if med > 0 {
		s.MediaSuccess = float64(medOK) / float64(med)
	}
	s.MedianMediaExpiryH = quantile(expiry, 0.5)

	// Cost.
	proxied := s.Requests + boots
	if proxied > 0 {
		s.WirePerRequest = float64(s.WireBytes) / float64(proxied)
	}
	if s.Pages > 0 {
		s.RequestsPerPage = float64(proxied) / float64(s.Pages)
		s.WirePerPage = float64(s.WireBytes) / float64(s.Pages)
	}
	s.RunCostUSD = float64(s.WireBytes) / 1e9 * s.CostPerGBUSD
	s.ProjectedMonthlyUSD = s.WirePerRequest * opt.RequestsPerDay * 30 / 1e9 * s.CostPerGBUSD

	// Verdict.
	best := -1.0
	for ord, o := range s.Completeness {
		if o.Median > best || (o.Median == best && ord < s.BestOrdering) {
			best, s.BestOrdering = o.Median, ord
		}
	}
	best = math.Max(best, 0)
	s.Go = true
	th := opt.Thresholds
	check := func(key string, ok bool, msg string) {
		mark := "PASS"
		if !ok {
			mark, s.Go = "FAIL", false
			s.Failed = append(s.Failed, key)
		}
		s.Verdict = append(s.Verdict, mark+" "+msg)
	}
	check(CheckCompleteness, best >= th.MinMedianCompleteness,
		fmt.Sprintf("median comment completeness %.2f (%s) >= %.2f", best, s.BestOrdering, th.MinMedianCompleteness))
	check(CheckBootstrap, s.BootstrapSuccess >= th.MinBootstrapSuccess,
		fmt.Sprintf("HTTP bootstrap success %.2f >= %.2f", s.BootstrapSuccess, th.MinBootstrapSuccess))
	check(CheckBlockRate, s.BlockRate < th.MaxBlockRate, fmt.Sprintf("block rate %.3f < %.3f", s.BlockRate, th.MaxBlockRate))
	depthMsg := fmt.Sprintf("median timeline depth %.0f days >= %.0f", s.MedianDays, th.MinMedianDays)
	if s.MedianDays < th.MinMedianDays && s.TimelineEnd["wall"] == 0 {
		depthMsg = fmt.Sprintf("timeline depth: median %.0f days, below %.0f, but no timeline hit a login wall: the pages have no older posts",
			s.MedianDays, th.MinMedianDays)
	}
	check(CheckDepth, s.MedianDays >= th.MinMedianDays || s.TimelineEnd["wall"] == 0, depthMsg)

	s.Strata = strata(pages, s.BestOrdering)
	return s
}

func strata(pages map[string]*pageAgg, ordering string) map[string][]StratumSummary {
	type acc struct {
		pages, boots, bootOK, requests, blocked int
		days, ratios                            []float64
	}
	dims := map[string]map[string]*acc{}
	for _, p := range pages {
		for dim, val := range p.labels {
			if dims[dim] == nil {
				dims[dim] = map[string]*acc{}
			}
			a := dims[dim][val]
			if a == nil {
				a = &acc{}
				dims[dim][val] = a
			}
			a.pages++
			a.boots += p.boots
			a.bootOK += p.bootOK
			a.requests += p.requests
			a.blocked += p.blocked
			a.days = append(a.days, p.days...)
			a.ratios = append(a.ratios, p.ratiosByOrdering[ordering]...)
		}
	}
	if len(dims) == 0 {
		return nil
	}
	out := map[string][]StratumSummary{}
	for dim, vals := range dims {
		for val, a := range vals {
			row := StratumSummary{Value: val, Pages: a.pages, MedianDays: quantile(a.days, 0.5), MedianCompleteness: quantile(a.ratios, 0.5)}
			if a.boots > 0 {
				row.BootstrapSuccess = float64(a.bootOK) / float64(a.boots)
			}
			if a.requests > 0 {
				row.BlockRate = float64(a.blocked) / float64(a.requests)
			}
			out[dim] = append(out[dim], row)
		}
		sort.Slice(out[dim], func(i, j int) bool { return out[dim][i].Value < out[dim][j].Value })
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func human(b float64) string {
	switch {
	case b >= 1e9:
		return fmt.Sprintf("%.2f GB", b/1e9)
	case b >= 1e6:
		return fmt.Sprintf("%.1f MB", b/1e6)
	case b >= 1e3:
		return fmt.Sprintf("%.1f KB", b/1e3)
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

// Format renders one summary for humans.
func Format(s Summary) string {
	var b strings.Builder
	fmt.Fprintf(&b, "tier: %s", s.Tier)
	if s.RPS > 0 {
		fmt.Fprintf(&b, "  rps/identity: %.2f", s.RPS)
	}
	fmt.Fprintf(&b, "\npages: %d  requests: %d\n", s.Pages, s.Requests)
	fmt.Fprintf(&b, "bootstrap success: %.1f%%  consent walls: %d  tokens:", 100*s.BootstrapSuccess, s.ConsentWalls)
	for _, k := range sortedKeys(s.TokensFoundRate) {
		fmt.Fprintf(&b, " %s=%.0f%%", k, 100*s.TokensFoundRate[k])
	}
	fmt.Fprintf(&b, "\nblock rate: %.2f%%  classes:", 100*s.BlockRate)
	for _, k := range sortedKeys(s.ClassRate) {
		fmt.Fprintf(&b, " %s=%.1f%%", k, 100*s.ClassRate[k])
	}
	fmt.Fprintf(&b, "\ntimeline: median %.0f posts, %.0f days; ends:", s.MedianPosts, s.MedianDays)
	for _, k := range sortedKeys(s.TimelineEnd) {
		fmt.Fprintf(&b, " %s=%d", k, s.TimelineEnd[k])
	}
	b.WriteString("\n")
	for _, ord := range sortedKeys(s.Completeness) {
		o := s.Completeness[ord]
		fmt.Fprintf(&b, "comments [%s]: %d posts, median %.2f, p10 %.2f, walls %d", ord, o.Posts, o.Median, o.P10, o.Walls)
		if o.Capped > 0 {
			fmt.Fprintf(&b, "; %d hit the spike's limits (median with them %.2f; raise --threads-per-post / --comment-pages)", o.Capped, o.MedianAll)
		}
		b.WriteString("\n")
	}
	queries := map[string]bool{}
	for q := range s.WireByQuery {
		queries[q] = true
	}
	for q := range s.BytesByQuery {
		queries[q] = true
	}
	for _, q := range sortedKeys(queries) {
		w, d := s.WireByQuery[q], s.BytesByQuery[q]
		fmt.Fprintf(&b, "bytes [%s]: n=%d on the wire mean=%s p95=%s", q, max(w.N, d.N), human(w.Mean), human(w.P95))
		if d.N > 0 {
			fmt.Fprintf(&b, ", decoded mean=%s", human(d.Mean))
		}
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "cursor portability: %.1f%%  media success: %.1f%%  median media expiry: %.0fh\n",
		100*s.Portability, 100*s.MediaSuccess, s.MedianMediaExpiryH)
	fmt.Fprintf(&b, "traffic: %s total, %s per request, %.0f requests and %s per page\n",
		human(float64(s.WireBytes)), human(s.WirePerRequest), s.RequestsPerPage, human(s.WirePerPage))
	if s.CostPerGBUSD > 0 {
		fmt.Fprintf(&b, "cost: this run $%.2f; projected $%.0f/month at %.0fM requests/day ($%.2f/GB)\n",
			s.RunCostUSD, s.ProjectedMonthlyUSD, s.RequestsPerDay/1e6, s.CostPerGBUSD)
	} else {
		b.WriteString("cost: set cost_per_gb_usd on the tier to get a projection\n")
	}
	for _, dim := range sortedKeys(s.Strata) {
		fmt.Fprintf(&b, "by %s:\n", dim)
		fmt.Fprintf(&b, "  %-14s %5s %9s %7s %6s %9s\n", "value", "pages", "bootstrap", "blocks", "days", "comments")
		for _, r := range s.Strata[dim] {
			fmt.Fprintf(&b, "  %-14s %5d %8.0f%% %6.1f%% %6.0f %9.2f\n", r.Value, r.Pages, 100*r.BootstrapSuccess,
				100*r.BlockRate, r.MedianDays, r.MedianCompleteness)
		}
	}
	for _, v := range s.Verdict {
		fmt.Fprintln(&b, v)
	}
	for _, k := range s.Failed {
		fmt.Fprintf(&b, "  -> %s: %s\n", k, Advice[k])
	}
	if s.Go {
		b.WriteString("VERDICT: GO\n")
	} else {
		b.WriteString("VERDICT: NO-GO (re-scope: see docs/runbooks/phase0-spike.md)\n")
	}
	return b.String()
}

// FormatAll renders a tier comparison followed by each tier's details.
func FormatAll(r Result) string {
	var b strings.Builder
	b.WriteString("== Phase 0 report ==\n")
	fmt.Fprintf(&b, "%-14s %5s %9s %7s %9s %6s %12s %8s\n", "tier", "pages", "bootstrap", "blocks", "comments", "days", "$/month", "verdict")
	for _, s := range r.Tiers {
		cost := "-"
		if s.CostPerGBUSD > 0 {
			cost = fmt.Sprintf("%.0f", s.ProjectedMonthlyUSD)
		}
		verdict := "NO-GO"
		if s.Go {
			verdict = "GO"
		}
		fmt.Fprintf(&b, "%-14s %5d %8.0f%% %6.1f%% %9.2f %6.0f %12s %8s\n", s.Tier, s.Pages, 100*s.BootstrapSuccess,
			100*s.BlockRate, s.Completeness[s.BestOrdering].Median, s.MedianDays, cost, verdict)
	}
	for _, s := range r.Tiers {
		b.WriteString("\n-- " + s.Tier + " --\n")
		b.WriteString(Format(s))
	}
	b.WriteString("\n")
	switch {
	case r.Go:
		fmt.Fprintf(&b, "OVERALL: GO (recommended tier: %s)\n", r.Recommended)
	case len(r.Tiers) == 0:
		b.WriteString("OVERALL: NO DATA\n")
	default:
		b.WriteString("OVERALL: NO-GO: no tier passes every check (see the advice under each tier)\n")
	}
	return b.String()
}
