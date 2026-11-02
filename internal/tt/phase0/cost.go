package phase0

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Window is a comment re-read window (tt.freshness.comment_windows).
type Window struct {
	MaxAge   time.Duration `json:"max_age"`
	Interval time.Duration `json:"interval"`
}

// Walk is a metrics walk (tt.freshness.metrics_walks).
type Walk struct {
	Every time.Duration `json:"every"`
	Back  time.Duration `json:"back"`
}

// Model turns the measurements into a day of the crawler's work per
// account, and that into resources and money. Its schedule is tks's
// configuration (tt.freshness); its prices are the owner's inputs (0 = not
// given: the resource is still counted, its cost is not). Every assumption
// that is not measured is a field here, printed with the report.
type Model struct {
	ListInterval   time.Duration `json:"list_interval"`
	PageRefresh    time.Duration `json:"page_refresh"`
	MetricsWalks   []Walk        `json:"metrics_walks"`
	CommentWindows []Window      `json:"comment_windows"`
	// CommentGrowth is the share of a post's re-read chances at which its
	// comment count went up (a read happens only then). 1 is the upper
	// bound: every chance taken.
	CommentGrowth float64 `json:"comment_growth"`
	// IdentityRequests is the requests an identity serves before a new one
	// loads a page (tt.identity_max_requests_*: their mean).
	IdentityRequests int `json:"identity_requests"`
	// PageMaxSigns: a signer page is reopened after this many signatures.
	PageMaxSigns int `json:"page_max_signs"`
	// Media counts video downloads (D5: off today).
	Media bool `json:"media"`
	// MediaDirect sends them from the downloader's own IP (T33); else
	// through the proxy tier.
	MediaDirect bool `json:"media_direct"`
	// SignerBusy is the share of its time a signer page can sign.
	SignerBusy float64 `json:"signer_busy"`
	// Rows stored, for storage: bytes per post (with its media rows), per
	// comment, per metrics row, and the share of posts on a page read
	// whose counts changed since the last read (a metrics row each).
	PostRowBytes    float64 `json:"post_row_bytes"`
	CommentRowBytes float64 `json:"comment_row_bytes"`
	MetricRowBytes  float64 `json:"metric_row_bytes"`
	MetricChange    float64 `json:"metric_change"`
	StorageMonths   float64 `json:"storage_months"`
	Sizes           []int   `json:"sizes"`
	IPMonthUSD      float64 `json:"ip_month_usd"`
	BrowserHourUSD  float64 `json:"browser_hour_usd"`
	ComputeUSD      float64 `json:"compute_usd_per_account_day"`
	StorageGBMonth  float64 `json:"storage_gb_month_usd"`
	AssumedRPSPerIP float64 `json:"assumed_rps_per_ip"`
}

func (m *Model) defaults() {
	if m.ListInterval <= 0 {
		m.ListInterval = 2*time.Hour + 45*time.Minute
	}
	if m.PageRefresh <= 0 {
		m.PageRefresh = 7 * 24 * time.Hour
	}
	if m.MetricsWalks == nil {
		m.MetricsWalks = []Walk{{Every: 24 * time.Hour, Back: 30 * 24 * time.Hour}, {Every: 7 * 24 * time.Hour, Back: 365 * 24 * time.Hour}}
	}
	if m.CommentWindows == nil {
		m.CommentWindows = []Window{{48 * time.Hour, 6 * time.Hour}, {7 * 24 * time.Hour, 24 * time.Hour}, {30 * 24 * time.Hour, 7 * 24 * time.Hour}}
	}
	if m.CommentGrowth <= 0 {
		m.CommentGrowth = 1
	}
	if m.IdentityRequests <= 0 {
		m.IdentityRequests = 600
	}
	if m.PageMaxSigns <= 0 {
		m.PageMaxSigns = 500
	}
	if m.SignerBusy <= 0 {
		m.SignerBusy = 0.5
	}
	if m.PostRowBytes <= 0 {
		m.PostRowBytes = 3000
	}
	if m.CommentRowBytes <= 0 {
		m.CommentRowBytes = 800
	}
	if m.MetricRowBytes <= 0 {
		m.MetricRowBytes = 120
	}
	if m.MetricChange <= 0 {
		m.MetricChange = 0.3
	}
	if m.StorageMonths <= 0 {
		m.StorageMonths = 12
	}
	if len(m.Sizes) == 0 {
		m.Sizes = []int{1_000, 10_000, 100_000, 300_000}
	}
	if m.AssumedRPSPerIP <= 0 {
		m.AssumedRPSPerIP = 0.33
	}
}

// ReadsPerPost is how often a post's comments are read whole over its
// life: one chance per window interval while it is young enough, times
// CommentGrowth.
func (m Model) ReadsPerPost() float64 {
	n, from := 0.0, time.Duration(0)
	for _, w := range m.CommentWindows {
		if w.Interval > 0 && w.MaxAge > from {
			n += float64(w.MaxAge-from) / float64(w.Interval)
		}
		from = max(from, w.MaxAge)
	}
	return n * m.CommentGrowth
}

// Day is one account's day of crawler work.
type Day struct {
	PostsPerDay   float64 `json:"posts_per_day"`
	Checks        float64 `json:"checks"`
	ListCalls     float64 `json:"list_calls"`
	CommentCalls  float64 `json:"comment_calls"` // signed: comments and replies
	Signatures    float64 `json:"signatures"`    // the comment calls, and the list calls if the list is signed
	PageLoads     float64 `json:"page_loads"`
	Calls         float64 `json:"calls"`
	ProxyBytes    float64 `json:"proxy_bytes"`
	BrowserSecs   float64 `json:"browser_seconds"`
	VideoBytes    float64 `json:"video_bytes"`
	StoredBytes   float64 `json:"stored_bytes"`
	ReadsPerPost  float64 `json:"comment_reads_per_post"`
	SignedShare   float64 `json:"signed_share"`
	RPSPerIP      float64 `json:"rps_per_ip"`
	RPSPerIPFrom  string  `json:"rps_per_ip_from"`
	CallsPerIPDay float64 `json:"calls_per_ip_day"`
}

// Size is the fleet at one number of accounts.
type Size struct {
	Accounts      int     `json:"accounts"`
	CallsPerDay   float64 `json:"calls_per_day"`
	IPs           int     `json:"ips"`
	ProxyGBDay    float64 `json:"proxy_gb_day"`
	SignerPages   int     `json:"signer_pages"`
	BrowserHours  float64 `json:"browser_hours_day"`
	StoredGB      float64 `json:"stored_gb"` // after Model.StorageMonths
	ProxyUSD      float64 `json:"proxy_usd_day"`
	BrowserUSD    float64 `json:"browser_usd_day"`
	ComputeUSD    float64 `json:"compute_usd_day"`
	StorageUSD    float64 `json:"storage_usd_day"`
	TotalUSD      float64 `json:"total_usd_day"`
	PerAccountUSD float64 `json:"per_account_usd_day"`
}

// Estimate is the cost projection.
type Estimate struct {
	Day   Day    `json:"day"`
	Sizes []Size `json:"sizes"`
	// Unpriced lists the resources whose price was not given.
	Unpriced  []string `json:"unpriced,omitempty"`
	Model     Model    `json:"model"`
	CostPerGB float64  `json:"cost_per_gb_usd"`
}

// Estimate projects a tier's measurements to the fleet sizes. tol is the
// ladder's tolerated rate per IP (0 = not measured: the crawler's pace is
// assumed).
func (m Model) Estimate(s Summary, tol, costPerGB float64) Estimate {
	m.defaults()
	e := Estimate{Model: m, CostPerGB: costPerGB}
	d := &e.Day
	d.PostsPerDay = s.PostsPerDay.Mean
	d.Checks = 24 * float64(time.Hour) / float64(m.ListInterval)
	perCheck := 1 + d.PostsPerDay/d.Checks/listPageSize
	d.ListCalls = d.Checks * perCheck
	for _, w := range m.MetricsWalks {
		if w.Every <= 0 {
			continue
		}
		perDay := 24 * float64(time.Hour) / float64(w.Every)
		pages := 1 + d.PostsPerDay*w.Back.Hours()/24/listPageSize
		// A walk stands in for the check that was due.
		d.ListCalls += perDay * max(0, pages-perCheck)
	}
	d.ReadsPerPost = m.ReadsPerPost()
	pagesPerPost := 0.0
	if s.Posts > 0 {
		pagesPerPost = float64(s.CommentPagesEst) / float64(s.Posts)
	}
	d.CommentCalls = d.PostsPerDay * pagesPerPost * s.PagesFactor * d.ReadsPerPost
	d.PageLoads = 24*float64(time.Hour)/float64(m.PageRefresh) + (d.ListCalls+d.CommentCalls)/float64(m.IdentityRequests)
	d.Calls = d.ListCalls + d.CommentCalls + d.PageLoads
	// The list is signed too when only its signed variant answered (the
	// variants step).
	listSigned := 0.0
	if o := s.Ops[OpListCheck]; o != nil && o.N > 0 {
		listSigned = float64(o.Signed) / float64(o.N)
	}
	d.Signatures = d.CommentCalls + d.ListCalls*listSigned
	if d.Calls > 0 {
		d.SignedShare = d.Signatures / d.Calls
	}
	wire := func(ops ...string) float64 {
		var sum float64
		var n int
		for _, op := range ops {
			if o := s.Ops[op]; o != nil {
				sum += o.Wire.Mean * float64(o.Wire.N)
				n += o.Wire.N
			}
		}
		return ratio(sum, float64(n))
	}
	d.ProxyBytes = d.ListCalls*wire(OpListCheck, OpListBackfill) + d.CommentCalls*wire(OpComments, OpReplies) + d.PageLoads*wire(OpPage)
	signSecs := 0.0
	var openSecs float64
	for _, op := range []string{OpComments, OpReplies, OpListCheck} {
		if o := s.Ops[op]; o != nil && o.SignMs.N > 0 {
			signSecs = math.Max(signSecs, o.SignMs.Mean/1000)
			openSecs = math.Max(openSecs, o.PageOpenMs.Mean/1000)
		}
	}
	d.BrowserSecs = d.Signatures * (signSecs + openSecs/float64(m.PageMaxSigns))
	if m.Media {
		size := 0.0
		if o := s.Ops[OpVideoProxy]; o != nil {
			size = o.Wire.Mean
		}
		share := ratio(float64(s.VideoPosts), float64(s.Posts))
		d.VideoBytes = d.PostsPerDay * share * size
		if !m.MediaDirect {
			d.ProxyBytes += d.VideoBytes
		}
	}
	commentsPerPost := ratio(float64(s.CommentCount), float64(s.Posts))
	d.StoredBytes = d.PostsPerDay*m.PostRowBytes + d.PostsPerDay*commentsPerPost*m.CommentRowBytes +
		d.ListCalls*listPageSize*m.MetricChange*m.MetricRowBytes
	d.RPSPerIP, d.RPSPerIPFrom = tol, "ladder (weakest IP)"
	if tol <= 0 {
		d.RPSPerIP, d.RPSPerIPFrom = m.AssumedRPSPerIP, "assumed: the crawler's identity pace (no ladder)"
	}
	d.CallsPerIPDay = d.RPSPerIP * 86400

	if m.IPMonthUSD <= 0 && costPerGB <= 0 {
		e.Unpriced = append(e.Unpriced, "proxy (--ip-month-usd or the tier's cost_per_gb_usd)")
	}
	if m.BrowserHourUSD <= 0 {
		e.Unpriced = append(e.Unpriced, "browser (--browser-hour-usd)")
	}
	if m.ComputeUSD <= 0 {
		e.Unpriced = append(e.Unpriced, "compute (--compute-usd)")
	}
	if m.StorageGBMonth <= 0 {
		e.Unpriced = append(e.Unpriced, "storage (--storage-gb-month-usd)")
	}
	for _, n := range m.Sizes {
		e.Sizes = append(e.Sizes, e.project(n))
	}
	return e
}

// project is the fleet at n accounts.
func (e Estimate) project(n int) Size {
	m, d := e.Model, e.Day
	z := Size{Accounts: n}
	f := float64(n)
	z.CallsPerDay = f * d.Calls
	if d.CallsPerIPDay > 0 {
		z.IPs = int(math.Ceil(z.CallsPerDay / d.CallsPerIPDay))
	}
	z.ProxyGBDay = f * d.ProxyBytes / 1e9
	z.BrowserHours = f * d.BrowserSecs / 3600
	z.SignerPages = int(math.Ceil(f * d.BrowserSecs / 86400 / m.SignerBusy))
	z.StoredGB = f * d.StoredBytes * 30 * m.StorageMonths / 1e9
	z.ProxyUSD = float64(z.IPs)*m.IPMonthUSD/30 + z.ProxyGBDay*e.CostPerGB
	z.BrowserUSD = z.BrowserHours * m.BrowserHourUSD
	z.ComputeUSD = f * m.ComputeUSD
	z.StorageUSD = z.StoredGB * m.StorageGBMonth / 30
	z.TotalUSD = z.ProxyUSD + z.BrowserUSD + z.ComputeUSD + z.StorageUSD
	z.PerAccountUSD = z.TotalUSD / f
	return z
}

func (e Estimate) perAccountAt(n int) string {
	z := e.project(n)
	s := fmt.Sprintf("%s per account per day at %d accounts (%d IPs, %d signer pages)", USD(z.PerAccountUSD), n, z.IPs, z.SignerPages)
	if len(e.Unpriced) > 0 {
		s += "; without " + strings.Join(e.Unpriced, ", ")
	}
	return s
}

// Format writes the projection.
func (e Estimate) Format() string {
	var b strings.Builder
	d := e.Day
	m := e.Model
	fmt.Fprintf(&b, "\none account's day (the crawler's schedule: a check every %s, a page every %s, walks %v; comment reads %.1f a post (growth %.2f))\n",
		m.ListInterval, m.PageRefresh, walks(m.MetricsWalks), d.ReadsPerPost, m.CommentGrowth)
	fmt.Fprintf(&b, "  %.2f posts; %.1f list calls, %.1f comment calls, %.2f page loads = %.1f calls (%.1f signed: %.0f%%)\n",
		d.PostsPerDay, d.ListCalls, d.CommentCalls, d.PageLoads, d.Calls, d.Signatures, 100*d.SignedShare)
	fmt.Fprintf(&b, "  %s through the proxy, %.1f s of browser time, %s stored", human(d.ProxyBytes), d.BrowserSecs, human(d.StoredBytes))
	if m.Media {
		fmt.Fprintf(&b, ", %s of video", human(d.VideoBytes))
	}
	fmt.Fprintf(&b, "\n  one IP takes %.2f requests/s, %.0f a day (%s)\n", d.RPSPerIP, d.CallsPerIPDay, d.RPSPerIPFrom)
	b.WriteString("\naccounts     calls/day      IPs   proxy GB/day  signer pages  browser h/day  stored GB  $/day       $/account/day\n")
	for _, z := range e.Sizes {
		fmt.Fprintf(&b, "%-10d %11.0f %8d %13.1f %13d %14.1f %10.0f  %-10.2f  %s\n", z.Accounts, z.CallsPerDay, z.IPs, z.ProxyGBDay, z.SignerPages,
			z.BrowserHours, z.StoredGB, z.TotalUSD, USD(z.PerAccountUSD))
	}
	fmt.Fprintf(&b, "prices: $%.2f per IP-month, $%.3f per browser-hour, $%.5f compute per account-day, $%.3f per GB-month stored (%.0f months kept)\n",
		m.IPMonthUSD, m.BrowserHourUSD, m.ComputeUSD, m.StorageGBMonth, m.StorageMonths)
	if len(e.Unpriced) > 0 {
		fmt.Fprintf(&b, "not priced: %s. Rerun with --from report and the prices to fill them in.\n", strings.Join(e.Unpriced, ", "))
	}
	return b.String()
}

// USD writes an amount with enough digits to show a fraction of a cent.
func USD(x float64) string {
	switch {
	case x == 0:
		return "$0"
	case math.Abs(x) >= 1:
		return fmt.Sprintf("$%.2f", x)
	case math.Abs(x) >= 0.01:
		return fmt.Sprintf("$%.4f", x)
	}
	return fmt.Sprintf("$%.3g", x)
}

func walks(ws []Walk) string {
	var p []string
	for _, w := range ws {
		p = append(p, fmt.Sprintf("every %s back %s", w.Every, w.Back))
	}
	return strings.Join(p, ", ")
}
