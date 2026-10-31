package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Preflight check statuses.
const (
	StatusPass = "PASS"
	StatusWarn = "WARN"
	StatusFail = "FAIL"
	StatusSkip = "SKIP"
)

// DefaultIPEchoURL returns the caller's IP as JSON ({"ip":"..."}).
const DefaultIPEchoURL = "https://api.ipify.org?format=json"

// PreflightOptions configure Preflight.
type PreflightOptions struct {
	Tiers      []string
	Page       string // a public page to load on every tier; empty skips the load
	IPEchoURL  string // empty skips the exit IP checks
	Exits      int    // exits sampled per tier (default 3)
	ConfigPath string // checked for permissions; empty skips
	Seed       registry.Seed
	BaseURL    string
}

// PreflightDeps are the clients Preflight uses.
type PreflightDeps struct {
	Proxies *proxy.Pool
	Pool    *session.Pool
	Loader  *session.Loader
	// EchoClient returns an HTTP client that sends requests through ep. The
	// default uses net/http with ep.URL as proxy and no connection reuse,
	// so each request opens a fresh connection through the exit.
	EchoClient func(ep proxy.Endpoint) *http.Client
}

// PreflightCheck is one line of the preflight table.
type PreflightCheck struct {
	Tier   string `json:"tier,omitempty"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

// PreflightResult lists every check; OK is false when any check failed.
type PreflightResult struct {
	Checks []PreflightCheck `json:"checks"`
	OK     bool             `json:"ok"`
}

func (p *PreflightResult) add(tier, name, status, detail string) {
	p.Checks = append(p.Checks, PreflightCheck{Tier: tier, Name: name, Status: status, Detail: Redact(detail)})
	if status == StatusFail {
		p.OK = false
	}
}

func defaultEchoClient(ep proxy.Endpoint) *http.Client {
	tr := &http.Transport{DisableKeepAlives: true, TLSHandshakeTimeout: 15 * time.Second}
	if ep.URL != "" {
		if u, err := url.Parse(ep.URL); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	}
	return &http.Client{Transport: tr, Timeout: 30 * time.Second}
}

func echoIP(ctx context.Context, c *http.Client, echoURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, echoURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ip echo: status %d", resp.StatusCode)
	}
	var v struct {
		IP string `json:"ip"`
	}
	if json.Unmarshal(b, &v) == nil && v.IP != "" {
		return v.IP, nil
	}
	if s := strings.TrimSpace(string(b)); s != "" && len(s) < 64 {
		return s, nil
	}
	return "", fmt.Errorf("ip echo: unexpected body")
}

// Preflight checks the configuration, every tier's exits and a Facebook
// page load before any measurement spends proxy traffic.
func Preflight(ctx context.Context, d PreflightDeps, o PreflightOptions) PreflightResult {
	res := PreflightResult{OK: true}
	if o.Exits <= 0 {
		o.Exits = 3
	}
	if d.EchoClient == nil {
		d.EchoClient = defaultEchoClient
	}
	fb := IsFacebook(o.BaseURL)

	if q := registry.PlaceholderQueries(o.Seed); len(q) > 0 && fb {
		res.add("", "registry", StatusWarn, "placeholder doc_ids ("+strings.Join(q, ", ")+"): the docids step (or fbs-spike har) must replace them before run or capture")
	} else {
		res.add("", "registry", StatusPass, fmt.Sprintf("%d queries", len(o.Seed.Queries)))
	}
	if o.ConfigPath != "" {
		for _, p := range strings.Split(o.ConfigPath, ",") {
			st, err := os.Stat(strings.TrimSpace(p))
			switch {
			case err != nil:
				res.add("", "config", StatusFail, err.Error())
			case st.Mode().Perm()&0o077 != 0:
				res.add("", "config", StatusWarn, fmt.Sprintf("%s is readable by others (%v): it holds proxy credentials, run chmod 600 %s", p, st.Mode().Perm(), p))
			default:
				res.add("", "config", StatusPass, p+" permissions "+st.Mode().Perm().String())
			}
		}
	}

	for _, tier := range o.Tiers {
		ep, err := d.Proxies.Next(tier)
		if err != nil {
			res.add(tier, "tier", StatusFail, err.Error())
			continue
		}
		if ep.URL == "" && fb {
			res.add(tier, "tier", StatusWarn, "direct egress: this measures this machine's IP, not a proxy tier")
		} else {
			res.add(tier, "tier", StatusPass, ep.Redacted())
		}
		if o.IPEchoURL == "" {
			res.add(tier, "exits", StatusSkip, "no --ip-echo-url")
		} else {
			preflightExits(ctx, &res, d, o, tier, ep)
		}
		if o.Page == "" {
			res.add(tier, "facebook", StatusSkip, "no page to load")
			continue
		}
		preflightLoad(ctx, &res, d, o, tier)
	}
	return res
}

func preflightExits(ctx context.Context, res *PreflightResult, d PreflightDeps, o PreflightOptions, tier string, first proxy.Endpoint) {
	ips := map[string]bool{}
	sticky, sessions := true, first.SessionID != ""
	ep := first
	for i := 0; i < o.Exits; i++ {
		if i > 0 {
			var err error
			if ep, err = d.Proxies.Next(tier); err != nil {
				res.add(tier, "exits", StatusFail, err.Error())
				return
			}
		}
		c := d.EchoClient(ep)
		a, err := echoIP(ctx, c, o.IPEchoURL)
		if err != nil {
			res.add(tier, "exits", StatusFail, fmt.Sprintf("%s: %v", ep.Redacted(), err))
			return
		}
		b, err := echoIP(ctx, c, o.IPEchoURL)
		if err != nil {
			res.add(tier, "exits", StatusFail, fmt.Sprintf("%s (second request): %v", ep.Redacted(), err))
			return
		}
		if a != b {
			sticky = false
		}
		ips[a] = true
	}
	ipList := make([]string, 0, len(ips))
	for ip := range ips {
		ipList = append(ipList, ip)
	}
	detail := fmt.Sprintf("%d exits sampled, %d distinct IPs (%s)", o.Exits, len(ips), strings.Join(sortStrings(ipList), ", "))
	switch {
	case first.URL == "":
		res.add(tier, "exits", StatusPass, "direct: "+detail)
	case !sticky:
		res.add(tier, "exits", StatusFail, detail+"; the exit IP changed within one session: fix the sticky session settings (username_template, sticky_ttl)")
	case sessions && len(ips) < o.Exits:
		res.add(tier, "exits", StatusWarn, detail+"; different sessions shared an exit IP: check that {session} is in username_template")
	default:
		res.add(tier, "exits", StatusPass, detail+", sticky within a session")
	}
}

func preflightLoad(ctx context.Context, res *PreflightResult, d PreflightDeps, o PreflightOptions, tier string) {
	ref, err := task.NormalizeRef(o.Page)
	if err != nil {
		res.add(tier, "facebook", StatusFail, err.Error())
		return
	}
	id, err := d.Pool.Acquire(ctx, tier)
	if err != nil {
		res.add(tier, "facebook", StatusFail, err.Error())
		return
	}
	defer d.Pool.Release(id)
	u, _ := id.Base.Parse(task.RefPath(ref))
	start := time.Now()
	pl, err := d.Loader.Load(ctx, id, u.String())
	ms := time.Since(start).Milliseconds()
	if err != nil {
		res.add(tier, "facebook", StatusFail, fmt.Sprintf("%s: %v", ref, err))
		return
	}
	tok := htmlx.ExtractTokens(pl.HTML)
	var missing []string
	for k, ok := range map[string]bool{"lsd": tok["lsd"] != "", "spin_r": tok["__spin_r"] != "", "hsi": tok["__hsi"] != ""} {
		if !ok {
			missing = append(missing, k)
		}
	}
	detail := fmt.Sprintf("%s: class %s, status %d, %d ms, %s on the wire", ref, pl.Class, pl.Status, ms, human(float64(pl.Bytes)))
	if htmlx.ConsentWall(pl.HTML) {
		detail += ", consent wall shown"
	}
	switch {
	case pl.Class != detect.OK:
		res.add(tier, "facebook", StatusFail, detail+" "+pl.Reason)
	case !id.Bootstrapped():
		res.add(tier, "facebook", StatusFail, detail+"; no tokens (missing "+strings.Join(sortStrings(missing), ", ")+")")
	case len(missing) > 0:
		res.add(tier, "facebook", StatusWarn, detail+"; missing tokens "+strings.Join(sortStrings(missing), ", "))
	default:
		res.add(tier, "facebook", StatusPass, detail)
	}
}

func sortStrings(xs []string) []string {
	out := slices.Clone(xs)
	sort.Strings(out)
	return out
}

// FormatPreflight renders the checks as a table.
func FormatPreflight(r PreflightResult) string {
	var b strings.Builder
	for _, c := range r.Checks {
		tier := c.Tier
		if tier == "" {
			tier = "-"
		}
		fmt.Fprintf(&b, "%-4s %-12s %-9s %s\n", c.Status, tier, c.Name, c.Detail)
	}
	if r.OK {
		b.WriteString("preflight: OK\n")
	} else {
		b.WriteString("preflight: FAILED (fix the FAIL lines above)\n")
	}
	return b.String()
}
