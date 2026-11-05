// Package cost prices tks's day (build brief §8 A5): the proxy IPs a month
// and any data priced per GB, the browser time of the signer pages, compute
// per account and storage, over the accounts tks checks. The leader
// scheduler prices the last 24 hours every tt.cost.interval and publishes
// tks_cost_per_account_usd; over the owner's budget (D9,
// tt.cost.budget_per_account_usd) backfills pause and checks go on (owner,
// 2026-11-05), through the traffic budget's controller (Meter.Over).
//
// The inputs are what the fleet counted, not a model: proxy bytes from the
// traffic budget's day counters (every crawler reports them), browser time
// from the signer pools (AddBrowserSeconds), storage from Postgres (the
// database's size and the media files stored and not yet deleted by the
// deleting service, D5). The Phase 0 kit's model (internal/tt/phase0)
// projects the same parts before deployment.
package cost

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
)

func dayKey(ns string, t time.Time) string {
	return redisx.KeyPrefix + ns + "cost:d:" + t.UTC().Format("20060102")
}

// AddBrowserSeconds adds browser time to the fleet's day counter.
func AddBrowserSeconds(ctx context.Context, rdb *redis.Client, ns string, now time.Time, seconds float64) error {
	if seconds <= 0 {
		return nil
	}
	key := dayKey(ns, now)
	_, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HIncrByFloat(ctx, key, "browser_seconds", seconds)
		p.Expire(ctx, key, 72*time.Hour)
		return nil
	})
	return err
}

// BrowserTimer is a signer pool (sign.Pool.BrowserSeconds).
type BrowserTimer interface {
	BrowserSeconds() float64
}

// RunBrowserTime reports pool's browser time every interval until ctx
// ends, and once more as it stops.
func RunBrowserTime(ctx context.Context, rdb *redis.Client, ns string, pool BrowserTimer, every time.Duration) error {
	flush := func(ctx context.Context) {
		s := pool.BrowserSeconds()
		metrics.SignerPageSeconds.Add(s)
		if err := AddBrowserSeconds(ctx, rdb, ns, time.Now(), s); err != nil {
			slog.Warn("cost: browser time not reported", "err", err)
		}
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			flush(sctx)
			cancel()
			return nil
		case <-t.C:
			flush(ctx)
		}
	}
}

// Inputs are what a day's cost is priced from.
type Inputs struct {
	Accounts int64 `json:"accounts"`
	IPs      int   `json:"ips"`
	// ProxyGBDay is the proxy data of the last 24 hours (GB, 1e9 bytes),
	// priced only for tiers with cost_per_gb_usd.
	ProxyGBDay      float64 `json:"proxy_gb_day"`
	BrowserHoursDay float64 `json:"browser_hours_day"`
	StoredGB        float64 `json:"stored_gb"`
}

// Estimate is a day's cost in USD.
type Estimate struct {
	Inputs
	At            time.Time `json:"at"`
	ProxyUSD      float64   `json:"proxy_usd"`
	BrowserUSD    float64   `json:"browser_usd"`
	ComputeUSD    float64   `json:"compute_usd"`
	StorageUSD    float64   `json:"storage_usd"`
	TotalUSD      float64   `json:"total_usd"`
	PerAccountUSD float64   `json:"per_account_usd"`
	// Unpriced lists the parts with no price (tt.cost 0).
	Unpriced []string `json:"unpriced,omitempty"`
}

// Price prices in by c; perGB is the proxy data's price per GB (0: data
// is not charged, as on the owner's ISP IPs). A month is 30 days, as in
// the Phase 0 kit.
func Price(in Inputs, c config.TTCost, perGB float64, at time.Time) Estimate {
	e := Estimate{Inputs: in, At: at}
	e.ProxyUSD = float64(in.IPs)*c.IPMonthUSD/30 + in.ProxyGBDay*perGB
	e.BrowserUSD = in.BrowserHoursDay * c.BrowserHourUSD
	e.ComputeUSD = float64(in.Accounts) * c.ComputeUSDPerAccount
	e.StorageUSD = in.StoredGB * c.StorageGBMonthUSD / 30
	e.TotalUSD = e.ProxyUSD + e.BrowserUSD + e.ComputeUSD + e.StorageUSD
	if in.Accounts > 0 {
		e.PerAccountUSD = e.TotalUSD / float64(in.Accounts)
	}
	for _, p := range []struct {
		name  string
		price float64
	}{{"proxy", c.IPMonthUSD + perGB}, {"browser", c.BrowserHourUSD}, {"compute", c.ComputeUSDPerAccount}, {"storage", c.StorageGBMonthUSD}} {
		if p.price <= 0 {
			e.Unpriced = append(e.Unpriced, p.name)
		}
	}
	return e
}

// IPs counts the endpoints of tiers' static lists: the IPs paid for when
// tt.cost.ips is 0. Gateway tiers are priced by their data.
func IPs(tiers []config.ProxyTier) int {
	n := 0
	for _, t := range tiers {
		if t.Kind == "static_list" {
			n += len(t.Endpoints)
		}
	}
	return n
}

// PerGB is the highest cost_per_gb_usd of tiers: the fleet's data is not
// counted per tier, so it is priced at the dearest one.
func PerGB(tiers []config.ProxyTier) float64 {
	var p float64
	for _, t := range tiers {
		p = max(p, t.CostPerGBUSD)
	}
	return p
}

// resumeMargin: backfills paused for the cost resume only under this
// share of the budget, so the pause does not flap.
const resumeMargin = 0.95

// Meter prices the last 24 hours (Tick) and decides the budget's pause
// (Over).
type Meter struct {
	DB  *pgxpool.Pool
	RDB *redis.Client
	// NS is the Redis namespace (kafka.topic_prefix), as the budget's.
	NS  string
	Cfg *config.Config
	Now func() time.Time

	mu   sync.Mutex
	last *Estimate
}

func (m *Meter) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// Last returns the latest estimate (nil before the first Tick).
func (m *Meter) Last() *Estimate {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

// Measure reads the inputs of the last 24 hours and prices them.
func (m *Meter) Measure(ctx context.Context) (Estimate, error) {
	now := m.now().UTC()
	c := m.Cfg.TT.Cost
	in := Inputs{IPs: c.IPs}
	if in.IPs == 0 {
		in.IPs = IPs(m.Cfg.TT.Proxy.Tiers)
	}
	if err := m.DB.QueryRow(ctx, `SELECT count(*) FROM account_schedule WHERE enabled`).Scan(&in.Accounts); err != nil {
		return Estimate{}, fmt.Errorf("cost: accounts: %w", err)
	}
	var dbBytes, mediaBytes int64
	if err := m.DB.QueryRow(ctx, `SELECT pg_database_size(current_database()),
		coalesce((SELECT sum(size_bytes) FROM media WHERE status IN ('stored', 'ready')), 0)::bigint`).Scan(&dbBytes, &mediaBytes); err != nil {
		return Estimate{}, fmt.Errorf("cost: storage: %w", err)
	}
	in.StoredGB = float64(dbBytes+mediaBytes) / 1e9
	// The last 24 hours: today so far and the part of yesterday after this
	// time of day (yesterday's counts spread evenly over it).
	day := now.Truncate(24 * time.Hour)
	rest := 1 - now.Sub(day).Hours()/24
	var bytes float64
	for i, w := range []float64{1, rest} {
		u, err := ratelimit.LoadDayUsage(ctx, m.RDB, m.NS, day.Add(-time.Duration(i)*24*time.Hour))
		if err != nil {
			return Estimate{}, fmt.Errorf("cost: proxy data: %w", err)
		}
		for _, b := range u.Bytes {
			bytes += w * float64(b)
		}
		s, err := m.RDB.HGet(ctx, dayKey(m.NS, day.Add(-time.Duration(i)*24*time.Hour)), "browser_seconds").Result()
		if err != nil && err != redis.Nil {
			return Estimate{}, fmt.Errorf("cost: browser time: %w", err)
		}
		if sec, _ := strconv.ParseFloat(s, 64); sec > 0 {
			in.BrowserHoursDay += w * sec / 3600
		}
	}
	in.ProxyGBDay = bytes / 1e9
	return Price(in, c, PerGB(m.Cfg.TT.Proxy.Tiers), now), nil
}

// Tick prices the last 24 hours and publishes the gauges.
func (m *Meter) Tick(ctx context.Context) error {
	e, err := m.Measure(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	first := m.last == nil
	m.last = &e
	m.mu.Unlock()
	c := m.Cfg.TT.Cost
	metrics.CostPerAccount.Set(e.PerAccountUSD)
	for part, v := range map[string]float64{"proxy": e.ProxyUSD, "browser": e.BrowserUSD, "compute": e.ComputeUSD,
		"storage": e.StorageUSD, "total": e.TotalUSD} {
		metrics.CostDay.WithLabelValues(part).Set(v)
	}
	for input, v := range map[string]float64{"accounts": float64(e.Accounts), "ips": float64(e.IPs), "proxy_gb_day": e.ProxyGBDay,
		"browser_hours_day": e.BrowserHoursDay, "stored_gb": e.StoredGB} {
		metrics.CostInputs.WithLabelValues(input).Set(v)
	}
	metrics.CostBudget.Set(c.BudgetPerAccountUSD)
	metrics.CostAlertShare.Set(c.AlertShare)
	if first && len(e.Unpriced) > 0 {
		slog.Warn("cost: parts without a price (tt.cost)", "unpriced", strings.Join(e.Unpriced, ","))
	}
	return nil
}

// Over is the traffic budget's further budget (ratelimit.BudgetController
// .Extra): pause backfills while the cost per account is over
// tt.cost.budget_per_account_usd; once paused, resume only under 95 % of
// it. Without a budget, accounts or a recent estimate it never pauses.
func (m *Meter) Over(_ context.Context, paused bool) (bool, string) {
	budget := m.Cfg.TT.Cost.BudgetPerAccountUSD
	e := m.Last()
	over := false
	switch {
	case budget <= 0 || e == nil || e.Accounts == 0:
	case m.now().Sub(e.At) > 3*m.Cfg.TT.Cost.Interval:
		// The estimate is stale (the database or Redis failing): do not
		// hold backfills on an old figure.
	case paused:
		over = e.PerAccountUSD > budget*resumeMargin
	default:
		over = e.PerAccountUSD > budget
	}
	v := 0.0
	if over {
		v = 1
	}
	metrics.CostOverBudget.Set(v)
	if !over {
		return false, ""
	}
	return true, fmt.Sprintf("cost $%.5f per account a day is over the budget $%.5f: backfill paused, checks go on", e.PerAccountUSD, budget)
}
