package cost_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/cost"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func defaults(t *testing.T) *config.Config {
	t.Helper()
	c, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// C4 attempt 4's day per 1,000 accounts (2 IPs, 16.3 browser hours, 174 GB
// stored) at the owner's prices of 2026-11-05 is about $1.26: $0.00126 per
// account, under the budget of $0.0019.
func TestPriceC4(t *testing.T) {
	c := defaults(t).TT.Cost
	e := cost.Price(cost.Inputs{Accounts: 1000, IPs: 2, BrowserHoursDay: 16.3, StoredGB: 174, ProxyGBDay: 6.1}, c, 0, time.Time{})
	if !near(e.ProxyUSD, 0.2) || !near(e.BrowserUSD, 16.3*0.017) || !near(e.ComputeUSD, 0.2) || !near(e.StorageUSD, 0.58) {
		t.Fatalf("parts: %+v", e)
	}
	if e.PerAccountUSD < 0.00125 || e.PerAccountUSD > 0.00127 || e.PerAccountUSD >= c.BudgetPerAccountUSD || len(e.Unpriced) != 0 {
		t.Fatalf("per account %v (budget %v), unpriced %v", e.PerAccountUSD, c.BudgetPerAccountUSD, e.Unpriced)
	}
	// Data priced per GB on a gateway tier; no accounts: no per-account cost.
	c.BrowserHourUSD = 0
	e = cost.Price(cost.Inputs{ProxyGBDay: 10}, c, 4, time.Time{})
	if !near(e.ProxyUSD, 40) || e.PerAccountUSD != 0 || len(e.Unpriced) != 1 || e.Unpriced[0] != "browser" {
		t.Fatalf("per GB: %+v", e)
	}
	tiers := []config.ProxyTier{{Kind: "static_list", Endpoints: []string{"a", "b", "c"}}, {Kind: "gateway", CostPerGBUSD: 3}, {Kind: "direct"}}
	if cost.IPs(tiers) != 3 || cost.PerGB(tiers) != 3 {
		t.Fatalf("ips %d, per GB %v", cost.IPs(tiers), cost.PerGB(tiers))
	}
}

// The meter prices the last 24 hours from what the fleet counted: accounts
// with a schedule, the static list's IPs, proxy bytes from the traffic
// budget's counters, the signer pages' browser time and Postgres's size;
// over the budget backfills pause, and resume only under 95 % of it.
func TestMeter(t *testing.T) {
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := defaults(t)
	cfg.TT.Proxy.Tiers = []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{"http://a", "http://b"}}}
	cfg.TT.Cost.StorageGBMonthUSD = 0 // the test database's size is not the point
	for _, ref := range []string{"u:a", "u:b", "u:c", "u:d"} {
		if _, err := db.Exec(ctx, `INSERT INTO account_schedule (account_ref, interval_seconds, next_run_at, admitted_at, enabled)
			VALUES ($1, 9900, now(), now(), $1 <> 'u:d')`, ref); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(ctx, `INSERT INTO media (media_id, source_media_id, kind, variant, status, size_bytes) VALUES
		('00000000-0000-0000-0000-000000000001', '1', 'video', 'hd', 'stored', 2000000000),
		('00000000-0000-0000-0000-000000000002', '2', 'video', 'hd', 'deleted', 9000000000)`); err != nil {
		t.Fatal(err)
	}
	// 06:00 UTC: today so far counts whole, yesterday three quarters.
	now := time.Date(2026, 11, 5, 6, 0, 0, 0, time.UTC)
	rep := ratelimit.NewReporter(rdb, "ns.")
	for range 4 {
		rep.Observe(detect.OK, "p1", 1e9) // 4 GB today
	}
	if err := rep.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for day, sec := range map[time.Time]float64{now: 3600, now.Add(-24 * time.Hour): 4 * 3600} {
		if err := cost.AddBrowserSeconds(ctx, rdb, "ns.", day, sec); err != nil {
			t.Fatal(err)
		}
	}
	m := &cost.Meter{DB: db, RDB: rdb, NS: "ns.", Cfg: cfg, Now: func() time.Time { return now }}
	if pause, _ := m.Over(ctx, false); pause {
		t.Fatal("no estimate yet: no pause")
	}
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	e := m.Last()
	if e.Accounts != 3 || e.IPs != 2 || !near(e.BrowserHoursDay, 1+3) || e.StoredGB < 2 {
		t.Fatalf("inputs: %+v", e.Inputs)
	}
	want := (2*3.0/30 + 4*0.017 + 3*0.0002) / 3
	if !near(e.PerAccountUSD, want) {
		t.Fatalf("per account %v, want %v", e.PerAccountUSD, want)
	}
	// Proxy data counted by the crawlers (priced 0 on ISP IPs).
	u, _ := ratelimit.LoadDayUsage(ctx, rdb, "ns.", time.Now())
	if u.Bytes[1] != 4e9 {
		t.Fatalf("reporter: %+v", u)
	}
	// The budget: over, then the 95 % hysteresis.
	cfg.TT.Cost.BudgetPerAccountUSD = want * 0.9
	if pause, why := m.Over(ctx, false); !pause || why == "" {
		t.Fatal("over the budget: pause")
	}
	cfg.TT.Cost.BudgetPerAccountUSD = want * 1.02
	if pause, _ := m.Over(ctx, true); !pause {
		t.Fatal("paused and within 95 % of the budget: stay paused")
	}
	if pause, _ := m.Over(ctx, false); pause {
		t.Fatal("not paused and under the budget: no pause")
	}
	cfg.TT.Cost.BudgetPerAccountUSD = want * 1.1
	if pause, _ := m.Over(ctx, true); pause {
		t.Fatal("under 95 % of the budget: resume")
	}
	// A stale estimate never holds backfills.
	cfg.TT.Cost.BudgetPerAccountUSD = want * 0.5
	now = now.Add(time.Hour)
	if pause, _ := m.Over(ctx, false); pause {
		t.Fatal("stale estimate: no pause")
	}
	cfg.TT.Cost.BudgetPerAccountUSD = 0
	now = now.Add(-time.Hour)
	if pause, _ := m.Over(ctx, false); pause {
		t.Fatal("no budget: no pause")
	}
}

type timer struct{ s float64 }

func (t *timer) BrowserSeconds() float64 { s := t.s; t.s = 0; return s }

// RunBrowserTime reports a pool's browser time, and once more as it stops.
func TestRunBrowserTime(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cost.RunBrowserTime(ctx, rdb, "", &timer{s: 90}, time.Hour); err != nil {
		t.Fatal(err)
	}
	got, err := rdb.HGet(context.Background(), "tks:cost:d:"+time.Now().UTC().Format("20060102"), "browser_seconds").Float64()
	if err != nil || got != 90 {
		t.Fatalf("%v %v", got, err)
	}
}
