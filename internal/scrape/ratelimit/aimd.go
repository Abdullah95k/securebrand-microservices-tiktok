package ratelimit

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
)

// The fleet-wide request ceiling is adjusted by AIMD (additive increase,
// multiplicative decrease) on the block rate Facebook shows the whole
// fleet: crawlers count outcomes into 10-second windows in Redis, the
// leader scheduler reads each finished window and moves the ceiling, and
// crawlers read the ceiling back for the global GCRA limiter. Adding pods
// therefore never adds load: they share the same ceiling.

// Window is the AIMD measurement window.
const Window = 10 * time.Second

// settle is how long after a window ends the controller waits for the last
// reporter flushes.
const settle = 3 * time.Second

func windowKey(ns string, start time.Time) string {
	return redisx.KeyPrefix + ns + "aimd:w:" + strconv.FormatInt(start.Unix(), 10)
}

func ceilingKey(ns string) string { return redisx.KeyPrefix + ns + "aimd:global_rps" }

// Reporter counts GraphQL outcomes and flushes them to Redis: block
// counts for AIMD, and requests and wire bytes per lane for the traffic
// budget. It is safe for concurrent use; Observe is a few atomic adds.
type Reporter struct {
	rdb            *redis.Client
	ns             string
	total, blocked atomic.Int64
	reqs, bytes    [3]atomic.Int64 // per lane: p0, p1, p2
	now            func() time.Time
}

// NewReporter returns a Reporter writing under namespace ns.
func NewReporter(rdb *redis.Client, ns string) *Reporter {
	return &Reporter{rdb: rdb, ns: ns, now: time.Now}
}

// Lanes are the budget's lane names, by index.
var Lanes = [3]string{"p0", "p1", "p2"}

func laneIndex(lane string) int {
	switch lane {
	case "p0":
		return 0
	case "p2":
		return 2
	default:
		return 1
	}
}

// Observe counts one response. Transport failures are the proxy's, not
// Facebook's answer: they count for the budget (they used traffic) but
// not for AIMD.
func (r *Reporter) Observe(c detect.Class, lane string, wireBytes int64) {
	i := laneIndex(lane)
	r.reqs[i].Add(1)
	r.bytes[i].Add(wireBytes)
	if c == detect.Transport {
		return
	}
	r.total.Add(1)
	if c.Blocking() {
		r.blocked.Add(1)
	}
}

// Flush writes the counts gathered since the last flush.
func (r *Reporter) Flush(ctx context.Context) error {
	total, blocked := r.total.Swap(0), r.blocked.Swap(0)
	var reqs, bytes [3]int64
	nonzero := total > 0
	for i := range reqs {
		reqs[i], bytes[i] = r.reqs[i].Swap(0), r.bytes[i].Swap(0)
		nonzero = nonzero || reqs[i] > 0
	}
	if !nonzero {
		return nil
	}
	now := r.now()
	_, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if total > 0 {
			key := windowKey(r.ns, now.Truncate(Window))
			p.HIncrBy(ctx, key, "total", total)
			p.HIncrBy(ctx, key, "blocked", blocked)
			p.Expire(ctx, key, 10*time.Minute)
		}
		for _, key := range []string{budgetDayKey(r.ns, now), budgetSliceKey(r.ns, now)} {
			for i, lane := range Lanes {
				if reqs[i] > 0 {
					p.HIncrBy(ctx, key, "req:"+lane, reqs[i])
					p.HIncrBy(ctx, key, "bytes:"+lane, bytes[i])
				}
			}
		}
		p.Expire(ctx, budgetDayKey(r.ns, now), 72*time.Hour)
		p.Expire(ctx, budgetSliceKey(r.ns, now), 3*time.Hour)
		return nil
	})
	if err != nil {
		// Keep the counts for the next attempt.
		r.total.Add(total)
		r.blocked.Add(blocked)
		for i := range reqs {
			r.reqs[i].Add(reqs[i])
			r.bytes[i].Add(bytes[i])
		}
	}
	return err
}

// Run flushes every two seconds until ctx is done.
func (r *Reporter) Run(ctx context.Context) error {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = r.Flush(fctx)
			cancel()
			return nil
		case <-t.C:
			if err := r.Flush(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("aimd: flush failed", "err", err)
			}
		}
	}
}

// Ceiling caches the fleet ceiling for the GraphQL client's global limiter.
type Ceiling struct {
	rdb      *redis.Client
	ns       string
	fallback int
	cur      atomic.Int64
}

// NewCeiling returns a Ceiling that reports fallback until a ceiling has
// been published.
func NewCeiling(rdb *redis.Client, ns string, fallback int) *Ceiling {
	c := &Ceiling{rdb: rdb, ns: ns, fallback: fallback}
	c.cur.Store(int64(fallback))
	return c
}

// RPS returns the current ceiling in requests per second.
func (c *Ceiling) RPS() int { return int(c.cur.Load()) }

// Refresh reads the published ceiling. A missing key keeps the fallback;
// an unreachable Redis keeps the last value read.
func (c *Ceiling) Refresh(ctx context.Context) error {
	v, err := c.rdb.Get(ctx, ceilingKey(c.ns)).Int()
	switch {
	case errors.Is(err, redis.Nil):
		c.cur.Store(int64(c.fallback))
		return nil
	case err != nil:
		return err
	case v > 0:
		c.cur.Store(int64(v))
	}
	return nil
}

// Run refreshes every five seconds until ctx is done.
func (c *Ceiling) Run(ctx context.Context) error {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("aimd: ceiling refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Decision is one AIMD step.
type Decision struct {
	From, To    int
	Total       int64
	BlockRate   float64
	Utilization float64 // requests / (ceiling x window)
	Reason      string  // increase | decrease | hold:<why>
}

// Decide applies one AIMD step to ceiling cur for a window with total
// responses of which blocked were blocks:
//
//   - block rate above HighBlockRate: the rate actually sent (at most cur)
//     x Decrease, so the cut lowers real load even when the fleet was below
//     the ceiling (multiplicative decrease);
//   - below LowBlockRate while the fleet uses at least MinUtilization of the
//     ceiling: cur + max(1, cur x Increase) (additive increase);
//   - otherwise, or with fewer than MinSamples responses: hold.
//
// The result stays within [MinRPS, MaxRPS].
func Decide(cfg config.AIMD, cur int, total, blocked int64, window time.Duration) Decision {
	d := Decision{From: cur, To: cur, Total: total}
	if total > 0 {
		d.BlockRate = float64(blocked) / float64(total)
	}
	if cur > 0 {
		d.Utilization = float64(total) / (float64(cur) * window.Seconds())
	}
	switch {
	case total < int64(cfg.MinSamples):
		d.Reason = "hold:few_samples"
	case d.BlockRate > cfg.HighBlockRate:
		sent := math.Min(float64(cur), float64(total)/window.Seconds())
		d.To, d.Reason = int(math.Floor(sent*cfg.Decrease)), "decrease"
	case d.BlockRate < cfg.LowBlockRate && d.Utilization >= cfg.MinUtilization:
		d.To, d.Reason = cur+max(1, int(float64(cur)*cfg.Increase)), "increase"
	case d.BlockRate < cfg.LowBlockRate:
		d.Reason = "hold:underused"
	default:
		d.Reason = "hold:block_rate"
	}
	if clamped := min(max(d.To, cfg.MinRPS), cfg.MaxRPS); clamped != d.To {
		d.To = clamped
		if d.To != d.From && d.Reason != "increase" && d.Reason != "decrease" {
			d.Reason = "clamp"
		}
	}
	return d
}

// Controller runs AIMD on the leader scheduler.
type Controller struct {
	rdb   *redis.Client
	ns    string
	cfg   config.AIMD
	start int
	now   func() time.Time
	last  time.Time // last window processed
}

// NewController returns a controller that starts the ceiling at start. The
// configured start is always allowed, even above cfg.MaxRPS.
func NewController(rdb *redis.Client, ns string, cfg config.AIMD, start int) *Controller {
	cfg.MaxRPS = max(cfg.MaxRPS, start)
	return &Controller{rdb: rdb, ns: ns, cfg: cfg, start: start, now: time.Now}
}

// Tick processes the latest finished window, once.
func (c *Controller) Tick(ctx context.Context) error {
	w := c.now().Add(-settle).Truncate(Window).Add(-Window)
	if !w.After(c.last) {
		return nil
	}
	c.last = w
	counts, err := c.rdb.HGetAll(ctx, windowKey(c.ns, w)).Result()
	if err != nil {
		return err
	}
	total, _ := strconv.ParseInt(counts["total"], 10, 64)
	blocked, _ := strconv.ParseInt(counts["blocked"], 10, 64)
	cur, err := c.rdb.Get(ctx, ceilingKey(c.ns)).Int()
	missing := errors.Is(err, redis.Nil)
	if missing || (err == nil && cur <= 0) {
		cur, err = c.start, nil
	}
	if err != nil {
		return err
	}
	d := Decide(c.cfg, cur, total, blocked, Window)
	metrics.FleetRPS.Set(float64(total) / Window.Seconds())
	metrics.FleetBlockRate.Set(d.BlockRate)
	metrics.AIMDCeiling.Set(float64(d.To))
	if d.To != d.From || missing {
		if err := c.rdb.Set(ctx, ceilingKey(c.ns), d.To, 0).Err(); err != nil {
			return err
		}
	}
	if d.To != d.From {
		metrics.AIMDAdjustments.WithLabelValues(d.Reason).Inc()
		slog.Info("aimd: ceiling", "from", d.From, "to", d.To, "reason", d.Reason, "responses", total,
			"block_rate", d.BlockRate, "utilization", d.Utilization)
	}
	return nil
}
