package ratelimit

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// The daily traffic budget: crawlers add requests and wire bytes per lane
// to a per-day hash and to 5-minute slices (Reporter.Flush); the leader
// scheduler projects the day's use from the recent slices and publishes a
// BudgetState that pauses backfill and stretches slow tiers.

// BudgetSlice is the resolution of the recent-use slices.
const BudgetSlice = 5 * time.Minute

func budgetDayKey(ns string, t time.Time) string {
	return redisx.KeyPrefix + ns + "budget:d:" + t.UTC().Format("20060102")
}

func budgetSliceKey(ns string, t time.Time) string {
	return redisx.KeyPrefix + ns + "budget:s:" + strconv.FormatInt(t.Truncate(BudgetSlice).Unix(), 10)
}

func budgetStateKey(ns string) string { return redisx.KeyPrefix + ns + "budget:state" }

// Usage is traffic per lane.
type Usage struct {
	Requests [3]int64 `json:"requests"`
	Bytes    [3]int64 `json:"bytes"`
}

func (u Usage) total(f func(Usage) [3]int64, lanes ...int) int64 {
	v := f(u)
	var n int64
	for _, i := range lanes {
		n += v[i]
	}
	return n
}

func reqCounts(u Usage) [3]int64  { return u.Requests }
func byteCounts(u Usage) [3]int64 { return u.Bytes }

func usageOf(h map[string]string) Usage {
	var u Usage
	for i, lane := range Lanes {
		u.Requests[i], _ = strconv.ParseInt(h["req:"+lane], 10, 64)
		u.Bytes[i], _ = strconv.ParseInt(h["bytes:"+lane], 10, 64)
	}
	return u
}

// BudgetState is what the controller publishes.
type BudgetState struct {
	PauseP2 bool    `json:"pause_p2"`
	Stretch float64 `json:"stretch"` // interval factor for StretchTiers; 1 = none
	// StretchTiers are the tiers the planner stretches.
	StretchTiers []string `json:"stretch_tiers,omitempty"`
	// P2Rate is backfill's requests and bytes per second when it last ran,
	// to decide when resuming it fits the budget.
	P2ReqRate   float64   `json:"p2_req_rate"`
	P2ByteRate  float64   `json:"p2_byte_rate"`
	Used        Usage     `json:"used"`
	Projected   [2]int64  `json:"projected"` // requests, bytes by the end of the day
	UpdatedAt   time.Time `json:"updated_at"`
	Explanation string    `json:"explanation,omitempty"`
}

// StretchFor returns the interval factor for tier.
func (s BudgetState) StretchFor(tier string) float64 {
	if s.Stretch <= 1 {
		return 1
	}
	for _, t := range s.StretchTiers {
		if t == tier {
			return s.Stretch
		}
	}
	return 1
}

// LoadBudgetState reads the published state; a missing state is the
// neutral one.
func LoadBudgetState(ctx context.Context, rdb *redis.Client, ns string) (BudgetState, error) {
	b, err := rdb.Get(ctx, budgetStateKey(ns)).Bytes()
	if errors.Is(err, redis.Nil) {
		return BudgetState{Stretch: 1}, nil
	}
	if err != nil {
		return BudgetState{Stretch: 1}, err
	}
	var s BudgetState
	if err := json.Unmarshal(b, &s); err != nil {
		return BudgetState{Stretch: 1}, err
	}
	return s, nil
}

// BudgetController projects the day's traffic and publishes BudgetState.
type BudgetController struct {
	rdb *redis.Client
	ns  string
	cfg config.Budget
	now func() time.Time
}

// NewBudgetController returns a controller for cfg.
func NewBudgetController(rdb *redis.Client, ns string, cfg config.Budget) *BudgetController {
	if cfg.ProjectionSpan < BudgetSlice {
		cfg.ProjectionSpan = time.Hour
	}
	if cfg.Stretch <= 1 {
		cfg.Stretch = 2
	}
	return &BudgetController{rdb: rdb, ns: ns, cfg: cfg, now: time.Now}
}

// Hysteresis: resume backfill or drop the stretch only when the projection
// is this far under the budget.
const resumeMargin = 0.95

// Tick reads today's use and the recent slices, decides and publishes.
func (b *BudgetController) Tick(ctx context.Context) error {
	now := b.now().UTC()
	prev, err := LoadBudgetState(ctx, b.rdb, b.ns)
	if err != nil {
		return err
	}
	dayH, err := b.rdb.HGetAll(ctx, budgetDayKey(b.ns, now)).Result()
	if err != nil {
		return err
	}
	used := usageOf(dayH)
	// Recent rate over the last ProjectionSpan of complete slices.
	var recent Usage
	n := int(b.cfg.ProjectionSpan / BudgetSlice)
	pipe := b.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, n)
	for i := range n {
		cmds[i] = pipe.HGetAll(ctx, budgetSliceKey(b.ns, now.Add(-time.Duration(i+1)*BudgetSlice)))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	for _, c := range cmds {
		u := usageOf(c.Val())
		for i := range 3 {
			recent.Requests[i] += u.Requests[i]
			recent.Bytes[i] += u.Bytes[i]
		}
	}
	span := b.cfg.ProjectionSpan.Seconds()
	endOfDay := now.Truncate(24 * time.Hour).Add(24 * time.Hour)
	left := endOfDay.Sub(now).Seconds()

	st := BudgetState{Stretch: 1, UpdatedAt: now, Used: used, StretchTiers: b.cfg.StretchTiers,
		P2ReqRate: prev.P2ReqRate, P2ByteRate: prev.P2ByteRate}
	p2Req, p2Bytes := float64(recent.Requests[2])/span, float64(recent.Bytes[2])/span
	if !prev.PauseP2 && p2Req > 0 {
		st.P2ReqRate, st.P2ByteRate = p2Req, p2Bytes
	}
	type dim struct {
		budget  int64
		p2Rate  float64
		project func(lanes ...int) float64
	}
	project := func(f func(Usage) [3]int64) func(lanes ...int) float64 {
		return func(lanes ...int) float64 {
			rate := float64(recent.total(f, lanes...)) / span
			return float64(used.total(f, 0, 1, 2)) + rate*left
		}
	}
	dims := []dim{
		{b.cfg.RequestsPerDay, st.P2ReqRate, project(reqCounts)},
		{b.cfg.BytesPerDay, st.P2ByteRate, project(byteCounts)},
	}
	for i, d := range dims {
		withP2, without := d.project(0, 1, 2), d.project(0, 1)
		st.Projected[i] = int64(withP2)
		if d.budget <= 0 {
			continue
		}
		limit := float64(d.budget)
		// Backfill: pause when the day would overrun; while paused, resume
		// only if its last known rate fits again.
		resumeAt := without + d.p2Rate*left
		if (!prev.PauseP2 && withP2 > limit) || (prev.PauseP2 && resumeAt > limit*resumeMargin) {
			st.PauseP2 = true
		}
		// Slow tiers: stretch when p0 + p1 alone would overrun.
		if without > limit || (prev.Stretch > 1 && without > limit*resumeMargin) {
			st.Stretch = b.cfg.Stretch
		}
	}
	switch {
	case st.Stretch > 1:
		st.Explanation = "on-demand and scheduled traffic alone would exceed the daily budget: backfill paused, slow tiers stretched"
	case st.PauseP2:
		st.Explanation = "the day's projected traffic exceeds the budget: backfill paused"
	}
	if st.PauseP2 != prev.PauseP2 || st.Stretch != prev.Stretch {
		slog.Warn("budget: state changed", "pause_p2", st.PauseP2, "stretch", st.Stretch,
			"projected_requests", st.Projected[0], "projected_bytes", st.Projected[1], "why", st.Explanation)
	}
	for i, lane := range Lanes {
		metrics.BudgetUsed.WithLabelValues("requests", lane).Set(float64(used.Requests[i]))
		metrics.BudgetUsed.WithLabelValues("bytes", lane).Set(float64(used.Bytes[i]))
	}
	metrics.BudgetProjected.WithLabelValues("requests").Set(float64(st.Projected[0]))
	metrics.BudgetProjected.WithLabelValues("bytes").Set(float64(st.Projected[1]))
	metrics.BudgetLimit.WithLabelValues("requests").Set(float64(b.cfg.RequestsPerDay))
	metrics.BudgetLimit.WithLabelValues("bytes").Set(float64(b.cfg.BytesPerDay))
	paused := 0.0
	if st.PauseP2 {
		paused = 1
	}
	metrics.BudgetBackfillPaused.Set(paused)
	metrics.BudgetStretch.Set(st.Stretch)
	js, _ := json.Marshal(st)
	// The state expires when the controller stops publishing (budget
	// switched off, leader gone): crawlers and the planner then fall back
	// to the neutral state instead of holding backfill forever.
	return b.rdb.Set(ctx, budgetStateKey(b.ns), js, max(10*b.cfg.Interval, 10*time.Minute)).Err()
}

// BudgetGate gives crawlers the published state.
type BudgetGate struct {
	rdb *redis.Client
	ns  string
	cur atomic.Pointer[BudgetState]
	// OnChange is called when PauseP2 changes.
	OnChange func(BudgetState)
}

// NewBudgetGate returns a gate in the neutral state.
func NewBudgetGate(rdb *redis.Client, ns string) *BudgetGate {
	g := &BudgetGate{rdb: rdb, ns: ns}
	g.cur.Store(&BudgetState{Stretch: 1})
	return g
}

// State returns the last state read.
func (g *BudgetGate) State() BudgetState { return *g.cur.Load() }

// Refresh reads the state and reports a backfill pause change.
func (g *BudgetGate) Refresh(ctx context.Context) error {
	st, err := LoadBudgetState(ctx, g.rdb, g.ns)
	if err != nil {
		return err
	}
	prev := g.cur.Swap(&st)
	if prev.PauseP2 != st.PauseP2 && g.OnChange != nil {
		g.OnChange(st)
	}
	return nil
}

// Run refreshes every 15 seconds until ctx is done.
func (g *BudgetGate) Run(ctx context.Context) error {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		if err := g.Refresh(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("budget: refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
