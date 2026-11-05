package accounts

import (
	"context"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
)

// Rate is how many new accounts an hour paced admission lets start their
// first read (docs/tiktok/discovery-plan.md §5.3), and Unlimited when
// nothing limits it: tt.admission.per_hour when set; else, with a request
// budget (scheduler.budget.requests_per_day), what the day's budget leaves
// after the projected traffic, spread over the hours left in the day (UTC,
// the budget's day) at tt.admission.requests_per_account each, none while
// the budget pauses backfills; else unlimited. None at all while backfills
// are paused, whichever budget paused them. st is the budget
// controller's last state; nil when it is not known, which then admits
// the budget's whole day evenly (requests_per_day / 24 / per account).
func Rate(cfg *config.Config, st *ratelimit.BudgetState, now time.Time) (perHour int, unlimited bool) {
	a, b := cfg.TT.Admission, cfg.Scheduler.Budget
	if st != nil && st.PauseP2 {
		// Backfills are paused (the request budget or tks's dollar budget,
		// tt.cost): an account admitted now would wait in the paused lane
		// until the reaper closed its first read.
		return 0, false
	}
	if a.PerHour > 0 {
		return a.PerHour, false
	}
	if !b.Enabled || b.RequestsPerDay <= 0 {
		return 0, true
	}
	per := float64(max(a.RequestsPerAccount, 1))
	if st == nil || st.UpdatedAt.IsZero() {
		return int(float64(b.RequestsPerDay) / 24 / per), false
	}
	now = now.UTC()
	end := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	hours := math.Max(end.Sub(now).Hours(), 1)
	left := math.Max(float64(b.RequestsPerDay-st.Projected[0]), 0)
	return int(left / hours / per), false
}

// Queue is the admission queue: accounts registered and not yet let start.
type Queue struct {
	Waiting int64            `json:"waiting"`
	ByWay   map[string]int64 `json:"by_origin"`
	// PerHour is the admission rate now; Unlimited when nothing limits it.
	PerHour   int  `json:"per_hour"`
	Unlimited bool `json:"unlimited"`
	// DrainHours and EstimatedFinish at PerHour; absent when unlimited or
	// when the rate is 0 (the budget pauses backfills).
	DrainHours      *float64   `json:"drain_hours,omitempty"`
	EstimatedFinish *time.Time `json:"estimated_finish,omitempty"`
	// InADay is what draining the queue within a day would take, from
	// Phase 0's per-IP and per-signer-page capacity (tt.admission).
	InADay Capacity `json:"to_drain_in_a_day"`
}

// Capacity is what a workload needs.
type Capacity struct {
	PerHour     int64 `json:"per_hour"`
	Requests    int64 `json:"requests"`
	IPs         int64 `json:"ips"`
	SignerPages int64 `json:"signer_pages"`
}

// QueueStatus reads the queue and prices it.
func QueueStatus(ctx context.Context, db *pgxpool.Pool, cfg *config.Config, st *ratelimit.BudgetState, now time.Time) (Queue, error) {
	q := Queue{ByWay: map[string]int64{}}
	rows, err := db.Query(ctx, `SELECT origin, count(*) FROM account_schedule WHERE admitted_at IS NULL AND enabled GROUP BY origin`)
	if err != nil {
		return q, err
	}
	for rows.Next() {
		var o string
		var n int64
		if err := rows.Scan(&o, &n); err != nil {
			rows.Close()
			return q, err
		}
		q.ByWay[o], q.Waiting = n, q.Waiting+n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return q, err
	}
	q.PerHour, q.Unlimited = Rate(cfg, st, now)
	if !q.Unlimited && q.PerHour > 0 && q.Waiting > 0 {
		h := float64(q.Waiting) / float64(q.PerHour)
		at := now.Add(time.Duration(h * float64(time.Hour))).UTC().Truncate(time.Minute)
		q.DrainHours, q.EstimatedFinish = &h, &at
	}
	q.InADay = Needs(cfg.TT.Admission, q.Waiting)
	return q, nil
}

// Needs is what reading n new accounts within a day takes.
func Needs(a config.Admission, n int64) Capacity {
	req := n * int64(a.RequestsPerAccount)
	ceil := func(x, per float64) int64 {
		if per <= 0 || x <= 0 {
			return 0
		}
		return int64(math.Ceil(x / per))
	}
	return Capacity{
		PerHour:     ceil(float64(n), 24),
		Requests:    req,
		IPs:         ceil(float64(req), float64(a.IPRequestsPerDay)),
		SignerPages: ceil(float64(req)*a.SignedShare, float64(a.PageSignsPerDay)),
	}
}

// Admit lets the queue's next accounts start: as many as the hour's rate
// leaves after those admitted in the last hour (all of them when
// unlimited, up to batch a call), the owner's first, then by priority and
// age. Accounts admitted at registration (their list was read already, so
// they need no backfill) do not count: only paced ones do. Their first check is due at once. It returns how many it admitted.
func Admit(ctx context.Context, db *pgxpool.Pool, cfg *config.Config, st *ratelimit.BudgetState, now time.Time, batch int) (int64, error) {
	perHour, unlimited := Rate(cfg, st, now)
	slots := int64(batch)
	if !unlimited {
		var last int64
		if err := db.QueryRow(ctx, `SELECT count(*) FROM account_schedule WHERE paced AND admitted_at > $1::timestamptz - interval '1 hour'
			AND admitted_at <= $1`, now).Scan(&last); err != nil {
			return 0, err
		}
		slots = min(slots, int64(perHour)-last)
	}
	if slots <= 0 {
		return 0, nil
	}
	tag, err := db.Exec(ctx, `UPDATE account_schedule s SET admitted_at = $2, paced = true, next_run_at = least(s.next_run_at, $2)
		FROM (SELECT account_ref FROM account_schedule WHERE admitted_at IS NULL AND enabled
			ORDER BY admit_priority DESC, created_at, account_ref LIMIT $1 FOR UPDATE SKIP LOCKED) d
		WHERE s.account_ref = d.account_ref`, slots, now)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
