package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
)

// Discovery is the leader's discovery loop (docs/tiktok/discovery-plan.md
// §4): it moves the discovered accounts that went quiet to the dormant list,
// registers the found accounts their entry check qualified, sends found
// accounts to their entry checks, and puts back the checks whose job ended
// without a decision. It does nothing while tt.discovery.enabled is off.
type Discovery struct {
	D        *discovery.Discoverer
	Dispatch *dispatch.Dispatcher
	// RecoverAfter: a check whose job ended undecided this long after it
	// was queued goes back to the queue (default 15 minutes).
	RecoverAfter time.Duration

	client uuid.UUID
}

// Tick runs one round.
func (s *Discovery) Tick(ctx context.Context) error {
	cfg := s.D.Cfg
	if !cfg.TT.Discovery.Enabled {
		return nil
	}
	if s.client == uuid.Nil {
		id, err := discovery.EnsureClient(ctx, s.D.DB, cfg.TT.Discovery.Client)
		if err != nil {
			return err
		}
		s.client = id
	}
	batch := cfg.Scheduler.PlannerBatch
	if n, err := s.D.Quiet(ctx, batch); err != nil {
		return err
	} else if n > 0 {
		slog.Info("discovery: quiet accounts moved to the dormant list", "accounts", n)
	}
	if n, err := s.D.Admit(ctx, s.client, batch); err != nil {
		return err
	} else if n > 0 {
		slog.Info("discovery: accounts added", "accounts", n)
	}
	after := s.RecoverAfter
	if after <= 0 {
		after = 15 * time.Minute
	}
	if _, err := s.D.Recover(ctx, after); err != nil {
		return err
	}
	if err := s.start(ctx); err != nil {
		return err
	}
	counts, err := discovery.Counts(ctx, s.D.DB)
	if err != nil {
		return err
	}
	for _, st := range []string{discovery.StatePending, discovery.StateChecking, discovery.StateQualified, discovery.StateAdded,
		discovery.StateNotIraqi, discovery.StateDormant, discovery.StateBlocked} {
		metrics.DiscoveryCandidates.WithLabelValues(st).Set(float64(counts[st]))
	}
	return nil
}

// start sends pending found accounts to their entry checks, up to the
// batch and the fleet-wide cap of running checks, in the backfill lane
// (which the budget pauses: discovery waits, monitoring does not).
func (s *Discovery) start(ctx context.Context) error {
	ec := s.D.Cfg.TT.Discovery.Entry
	running, err := s.D.Running(ctx)
	if err != nil {
		return err
	}
	claimed, err := s.D.Claim(ctx, min(ec.Batch, ec.MaxRunning-running))
	if err != nil {
		return err
	}
	for _, c := range claimed {
		// The job's id is recorded first: the job may end before Create
		// returns.
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		if err := s.D.Started(ctx, c.Ref, id); err != nil {
			return err
		}
		client := s.client
		if _, _, err := s.Dispatch.Create(ctx, dispatch.Request{ClientID: &client, Kind: dispatch.KindEntry, JobID: id,
			Lane: tkstaskv1.Lane_LANE_P2, AccountRef: c.Ref, UserID: c.UserID, Username: c.Username}, 0); err != nil {
			slog.Warn("discovery: entry check not started", "account", c.Ref, "err", err)
			if err := s.D.Unclaim(ctx, c.Ref); err != nil {
				return err
			}
		}
	}
	return nil
}

// Entry is the finalizer's hook for entry jobs.
func (s *Discovery) Entry(ctx context.Context, job uuid.UUID, final string, counters map[string]int64) error {
	_, err := s.D.Decide(ctx, discovery.Entry{JobID: job, Final: final, Counters: counters})
	return err
}
