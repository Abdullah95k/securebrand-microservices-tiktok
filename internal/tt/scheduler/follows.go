package scheduler

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
)

// follows is discovery part D's round (docs/tiktok/discovery-plan.md §9):
// it gives newly monitored accounts their lists, moves on without a call
// the lists TikTok would not serve (a hidden following list, an account
// that is private or gone), and sends due lists to be read, in the
// backfill lane (which the budget pauses), up to the batch, the reads
// running fleet-wide and the day's pages.
func (s *Discovery) follows(ctx context.Context) error {
	dirs := s.D.ListDirections()
	if len(dirs) == 0 {
		return nil
	}
	fl := s.D.Cfg.TT.Discovery.Follows
	if _, err := s.D.SeedLists(ctx, dirs, s.D.Cfg.Scheduler.PlannerBatch); err != nil {
		return err
	}
	skipped, err := s.D.SkipLists(ctx, dirs)
	if err != nil {
		return err
	}
	for st, n := range skipped {
		slog.Debug("discovery: lists skipped without a call", "state", st, "lists", n)
	}
	running, err := s.D.ListsRunning(ctx)
	if err != nil {
		return err
	}
	n := min(fl.Batch, fl.MaxRunning-running)
	pages := fl.MaxPages
	if fl.DayPages > 0 {
		used, err := s.D.ListPagesToday(ctx)
		if err != nil {
			return err
		}
		left := fl.DayPages - used
		// Each read may take MaxPages; the last ones of the day fewer.
		n = min(n, (left+fl.MaxPages-1)/max(fl.MaxPages, 1))
		pages = min(pages, left)
	}
	if n > 0 && pages > 0 {
		lists, err := s.D.ClaimLists(ctx, dirs, n)
		if err != nil {
			return err
		}
		for _, l := range lists {
			if err := s.startList(ctx, l, pages); err != nil {
				return err
			}
		}
	}
	now := time.Now()
	if s.D.Now != nil {
		now = s.D.Now()
	}
	states, due, err := discovery.ListCounts(ctx, s.D.DB, now)
	if err != nil {
		return err
	}
	for _, dir := range discovery.PartD {
		metrics.FollowListsDue.WithLabelValues(dir).Set(float64(due[dir]))
		for _, st := range []string{"new", "ok", "hidden", "gone", "refused"} {
			metrics.FollowListState.WithLabelValues(dir, st).Set(float64(states[[2]string{dir, st}]))
		}
	}
	return nil
}

// startList dispatches one list's read.
func (s *Discovery) startList(ctx context.Context, l discovery.DueList, pages int) error {
	dir := ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWER
	if l.Direction == discovery.SourceFollowing {
		dir = ttv1.FollowDirection_FOLLOW_DIRECTION_FOLLOWING
	}
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if err := s.D.ListStarted(ctx, l.UserID, l.Direction, id); err != nil {
		return err
	}
	client := s.client
	scope := &tkstaskv1.FollowScope{UserId: l.UserID, SecUid: l.SecUID, Username: l.Username, Direction: dir,
		DeepCursor: l.DeepCursor, DeepDone: l.DeepDone, ReadBefore: l.ReadBefore, MaxPages: int32(pages)}
	if _, _, err := s.Dispatch.Create(ctx, dispatch.Request{ClientID: &client, Kind: dispatch.KindFollows, JobID: id,
		Lane: tkstaskv1.Lane_LANE_P2, AccountRef: "id:" + strconv.FormatInt(l.UserID, 10), Follows: scope}, 0); err != nil {
		slog.Warn("discovery: list read not started", "user_id", l.UserID, "direction", l.Direction, "err", err)
		return s.D.ListUnclaim(ctx, l.UserID, l.Direction)
	}
	return nil
}
