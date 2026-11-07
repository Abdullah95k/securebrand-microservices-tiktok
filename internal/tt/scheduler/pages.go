package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
)

// pages is discovery part C's round (docs/tiktok/discovery-plan.md §8): it
// brings the targets of the sources switched on up to date (the keywords,
// their hashtags, the feed and monitored accounts' new posts every tick;
// the hashtags and sounds monitored accounts use every
// tt.discovery.pages.refresh) and sends due targets to be read, in the
// backfill lane (which the budget pauses), up to the batch, the reads
// running fleet-wide and the day's pages. A read bootstraps its identity on
// a monitored account's profile page, so nothing is read before tks
// monitors one.
func (s *Discovery) pages(ctx context.Context) error {
	sources := s.D.PageSources()
	if len(sources) == 0 {
		return nil
	}
	cfg := s.D.Cfg.TT.Discovery
	now := time.Now()
	if s.D.Now != nil {
		now = s.D.Now()
	}
	refresh := s.refreshed.IsZero() || now.Sub(s.refreshed) >= cfg.Pages.Refresh
	if err := s.D.SeedTargets(ctx, sources, refresh); err != nil {
		return err
	}
	if refresh {
		s.refreshed = now
	}
	running, err := s.D.TargetsRunning(ctx)
	if err != nil {
		return err
	}
	n := min(cfg.Pages.Batch, cfg.Pages.MaxRunning-running)
	left := -1 // no day cap
	if cfg.Pages.DayPages > 0 {
		used, err := s.D.TargetPagesToday(ctx)
		if err != nil {
			return err
		}
		left = max(cfg.Pages.DayPages-used, 0)
	}
	if n > 0 && left != 0 {
		boot, err := s.D.BootUsernames(ctx, 100)
		if err != nil {
			return err
		}
		if len(boot) > 0 {
			targets, err := s.D.ClaimTargets(ctx, sources, n)
			if err != nil {
				return err
			}
			for i, t := range targets {
				pages := maxPages(cfg, t.Source)
				if left >= 0 {
					if left == 0 {
						if err := s.D.TargetUnclaim(ctx, t.Source, t.Key); err != nil {
							return err
						}
						continue
					}
					pages = min(pages, left)
					left -= pages
				}
				if err := s.startTarget(ctx, t, boot[(s.boots+i)%len(boot)], pages); err != nil {
					return err
				}
			}
			s.boots += len(targets)
		}
	}
	states, due, err := discovery.TargetCounts(ctx, s.D.DB, now)
	if err != nil {
		return err
	}
	for _, src := range discovery.PartC {
		metrics.DiscoverDue.WithLabelValues(src).Set(float64(due[src]))
		for _, st := range []string{"new", "ok", "gone", "refused"} {
			metrics.DiscoverTargets.WithLabelValues(src, st).Set(float64(states[[2]string{src, st}]))
		}
	}
	return nil
}

// maxPages is a source's pages a read (the feed's reads).
func maxPages(cfg config.Discovery, source string) int {
	switch source {
	case discovery.SourceSearch:
		return cfg.Search.MaxPages
	case discovery.SourceHashtag:
		return cfg.Hashtags.MaxPages
	case discovery.SourceSound:
		return cfg.Sounds.MaxPages
	case discovery.SourceFeed:
		return cfg.Feed.Reads
	}
	return 1 // related: one page
}

// startTarget dispatches one target's read.
func (s *Discovery) startTarget(ctx context.Context, t discovery.DueTarget, boot string, pages int) error {
	cfg := s.D.Cfg.TT.Discovery
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if err := s.D.TargetStarted(ctx, t.Source, t.Key, id); err != nil {
		return err
	}
	scope := &tkstaskv1.DiscoverScope{Source: t.Source, Key: t.Key, Name: t.Name, MaxPages: int32(pages), BootUsername: boot,
		ItemId: t.ItemID, PostUserId: t.PostUserID}
	switch t.Source {
	case discovery.SourceSearch:
		scope.Tier = cfg.Search.Tier
	case discovery.SourceFeed:
		scope.Tier = cfg.Feed.Tier
	case discovery.SourceRelated:
		// The post's own author's page, when tks knows its username.
		if t.PostUsername != "" {
			scope.BootUsername = t.PostUsername
		}
	}
	client := s.client
	if _, _, err := s.Dispatch.Create(ctx, dispatch.Request{ClientID: &client, Kind: dispatch.KindDiscover, JobID: id,
		Lane: tkstaskv1.Lane_LANE_P2, Discover: scope}, 0); err != nil {
		slog.Warn("discovery: page read not started", "source", t.Source, "err", err)
		return s.D.TargetUnclaim(ctx, t.Source, t.Key)
	}
	return nil
}
