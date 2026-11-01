// Command tks-scheduler runs tks's leader-elected control loops (docs/tiktok
// ADR 0007 §12): the planner (each registered account's check every
// tt.freshness.list_interval, backfills and metrics walks), the finalizer,
// the reaper, partitions (and retention, off by default: ADR 0007 §14), the
// traffic budget and AIMD.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/leader"
	plog "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	sharedsched "github.com/abdullah95k/securebrand-microservices-tiktok/internal/scheduler"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/scheduler"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// leaderKey is tks's scheduler lock (ADR 0006 §5), taken in tks's own
// database.
const leaderKey = 4_400_100

func main() {
	fs := flag.NewFlagSet("tks-scheduler", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("tks-scheduler", cfg.Log.Level, cfg.Log.Format)
	if err := runScheduler(context.Background(), cfg); err != nil {
		slog.Error("tks-scheduler exited", "err", err)
		os.Exit(1)
	}
}

func runScheduler(ctx context.Context, cfg *config.Config) error {
	db, err := ttstore.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := ttstore.CheckSchema(ctx, db); err != nil {
		return err
	}
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	kcfg, err := app.KafkaClient(cfg, "-scheduler")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()

	tracker := jobs.New(rdb)
	disp := &dispatch.Dispatcher{DB: db, Jobs: tracker, Producer: producer}
	planner := &scheduler.Planner{DB: db, Dispatch: disp, Cfg: cfg}
	fin := &scheduler.Finalizer{DB: db, Jobs: tracker, Producer: producer, Cfg: cfg, Timeout: cfg.Scheduler.FinalizeTimeout}
	admin := kadm.NewClient(producer.Client())
	reaper := &scheduler.Reaper{DB: db, Jobs: tracker, Producer: producer, After: cfg.Scheduler.ReaperAfter, MaxAge: cfg.Scheduler.ReaperMaxAge,
		Backlog: func(ctx context.Context) (map[int16]int64, error) {
			return scheduler.TaskBacklog(ctx, admin, cfg.Kafka.TopicPrefix)
		}}
	maint := &scheduler.Maintenance{DB: db, Cfg: cfg}

	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	loops := []run.Func{
		func(ctx context.Context) error {
			return sharedsched.Every(ctx, "gauges", 5*time.Minute, planner.TierGauges)
		},
		func(ctx context.Context) error {
			lag := &sharedsched.LagGauges{Admin: admin, Prefix: cfg.Kafka.TopicPrefix}
			return sharedsched.Every(ctx, "consumer-lag", 30*time.Second, lag.Tick)
		},
		func(ctx context.Context) error { return sharedsched.Every(ctx, "maintenance", time.Hour, maint.Tick) },
		func(ctx context.Context) error {
			return sharedsched.Every(ctx, "planner", cfg.Scheduler.PlannerInterval, planner.Tick)
		},
		func(ctx context.Context) error {
			return sharedsched.Every(ctx, "finalizer", cfg.Scheduler.FinalizerInterval, fin.Tick)
		},
		func(ctx context.Context) error { return sharedsched.Every(ctx, "reaper", time.Minute, reaper.Tick) },
	}
	if b := cfg.Scheduler.Budget; b.Enabled && (b.RequestsPerDay > 0 || b.BytesPerDay > 0) {
		budget := ratelimit.NewBudgetController(rdb, cfg.Kafka.TopicPrefix, b)
		planner.Budget = func(ctx context.Context) ratelimit.BudgetState {
			st, err := ratelimit.LoadBudgetState(ctx, rdb, cfg.Kafka.TopicPrefix)
			if err != nil {
				slog.Warn("planner: budget state", "err", err)
			}
			return st
		}
		loops = append(loops, func(ctx context.Context) error {
			return sharedsched.Every(ctx, "budget", b.Interval, budget.Tick)
		})
	}
	if cfg.TT.AIMD.Enabled {
		aimd := ratelimit.NewController(rdb, cfg.Kafka.TopicPrefix, cfg.TT.AIMD, cfg.TT.GlobalRPS)
		loops = append(loops, func(ctx context.Context) error {
			return sharedsched.Every(ctx, "aimd", cfg.TT.AIMD.Interval, aimd.Tick)
		})
	}
	return run.Group(ctx, ops.Run, func(ctx context.Context) error {
		return leader.Run(ctx, db, leaderKey, func(ctx context.Context) error {
			// Leader-only gauges are read joined on tks_scheduler_leader, so a
			// former leader's last values do not linger in dashboards.
			metrics.SchedulerLeader.Set(1)
			defer func() {
				metrics.SchedulerLeader.Set(0)
				metrics.ConsumerLag.Reset()
			}()
			return run.Group(ctx, loops...)
		})
	})
}
