// Command fbs-scheduler runs the leader-elected control loops.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/leader"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
)

const leaderKey = 4_300_100

func main() {
	fs := flag.NewFlagSet("fbs-scheduler", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("fbs-scheduler", cfg.Log.Level, cfg.Log.Format)
	if err := runScheduler(context.Background(), cfg); err != nil {
		slog.Error("fbs-scheduler exited", "err", err)
		os.Exit(1)
	}
}

func runScheduler(ctx context.Context, cfg *config.Config) error {
	db, err := pg.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
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
	s3, err := s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, cfg.S3.UseSSL, cfg.S3.MediaBucket)
	if err != nil {
		return err
	}
	tracker := jobs.New(rdb)
	disp := &dispatch.Dispatcher{DB: db, Jobs: tracker, Producer: producer, BaseURL: cfg.FB.BaseURL}
	adaptive := &scheduler.Adaptive{DB: db, Cfg: cfg}
	fin := &scheduler.Finalizer{DB: db, Jobs: tracker, Producer: producer, Timeout: cfg.Scheduler.FinalizeTimeout, Adaptive: adaptive,
		Deletions: scheduler.DeletionsFrom(cfg)}
	planner := &scheduler.Planner{DB: db, Dispatch: disp, Cfg: cfg, Adaptive: adaptive}
	reaper := &scheduler.Reaper{DB: db, Jobs: tracker, Producer: producer, After: cfg.Scheduler.ReaperAfter}
	maint := &scheduler.Maintenance{DB: db, Cfg: cfg}
	eraser := &gdpr.Eraser{DB: db, S3: s3, Producer: producer}

	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	leaderLoops := []run.Func{
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "tier-gauges", 5*time.Minute, planner.TierGauges)
		},
		func(ctx context.Context) error {
			lag := &scheduler.LagGauges{Admin: kadm.NewClient(producer.Client()), Prefix: cfg.Kafka.TopicPrefix}
			return scheduler.Every(ctx, "consumer-lag", 30*time.Second, lag.Tick)
		},
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
		leaderLoops = append(leaderLoops, func(ctx context.Context) error {
			return scheduler.Every(ctx, "budget", b.Interval, budget.Tick)
		})
	}
	if cfg.FB.AIMD.Enabled {
		aimd := ratelimit.NewController(rdb, cfg.Kafka.TopicPrefix, cfg.FB.AIMD, cfg.FB.GlobalRPS)
		leaderLoops = append(leaderLoops, func(ctx context.Context) error {
			return scheduler.Every(ctx, "aimd", cfg.FB.AIMD.Interval, aimd.Tick)
		})
	}
	return run.Group(ctx, ops.Run, func(ctx context.Context) error {
		return leader.Run(ctx, db, leaderKey, func(ctx context.Context) error {
			// Leader-only gauges are read joined on igs_scheduler_leader, so a
			// former leader's last values do not linger in dashboards.
			metrics.SchedulerLeader.Set(1)
			defer func() {
				metrics.SchedulerLeader.Set(0)
				metrics.ConsumerLag.Reset()
			}()
			return run.Group(ctx, append(leaderLoops,
				func(ctx context.Context) error { return scheduler.Every(ctx, "maintenance", time.Hour, maint.Tick) },
				func(ctx context.Context) error {
					return scheduler.Every(ctx, "planner", cfg.Scheduler.PlannerInterval, planner.Tick)
				},
				func(ctx context.Context) error {
					return scheduler.Every(ctx, "finalizer", cfg.Scheduler.FinalizerInterval, fin.Tick)
				},
				func(ctx context.Context) error { return scheduler.Every(ctx, "reaper", time.Minute, reaper.Tick) },
				func(ctx context.Context) error { return scheduler.Every(ctx, "erasure", 10*time.Second, eraser.Tick) },
			)...)
		})
	})
}
