// Package app wires service components from config. Binaries and the
// end-to-end tests share it, so tests exercise the real wiring.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/lanes"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/rawsink"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/shadow"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/stage"
	fbclient "github.com/abdullah95k/securebrand-microservices-igs/internal/fb/client"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Crawler is a configured crawler process.
type Crawler struct {
	Deps        *stage.Deps
	Worker      *stage.Worker
	Registry    *registry.DB
	Suppression *gdpr.Suppression
	Components  []run.Func
	// Lanes are the backfill (p2) lanes of every stage, which the traffic
	// budget pauses.
	Lanes []laneRef
}

type laneRef struct {
	s    *lanes.Scheduler
	lane int // index of the p2 lane in s
	max  int // its configured cap
}

// CrawlerOptions customise construction (tests).
type CrawlerOptions struct {
	Stages      []string
	DoerFactory session.DoerFactory // nil = fingerprinted tls-client
	RawS3       *minio.Client       // nil disables raw capture
	// DrainTimeout bounds how long in-flight tasks may run at shutdown
	// before they are abandoned (default 75s).
	DrainTimeout time.Duration
}

// NewCrawler builds the crawler: identity pool, GraphQL client, stages,
// one consumer per stage lane and the retry relay.
func NewCrawler(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, rdb *redis.Client, producer *kafka.Producer, o CrawlerOptions) (*Crawler, error) {
	seedBytes, err := configs.FS.ReadFile(cfg.FB.RegistrySeed)
	if err != nil {
		return nil, fmt.Errorf("registry seed: %w", err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, err
	}
	reg, err := registry.NewDB(ctx, db, seed)
	if err != nil {
		return nil, err
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, err
	}
	proxies, err := proxy.New(cfg.FB.Proxy)
	if err != nil {
		return nil, err
	}
	slots := cfg.Crawler.Slots
	if slots <= 0 {
		for _, v := range cfg.Crawler.Lanes {
			slots += v
		}
	}
	pool, err := session.NewPool(session.PoolConfig{
		BaseURL: cfg.FB.BaseURL, Profile: cfg.FB.FingerprintProfile, Timeout: cfg.FB.RequestTimeout,
		IdentityRPS: cfg.FB.IdentityRPS, IdentityBurst: cfg.FB.IdentityBurst,
		MaxRequestsMin: cfg.FB.IdentityMaxRequestsMin, MaxRequestsMax: cfg.FB.IdentityMaxRequestsMax,
		MaxPerTier: max(16, slots*len(cfg.Crawler.Stages)*13/10),
	}, proxies, fps, o.DoerFactory)
	if err != nil {
		return nil, err
	}
	rules := detect.DefaultRules()
	gql := &graphql.Client{
		Registry: reg, Rules: rules, Limiter: ratelimit.NewRedis(rdb),
		PageRPS: cfg.FB.PageRPS, PageRPSFast: cfg.FB.PageRPSP0, GlobalRPS: cfg.FB.GlobalRPS,
		Builder:  fbclient.Builder{AcceptLanguage: cfg.FB.AcceptLanguage, Locale: cfg.FB.Locale},
		Breakers: breaker.New(cfg.FB.QueryBreaker),
	}
	gql.Breakers.OnChange = func(key string, from, to breaker.State) {
		metrics.BreakerState.WithLabelValues("query:" + key).Set(float64(to))
		slog.Warn("graphql: query breaker", "query", key, "from", from.String(), "to", to.String())
	}
	supp := gdpr.NewSuppression(db)
	loader := &session.Loader{Rules: rules, AcceptLanguage: cfg.FB.AcceptLanguage, Hooks: fbclient.Hooks(cfg.FB.AcceptLanguage)}
	if cfg.FB.Browser.Enabled {
		b, err := browser.NewBootstrapper(cfg.FB.Browser, cfg.FB.AcceptLanguage, fbclient.Hooks(cfg.FB.AcceptLanguage))
		if err != nil {
			return nil, fmt.Errorf("fb.browser is enabled: %w", err)
		}
		loader.Browser = b
	}
	deps := &stage.Deps{
		Cfg: cfg, Pool: pool, Proxies: proxies, Loader: loader,
		GQL: gql, Producer: producer, Jobs: jobs.New(rdb), Ckpt: checkpoint.New(rdb), DB: db, Suppression: supp,
		Emit: &emit.Emitter{Producer: producer, HMACKey: []byte(cfg.Privacy.AuthorHMACKey),
			Pseudonymize: cfg.Privacy.PIIMode == "pseudonymized", Suppression: supp},
	}
	worker := &stage.Worker{D: deps, MaxAttempts: uint32(cfg.Crawler.MaxAttempts), MaxTotalAttempts: uint32(cfg.Crawler.MaxTotalAttempts),
		LeaseTTL: cfg.Crawler.LeaseTTL,
		Stages: map[taskv1.Kind]stage.Stage{
			taskv1.Kind_KIND_PAGE:   &stage.PageStage{D: deps},
			taskv1.Kind_KIND_POST:   &stage.PostStage{D: deps},
			taskv1.Kind_KIND_THREAD: &stage.ThreadStage{D: deps},
		}}
	c := &Crawler{Deps: deps, Worker: worker, Registry: reg, Suppression: supp}
	c.Components = []run.Func{
		func(ctx context.Context) error { return reg.Run(ctx, cfg.FB.RegistryRefresh) },
		func(ctx context.Context) error { return supp.Run(ctx, cfg.Crawler.SuppressionRefresh) },
	}
	// The reporter feeds both AIMD (block rate) and the traffic budget.
	if cfg.FB.AIMD.Enabled || cfg.Scheduler.Budget.Enabled {
		rep := ratelimit.NewReporter(rdb, cfg.Kafka.TopicPrefix)
		gql.Outcomes = rep
		c.Components = append(c.Components, rep.Run)
	}
	if cfg.FB.AIMD.Enabled {
		ceil := ratelimit.NewCeiling(rdb, cfg.Kafka.TopicPrefix, cfg.FB.GlobalRPS)
		gql.Ceiling = ceil
		c.Components = append(c.Components, ceil.Run)
	}
	if o.RawS3 != nil && cfg.FB.CaptureRaw != "none" {
		sink := rawsink.New(o.RawS3, cfg.S3.RawBucket)
		gql.Raw, gql.CaptureAll = sink, cfg.FB.CaptureRaw == "all"
		c.Components = append(c.Components, func(ctx context.Context) error { return sink.Run(ctx, time.Minute) })
	}
	deps.Shadow = shadow.Configured(cfg.FB.ShadowRate, gql.Raw)
	kcfg, err := KafkaClient(cfg, "-crawler")
	if err != nil {
		return nil, err
	}
	stages := o.Stages
	if len(stages) == 0 {
		stages = cfg.Crawler.Stages
	}
	drain := o.DrainTimeout
	if drain <= 0 {
		drain = 75 * time.Second
	}
	kinds := map[string]taskv1.Kind{"page": taskv1.Kind_KIND_PAGE, "post": taskv1.Kind_KIND_POST, "thread": taskv1.Kind_KIND_THREAD}
	for _, s := range stages {
		s = strings.TrimSpace(s)
		if s == "retry" {
			relay := &stage.RetryRelay{Producer: producer}
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "igs-retry-relay",
				Topics: topics.RetryTopics, MaxInflight: 64, MaxInflightPerPartition: 1, DrainTimeout: 5 * time.Second}, relay.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
			continue
		}
		kind, ok := kinds[s]
		if !ok {
			return nil, fmt.Errorf("unknown stage %q", s)
		}
		sched := laneScheduler(cfg.Crawler, s)
		if sched != nil {
			c.Lanes = append(c.Lanes, laneRef{s: sched, lane: 2, max: cfg.Crawler.Lanes["p2"]})
		}
		for i, lane := range []taskv1.Lane{taskv1.Lane_LANE_P0, taskv1.Lane_LANE_P1, taskv1.Lane_LANE_P2} {
			n := cfg.Crawler.Lanes[topics.LaneName(lane)]
			if n <= 0 {
				continue
			}
			var shared kafka.Slots
			switch {
			case sched != nil:
				shared = lanes.Slots{S: sched, Lane: i}
			case lane == taskv1.Lane_LANE_P2:
				// Fixed slots: backfill still gets a scheduler of its own so
				// the budget can pause it.
				own := lanes.New(n, lanes.Lane{Name: "p2", Max: n})
				c.Lanes = append(c.Lanes, laneRef{s: own, lane: 0, max: n})
				shared = lanes.Slots{S: own, Lane: 0}
			}
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
				ClientConfig: kcfg, Group: "igs-crawler-" + s + "-" + topics.LaneName(lane), Topics: []string{topics.Task(kind, lane)},
				MaxInflight: n, MaxInflightPerPartition: cfg.Crawler.MaxInflightPerPartition, DrainTimeout: drain,
				Slots: shared,
			}, worker.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
		}
		slog.Info("stage enabled", "stage", s)
	}
	if cfg.Scheduler.Budget.Enabled && len(c.Lanes) > 0 {
		// The daily budget pauses backfill by closing the p2 lane.
		gate := ratelimit.NewBudgetGate(rdb, cfg.Kafka.TopicPrefix)
		gate.OnChange = func(st ratelimit.BudgetState) {
			for _, l := range c.Lanes {
				n := l.max
				if st.PauseP2 {
					n = 0
				}
				l.s.SetMax(l.lane, n)
			}
			slog.Warn("budget: backfill lane", "paused", st.PauseP2, "why", st.Explanation)
		}
		c.Components = append(c.Components, gate.Run)
	}
	return c, nil
}

// laneScheduler shares a stage's slots between its lanes, or returns nil
// when crawler.slots is 0 (fixed slots per lane).
func laneScheduler(c config.Crawler, stage string) *lanes.Scheduler {
	if c.Slots <= 0 {
		return nil
	}
	var ls []lanes.Lane
	for _, name := range []string{"p0", "p1", "p2"} {
		ls = append(ls, lanes.Lane{Name: name, Max: c.Lanes[name], Min: c.LaneMin})
	}
	s := lanes.New(c.Slots, ls...)
	s.OnChange = func(lane string, used, waiting int) {
		metrics.LaneSlots.WithLabelValues(stage, lane, "used").Set(float64(used))
		metrics.LaneSlots.WithLabelValues(stage, lane, "waiting").Set(float64(waiting))
	}
	return s
}
