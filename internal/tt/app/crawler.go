// Package app wires tks's components from the configuration (docs/tiktok
// ADR 0006 §2): the crawler's stages on tks.task.*, the browser signer and
// the persister on tt.*.v1. Binaries and the end-to-end tests share it, so
// the tests exercise the real wiring. It is internal/app's counterpart for
// TikTok and imports no Facebook code.
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

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/lanes"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/rawsink"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/media"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/connect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttdetect "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/sign"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/stage"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// Stages are the crawler's stages: the account stage (post lists and the
// account), the post stage (comments), the thread stage (replies) and the
// retry relay.
var Stages = []string{"account", "post", "thread", "retry"}

// Crawler is a configured tks crawler process.
type Crawler struct {
	Deps        *stage.Deps
	Worker      *stage.Worker
	Registry    *registry.DB
	Suppression *gdpr.Suppression
	// Signer is the browser signer's pool (nil with tt.signer.mode none or
	// a test signer).
	Signer     *sign.Pool
	Components []run.Func
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
	// Stages to run (default Stages).
	Stages      []string
	DoerFactory session.DoerFactory // nil = fingerprinted tls-client
	RawS3       *minio.Client       // nil disables raw capture
	// Signer replaces the browser signer's pages (tests; tt.signer.mode
	// still decides whether calls are signed).
	Signer ttclient.URLSigner
	// DrainTimeout bounds how long in-flight tasks may run at shutdown
	// before they are abandoned (default 75s).
	DrainTimeout time.Duration
}

// NewCrawler builds the crawler on schema tt (db: a pool from ttstore.Open):
// identity pool, the TikTok client with its signer, the stages, one
// consumer per stage lane and the retry relays.
func NewCrawler(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, rdb *redis.Client, producer *kafka.Producer, o CrawlerOptions) (*Crawler, error) {
	tc := cfg.TT
	seedBytes, err := configs.FS.ReadFile(tc.OperationSeed)
	if err != nil {
		return nil, fmt.Errorf("operation seed: %w", err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, err
	}
	reg, err := registry.NewDBFrom(ctx, db, ttstore.Registry, seed)
	if err != nil {
		return nil, err
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, err
	}
	proxies, err := proxy.New(tc.Proxy)
	if err != nil {
		return nil, err
	}
	stages := o.Stages
	if len(stages) == 0 {
		stages = Stages
	}
	slots := cfg.Crawler.Slots
	if slots <= 0 {
		for _, v := range cfg.Crawler.Lanes {
			slots += v
		}
	}
	pool, err := session.NewPool(session.PoolConfig{
		BaseURL: tc.BaseURL, Profile: tc.FingerprintProfile, Profiles: tc.FingerprintProfiles,
		Timeout:     tc.RequestTimeout,
		IdentityRPS: tc.IdentityRPS, IdentityBurst: tc.IdentityBurst,
		MaxRequestsMin: tc.IdentityMaxRequestsMin, MaxRequestsMax: tc.IdentityMaxRequestsMax,
		MaxPerTier: max(16, slots*len(stages)*13/10),
	}, proxies, fps, o.DoerFactory)
	if err != nil {
		return nil, err
	}
	rules, err := ttdetect.Rules().Apply(detect.Overrides(tc.Detect))
	if err != nil {
		return nil, fmt.Errorf("tt.detect: %w", err)
	}
	client := &graphql.Client{
		Registry: reg, Rules: rules, Limiter: ratelimit.NewRedis(rdb),
		PageRPS: tc.AccountRPS, PageRPSFast: tc.AccountRPSP0, GlobalRPS: tc.GlobalRPS,
		Builder:  ttclient.Builder{AcceptLanguage: tc.AcceptLanguage},
		Decode:   ttclient.Decode,
		Breakers: breaker.New(tc.QueryBreaker),
	}
	client.Breakers.OnChange = func(key string, from, to breaker.State) {
		metrics.BreakerState.WithLabelValues("query:" + key).Set(float64(to))
		slog.Warn("tt: operation breaker", "operation", key, "from", from.String(), "to", to.String())
	}
	c := &Crawler{Registry: reg}
	// Comments and replies are signed by TikTok's own script in a browser
	// page (build brief D11); everything else goes out as built.
	signer := o.Signer
	if signer == nil && tc.Signer.Mode == config.SignerBrowser {
		c.Signer = sign.New(tc.Signer, sign.BrowserOpener(tc.Browser.Binary, tc.Browser.NoSandbox, tc.AcceptLanguage, tc.Signer.PageTimeout))
		signer = c.Signer
	}
	if signer != nil {
		client.Signer = ttclient.SignerFor(tc.Signer.Mode, signer)
	}
	supp := gdpr.NewSuppression(db).ForAccounts()
	loader := &session.Loader{Rules: rules, AcceptLanguage: tc.AcceptLanguage, Hooks: ttclient.Hooks(), Gate: proxies}
	if tc.Browser.Enabled {
		b, err := browser.NewBootstrapper(tc.Browser, tc.AcceptLanguage, ttclient.Hooks())
		if err != nil {
			return nil, fmt.Errorf("tt.browser is enabled: %w", err)
		}
		b.Rules = rules
		loader.Browser = b
	}
	deps := &stage.Deps{
		Cfg: cfg, Pool: pool, Proxies: proxies, Loader: loader, Client: client,
		Producer: producer, Jobs: jobs.New(rdb), Ckpt: checkpoint.New(rdb), DB: db, Suppression: supp,
		Emit: &emit.Emitter{Producer: producer, HMACKey: []byte(cfg.Privacy.AuthorHMACKey),
			Pseudonymize: cfg.Privacy.PIIMode == "pseudonymized", Suppression: supp},
	}
	worker := &stage.Worker{D: deps, MaxAttempts: uint32(cfg.Crawler.MaxAttempts), MaxTotalAttempts: uint32(cfg.Crawler.MaxTotalAttempts),
		MaxWaits: uint32(cfg.Crawler.MaxWaits), LeaseTTL: cfg.Crawler.LeaseTTL,
		Stages: map[tkstaskv1.Kind]stage.Stage{
			tkstaskv1.Kind_KIND_ACCOUNT: &stage.AccountStage{D: deps},
			tkstaskv1.Kind_KIND_POST:    &stage.PostStage{D: deps},
			tkstaskv1.Kind_KIND_THREAD:  &stage.ThreadStage{D: deps},
		}}
	c.Deps, c.Worker, c.Suppression = deps, worker, supp
	c.Components = []run.Func{
		func(ctx context.Context) error { return reg.Run(ctx, tc.RegistryRefresh) },
		func(ctx context.Context) error { return supp.Run(ctx, cfg.Crawler.SuppressionRefresh) },
	}
	if c.Signer != nil {
		// The pages close with the process.
		c.Components = append(c.Components, func(ctx context.Context) error {
			<-ctx.Done()
			c.Signer.Close()
			return nil
		})
	}
	// The reporter feeds both AIMD (block rate) and the traffic budget.
	if tc.AIMD.Enabled || cfg.Scheduler.Budget.Enabled {
		rep := ratelimit.NewReporter(rdb, cfg.Kafka.TopicPrefix)
		client.Outcomes = rep
		c.Components = append(c.Components, rep.Run)
	}
	if tc.AIMD.Enabled {
		ceil := ratelimit.NewCeiling(rdb, cfg.Kafka.TopicPrefix, tc.GlobalRPS)
		client.Ceiling = ceil
		c.Components = append(c.Components, ceil.Run)
	}
	if o.RawS3 != nil && tc.CaptureRaw != "none" {
		// Raw answers hold comment text and authors: they expire after
		// privacy.retention.raw_payload_days by a bucket lifecycle rule.
		if days := cfg.Privacy.Retention.RawPayloadDays; days > 0 {
			media.RawLifecycle(ctx, o.RawS3, cfg.S3.RawBucket, days)
		}
		sink := rawsink.New(o.RawS3, cfg.S3.RawBucket)
		client.Raw, client.CaptureAll = sink, tc.CaptureRaw == "all"
		c.Components = append(c.Components, func(ctx context.Context) error { return sink.Run(ctx, time.Minute) })
	}
	kcfg, err := connect.Kafka(cfg, "-crawler")
	if err != nil {
		return nil, err
	}
	if err := c.consumers(cfg, kcfg, producer, stages, o.DrainTimeout); err != nil {
		return nil, err
	}
	if cfg.Scheduler.Budget.Enabled && len(c.Lanes) > 0 {
		// The daily budget pauses backfills by closing the p2 lanes.
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

// ConsumerGroup is a stage lane's consumer group, apart from the copied
// Facebook pipeline's (tks-crawler-<stage>-<lane>).
func ConsumerGroup(stage string, lane tkstaskv1.Lane) string {
	return "tks-tt-crawler-" + stage + "-" + topics.LaneName(lane)
}

func (c *Crawler) consumers(cfg *config.Config, kcfg kafka.ClientConfig, producer *kafka.Producer, stages []string, drain time.Duration) error {
	if drain <= 0 {
		drain = 75 * time.Second
	}
	kinds := map[string]tkstaskv1.Kind{"account": tkstaskv1.Kind_KIND_ACCOUNT, "post": tkstaskv1.Kind_KIND_POST,
		"thread": tkstaskv1.Kind_KIND_THREAD}
	for _, s := range stages {
		s = strings.TrimSpace(s)
		if s == "retry" {
			relays, err := stage.RetryRelays(kcfg, producer)
			if err != nil {
				return err
			}
			for _, r := range relays {
				c.Components = append(c.Components, r.Run)
			}
			continue
		}
		kind, ok := kinds[s]
		if !ok {
			return fmt.Errorf("unknown stage %q (want %s)", s, strings.Join(Stages, ", "))
		}
		sched := laneScheduler(cfg.Crawler, s)
		if sched != nil {
			c.Lanes = append(c.Lanes, laneRef{s: sched, lane: 2, max: cfg.Crawler.Lanes["p2"]})
		}
		for i, lane := range []tkstaskv1.Lane{tkstaskv1.Lane_LANE_P0, tkstaskv1.Lane_LANE_P1, tkstaskv1.Lane_LANE_P2} {
			n := cfg.Crawler.Lanes[topics.LaneName(lane)]
			if n <= 0 {
				continue
			}
			var shared kafka.Slots
			switch {
			case sched != nil:
				shared = lanes.Slots{S: sched, Lane: i}
			case lane == tkstaskv1.Lane_LANE_P2:
				// Fixed slots: backfills still get a scheduler of their own so
				// the budget can pause them.
				own := lanes.New(n, lanes.Lane{Name: "p2", Max: n})
				c.Lanes = append(c.Lanes, laneRef{s: own, lane: 0, max: n})
				shared = lanes.Slots{S: own, Lane: 0}
			}
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
				ClientConfig: kcfg, Group: ConsumerGroup(s, lane), Topics: []string{topics.Task(kind, lane)},
				MaxInflight: n, MaxInflightPerPartition: cfg.Crawler.MaxInflightPerPartition, DrainTimeout: drain,
				Slots: shared,
			}, c.Worker.Handle)
			if err != nil {
				return err
			}
			c.Components = append(c.Components, cons.Run)
		}
		slog.Info("stage enabled", "stage", s)
	}
	return nil
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
