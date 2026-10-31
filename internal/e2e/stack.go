// Package e2e runs the whole fbs pipeline in-process against real
// Postgres, Kafka (Redpanda) and Valkey, an in-memory S3 and fakefb.
package e2e

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"gopkg.in/yaml.v3"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/media"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Stack is a running pipeline.
type Stack struct {
	Cfg      *config.Config
	DB       *pgxpool.Pool
	Redis    *redis.Client
	Producer *kafka.Producer
	S3       *minio.Client
	Fake     *fakefb.Server
	FakeURL  string
	API      *httptest.Server
	Crawler  *app.Crawler
	Janitor  *media.Janitor
	Eraser   *gdpr.Eraser
	Planner  *scheduler.Planner
	Adaptive *scheduler.Adaptive
	Dispatch *dispatch.Dispatcher

	mu      sync.Mutex
	groups  map[string]*group
	kcfg    kafka.ClientConfig
	copts   app.CrawlerOptions
	tracker *jobs.Tracker
}

// group is a set of components started and stopped together, so a test can
// restart one part of the pipeline.
type group struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Options tweak the stack.
type Options struct {
	Spec   fakefb.Spec
	Faults fakefb.Faults
	Config func(*config.Config)
	// CrawlerDrain overrides how long a stopping crawler lets in-flight
	// tasks run before abandoning them (a short one is a crash).
	CrawlerDrain time.Duration
}

// Start brings up the stack; it stops at test cleanup.
func Start(t *testing.T, o Options) *Stack {
	t.Helper()
	if os.Getenv("IGS_E2E_DEBUG") != "" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	brokers := testenv.Kafka(t)
	db := testenv.Postgres(t)
	rdb := testenv.Redis(t)
	ctx := context.Background()

	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	prefix := testenv.TopicPrefix()
	cfg.Kafka.Brokers, cfg.Kafka.TopicPrefix, cfg.Kafka.ProduceLinger = brokers, prefix, time.Millisecond
	cfg.FB.IdentityRPS, cfg.FB.IdentityBurst = 200, 20
	cfg.FB.PageRPS, cfg.FB.PageRPSP0, cfg.FB.GlobalRPS = 2000, 2000, 5000
	cfg.FB.RequestTimeout = 10 * time.Second
	cfg.Crawler.YieldPages = 2 // exercise continuations
	cfg.Crawler.TimelinePageSize = 3
	cfg.Crawler.Slots, cfg.Crawler.LaneMin = 24, 2
	cfg.Crawler.Lanes = map[string]int{"p0": 24, "p1": 16, "p2": 8}
	cfg.FB.AIMD.MinSamples = 10 // the test pages are small
	cfg.Crawler.SuppressionRefresh = time.Hour
	cfg.FB.CaptureRaw = "none"
	cfg.Scheduler.FinalizeTimeout = 30 * time.Second
	cfg.Media.TTL = 72 * time.Hour
	if o.Config != nil {
		o.Config(cfg)
	}

	// fakefb
	spec := o.Spec
	if spec.Pages == 0 {
		spec = fakefb.DefaultSpec()
	}
	seedBytes, _ := configs.FS.ReadFile("queries.seed.yaml")
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string][]string{}
	for _, q := range seed.Queries {
		docs[q.FriendlyName] = append(docs[q.FriendlyName], q.DocID)
	}
	fake := fakefb.NewServer(fakefb.NewWorld(spec), fakefb.Options{Docs: docs, InlineReply: true, Faults: o.Faults})
	fhs := httptest.NewServer(fake)
	t.Cleanup(fhs.Close)
	cfg.FB.BaseURL = fhs.URL

	// S3 (in memory)
	s3srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(s3srv.Close)
	u, _ := url.Parse(s3srv.URL)
	cfg.S3.Endpoint, cfg.S3.UseSSL = u.Host, false
	s3, err := s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, false, cfg.S3.MediaBucket, cfg.S3.RawBucket)
	if err != nil {
		t.Fatal(err)
	}

	// Schema, registry, partitions, topics.
	if err := registry.ApplySeed(ctx, db, seed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{MonthsBack: 14, MonthsAhead: 1, DaysBack: 1, DaysAhead: 2}); err != nil {
		t.Fatal(err)
	}
	producer, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, ClientID: "e2e", TopicPrefix: prefix, Linger: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	tb, _ := configs.FS.ReadFile("topics.yaml")
	var doc struct {
		Topics []kafka.TopicSpec `yaml:"topics"`
	}
	_ = yaml.Unmarshal(tb, &doc)
	for i := range doc.Topics {
		doc.Topics[i].Partitions = min(doc.Topics[i].Partitions, 3)
	}
	if err := kafka.EnsureTopics(ctx, producer.Client(), prefix, doc.Topics, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		names := make([]string, len(doc.Topics))
		for i, tp := range doc.Topics {
			names[i] = prefix + tp.Name
		}
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, _ = kadm.NewClient(producer.Client()).DeleteTopics(cctx, names...)
	})

	kcfg := kafka.ClientConfig{Brokers: brokers, ClientID: "e2e", TopicPrefix: prefix, Linger: time.Millisecond}
	s := &Stack{Cfg: cfg, DB: db, Redis: rdb, Producer: producer, S3: s3, Fake: fake, FakeURL: fhs.URL,
		groups: map[string]*group{}, kcfg: kcfg, tracker: jobs.New(rdb),
		copts: app.CrawlerOptions{Stages: []string{"page", "post", "thread", "retry"}, DrainTimeout: o.CrawlerDrain}}
	t.Cleanup(s.Stop)

	s.Crawler, err = app.NewCrawler(ctx, cfg, db, rdb, producer, s.copts)
	if err != nil {
		t.Fatal(err)
	}
	s.Dispatch = &dispatch.Dispatcher{DB: db, Jobs: s.tracker, Producer: producer, BaseURL: cfg.FB.BaseURL}
	s.Adaptive = &scheduler.Adaptive{DB: db, Cfg: cfg}
	s.Planner = &scheduler.Planner{DB: db, Dispatch: s.Dispatch, Cfg: cfg, Adaptive: s.Adaptive}
	s.Eraser = &gdpr.Eraser{DB: db, S3: s3, Producer: producer}
	dl := &media.Downloader{DB: db, S3: s3, Bucket: cfg.S3.MediaBucket, Producer: producer, HTTP: media.NewHTTPClient(),
		TTL: cfg.Media.TTL, MaxBytes: cfg.Media.MaxBytes}
	dlc, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "igs-media-downloader",
		Topics: []string{topics.TaskMedia}, MaxInflight: 16, DrainTimeout: 5 * time.Second}, dl.Handle)
	if err != nil {
		t.Fatal(err)
	}
	s.Janitor = &media.Janitor{DB: db, S3: s3, Bucket: cfg.S3.MediaBucket, Producer: producer,
		RequiredConsumers: cfg.Media.RequiredConsumers, MaxRetryableFailures: 3, TTL: cfg.Media.TTL}
	jc, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "igs-media-janitor",
		Topics: []string{topics.MediaProcessd}, MaxInflight: 8, DrainTimeout: 5 * time.Second}, s.Janitor.Handle)
	if err != nil {
		t.Fatal(err)
	}
	srv := &api.Server{Cfg: cfg, DB: db, Redis: rdb, Dispatch: s.Dispatch}
	s.API = httptest.NewServer(srv.Handler())
	t.Cleanup(s.API.Close)

	aimd := ratelimit.NewController(rdb, prefix, cfg.FB.AIMD, cfg.FB.GlobalRPS)
	s.start("crawler", s.Crawler.Components...)
	s.RestartPersister(t)
	s.RestartFinalizer()
	s.start("media", dlc.Run, jc.Run)
	s.start("aimd", func(ctx context.Context) error { return scheduler.Every(ctx, "aimd", time.Second, aimd.Tick) })
	return s
}

func (s *Stack) start(name string, comps ...run.Func) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	g := &group{cancel: cancel, done: make(chan struct{})}
	s.groups[name] = g
	go func() {
		defer close(g.done)
		if err := run.Group(ctx, comps...); err != nil && ctx.Err() == nil {
			slog.Error("e2e: component group failed", "group", name, "err", err)
		}
	}()
}

func (s *Stack) stop(name string) {
	s.mu.Lock()
	g := s.groups[name]
	delete(s.groups, name)
	s.mu.Unlock()
	if g != nil {
		g.cancel()
		<-g.done
	}
}

// RestartCrawler stops the crawler (in-flight tasks get Options.CrawlerDrain
// to finish) and starts a new one, as a pod replacement would.
func (s *Stack) RestartCrawler(t *testing.T) {
	t.Helper()
	s.stop("crawler")
	c, err := app.NewCrawler(context.Background(), s.Cfg, s.DB, s.Redis, s.Producer, s.copts)
	if err != nil {
		t.Fatal(err)
	}
	s.Crawler = c
	s.start("crawler", c.Components...)
}

// RestartPersister replaces the persister consumer.
func (s *Stack) RestartPersister(t *testing.T) {
	t.Helper()
	s.stop("persister")
	pc, err := persist.NewConsumer(s.kcfg, &persist.Writer{DB: s.DB}, 2000, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	s.start("persister", pc.Run)
}

// RestartFinalizer replaces the finalizer, as a scheduler leader change
// would: whatever the old one held in memory is gone.
func (s *Stack) RestartFinalizer() {
	s.stop("finalizer")
	cfg := s.Cfg
	fin := &scheduler.Finalizer{DB: s.DB, Jobs: s.tracker, Producer: s.Producer, Timeout: cfg.Scheduler.FinalizeTimeout, Adaptive: s.Adaptive,
		Deletions: scheduler.DeletionsFrom(cfg)}
	s.start("finalizer", func(ctx context.Context) error {
		return scheduler.Every(ctx, "finalizer", 200*time.Millisecond, fin.Tick)
	})
}

// Stop shuts the stack down.
func (s *Stack) Stop() {
	s.mu.Lock()
	names := make([]string, 0, len(s.groups))
	for n := range s.groups {
		names = append(names, n)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, n := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.stop(n)
		}()
	}
	wg.Wait()
}

// Key creates an API key with scopes.
func (s *Stack) Key(t *testing.T, scopes ...string) string {
	t.Helper()
	k, _, err := api.CreateClient(context.Background(), s.DB, "e2e", scopes, 0)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// Do performs an API request.
func (s *Stack) Do(t *testing.T, key, method, path string, body []byte) *http.Response {
	t.Helper()
	var rdr *bytesReader
	if body != nil {
		rdr = &bytesReader{b: body}
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, s.API.URL+path, rdr.reader())
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
