// Package e2e runs tks's pipeline in-process against fakett, with real
// Postgres (schema tt), Kafka (Redpanda) and Valkey: the crawler and the
// persister as internal/tt/app wires them for cmd/tks-crawler and
// cmd/tks-persister. Its tests compare what the store holds with fakett's
// ground truth (docs/tiktok build brief §8, A6).
package e2e

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/app"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	ttpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// Stack is a running pipeline.
type Stack struct {
	Cfg      *config.Config
	DB       *pgxpool.Pool
	Redis    *redis.Client
	Producer *kafka.Producer
	World    *fakett.World
	Fake     *fakett.Server
	Crawler  *app.Crawler
	Signer   *Signer
	Tracker  *jobs.Tracker

	mu     sync.Mutex
	groups map[string]*group
	copts  app.CrawlerOptions
}

// group is a set of components started and stopped together, so a test can
// restart one part of the pipeline.
type group struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Options tweak the stack.
type Options struct {
	Spec   fakett.Spec
	Faults fakett.Faults
	Config func(*config.Config)
	// CrawlerDrain overrides how long a stopping crawler lets in-flight
	// tasks run before abandoning them (a short one is a crash).
	CrawlerDrain time.Duration
}

// Signer stands in for the browser signer's pages (internal/tt/sign has its
// own tests with Chromium): it adds the parameter fakett checks, as
// TikTok's script adds its signature, and counts what it signed.
type Signer struct{ Calls atomic.Int64 }

func (s *Signer) SignURL(_ context.Context, _ *session.Identity, _, raw, _ string) (string, error) {
	s.Calls.Add(1)
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("X-Gnarly", "e2e-signature")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Start brings up the stack; it stops at test cleanup. fakett answers the
// comment and reply calls only when signed, as TikTok does (C2, C3).
func Start(t *testing.T, o Options) *Stack {
	t.Helper()
	if os.Getenv("TKS_E2E_DEBUG") != "" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	ctx := context.Background()
	brokers := testenv.Kafka(t)
	rdb := testenv.Redis(t)
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	prefix := testenv.TopicPrefix()
	cfg.Kafka.Brokers, cfg.Kafka.TopicPrefix, cfg.Kafka.ProduceLinger = brokers, prefix, time.Millisecond
	cfg.Privacy.AuthorHMACKey = "tks-e2e-secret"
	cfg.TT.IdentityRPS, cfg.TT.IdentityBurst = 500, 20
	cfg.TT.AccountRPS, cfg.TT.AccountRPSP0, cfg.TT.GlobalRPS = 2000, 2000, 5000
	cfg.TT.RequestTimeout = 10 * time.Second
	cfg.TT.AIMD.Enabled = false
	cfg.TT.CaptureRaw = "none"
	cfg.TT.Signer.Mode = config.SignerBrowser
	cfg.TT.CommentsPageSize = 10 // continuations within a post's comments
	cfg.Crawler.YieldPages = 2   // and within a list
	cfg.Crawler.Slots, cfg.Crawler.LaneMin = 24, 2
	cfg.Crawler.Lanes = map[string]int{"p0": 24, "p1": 16, "p2": 8}
	if o.Config != nil {
		o.Config(cfg)
	}

	spec := o.Spec
	if spec.Accounts == 0 {
		spec = fakett.DefaultSpec()
	}
	faults := o.Faults
	faults.RequireSignature = true
	faults.SignedPaths = []string{"/api/comment/list/", "/api/comment/list/reply/"}
	w := fakett.NewWorld(spec)
	fake := fakett.NewServer(w, fakett.Options{Faults: faults})
	hs := httptest.NewServer(fake)
	t.Cleanup(hs.Close)
	cfg.TT.BaseURL = hs.URL

	// Schema, registry, partitions, topics.
	seed, err := ttclient.Seed()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.ApplySeedTo(ctx, db, ttstore.Registry, seed); err != nil {
		t.Fatal(err)
	}
	if _, err := ttstore.EnsurePartitions(ctx, db, time.Now(),
		store.PartitionWindow{MonthsBack: 14, MonthsAhead: 1, DaysBack: 1, DaysAhead: 3}); err != nil {
		t.Fatal(err)
	}
	producer, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, ClientID: "e2e", TopicPrefix: prefix, Linger: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	tb, _ := configs.FS.ReadFile("tt/topics.yaml")
	var doc struct {
		Topics []kafka.TopicSpec `yaml:"topics"`
	}
	if err := yaml.Unmarshal(tb, &doc); err != nil {
		t.Fatal(err)
	}
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

	s := &Stack{Cfg: cfg, DB: db, Redis: rdb, Producer: producer, World: w, Fake: fake, Signer: &Signer{},
		Tracker: jobs.New(rdb), groups: map[string]*group{}}
	s.copts = app.CrawlerOptions{Signer: s.Signer, DrainTimeout: o.CrawlerDrain,
		DoerFactory: func(transport.Options) (transport.Doer, error) { return transport.NewStd(10 * time.Second), nil }}
	t.Cleanup(s.Stop)
	s.RestartCrawler(t)
	s.RestartPersister(t)
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
// to finish) and starts a new one, as a pod replacement would. Its clock
// follows fakett's world, so a test can move time forward.
func (s *Stack) RestartCrawler(t *testing.T) {
	t.Helper()
	s.stop("crawler")
	c, err := app.NewCrawler(context.Background(), s.Cfg, s.DB, s.Redis, s.Producer, s.copts)
	if err != nil {
		t.Fatal(err)
	}
	c.Deps.Now, c.Deps.Emit.Now = s.World.Clock, s.World.Clock
	s.mu.Lock()
	s.Crawler = c
	s.mu.Unlock()
	s.start("crawler", c.Components...)
}

// RestartPersister replaces the persister.
func (s *Stack) RestartPersister(t *testing.T) {
	t.Helper()
	s.stop("persister")
	pc, err := app.NewPersister(s.Cfg, s.DB, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	s.start("persister", pc.Run)
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
		wg.Go(func() { s.stop(n) })
	}
	wg.Wait()
}

func (s *Stack) crawler() *app.Crawler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Crawler
}

// Queue starts a check of the account behind ref as job, with the
// account's stored state in its scope, as the scheduler sets it (M6):
// the user id and secUid once known (so a renamed account is still found),
// and when the account and its profile page were last read.
func (s *Stack) Queue(t *testing.T, job, ref string, mode commonv1.CrawlMode) {
	t.Helper()
	ctx := context.Background()
	sc := task.ScopeFromRef(ref)
	if sc.Username != "" {
		var (
			userID           int64
			sec, username    string
			refreshed, paged *time.Time
		)
		err := s.DB.QueryRow(ctx, `SELECT a.user_id, a.sec_uid, a.username, a.profile_refreshed_at, a.page_read_at
			FROM account_usernames u JOIN accounts a USING (user_id) WHERE lower(u.username) = lower($1)
			ORDER BY u.last_seen_at DESC LIMIT 1`, sc.Username).Scan(&userID, &sec, &username, &refreshed, &paged)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			t.Fatal(err)
		default:
			sc.UserId, sc.SecUid, sc.Username = userID, sec, username
			sc.AccountSeenAt, sc.PageSeenAt = ts(refreshed), ts(paged)
		}
	}
	full := mode == commonv1.CrawlMode_CRAWL_MODE_FULL
	lane := tkstaskv1.Lane_LANE_P1
	if full {
		lane = tkstaskv1.Lane_LANE_P2
	}
	root := task.NewAccount(job, lane, mode, sc, task.DefaultLimits(s.Cfg.Limits, s.Cfg.TT, full))
	if err := s.crawler().Deps.Start(ctx, root); err != nil {
		t.Fatal(err)
	}
}

// Wait waits until every job has nothing pending and the persister has
// written every record published so far; it returns the jobs' statuses.
func (s *Stack) Wait(t *testing.T, timeout time.Duration, ids ...string) map[string]jobs.Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out := map[string]jobs.Status{}
	for _, id := range ids {
		for {
			st, err := s.Tracker.Status(ctx, id)
			if err == nil && st.Spawned > 0 && st.Pending <= 0 {
				out[id] = st
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("job %s did not finish: %+v (%v)", id, st, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	s.WaitPersisted(t, ctx)
	return out
}

// WaitPersisted waits until the persister committed every entity record
// published so far.
func (s *Stack) WaitPersisted(t *testing.T, ctx context.Context) {
	t.Helper()
	names := make([]string, len(topics.Entity))
	for i, tp := range topics.Entity {
		names[i] = s.Cfg.Kafka.TopicPrefix + tp
	}
	ends, err := kafka.EndOffsets(ctx, s.Producer.Client(), names...)
	if err != nil {
		t.Fatal(err)
	}
	for {
		ok, err := kafka.GroupCaughtUp(ctx, s.Producer.Client(), s.Cfg.Kafka.TopicPrefix+ttpersist.Group, ends)
		if err == nil && ok {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("the persister did not catch up: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func ts(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}
