package stage

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/tttest"
)

// harness runs tks's worker against fakett with real Valkey, Redpanda and
// Postgres (schema tt), as the crawler does, without the consumers' timing:
// it reads every task record the stages produce and hands it to the worker
// at once (parked retries too, without their wait), and keeps every public
// record for the test to compare with fakett's ground truth. Post and thread
// tasks run too when runComments is set; otherwise they are kept, not run,
// and counted done (the account stage's own tests).
type harness struct {
	t      *testing.T
	env    *tttest.Env
	d      *Deps
	w      *Worker
	prefix string
	cl     *kgo.Client
	// redeliver hands every task record to the worker twice, as Kafka may
	// after a crash before the offset commit.
	redeliver bool
	// runComments runs post and thread tasks through the worker.
	runComments bool

	mu   sync.Mutex
	recs map[string][]*kgo.Record // by topic, without the prefix
}

func newHarness(t *testing.T, o tttest.Options) *harness {
	t.Helper()
	ctx := context.Background()
	rdb := testenv.Redis(t)
	brokers := testenv.Kafka(t)
	dsn := testenv.EmptyPostgres(t)
	db, err := ttstore.Open(ctx, dsn, 4)
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
	cfg.Crawler.YieldPages = 3 // continuations on every account
	cfg.Crawler.YieldAfter = time.Minute
	env := tttest.New(t, o)
	prefix := testenv.TopicPrefix()
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	// Every topic up front, one partition each: auto-creation is refused now
	// and then while other suites create topics on the same broker.
	var specs []kafka.TopicSpec
	for _, name := range append([]string{topics.Account, topics.Post, topics.PostMetrics, topics.Comment, topics.Music, topics.Media,
		topics.TaskMedia, topics.DLQ}, topics.RetryTopics...) {
		specs = append(specs, kafka.TopicSpec{Name: name, Partitions: 1})
	}
	for _, k := range []tkstaskv1.Kind{tkstaskv1.Kind_KIND_ACCOUNT, tkstaskv1.Kind_KIND_POST, tkstaskv1.Kind_KIND_THREAD} {
		for _, l := range []tkstaskv1.Lane{tkstaskv1.Lane_LANE_P0, tkstaskv1.Lane_LANE_P1, tkstaskv1.Lane_LANE_P2} {
			specs = append(specs, kafka.TopicSpec{Name: topics.Task(k, l), Partitions: 1})
		}
	}
	if err := kafka.EnsureTopics(ctx, prod.Client(), prefix, specs, 1); err != nil {
		t.Fatal(err)
	}
	// Dropped afterwards: a broker holds a bounded number of partitions
	// (Redpanda's topic_partitions_per_shard), and every test makes its own.
	t.Cleanup(func() {
		names := make([]string, len(specs))
		for i, sp := range specs {
			names[i] = prefix + sp.Name
		}
		_, _ = kadm.NewClient(prod.Client()).DeleteTopics(context.Background(), names...)
	})
	proxies, err := proxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	supp := gdpr.NewSuppression(db).ForAccounts()
	now := func() time.Time { return env.World.Now }
	d := &Deps{Cfg: cfg, Pool: env.Pool, Proxies: proxies, Loader: env.Loader, Client: env.Client,
		Emit:     &emit.Emitter{Producer: prod, HMACKey: []byte("tks-test-secret"), Suppression: supp, Now: now},
		Producer: prod, Jobs: jobs.New(rdb), Ckpt: checkpoint.New(rdb), DB: db, Suppression: supp, Now: now}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeRegex(), kgo.ConsumeTopics("^"+regexp.QuoteMeta(prefix)+".*"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.MetadataMinAge(100*time.Millisecond), kgo.MetadataMaxAge(500*time.Millisecond), kgo.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	h := &harness{t: t, env: env, d: d, prefix: prefix, cl: cl, recs: map[string][]*kgo.Record{}}
	h.w = &Worker{D: d, LeaseTTL: time.Minute, MaxAttempts: 6,
		Stages: map[tkstaskv1.Kind]Stage{tkstaskv1.Kind_KIND_ACCOUNT: &AccountStage{D: d, SoftPause: 10 * time.Millisecond},
			tkstaskv1.Kind_KIND_POST: &PostStage{D: d}, tkstaskv1.Kind_KIND_THREAD: &ThreadStage{D: d}}}
	return h
}

// check queues one account check as a job of its own and runs it to the
// end; it returns the job's status.
func (h *harness) check(job string, mode commonv1.CrawlMode, sc *tkstaskv1.AccountScope) jobs.Status {
	h.t.Helper()
	ctx := context.Background()
	lim := task.DefaultLimits(h.d.Cfg.Limits, h.d.Cfg.TT, mode == commonv1.CrawlMode_CRAWL_MODE_FULL)
	lane := tkstaskv1.Lane_LANE_P1
	if mode == commonv1.CrawlMode_CRAWL_MODE_FULL {
		lane = tkstaskv1.Lane_LANE_P2
	}
	root := task.NewAccount(job, lane, mode, sc, lim)
	if err := h.d.spawn(ctx, root); err != nil {
		h.t.Fatal(err)
	}
	return h.drain(job)
}

// drain hands task records to the worker until the job has nothing
// pending and the topics are quiet.
func (h *harness) drain(job string) jobs.Status {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	quiet := 0
	for {
		pctx, pcancel := context.WithTimeout(ctx, 300*time.Millisecond)
		fs := h.cl.PollFetches(pctx)
		pcancel()
		if ctx.Err() != nil {
			st, _ := h.d.Jobs.Status(context.Background(), job)
			h.mu.Lock()
			seen := map[string]int{}
			for k, v := range h.recs {
				seen[k] = len(v)
			}
			h.mu.Unlock()
			h.t.Fatalf("job %s did not finish: %v; status %+v; records %v", job, ctx.Err(), st, seen)
		}
		n := 0
		fs.EachRecord(func(r *kgo.Record) {
			n++
			base := strings.TrimPrefix(r.Topic, h.prefix)
			h.mu.Lock()
			h.recs[base] = append(h.recs[base], r)
			h.mu.Unlock()
			switch {
			case strings.HasPrefix(base, "tks.task.account."), strings.HasPrefix(base, "tks.task.retry."),
				h.runComments && (strings.HasPrefix(base, "tks.task.post.") || strings.HasPrefix(base, "tks.task.thread.")):
				for range 1 + btoi(h.redeliver) {
					if err := h.w.Handle(ctx, r); err != nil {
						h.t.Fatalf("handle %s: %v", base, err)
					}
				}
			case strings.HasPrefix(base, "tks.task.post."), strings.HasPrefix(base, "tks.task.thread."):
				var tk tkstaskv1.Task
				if err := proto.Unmarshal(r.Value, &tk); err != nil {
					h.t.Fatal(err)
				}
				if _, err := h.d.Jobs.Done(ctx, tk.JobId, tk.TaskId, nil); err != nil {
					h.t.Fatal(err)
				}
			}
		})
		st, err := h.d.Jobs.Status(ctx, job)
		if err != nil {
			h.t.Fatal(err)
		}
		if n == 0 && st.Spawned > 0 && st.Pending <= 0 {
			if quiet++; quiet >= 3 {
				return st
			}
		} else {
			quiet = 0
		}
	}
}

// records returns the records of a topic seen so far whose job is job (""
// for every job).
func records[M proto.Message](h *harness, topic, job string, newM func() M) []M {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []M
	for _, r := range h.recs[topic] {
		m := newM()
		if err := proto.Unmarshal(r.Value, m); err != nil {
			h.t.Fatal(err)
		}
		if job != "" && !hasJob(m, job) {
			continue
		}
		out = append(out, m)
	}
	return out
}

func hasJob(m proto.Message, job string) bool {
	var obs *commonv1.Observation
	switch v := any(m).(type) {
	case *ttv1.Account:
		obs = v.Obs
	case *ttv1.Post:
		obs = v.Obs
	case *ttv1.PostMetrics:
		obs = v.Obs
	case *ttv1.Music:
		obs = v.Obs
	case *ttv1.Media:
		obs = v.Obs
	case *ttv1.Comment:
		obs = v.Obs
	case *tkstaskv1.Task:
		return v.JobId == job
	}
	for _, j := range obs.GetJobIds() {
		if j == job {
			return true
		}
	}
	return false
}

func posts(h *harness, job string) map[int64]*ttv1.Post {
	out := map[int64]*ttv1.Post{}
	for _, p := range records(h, topics.Post, job, func() *ttv1.Post { return &ttv1.Post{} }) {
		out[p.PostId] = p
	}
	return out
}

func accounts(h *harness, job string) []*ttv1.Account {
	return records(h, topics.Account, job, func() *ttv1.Account { return &ttv1.Account{} })
}

func postTasks(h *harness, job string) map[int64]*tkstaskv1.Task {
	out := map[int64]*tkstaskv1.Task{}
	for _, lane := range []string{"p0", "p1", "p2"} {
		for _, tk := range records(h, "tks.task.post."+lane, job, func() *tkstaskv1.Task { return &tkstaskv1.Task{} }) {
			out[tk.GetPost().GetPostId()] = tk
		}
	}
	return out
}

// inWindow returns an account's posts fakett serves within the age window.
func inWindow(w *fakett.World, acc *fakett.Account, days int) []*fakett.Post {
	var out []*fakett.Post
	cut := w.Now.Add(-time.Duration(days) * 24 * time.Hour)
	for _, p := range acc.Posts {
		if !p.Created.Before(cut) {
			out = append(out, p)
		}
	}
	return out
}

func nowTS(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
