package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Load tests run the in-process pipeline at volume. They are opt-in
// (IGS_LOADTEST=1, `make loadtest`) and print a report; IGS_LOADTEST_OUT
// also writes it to a file. They measure this machine, one process for
// every component, so they find the pipeline's own ceilings (Kafka,
// Postgres, the persister, the stages) rather than Facebook's.

func loadEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("IGS_LOADTEST") == "" {
		t.Skip("load test: set IGS_LOADTEST=1 (make loadtest)")
	}
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

type report struct {
	title string
	rows  [][2]string
}

func (r *report) add(k string, format string, args ...any) {
	r.rows = append(r.rows, [2]string{k, fmt.Sprintf(format, args...)})
}

func (r *report) write(t *testing.T) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "### %s\n\n| Measure | Value |\n|---|---|\n", r.title)
	for _, row := range r.rows {
		fmt.Fprintf(&b, "| %s | %s |\n", row[0], row[1])
	}
	t.Log("\n" + b.String())
	if p := os.Getenv("IGS_LOADTEST_OUT"); p != "" {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		_, _ = f.WriteString(b.String() + "\n")
	}
}

func machine() string {
	return fmt.Sprintf("%d CPUs, %s/%s, %s", runtime.NumCPU(), runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// sample sums a metric's children matching labels: a counter's value, or
// a histogram's sample count and sum.
func sample(name string, labels map[string]string) (value float64, count uint64, sum float64) {
	mfs, _ := metrics.Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if !matches(m, labels) {
				continue
			}
			value += m.GetCounter().GetValue()
			count += m.GetHistogram().GetSampleCount()
			sum += m.GetHistogram().GetSampleSum()
		}
	}
	return value, count, sum
}

func matches(m *dto.Metric, labels map[string]string) bool {
	for k, v := range labels {
		found := false
		for _, l := range m.GetLabel() {
			if l.GetName() == k && l.GetValue() == v {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	return xs[min(len(xs)-1, int(p*float64(len(xs))))]
}

// TestLoadPipeline backfills many pages at once through the API, crawler
// stages, persister and finalizer, and checks every comment arrived.
//
//	IGS_LOADTEST_PAGES (30), IGS_LOADTEST_POSTS (25 per page),
//	IGS_LOADTEST_COMMENTS (150 max per post), IGS_LOADTEST_LATENCY (fakefb, 0)
func TestLoadPipeline(t *testing.T) {
	loadEnabled(t)
	pages, posts := envInt("IGS_LOADTEST_PAGES", 30), envInt("IGS_LOADTEST_POSTS", 25)
	s := Start(t, Options{
		Spec: fakefb.Spec{Seed: 77, Pages: pages, PostsPerPage: posts, PostSpacing: 10 * time.Hour,
			MaxComments: envInt("IGS_LOADTEST_COMMENTS", 150), ReplyProbability: 0.3, MaxReplies: 10, Depth2Prob: 0.15, AttachmentProb: 0.05},
		Faults: fakefb.Faults{Latency: envDuration("IGS_LOADTEST_LATENCY", 0)},
		Config: func(c *config.Config) {
			c.Crawler.Slots, c.Crawler.LaneMin = 96, 4
			c.Crawler.Lanes = map[string]int{"p0": 96, "p1": 32, "p2": 32}
			c.Crawler.YieldPages = 20
			c.Crawler.TimelinePageSize = 10
			c.API.MaxRunningP0JobsPerClient = pages + 10
			c.FB.IdentityRPS, c.FB.IdentityBurst = 1000, 50
			c.FB.GlobalRPS = 100000
			c.FB.AIMD.Enabled = false
		},
	})
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	world := s.Fake.World.Pages()
	want := 0
	wantPosts := 0
	for _, p := range world {
		truth, _ := s.Fake.World.TruthFor(p.ID, time.Time{})
		wantPosts += len(truth.Posts)
		for _, tp := range truth.Posts {
			want += tp.TotalComments
		}
	}

	before := s.Fake.Stats()
	_, bc0, bs0 := sample("igs_persist_batch_seconds", map[string]string{"entity": "comment"})
	if path := os.Getenv("IGS_LOADTEST_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
		}()
	}
	stopBacklog := watchBacklog(s, 2*time.Second)
	start := time.Now()
	// Submitted one after the other (a few ms each): every job starts as
	// soon as it is accepted, and a refused submission fails the test now.
	ids := make([]string, len(world))
	for i, p := range world {
		j := decodeBody[jobView](t, s.Do(t, key, "POST", "/v1/jobs", []byte(fmt.Sprintf(`{"page":%q,"mode":"full"}`, "https://www.facebook.com/"+p.Vanity))), http.StatusAccepted)
		ids[i] = j.ID
	}
	deadline := time.Now().Add(envDuration("IGS_LOADTEST_TIMEOUT", 20*time.Minute))
	for time.Now().Before(deadline) {
		if count(t, s, `SELECT count(*) FROM jobs WHERE status IN ('completed','partial','failed','cancelled')`) == len(ids) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	elapsed := time.Since(start)
	after := s.Fake.Stats()
	backlog := stopBacklog()

	ctx := context.Background()
	statuses := map[string]int{}
	var durations []float64
	rows, err := s.DB.Query(ctx, `SELECT status, extract(epoch FROM coalesce(finished_at, now()) - created_at) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var st string
		var d float64
		if err := rows.Scan(&st, &d); err != nil {
			t.Fatal(err)
		}
		statuses[st]++
		durations = append(durations, d)
	}
	rows.Close()
	gotPosts := count(t, s, `SELECT count(*) FROM posts`)
	gotComments := count(t, s, `SELECT count(*) FROM comments`)
	requests := 0
	for q, n := range after.ByQuery {
		requests += n - before.ByQuery[q]
	}
	_, bc, bs := sample("igs_persist_batch_seconds", map[string]string{"entity": "comment"})
	bc, bs = bc-bc0, bs-bs0

	r := &report{title: fmt.Sprintf("Pipeline: %d pages × %d posts, all backfilled at once", pages, posts)}
	r.add("Machine", "%s", machine())
	r.add("Jobs", "%v", statuses)
	r.add("Wall time", "%s", elapsed.Round(time.Millisecond))
	r.add("Facebook requests (fakefb)", "%d (%.0f/s)", requests, float64(requests)/elapsed.Seconds())
	r.add("Posts stored", "%d of %d (%.0f/s)", gotPosts, wantPosts, float64(gotPosts)/elapsed.Seconds())
	r.add("Comments and replies stored", "%d of %d (%.0f/s)", gotComments, want, float64(gotComments)/elapsed.Seconds())
	r.add("Job duration p50 / p95 / max", "%.1fs / %.1fs / %.1fs", pct(durations, 0.5), pct(durations, 0.95), pct(durations, 1))
	if bc > 0 {
		r.add("Persister comment batches", "%d, mean %.1f ms", bc, 1000*bs/float64(bc))
	}
	for _, st := range []string{"page", "post", "thread", "persister"} {
		b := backlog[st]
		r.add("Backlog: "+st, "peak %d, mean %.0f records", b.peak, b.mean)
	}
	r.add("fakefb latency", "%s", envDuration("IGS_LOADTEST_LATENCY", 0))
	r.write(t)
	if statuses["completed"] != len(ids) || gotComments != want || gotPosts != wantPosts {
		t.Fatalf("incomplete: jobs %v, posts %d/%d, comments %d/%d", statuses, gotPosts, wantPosts, gotComments, want)
	}
}

type backlogStat struct {
	peak int64
	mean float64
}

// watchBacklog samples, every interval, the records waiting for each crawl
// stage (all lanes) and for the persister: the stage whose backlog grows
// is the one that saturates first. The returned func stops it.
func watchBacklog(s *Stack, every time.Duration) func() map[string]backlogStat {
	adm := kadm.NewClient(s.Producer.Client())
	label := map[string]string{s.Producer.Topic(persist.Group): "persister"}
	for _, st := range []string{"page", "post", "thread"} {
		for _, l := range []string{"p0", "p1", "p2"} {
			label[s.Producer.Topic("igs-crawler-"+st+"-"+l)] = st
		}
	}
	groups := make([]string, 0, len(label))
	for g := range label {
		groups = append(groups, g)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	peak, sum := map[string]int64{}, map[string]int64{}
	n := 0
	go func() {
		defer close(done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			lags, err := adm.Lag(ctx, groups...)
			if err != nil {
				continue
			}
			tot := map[string]int64{}
			for g, l := range lags {
				if l.Error() == nil {
					tot[label[g]] += l.Lag.Total()
				}
			}
			for st, v := range tot {
				peak[st] = max(peak[st], v)
				sum[st] += v
			}
			n++
		}
	}()
	return func() map[string]backlogStat {
		cancel()
		<-done
		out := map[string]backlogStat{}
		for st, p := range peak {
			out[st] = backlogStat{peak: p, mean: float64(sum[st]) / float64(max(n, 1))}
		}
		return out
	}
}

// TestLoadPersister measures the persister alone: synthetic comments are
// produced to fb.comment.v1 as fast as Kafka takes them, then the same
// records again (the no-op upsert path every re-scrape takes).
//
//	IGS_LOADTEST_ROWS (200000)
func TestLoadPersister(t *testing.T) {
	loadEnabled(t)
	n := envInt("IGS_LOADTEST_ROWS", 200_000)
	s := Start(t, Options{Spec: fakefb.Spec{Seed: 1, Pages: 1, PostsPerPage: 1, PostSpacing: time.Hour, MaxComments: 1}})
	ctx := context.Background()
	postCreated := time.Now().Add(-48 * time.Hour)
	obs := &fbv1.Observation{ObservedAt: timestamppb.Now(), ContentHash: []byte{1}}
	produce := func() time.Duration {
		start := time.Now()
		const batch = 5000
		for i := 0; i < n; i += batch {
			b := s.Producer.NewBatch()
			for j := i; j < min(i+batch, n); j++ {
				post := int64(1_000_000 + j/200) // 200 comments per post
				c := &fbv1.Comment{Obs: obs, CommentId: int64(10_000_000 + j), PostId: post, PageId: 42,
					PostCreatedAt: timestamppb.New(postCreated), Depth: int32(j % 2), Text: "load test comment " + strconv.Itoa(j),
					CreatedAt: timestamppb.New(postCreated.Add(time.Duration(j) * time.Second)), FeedbackId: "fb" + strconv.Itoa(j)}
				if err := b.AddProto(ctx, topics.Comment, strconv.FormatInt(post, 10), c); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return time.Since(start)
	}
	// processed counts comment rows the persister handled (written or
	// unchanged).
	processed := func() float64 {
		v, _, _ := sample("igs_persist_rows_total", map[string]string{"entity": "comment"})
		return v
	}
	wait := func(target float64) time.Duration {
		start := time.Now()
		deadline := start.Add(10 * time.Minute)
		for processed() < target {
			if time.Now().After(deadline) {
				t.Fatalf("the persister handled %.0f of %.0f rows in 10m", processed(), target)
			}
			time.Sleep(100 * time.Millisecond)
		}
		return time.Since(start)
	}

	base := processed()
	_, bc0, bs0 := sample("igs_persist_batch_seconds", map[string]string{"entity": "comment"})
	start := time.Now()
	prodNew := produce()
	wait(base + float64(n))
	fresh := time.Since(start)
	if got := count(t, s, `SELECT count(*) FROM comments WHERE page_id = 42`); got != n {
		t.Fatalf("comments stored %d, want %d", got, n)
	}
	start = time.Now()
	prodAgain := produce()
	wait(base + 2*float64(n))
	again := time.Since(start)
	_, bc, bs := sample("igs_persist_batch_seconds", map[string]string{"entity": "comment"})
	bc, bs = bc-bc0, bs-bs0

	r := &report{title: fmt.Sprintf("Persister: %d comments", n)}
	r.add("Machine", "%s", machine())
	r.add("New rows (COPY + insert)", "%s, %.0f rows/s (producing took %s)", fresh.Round(time.Millisecond), float64(n)/fresh.Seconds(), prodNew.Round(time.Millisecond))
	r.add("Same rows again (no-op upsert)", "%s, %.0f rows/s (producing took %s)", again.Round(time.Millisecond), float64(n)/again.Seconds(), prodAgain.Round(time.Millisecond))
	if bc > 0 {
		r.add("Comment batches", "%d, mean %.1f ms, %.0f rows each", bc, 1000*bs/float64(bc), 2*float64(n)/float64(bc))
	}
	r.write(t)
}
