package e2e

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/dispatch"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	ttpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
)

// Load tests run tks's in-process pipeline at volume against fakett. They
// are opt-in (TKS_LOADTEST=1, `make loadtest`) and print a report;
// TKS_LOADTEST_OUT also appends it to a file. They measure this machine,
// one process for every component, so they find the pipeline's own
// ceilings (Kafka, Postgres, the persister, the stages), not TikTok's: the
// per-IP tolerance and the signer's pages were measured by Phase 0 (C4).

func loadEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("TKS_LOADTEST") == "" {
		t.Skip("load test: set TKS_LOADTEST=1 (make loadtest)")
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
	if p := os.Getenv("TKS_LOADTEST_OUT"); p != "" {
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

// TestLoadPipeline registers many accounts at once and lets the scheduler
// backfill them all together (planner, crawler stages with every comment
// and reply signed, persister, finalizer); then the store must equal
// fakett's world for every account (A6), and each job must be completed.
//
//	TKS_LOADTEST_ACCOUNTS (30), TKS_LOADTEST_POSTS (60 per account),
//	TKS_LOADTEST_COMMENTS (40 max per post), TKS_LOADTEST_REPLIES (5 max
//	per comment), TKS_LOADTEST_LATENCY (fakett's, 0),
//	TKS_LOADTEST_TIMEOUT (20m), TKS_LOADTEST_CPUPROFILE=file
func TestLoadPipeline(t *testing.T) {
	loadEnabled(t)
	n, posts := envInt("TKS_LOADTEST_ACCOUNTS", 30), envInt("TKS_LOADTEST_POSTS", 60)
	s := Start(t, Options{
		Spec: fakett.Spec{Seed: 77, Accounts: n, PostsPerAccount: posts, PostSpacing: 20 * time.Hour,
			MaxComments: envInt("TKS_LOADTEST_COMMENTS", 40), MaxReplies: envInt("TKS_LOADTEST_REPLIES", 5)},
		Latency: envDuration("TKS_LOADTEST_LATENCY", 0),
		Config: func(c *config.Config) {
			c.Crawler.Slots, c.Crawler.LaneMin = 96, 4
			c.Crawler.Lanes = map[string]int{"p0": 32, "p1": 32, "p2": 96}
			c.Crawler.YieldPages = 20
			c.TT.CommentsPageSize = 20
			c.TT.IdentityRPS, c.TT.IdentityBurst = 1000, 50
			c.TT.AccountRPS, c.TT.AccountRPSP0, c.TT.GlobalRPS = 1000, 1000, 100000
		},
	})
	accs := s.World.Accounts()
	wantPosts, wantComments := 0, 0
	for _, a := range accs {
		wantPosts += len(a.Posts)
		for _, p := range a.Posts {
			wantComments += len(p.Comments)
			for _, c := range p.Comments {
				wantComments += len(c.Replies)
			}
		}
	}
	if path := os.Getenv("TKS_LOADTEST_CPUPROFILE"); path != "" {
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
	_, bc0, bs0 := sample("tks_persist_batch_seconds", map[string]string{"entity": "comment"})
	req0, signed0 := s.Requests.Load(), s.Signer.Calls.Load()
	sc := newSched(s)
	sc.timeout = envDuration("TKS_LOADTEST_TIMEOUT", 20*time.Minute)
	sc.register(t, accs...)
	stopBacklog := watchBacklog(s, 2*time.Second)
	start := time.Now()
	got := sc.tick(t)
	elapsed := time.Since(start)
	backlog := stopBacklog()

	ctx := context.Background()
	statuses := map[string]int{}
	rows, err := s.DB.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var st string
		var k int
		if err := rows.Scan(&st, &k); err != nil {
			t.Fatal(err)
		}
		statuses[st] = k
	}
	rows.Close()
	gotPosts := count(t, s, `SELECT count(*) FROM posts`)
	gotComments := count(t, s, `SELECT count(*) FROM comments WHERE status = 'visible'`)
	requests, signed := s.Requests.Load()-req0, s.Signer.Calls.Load()-signed0
	_, bc, bs := sample("tks_persist_batch_seconds", map[string]string{"entity": "comment"})
	bc, bs = bc-bc0, bs-bs0

	r := &report{title: fmt.Sprintf("Pipeline: %d accounts × %d posts, all backfilled at once", n, posts)}
	r.add("Machine", "%s", machine())
	r.add("Jobs", "%v", statuses)
	r.add("Wall time", "%s", elapsed.Round(time.Millisecond))
	r.add("TikTok requests (fakett)", "%d (%.0f/s), %d signed (%.0f %%)", requests, float64(requests)/elapsed.Seconds(),
		signed, 100*float64(signed)/float64(max(requests, 1)))
	r.add("Posts stored", "%d of %d (%.0f/s)", gotPosts, wantPosts, float64(gotPosts)/elapsed.Seconds())
	r.add("Comments and replies stored", "%d of %d (%.0f/s)", gotComments, wantComments, float64(gotComments)/elapsed.Seconds())
	// Jobs' own times follow fakett's clock, which stands still here.
	r.add("Per account", "%.0f requests, %.1f s of the run", float64(requests)/float64(n), elapsed.Seconds()/float64(n))
	if bc > 0 {
		r.add("Persister comment batches", "%d, mean %.1f ms", bc, 1000*bs/float64(bc))
	}
	for _, st := range []string{"account", "post", "thread", "persister"} {
		b := backlog[st]
		r.add("Backlog: "+st, "peak %d, mean %.0f records", b.peak, b.mean)
	}
	r.add("fakett latency", "%s", envDuration("TKS_LOADTEST_LATENCY", 0))
	r.write(t)
	if len(got) != n || statuses["completed"] != n {
		t.Fatalf("jobs %v, want %d completed", statuses, n)
	}
	for ref, j := range got {
		if j.kind != dispatch.KindBackfill {
			t.Errorf("%s: job %+v, want a backfill", ref, j)
		}
	}
	for _, a := range accs {
		sameStore(t, s, a, truth{})
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
	label := map[string]string{s.Producer.Topic(ttpersist.Group): "persister"}
	for _, st := range []string{"account", "post", "thread"} {
		for _, l := range []string{"p0", "p1", "p2"} {
			label[s.Producer.Topic("tks-tt-crawler-"+st+"-"+l)] = st
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

// TestLoadPersister measures the persister alone: synthetic comments, with
// their authors, are produced to tt.comment.v1 as fast as Kafka takes
// them, then the same records again (the no-op upsert every re-read of a
// post takes).
//
//	TKS_LOADTEST_ROWS (200000)
func TestLoadPersister(t *testing.T) {
	loadEnabled(t)
	n := envInt("TKS_LOADTEST_ROWS", 200_000)
	s := Start(t, Options{Spec: fakett.Spec{Seed: 1, Accounts: 1, PostsPerAccount: 1, PostSpacing: time.Hour}})
	ctx := context.Background()
	postCreated := time.Now().Add(-48 * time.Hour)
	em := &emit.Emitter{Producer: s.Producer, HMACKey: []byte(s.Cfg.Privacy.AuthorHMACKey), Now: time.Now}
	src := emit.Source{JobID: "load", Query: "comments", Variant: "load", Mode: commonv1.CrawlMode_CRAWL_MODE_FULL}
	const user = int64(42)
	produce := func() time.Duration {
		start := time.Now()
		const batch = 5000
		for i := 0; i < n; i += batch {
			b := s.Producer.NewBatch()
			for j := i; j < min(i+batch, n); j++ {
				post := int64(7_000_000_000_000_000_000 + j/200) // 200 comments per post
				likes, author := int64(j%50), int64(6_900_000_000_000_000_000+j%5000)
				c := &ttv1.Comment{CommentId: int64(7_100_000_000_000_000_000 + j), PostId: post, UserId: user,
					PostCreatedAt: timestamppb.New(postCreated), Text: "load test comment " + strconv.Itoa(j), LikeCount: &likes,
					CreatedAt: timestamppb.New(postCreated.Add(time.Duration(j) * time.Second)),
					Author: &ttv1.Person{UserId: &author, Username: "author_" + strconv.Itoa(j%5000)}}
				if j%4 == 3 { // a reply to the comment before it
					parent := c.CommentId - 1
					c.Depth, c.ParentCommentId = 1, &parent
				}
				if err := em.Comment(ctx, b, src, c); err != nil {
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
		v, _, _ := sample("tks_persist_rows_total", map[string]string{"entity": "comment"})
		return v
	}
	wait := func(target float64) {
		deadline := time.Now().Add(10 * time.Minute)
		for processed() < target {
			if time.Now().After(deadline) {
				t.Fatalf("the persister handled %.0f of %.0f rows in 10m", processed(), target)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	base := processed()
	_, bc0, bs0 := sample("tks_persist_batch_seconds", map[string]string{"entity": "comment"})
	start := time.Now()
	prodNew := produce()
	wait(base + float64(n))
	fresh := time.Since(start)
	if got := count(t, s, `SELECT count(*) FROM comments WHERE user_id = $1`, user); got != int64(n) {
		t.Fatalf("comments stored %d, want %d", got, n)
	}
	start = time.Now()
	prodAgain := produce()
	wait(base + 2*float64(n))
	again := time.Since(start)
	_, bc, bs := sample("tks_persist_batch_seconds", map[string]string{"entity": "comment"})
	bc, bs = bc-bc0, bs-bs0

	r := &report{title: fmt.Sprintf("Persister: %d comments", n)}
	r.add("Machine", "%s", machine())
	r.add("New rows (COPY + insert)", "%s, %.0f rows/s (producing took %s)", fresh.Round(time.Millisecond), float64(n)/fresh.Seconds(), prodNew.Round(time.Millisecond))
	r.add("Same rows again (no-op upsert)", "%s, %.0f rows/s (producing took %s)", again.Round(time.Millisecond), float64(n)/again.Seconds(), prodAgain.Round(time.Millisecond))
	if bc > 0 {
		r.add("Comment batches", "%d, mean %.1f ms, %.0f rows each", bc, 1000*bs/float64(bc), 2*float64(n)/float64(bc))
	}
	r.write(t)
	if n := count(t, s, `SELECT count(*) FROM comments WHERE edit_count > 0`); n != 0 {
		t.Errorf("%d comments counted as edited by the same records again", n)
	}
}
