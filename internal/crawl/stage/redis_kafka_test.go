package stage

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/topics"
)

// Tasks parked for later never hold up one due now, in another retry topic
// or in the same one.
func TestRetryRelayIsNotHeldUpByLaterTasks(t *testing.T) {
	brokers := testenv.Kafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	kcfg := kafka.ClientConfig{Brokers: brokers, TopicPrefix: testenv.TopicPrefix()}
	prod, err := kafka.NewProducer(kcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	const orig = "fbs.test.relayed"
	park := func(topic, key string, due time.Time) {
		t.Helper()
		rec := &kgo.Record{Topic: prod.Topic(topic), Key: []byte(key), Value: []byte(key), Headers: []kgo.RecordHeader{
			{Key: kafka.HeaderOrigTopic, Value: []byte(orig)},
			{Key: kafka.HeaderNotBefore, Value: []byte(strconv.FormatInt(due.UnixMilli(), 10))},
		}}
		if err := prod.ProduceSync(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	// Several on one partition of the 1h and 5m topics, then one due now.
	for i := range 3 {
		park(topics.Retry1h, "same-key", time.Now().Add(time.Hour))
		park(topics.Retry5m, "same-key", time.Now().Add(5*time.Minute+time.Duration(i)*time.Second))
	}
	park(topics.Retry30s, "due", time.Now())
	park(topics.Retry5m, "due-5m", time.Now()) // behind later ones in its own topic

	relays, err := RetryRelays(kcfg, prod)
	if err != nil {
		t.Fatal(err)
	}
	rctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, r := range relays {
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.Run(rctx) }()
	}
	defer func() { stop(); wg.Wait() }()

	time.Sleep(2 * time.Second) // the relay is holding the parked tasks
	park(topics.Retry30s, "due-later", time.Now())

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(prod.Topic(orig)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	got := map[string]bool{}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && (!got["due"] || !got["due-later"] || !got["due-5m"]) {
		pctx, pcancel := context.WithTimeout(ctx, time.Second)
		for _, r := range cl.PollFetches(pctx).Records() {
			got[string(r.Value)] = true
		}
		pcancel()
	}
	if !got["due"] || !got["due-later"] || !got["due-5m"] || got["same-key"] {
		t.Fatalf("relayed %v: want the due tasks and none of the parked ones", got)
	}
}

// A reply thread lost after its post's pass (dead-lettered here; a cut
// short thread does the same) marks the post: planning never skips it, and
// clearing the mark takes the exact mark a whole read began with, so one
// set since (a thread of that read lost too) stays.
func TestLostThreadMarksThePost(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	kcfg := kafka.ClientConfig{Brokers: testenv.Kafka(t), TopicPrefix: testenv.TopicPrefix()}
	prod, err := kafka.NewProducer(kcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	d := &Deps{Producer: prod, Jobs: jobs.New(rdb), Ckpt: checkpoint.New(rdb)}
	ps := &PageStage{D: d}
	page := task.NewPage("job1", taskv1.Lane_LANE_P1, fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL, "v:brand", 9, "", &taskv1.Limits{IncludeComments: true})
	n := int64(5)
	post := &fbv1.Post{PostId: 77, PageId: 9, CommentCount: &n, CreatedAt: timestamppb.Now()}
	if err := d.Ckpt.SetPost(ctx, 77, checkpoint.PostCache{Reported: 5, NewestComment: time.Now().Unix(), ThreadsComplete: true}); err != nil {
		t.Fatal(err)
	}
	skipped := func() bool {
		pt, state := ps.commentsTask(ctx, page, fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL, post)
		return pt == nil && state == jobs.PostSkipped
	}
	if !skipped() {
		t.Fatal("an unchanged, complete post is skipped")
	}

	pt := task.NewPost(page, fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL, &taskv1.PostScope{PostId: 77, PageId: 9})
	thread := task.NewThread(pt, fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL, &taskv1.ThreadScope{PostId: 77, PageId: 9, CommentId: 1})
	if err := (&Worker{D: d}).deadLetter(ctx, thread, "max_attempts", "test", nil); err != nil {
		t.Fatal(err)
	}
	first := d.Ckpt.Incomplete(ctx, 77)
	if first == "" || skipped() {
		t.Fatal("a post with a lost thread is marked and not skipped")
	}
	time.Sleep(time.Millisecond)
	if err := d.Ckpt.MarkIncomplete(ctx, 77); err != nil { // lost again during the read
		t.Fatal(err)
	}
	_ = d.Ckpt.ClearIncomplete(ctx, 77, first)
	second := d.Ckpt.Incomplete(ctx, 77)
	if second == "" || second == first {
		t.Fatal("a mark set after the read began must stay")
	}
	_ = d.Ckpt.ClearIncomplete(ctx, 77, second)
	if d.Ckpt.Incomplete(ctx, 77) != "" || !skipped() {
		t.Fatal("the read's own mark is cleared")
	}
}
