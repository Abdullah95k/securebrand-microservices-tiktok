package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
)

// waitCount polls q until it counts want rows.
func waitCount(t *testing.T, s *Stack, want int64, q string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		n := count(t, s, q, args...)
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d rows, want %d: %s", n, want, q)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// mediaReady reads tt.media.ready.v1 until it holds n events.
func mediaReady(t *testing.T, s *Stack, n int) []*commonv1.MediaReady {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Cfg.Kafka.Brokers...), kgo.ConsumeTopics(s.Cfg.Kafka.TopicPrefix+topics.MediaReady),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []*commonv1.MediaReady
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("%d ready events, want %d", len(out), n)
		}
		fs.EachRecord(func(r *kgo.Record) {
			ev := &commonv1.MediaReady{}
			if err := proto.Unmarshal(r.Value, ev); err != nil {
				t.Fatal(err)
			}
			out = append(out, ev)
		})
	}
	return out
}

// TestMediaDownloads runs the crawler with media on (build brief D5's
// switch) and tks-media-downloader against fakett, whose video host refuses
// a video without a tt_chain_token a page issued (T18):
//  1. a backfill (the thumbnail policy) stores and announces every post's
//     cover and the account's avatar, and no video;
//  2. a check (the sd policy) downloads the videos with the reading
//     identity's cookie: none refused; covers handed over before are not
//     downloaded again;
//  3. tks deletes nothing: the files stay, no deletion time is announced,
//     and a file the deleting service removes is only marked deleted.
func TestMediaDownloads(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 21, Accounts: 2, PostsPerAccount: 6, MaxComments: 3, MaxReplies: 1,
		PostSpacing: 20 * time.Hour}, Faults: fakett.Faults{MediaNeedsCookie: true}, Config: func(c *config.Config) {
		c.TT.Media.Include = true
		c.TT.Media.CookieDomains = []string{"127.0.0.1"} // fakett's media host
		c.S3.MediaBucket = "tks-media"
	}})
	s3 := s.StartMedia(t)
	w := s.World
	accs := w.Accounts()
	ctx := context.Background()

	// 1. Backfills.
	refs := make([]string, len(accs))
	for i, a := range accs {
		refs[i] = "u:" + a.Username
	}
	round(t, s, "media-backfill", full, refs...)
	var posts, videoPosts int64
	for _, a := range accs {
		posts += int64(len(a.Posts))
		for _, p := range a.Posts {
			if p.Kind == "video" {
				videoPosts++
			}
		}
	}
	waitCount(t, s, posts, `SELECT count(*) FROM media WHERE kind = 'cover' AND status = 'ready'`)
	waitCount(t, s, int64(len(accs)), `SELECT count(*) FROM media WHERE kind = 'avatar' AND status = 'ready'`)
	if n := s.Fake.VideoDownloads.Load(); n != 0 {
		t.Errorf("%d videos downloaded by backfills (thumbnail policy)", n)
	}
	if n := count(t, s, `SELECT count(*) FROM media WHERE status = 'ready' AND (s3_key IS NULL OR delete_after IS NOT NULL
		OR stored_at IS NULL OR size_bytes IS NULL)`); n != 0 {
		t.Errorf("%d ready rows without an object, or with a deletion time", n)
	}
	covers := count(t, s, `SELECT count(*) FROM media WHERE status = 'ready'`)

	// 2. A check downloads videos with the reading identity's cookie.
	w.Advance(3 * time.Hour)
	np := w.AddPost(accs[0], 0)
	round(t, s, "media-check", incremental, refs...)
	waitCount(t, s, videoPosts+1, `SELECT count(*) FROM media WHERE kind = 'video' AND status = 'ready'`)
	if n := count(t, s, `SELECT count(*) FROM media WHERE kind = 'video' AND status = 'ready' AND post_id = $1`, np.ID); n != 1 {
		t.Error("the new post's video was not stored")
	}
	if got, refused := s.Fake.VideoDownloads.Load(), s.Fake.MediaRefused.Load(); got != videoPosts+1 || refused != 0 {
		t.Errorf("%d videos served and %d refused, want %d and 0", got, refused, videoPosts+1)
	}
	if n := count(t, s, `SELECT count(*) FROM media WHERE kind <> 'video' AND status = 'ready' AND attempts <> 1`); n != 0 {
		t.Errorf("%d files handed over before were downloaded again", n)
	}
	all := count(t, s, `SELECT count(*) FROM media WHERE status = 'ready'`)
	if all != covers+videoPosts+2 { // the videos, and the new post's cover
		t.Errorf("%d files ready, want %d", all, covers+videoPosts+2)
	}
	evs := mediaReady(t, s, int(all))
	byKind := map[commonv1.MediaKind]int{}
	for _, ev := range evs {
		byKind[ev.Kind]++
		if ev.Platform != commonv1.Platform_PLATFORM_TIKTOK || ev.DeleteAfter != nil || ev.AccountId == 0 || ev.Object.GetKey() == "" ||
			ev.Attempt != 1 {
			t.Errorf("ready event %+v", ev)
		}
	}
	if byKind[commonv1.MediaKind_MEDIA_KIND_VIDEO] != int(videoPosts+1) || byKind[commonv1.MediaKind_MEDIA_KIND_COVER] != int(posts+1) {
		t.Errorf("ready events by kind: %v", byKind)
	}
	if t.Failed() {
		t.FailNow()
	}

	// 3. The deleting service removes a video and says so; tks only marks it.
	var id, bucket, key string
	if err := s.DB.QueryRow(ctx, `SELECT media_id::text, s3_bucket, s3_key FROM media WHERE kind = 'video' AND post_id = $1`, np.ID).
		Scan(&id, &bucket, &key); err != nil {
		t.Fatal(err)
	}
	b := s.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.MediaDeleted, id, &commonv1.MediaDeleted{EventId: id + ":acked", MediaId: id,
		Object: &commonv1.S3Object{Bucket: bucket, Key: key}, Reason: commonv1.DeleteReason_DELETE_REASON_ACKED,
		DeletedAt: timestamppb.New(w.Clock()), Platform: commonv1.Platform_PLATFORM_TIKTOK}); err != nil {
		t.Fatal(err)
	}
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	waitCount(t, s, 1, `SELECT count(*) FROM media WHERE media_id = $1 AND status = 'deleted' AND deleted_at IS NOT NULL`, id)
	var objects int
	for o := range s3.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: "media/", Recursive: true}) {
		if o.Err != nil {
			t.Fatal(o.Err)
		}
		objects++
	}
	if int64(objects) != all {
		t.Errorf("%d objects in the bucket, want all %d: tks deletes none", objects, all)
	}
	// Read again: the removed file is not downloaded again.
	before := s.Fake.VideoDownloads.Load()
	round(t, s, "media-again", incremental, refs[0])
	time.Sleep(2 * time.Second) // the downloader's tasks of that check
	if got := s.Fake.VideoDownloads.Load(); got != before {
		t.Errorf("%d videos downloaded again after the files were handed over", got-before)
	}
}
