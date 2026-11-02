package media

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

type env struct {
	db      *pgxpool.Pool
	s3      *minio.Client
	prod    *kafka.Producer
	brokers []string
	prefix  string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	e := &env{db: db, brokers: testenv.Kafka(t), prefix: testenv.TopicPrefix()}
	e.prod, err = kafka.NewProducer(kafka.ClientConfig{Brokers: e.brokers, TopicPrefix: e.prefix, Linger: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.prod.Close)
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	if e.s3, err = s3x.Open(ctx, u.Host, "k", "secretsecret", "us-east-1", false, "tks-media"); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) downloader(cdn *http.Client) *Downloader {
	return &Downloader{DB: e.db, S3: e.s3, Bucket: "tks-media", Producer: e.prod, HTTP: cdn, CookieDomains: []string{"127.0.0.1"},
		MaxBytes: 1 << 20}
}

// ready reads n events from tt.media.ready.v1.
func (e *env) ready(t *testing.T, n int) []*commonv1.MediaReady {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.ConsumeTopics(e.prefix+topics.MediaReady),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

func mediaRecord(t *testing.T, m *ttv1.Media, token, ua string) *kgo.Record {
	t.Helper()
	mt := task.NewMedia("00000000-0000-0000-0000-0000000000c1", m)
	mt.GetMedia().ChainToken, mt.GetMedia().UserAgent = token, ua
	v, err := proto.Marshal(mt)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Value: v}
}

func status(t *testing.T, db *pgxpool.Pool, id string) (st, lastErr string) {
	t.Helper()
	_ = db.QueryRow(context.Background(), `SELECT status, coalesce(last_error, '') FROM media WHERE media_id = $1`, id).Scan(&st, &lastErr)
	return st, lastErr
}

// cdn is a media host that, like TikTok's video hosts (T18), refuses a
// video without the tt_chain_token cookie, and records what each request
// carried.
type cdn struct {
	mu   sync.Mutex
	seen map[string]string // path -> Cookie header
	srv  *httptest.Server
}

func newCDN(t *testing.T) *cdn {
	c := &cdn{seen: map[string]string{}}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.seen[r.URL.Path] = r.Header.Get("Cookie")
		c.mu.Unlock()
		switch {
		case strings.HasPrefix(r.URL.Path, "/redirect/"):
			http.Redirect(w, r, r.URL.Query().Get("to"), http.StatusFound)
		case strings.HasPrefix(r.URL.Path, "/video/"):
			if ck, err := r.Cookie("tt_chain_token"); err != nil || ck.Value != "tok" || r.Header.Get("Range") != "bytes=0-" {
				http.Error(w, "Access Denied", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "video/mp4")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("\x00\x00\x00\x18ftypmp42 video bytes"))
		default:
			w.Header().Set("Content-Type", "image/jpeg")
			_, _ = w.Write([]byte("jpeg bytes"))
		}
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *cdn) cookie(path string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.seen[path]
	return v, ok
}

func video(id, src string) *ttv1.Media {
	return &ttv1.Media{MediaId: id, Kind: ttv1.MediaKind_MEDIA_KIND_VIDEO, Variant: "sd", UserId: 42, PostId: proto.Int64(7),
		SourceUrl: src, SourceExpiresAt: timestamppb.New(time.Now().Add(time.Hour)), Width: 576, Height: 1024, DurationMs: 9000,
		Obs: &commonv1.Observation{ObservedAt: timestamppb.New(time.Now().Add(-time.Minute))}}
}

// A video is downloaded with the reading identity's cookie, sent to the
// cookie domains only; it is stored, announced with tks's platform and no
// deletion time, and kept. A video without the cookie is refused and
// marked for the next read; an image needs none.
func TestDownloads(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c := newCDN(t)
	d := e.downloader(c.srv.Client())
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.seen["other:"+r.URL.Path] = r.Header.Get("Cookie")
		c.mu.Unlock()
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte("jpeg bytes"))
	}))
	defer other.Close()
	otherURL := strings.Replace(other.URL, "127.0.0.1", "localhost", 1) // outside the cookie domains

	const (
		vid      = "00000000-0000-0000-0000-0000000000a1"
		refused  = "00000000-0000-0000-0000-0000000000a2"
		img      = "00000000-0000-0000-0000-0000000000a3"
		viaRedir = "00000000-0000-0000-0000-0000000000a4"
		away     = "00000000-0000-0000-0000-0000000000a5"
		expired  = "00000000-0000-0000-0000-0000000000a6"
	)
	handle := func(m *ttv1.Media, token string) {
		t.Helper()
		if err := d.Handle(ctx, mediaRecord(t, m, token, "Mozilla/5.0 test")); err != nil {
			t.Fatal(err)
		}
	}
	handle(video(vid, c.srv.URL+"/video/1.mp4?expire=1&signature=secret"), "tok")
	handle(video(refused, c.srv.URL+"/video/2.mp4?signature=secret"), "")
	cover := &ttv1.Media{MediaId: img, Kind: ttv1.MediaKind_MEDIA_KIND_COVER, Variant: "original", UserId: 42, PostId: proto.Int64(7),
		SourceUrl: c.srv.URL + "/cover/1.jpg"}
	handle(cover, "tok") // a token on an image task is not sent
	// TikTok's play links redirect to the video host: the cookie follows
	// within the cookie domains, not outside them.
	handle(video(viaRedir, c.srv.URL+"/redirect/3?to="+url.QueryEscape(c.srv.URL+"/video/3.mp4")), "tok")
	awayM := video(away, c.srv.URL+"/redirect/4?to="+url.QueryEscape(otherURL+"/video/4.mp4"))
	handle(awayM, "tok")
	old := video(expired, c.srv.URL+"/video/5.mp4")
	old.SourceExpiresAt = timestamppb.New(time.Now().Add(-time.Minute))
	handle(old, "tok")

	for _, id := range []string{vid, img, viaRedir, away} {
		if st, le := status(t, e.db, id); st != "ready" {
			t.Errorf("%s: status %q (%s), want ready", id, st, le)
		}
	}
	if st, le := status(t, e.db, refused); st != "source_expired" || !strings.Contains(le, "403") || strings.Contains(le, "secret") {
		t.Errorf("video without its cookie: status %q, last error %q", st, le)
	}
	if st, _ := status(t, e.db, expired); st != "source_expired" {
		t.Errorf("expired link: status %q", st)
	}
	if _, ok := c.cookie("/video/5.mp4"); ok {
		t.Error("an expired link was fetched")
	}
	if ck, _ := c.cookie("/video/1.mp4"); ck != "tt_chain_token=tok" {
		t.Errorf("video sent cookie %q", ck)
	}
	if ck, _ := c.cookie("/cover/1.jpg"); ck != "" {
		t.Errorf("image sent cookie %q", ck)
	}
	if ck, _ := c.cookie("/video/3.mp4"); ck != "tt_chain_token=tok" {
		t.Errorf("redirect within the cookie domains sent cookie %q", ck)
	}
	if ck, ok := c.cookie("other:/video/4.mp4"); !ok || ck != "" {
		t.Errorf("redirect outside the cookie domains: fetched %v, cookie %q", ok, ck)
	}

	evs := e.ready(t, 4)
	byID := map[string]*commonv1.MediaReady{}
	for _, ev := range evs {
		byID[ev.MediaId] = ev
	}
	ev := byID[vid]
	if ev == nil || ev.Platform != commonv1.Platform_PLATFORM_TIKTOK || ev.Kind != commonv1.MediaKind_MEDIA_KIND_VIDEO ||
		ev.PlatformKind != "video" || ev.AccountId != 42 || ev.GetPostId() != 7 || ev.DeleteAfter != nil || ev.Attempt != 1 ||
		ev.EventId != vid+":ready:1" || ev.Object.GetKey() != ObjectKey(video(vid, ""), "video/mp4") || len(ev.JobIds) != 1 {
		t.Errorf("video's ready event %+v", ev)
	}
	if ev := byID[img]; ev == nil || ev.Kind != commonv1.MediaKind_MEDIA_KIND_COVER || ev.Object.GetContentType() != "image/jpeg" {
		t.Errorf("cover's ready event %+v", ev)
	}
	var deleteAfter *time.Time
	var size int64
	if err := e.db.QueryRow(ctx, `SELECT delete_after, size_bytes FROM media WHERE media_id = $1`, vid).Scan(&deleteAfter, &size); err != nil {
		t.Fatal(err)
	}
	if deleteAfter != nil || size == 0 {
		t.Errorf("stored video: delete_after %v, size %d; want no deletion time", deleteAfter, size)
	}
	if _, err := e.s3.StatObject(ctx, "tks-media", ObjectKey(video(vid, ""), "video/mp4"), minio.StatObjectOptions{}); err != nil {
		t.Errorf("video object: %v", err)
	}

	// Read again (a later check): handed over once, never downloaded again.
	c.mu.Lock()
	delete(c.seen, "/video/1.mp4")
	c.mu.Unlock()
	handle(video(vid, c.srv.URL+"/video/1.mp4?expire=2"), "tok")
	if _, ok := c.cookie("/video/1.mp4"); ok {
		t.Error("a ready video was downloaded again")
	}
	// The refused one comes back with a cookie at the next read.
	handle(video(refused, c.srv.URL+"/video/2.mp4?expire=2"), "tok")
	if st, _ := status(t, e.db, refused); st != "ready" {
		t.Errorf("refused video read again with its cookie: status %q", st)
	}
}

// The ledger records acknowledgements and deletions and deletes nothing:
// a retryable failure is announced again up to the limit; a file the
// deleting service removed is marked deleted and never downloaded again.
func TestLedger(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c := newCDN(t)
	d := e.downloader(c.srv.Client())
	l := &Ledger{DB: e.db, Producer: e.prod, MaxRetryableFailures: 1}
	const id = "00000000-0000-0000-0000-0000000000b1"
	m := video(id, c.srv.URL+"/video/9.mp4")
	if err := d.Handle(ctx, mediaRecord(t, m, "tok", "")); err != nil {
		t.Fatal(err)
	}
	send := func(topic string, msg proto.Message) {
		t.Helper()
		v, err := proto.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Handle(ctx, &kgo.Record{Topic: e.prefix + topic, Value: v}); err != nil {
			t.Fatal(err)
		}
	}
	retryable := &commonv1.MediaProcessed{MediaId: id, Consumer: "analysis", Status: commonv1.ProcessStatus_PROCESS_STATUS_FAILED_RETRYABLE}
	send(topics.MediaProcessed, retryable)
	send(topics.MediaProcessed, retryable) // past the limit: nothing more
	send(topics.MediaProcessed, &commonv1.MediaProcessed{MediaId: id, Consumer: "analysis",
		Status: commonv1.ProcessStatus_PROCESS_STATUS_PROCESSED, ProcessedAt: timestamppb.Now()})
	send(topics.MediaProcessed, &commonv1.MediaProcessed{MediaId: "00000000-0000-0000-0000-0000000000ff", Consumer: "analysis",
		Status: commonv1.ProcessStatus_PROCESS_STATUS_PROCESSED}) // not tks's
	evs := e.ready(t, 2)
	if evs[1].Attempt != 2 || evs[1].EventId != id+":ready:2" || evs[1].Object.GetKey() == "" ||
		evs[1].Kind != commonv1.MediaKind_MEDIA_KIND_VIDEO || evs[1].AccountId != 42 || evs[1].DeleteAfter != nil {
		t.Errorf("announced again: %+v", evs[1])
	}
	var acks int
	var processed *time.Time
	if err := e.db.QueryRow(ctx, `SELECT (SELECT count(*) FROM media_acks WHERE media_id = $1 AND status = 'processed'),
		processed_at FROM media WHERE media_id = $1`, id).Scan(&acks, &processed); err != nil {
		t.Fatal(err)
	}
	if acks != 1 || processed == nil {
		t.Errorf("ack recorded %d times, processed_at %v", acks, processed)
	}
	if st, _ := status(t, e.db, id); st != "ready" {
		t.Errorf("acknowledged file: status %q, want ready (tks deletes nothing)", st)
	}
	if _, err := e.s3.StatObject(ctx, "tks-media", ObjectKey(m, "video/mp4"), minio.StatObjectOptions{}); err != nil {
		t.Errorf("acknowledged file's object: %v (tks deletes nothing)", err)
	}

	// The deleting service removed it.
	send(topics.MediaDeleted, &commonv1.MediaDeleted{MediaId: id, Reason: commonv1.DeleteReason_DELETE_REASON_ACKED,
		DeletedAt: timestamppb.Now(), Platform: commonv1.Platform_PLATFORM_TIKTOK})
	var deletedAt *time.Time
	if err := e.db.QueryRow(ctx, `SELECT deleted_at FROM media WHERE media_id = $1`, id).Scan(&deletedAt); err != nil {
		t.Fatal(err)
	}
	if st, _ := status(t, e.db, id); st != "deleted" || deletedAt == nil {
		t.Errorf("after the deletion event: status %q, deleted_at %v", st, deletedAt)
	}
	c.mu.Lock()
	delete(c.seen, "/video/9.mp4")
	c.mu.Unlock()
	if err := d.Handle(ctx, mediaRecord(t, video(id, c.srv.URL+"/video/9.mp4?expire=3"), "tok", "")); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.cookie("/video/9.mp4"); ok {
		t.Error("a deleted file was downloaded again")
	}
}

func TestInDomains(t *testing.T) {
	for host, want := range map[string]bool{"tiktok.com": true, "v16-webapp-prime.tiktok.com": true, "www.tiktok.com.": true,
		"WWW.TIKTOK.COM": true, "nottiktok.com": false, "tiktok.com.evil.example": false, "p16-sign.tiktokcdn-us.com": false} {
		if got := InDomains(host, []string{"tiktok.com"}); got != want {
			t.Errorf("InDomains(%q) = %v, want %v", host, got, want)
		}
	}
}
