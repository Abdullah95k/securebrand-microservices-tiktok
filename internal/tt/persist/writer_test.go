package persist

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

var secret = []byte("tks-test-secret")

func newWriter(t *testing.T) *Writer {
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
	return &Writer{DB: db}
}

func obs(at time.Time, hash string) *commonv1.Observation {
	return &commonv1.Observation{ObservedAt: timestamppb.New(at), ContentHash: []byte(hash)}
}

func one[T any](t *testing.T, db *pgxpool.Pool, q string, args ...any) T {
	t.Helper()
	var v T
	if err := db.QueryRow(context.Background(), q, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func account(at time.Time, src ttv1.AccountSource, username string, followers int64, exact bool) *ttv1.Account {
	a := &ttv1.Account{Obs: obs(at, username+string(rune(followers))), UserId: 7001, SecUid: "sec-7001", Username: username,
		Nickname: "Nick " + username, Bio: "bio", AuthorKey: emit.AuthorKey(secret, 7001), Source: src,
		Verified: proto.Bool(true), Counts: &ttv1.AccountCounts{Followers: proto.Int64(followers), Videos: proto.Int64(10), Exact: exact}}
	if src == ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE {
		a.BioLink = proto.String("https://example.org")
	}
	return a
}

// Exact counts and the bio link come only from the profile page; a list
// read refreshes the rest, a rename adds to the username history, and an
// older observation, a replay or a status record cannot undo a newer one.
func TestAccountsFromListAndPage(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	if _, err := ttstore.EnsurePartitions(ctx, w.DB, t0, storeWindow); err != nil {
		t.Fatal(err)
	}
	list, page := ttv1.AccountSource_ACCOUNT_SOURCE_POST_LIST, ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE
	if err := w.Accounts(ctx, []*ttv1.Account{account(t0, list, "first", 1200, false)}); err != nil {
		t.Fatal(err)
	}
	if got := one[int64](t, w.DB, `SELECT follower_count FROM accounts WHERE user_id = 7001`); got != 1200 {
		t.Fatalf("rounded counts before any page read: %d", got)
	}
	// A check's page and list reads in one batch: rounded counts don't
	// replace exact ones, and no bio link means none was read, not that it
	// was removed.
	if err := w.Accounts(ctx, []*ttv1.Account{account(t0.Add(2*time.Minute), list, "second", 1300, false),
		account(t0.Add(time.Minute), page, "first", 1234, true)}); err != nil {
		t.Fatal(err)
	}
	var (
		username, link string
		followers      int64
		pageRead       *time.Time
	)
	if err := w.DB.QueryRow(ctx, `SELECT username, bio_link, follower_count, page_read_at FROM accounts WHERE user_id = 7001`).
		Scan(&username, &link, &followers, &pageRead); err != nil {
		t.Fatal(err)
	}
	if username != "second" || link != "https://example.org" || followers != 1234 || pageRead == nil || !pageRead.Equal(t0.Add(time.Minute)) {
		t.Fatalf("account: %s %s %d %v", username, link, followers, pageRead)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM account_usernames WHERE user_id = 7001`); n != 2 {
		t.Fatalf("%d usernames, want 2", n)
	}
	// Each read changed something: three snapshots.
	if n := one[int64](t, w.DB, `SELECT count(*) FROM account_snapshots WHERE user_id = 7001`); n != 3 {
		t.Fatalf("%d snapshots, want 3", n)
	}
	// Older observations and replays change nothing.
	old := account(t0.Add(30*time.Second), page, "first", 999, true)
	if err := w.Accounts(ctx, []*ttv1.Account{old, account(t0.Add(2*time.Minute), list, "second", 1300, false)}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, `SELECT username FROM accounts WHERE user_id = 7001`); got != "second" {
		t.Fatalf("an older read overwrote the account: %s", got)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM account_snapshots WHERE user_id = 7001`); n != 3 {
		t.Fatalf("%d snapshots after a replay, want 3", n)
	}
	// A read without counts keeps them.
	bare := account(t0.Add(150*time.Second), list, "second", 0, false)
	bare.Counts = nil
	if err := w.Accounts(ctx, []*ttv1.Account{bare}); err != nil {
		t.Fatal(err)
	}
	if got := one[int64](t, w.DB, `SELECT follower_count FROM accounts WHERE user_id = 7001`); got != 1234 {
		t.Fatalf("a read without counts changed them: %d", got)
	}
	// A status record: the account went private, then came back.
	priv := &ttv1.Account{Obs: obs(t0.Add(3*time.Minute), "p"), UserId: 7001, Status: ttv1.AccountStatus_ACCOUNT_STATUS_PRIVATE}
	if err := w.Accounts(ctx, []*ttv1.Account{priv}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, `SELECT status FROM accounts WHERE user_id = 7001`); got != "private" {
		t.Fatalf("status %s", got)
	}
	if err := w.Accounts(ctx, []*ttv1.Account{account(t0.Add(4*time.Minute), list, "second", 1300, false)}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, `SELECT status FROM accounts WHERE user_id = 7001`); got != "active" {
		t.Fatalf("status %s", got)
	}
	// A status record of an account never stored, with its secUid.
	gone := &ttv1.Account{Obs: obs(t0, "g"), UserId: 7002, SecUid: "sec-7002", AuthorKey: emit.AuthorKey(secret, 7002),
		Status: ttv1.AccountStatus_ACCOUNT_STATUS_BANNED}
	if err := w.Accounts(ctx, []*ttv1.Account{gone}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, `SELECT status FROM accounts WHERE user_id = 7002`); got != "banned" {
		t.Fatalf("status %s", got)
	}
}

func post(id int64, at time.Time, likes int64, pinned *bool) *ttv1.Post {
	created := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC).Add(time.Duration(id) * time.Hour)
	return &ttv1.Post{Obs: obs(at, "post"+string(rune(likes))), PostId: id, UserId: 7001, Kind: ttv1.PostKind_POST_KIND_VIDEO,
		CreatedAt: timestamppb.New(created), Caption: "caption #tag @someone", Pinned: pinned,
		Hashtags: []*ttv1.Hashtag{{Id: 5, Name: "Tag"}}, MusicId: proto.Int64(9),
		TextRanges: []*ttv1.TextRange{{Offset: 13, Length: 8, Type: "mention", Name: "someone",
			Person: &ttv1.Person{AuthorKey: emit.AuthorKey(secret, 8001), UserId: proto.Int64(8001), Username: "someone"}}},
		Counts: &ttv1.PostCounts{Plays: proto.Int64(100), Likes: proto.Int64(likes), Comments: proto.Int64(3)}}
}

func metricsOf(p *ttv1.Post, at time.Time) *ttv1.PostMetrics {
	return &ttv1.PostMetrics{Obs: obs(at, "m"), PostId: p.PostId, UserId: p.UserId, PostCreatedAt: p.CreatedAt,
		CapturedAt: timestamppb.New(at), Counts: proto.Clone(p.Counts).(*ttv1.PostCounts)}
}

// Posts are upserted once per content, keep the pinned flag a read
// without it does not carry, index and mention rows follow, and a metrics
// row is written only when the counts changed.
func TestPostsAndMetricsOnChange(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := ttstore.EnsurePartitions(ctx, w.DB, now, storeWindow); err != nil {
		t.Fatal(err)
	}
	p := post(1, now.Add(-3*time.Minute), 10, proto.Bool(true))
	if err := w.Posts(ctx, []*ttv1.Post{p, p}); err != nil {
		t.Fatal(err)
	}
	if err := w.PostMetrics(ctx, []*ttv1.PostMetrics{metricsOf(p, now.Add(-3*time.Minute))}); err != nil {
		t.Fatal(err)
	}
	// Same counts a minute later: no metrics row. A post page read without
	// the pinned flag keeps it.
	same := post(1, now.Add(-2*time.Minute), 10, nil)
	if err := w.Posts(ctx, []*ttv1.Post{same}); err != nil {
		t.Fatal(err)
	}
	if err := w.PostMetrics(ctx, []*ttv1.PostMetrics{metricsOf(same, now.Add(-2*time.Minute))}); err != nil {
		t.Fatal(err)
	}
	grown := post(1, now.Add(-time.Minute), 11, proto.Bool(false))
	if err := w.Posts(ctx, []*ttv1.Post{grown}); err != nil {
		t.Fatal(err)
	}
	if err := w.PostMetrics(ctx, []*ttv1.PostMetrics{metricsOf(grown, now.Add(-time.Minute))}); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM post_metrics WHERE post_id = 1`); n != 2 {
		t.Fatalf("%d metrics rows, want 2 (one per change)", n)
	}
	var likes int64
	var pinned bool
	var tags []string
	if err := w.DB.QueryRow(ctx, `SELECT like_count, is_pinned, hashtags FROM posts WHERE post_id = 1`).Scan(&likes, &pinned, &tags); err != nil {
		t.Fatal(err)
	}
	if likes != 11 || pinned || len(tags) != 1 || tags[0] != "tag" {
		t.Fatalf("post: likes %d pinned %v tags %v", likes, pinned, tags)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM post_index WHERE post_id = 1 AND user_id = 7001`); n != 1 {
		t.Fatal("no post_index row")
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM post_mentions WHERE post_id = 1 AND user_id = 8001`); n != 1 {
		t.Fatal("no mention row")
	}
	// A replay of the first read changes nothing.
	if err := w.Posts(ctx, []*ttv1.Post{p}); err != nil {
		t.Fatal(err)
	}
	if got := one[int64](t, w.DB, `SELECT like_count FROM posts WHERE post_id = 1`); got != 11 {
		t.Fatalf("a replay overwrote the post: likes %d", got)
	}
}

func comment(id int64, parent int64, at time.Time, text string, author int64) *ttv1.Comment {
	created := time.Date(2026, 9, 1, 13, 0, 0, 0, time.UTC)
	c := &ttv1.Comment{Obs: obs(at, text), CommentId: id, PostId: 1, UserId: 7001, PostCreatedAt: timestamppb.New(created),
		Author: &ttv1.Person{AuthorKey: emit.AuthorKey(secret, author), UserId: proto.Int64(author), Username: "u" + text},
		Text:   text, CreatedAt: timestamppb.New(created.Add(time.Minute)), LikeCount: proto.Int64(1),
		Status: ttv1.CommentStatus_COMMENT_STATUS_VISIBLE}
	if parent != 0 {
		c.ParentCommentId, c.Depth = proto.Int64(parent), 1
	}
	return c
}

// Comments: an edit is counted, a deleted comment and its replies come
// back together when the comment is seen again, and every comment and
// author is stored, with the author's predicted age band (tks erases no
// one: ADR 0007 §14).
func TestCommentsEditsDeletionsAndAuthors(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	if err := w.Comments(ctx, []*ttv1.Comment{comment(10, 0, t0, "hello", 9001), comment(11, 10, t0, "reply", 9002)}); err != nil {
		t.Fatal(err)
	}
	if err := w.Comments(ctx, []*ttv1.Comment{comment(10, 0, t0.Add(time.Minute), "hello again", 9001)}); err != nil {
		t.Fatal(err)
	}
	if got := one[int32](t, w.DB, `SELECT edit_count FROM comments WHERE comment_id = 10`); got != 1 {
		t.Fatalf("edit_count %d", got)
	}
	del := t0.Add(2 * time.Minute)
	for _, id := range []int64{10, 11} {
		if _, err := MarkDeleted(ctx, w.DB, []*ttv1.Comment{{CommentId: id, PostId: 1, PostCreatedAt: comment(id, 0, del, "", 1).PostCreatedAt,
			Obs: obs(del, "")}}); err != nil {
			t.Fatal(err)
		}
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM comments WHERE status = 'deleted'`); n != 2 {
		t.Fatalf("%d deleted", n)
	}
	if err := w.Comments(ctx, []*ttv1.Comment{comment(10, 0, t0.Add(3*time.Minute), "hello again", 9001)}); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM comments WHERE status = 'visible'`); n != 2 {
		t.Fatalf("%d visible after the comment came back, want 2 (with its reply)", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM people`); n != 2 {
		t.Fatalf("%d people", n)
	}
	// Any author and account: all written, the age band with the person.
	aged := comment(12, 0, t0, "kept", 9003)
	aged.Author.PredictedAgeGroup = "band"
	other := comment(13, 0, t0, "other account", 9004)
	other.UserId = 7003
	if err := w.Comments(ctx, []*ttv1.Comment{aged, other}); err != nil {
		t.Fatal(err)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM comments WHERE comment_id IN (12, 13) AND text IS NOT NULL AND NOT redacted`); n != 2 {
		t.Fatalf("%d of 2 comments stored whole", n)
	}
	if got := one[string](t, w.DB, `SELECT predicted_age_group FROM people WHERE user_id = 9003`); got != "band" {
		t.Fatalf("age band %q", got)
	}
	// A later read without the band keeps the stored one (nothing is
	// deleted); a new band replaces it.
	again := comment(12, 0, t0.Add(48*time.Hour), "kept", 9003)
	if err := w.Comments(ctx, []*ttv1.Comment{again}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, `SELECT predicted_age_group FROM people WHERE user_id = 9003`); got != "band" {
		t.Fatalf("age band %q after a read without one", got)
	}
	newer := comment(12, 0, t0.Add(96*time.Hour), "kept", 9003)
	newer.Author.PredictedAgeGroup = "other-band"
	if err := w.Comments(ctx, []*ttv1.Comment{newer}); err != nil {
		t.Fatal(err)
	}
	if got := one[string](t, w.DB, `SELECT predicted_age_group FROM people WHERE user_id = 9003`); got != "other-band" {
		t.Fatalf("age band %q, want the newer one", got)
	}
}

// The same media and music are written once.
func TestMediaAndMusicOnce(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	m := &ttv1.Media{Obs: obs(now, "m"), MediaId: "6f1c1a2e-6a3b-5c1d-8e2f-0a1b2c3d4e5f", Kind: ttv1.MediaKind_MEDIA_KIND_COVER,
		Variant: "original", UserId: 7005, PostId: proto.Int64(2), SourceUrl: "https://example.org/c.jpg"}
	mu := &ttv1.Music{Obs: obs(now, "mu"), MusicId: 9, Title: "sound", Original: proto.Bool(true)}
	for range 2 {
		if err := w.Media(ctx, []*ttv1.Media{m}); err != nil {
			t.Fatal(err)
		}
		if err := w.Music(ctx, []*ttv1.Music{mu}); err != nil {
			t.Fatal(err)
		}
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM media WHERE kind = 'cover' AND variant = 'original'`); n != 1 {
		t.Fatalf("%d media rows", n)
	}
	if n := one[int64](t, w.DB, `SELECT count(*) FROM music WHERE music_id = 9`); n != 1 {
		t.Fatalf("%d music rows", n)
	}
}

var storeWindow = store.PartitionWindow{MonthsBack: 3, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}

// A new post keeps when its row was first written and the mode of the read
// that found it; its delay from createTime counts for build brief §8 A2 only
// when a scheduled check or a metrics walk found it, and only once.
func TestNewPostDelay(t *testing.T) {
	w := newWriter(t)
	ctx := context.Background()
	p1 := post(1, time.Now(), 10, nil)
	if _, err := ttstore.EnsurePartitions(ctx, w.DB, p1.CreatedAt.AsTime(), storeWindow); err != nil {
		t.Fatal(err)
	}
	stored := p1.CreatedAt.AsTime().Add(90 * time.Minute)
	w.Now = func() time.Time { return stored }
	count := func(mode string) uint64 {
		t.Helper()
		var m dto.Metric
		if err := metrics.PostStoreDelay.WithLabelValues(mode).(prometheus.Histogram).Write(&m); err != nil {
			t.Fatal(err)
		}
		return m.GetHistogram().GetSampleCount()
	}
	sum := func(mode string) float64 {
		t.Helper()
		var m dto.Metric
		_ = metrics.PostStoreDelay.WithLabelValues(mode).(prometheus.Histogram).Write(&m)
		return m.GetHistogram().GetSampleSum()
	}
	inc0, incSum0, ref0, full0 := count("incremental"), sum("incremental"), count("refresh"), count("full")

	with := func(p *ttv1.Post, mode commonv1.CrawlMode) *ttv1.Post {
		p.Obs.Mode = mode
		return p
	}
	backfilled := with(post(2, time.Now(), 10, nil), commonv1.CrawlMode_CRAWL_MODE_FULL)
	walked := with(post(3, time.Now(), 10, nil), commonv1.CrawlMode_CRAWL_MODE_REFRESH)
	if err := w.Posts(ctx, []*ttv1.Post{with(p1, commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL), backfilled, walked}); err != nil {
		t.Fatal(err)
	}
	// Read again by later checks: not new.
	w.Now = func() time.Time { return stored.Add(3 * time.Hour) }
	if err := w.Posts(ctx, []*ttv1.Post{with(post(1, time.Now().Add(time.Minute), 11, nil), commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL),
		with(post(2, time.Now().Add(time.Minute), 11, nil), commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL)}); err != nil {
		t.Fatal(err)
	}
	if got := count("incremental") - inc0; got != 1 {
		t.Errorf("%d incremental delays observed, want 1", got)
	}
	if got := sum("incremental") - incSum0; got != 5400 {
		t.Errorf("delay %v s, want 5400 (1.5 h)", got)
	}
	if count("refresh")-ref0 != 1 || count("full") != full0 {
		t.Errorf("refresh %d, full %d delays observed; want 1 and 0", count("refresh")-ref0, count("full")-full0)
	}
	for id, want := range map[int64]string{1: "incremental", 2: "full", 3: "refresh"} {
		var mode string
		var at time.Time
		if err := w.DB.QueryRow(ctx, `SELECT first_mode, stored_at FROM posts WHERE post_id = $1`, id).Scan(&mode, &at); err != nil {
			t.Fatal(err)
		}
		if mode != want || !at.Equal(stored) {
			t.Errorf("post %d: first_mode %q, stored_at %v; want %q, %v", id, mode, at, want, stored)
		}
	}
}
