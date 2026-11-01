package erasure

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
	ttpersist "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/topics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

var secret = []byte("tks-erasure-test-secret")

// Synthetic users: two monitored accounts, a commenter, a replier.
const (
	acct      = int64(7001)
	other     = int64(7002)
	commenter = int64(9001)
	replier   = int64(9002)
)

var t0 = time.Now().UTC().Truncate(time.Second).Add(-48 * time.Hour)

func key(uid int64) []byte { return emit.AuthorKey(secret, uid) }

func person(uid int64, name string) *ttv1.Person {
	return &ttv1.Person{AuthorKey: key(uid), UserId: proto.Int64(uid), SecUid: fmt.Sprintf("sec-%d", uid), Username: name, Nickname: "N " + name}
}

func obs(at time.Time, h string) *commonv1.Observation {
	return &commonv1.Observation{ObservedAt: timestamppb.New(at), ContentHash: []byte(h)}
}

type fixture struct {
	db       *pgxpool.Pool
	eraser   *Eraser
	prefix   string
	brokers  []string
	postAcct *ttv1.Post // acct's post, mentioning the commenter
	postOth  *ttv1.Post // other's post, commented on by everyone
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	brokers := testenv.Kafka(t)
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := ttstore.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 3, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	prefix := testenv.TopicPrefix()
	producer, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, ClientID: "erasure-test", TopicPrefix: prefix, Linger: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	specs := []kafka.TopicSpec{{Name: topics.Erasure, Partitions: 1}, {Name: topics.MediaDeleted, Partitions: 1}}
	if err := kafka.EnsureTopics(ctx, producer.Client(), prefix, specs, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = kadm.NewClient(producer.Client()).DeleteTopics(context.Background(), prefix+topics.Erasure, prefix+topics.MediaDeleted)
	})
	f := &fixture{db: db, prefix: prefix, brokers: brokers, eraser: &Eraser{DB: db, Producer: producer, HMACKey: secret}}
	f.seed(t)
	return f
}

// seed stores, through tks's writer: acct (renamed once) with a post that
// mentions the commenter and is commented on by other; other with a post
// commented on by acct, the commenter and the replier (whose reply tags
// the commenter); media, a schedule under a username and one under the id,
// a registration, a job naming acct by its old username, the list cursor.
func (f *fixture) seed(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	w := &ttpersist.Writer{DB: f.db}
	page := ttv1.AccountSource_ACCOUNT_SOURCE_PROFILE_PAGE
	ac := func(at time.Time, uid int64, name string) *ttv1.Account {
		return &ttv1.Account{Obs: obs(at, name), UserId: uid, SecUid: fmt.Sprintf("sec-%d", uid), Username: name, Nickname: "N " + name,
			Bio: "bio", BioLink: proto.String("https://example.org"), AuthorKey: key(uid), Source: page,
			Counts: &ttv1.AccountCounts{Followers: proto.Int64(int64(len(name))), Exact: true}}
	}
	if err := w.Accounts(ctx, []*ttv1.Account{ac(t0, acct, "acct_old"), ac(t0.Add(time.Hour), acct, "acct_new"), ac(t0, other, "other")}); err != nil {
		t.Fatal(err)
	}
	mention := func(uid int64, name string, at int32) *ttv1.TextRange {
		return &ttv1.TextRange{Offset: at, Length: int32(len(name)) + 1, Type: "mention", Name: name, Person: person(uid, name)}
	}
	mk := func(uid int64, i int, ranges ...*ttv1.TextRange) *ttv1.Post {
		at := t0.Add(time.Duration(i) * time.Hour)
		return &ttv1.Post{Obs: obs(t0.Add(30*time.Hour), fmt.Sprint(uid, i)), PostId: at.Unix()<<32 | int64(i), UserId: uid,
			Kind: ttv1.PostKind_POST_KIND_VIDEO, CreatedAt: timestamppb.New(at), Caption: "hi @commenter", TextRanges: ranges,
			Counts: &ttv1.PostCounts{Plays: proto.Int64(1), Comments: proto.Int64(4)}}
	}
	f.postAcct, f.postOth = mk(acct, 1, mention(commenter, "commenter", 3)), mk(other, 2)
	if err := w.Posts(ctx, []*ttv1.Post{f.postAcct, f.postOth}); err != nil {
		t.Fatal(err)
	}
	if err := w.PostMetrics(ctx, []*ttv1.PostMetrics{{Obs: obs(t0, "m"), PostId: f.postAcct.PostId, UserId: acct,
		PostCreatedAt: f.postAcct.CreatedAt, CapturedAt: timestamppb.New(t0.Add(30 * time.Hour)), Counts: f.postAcct.Counts}}); err != nil {
		t.Fatal(err)
	}
	cm := func(p *ttv1.Post, id, parent int64, author *ttv1.Person, text string, ranges ...*ttv1.TextRange) *ttv1.Comment {
		c := &ttv1.Comment{Obs: obs(t0.Add(31*time.Hour), text), CommentId: id, PostId: p.PostId, UserId: p.UserId,
			PostCreatedAt: p.CreatedAt, Author: author, Text: text, TextRanges: ranges, CreatedAt: timestamppb.New(t0.Add(3 * time.Hour)),
			Status: ttv1.CommentStatus_COMMENT_STATUS_VISIBLE}
		if parent != 0 {
			c.ParentCommentId, c.Depth = proto.Int64(parent), 1
		}
		return c
	}
	if err := w.Comments(ctx, []*ttv1.Comment{
		cm(f.postAcct, 11, 0, person(other, "other"), "nice"),
		cm(f.postOth, 21, 0, person(commenter, "commenter"), "first"),
		cm(f.postOth, 22, 21, person(replier, "replier"), "@commenter agreed", mention(commenter, "commenter", 0)),
		cm(f.postOth, 23, 0, person(acct, "acct_new"), "from the account"),
	}); err != nil {
		t.Fatal(err)
	}
	md := func(name string, kind ttv1.MediaKind, uid int64, post *int64, author []byte) *ttv1.Media {
		return &ttv1.Media{Obs: obs(t0, name), MediaId: uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String(), Kind: kind,
			Variant: "original", UserId: uid, PostId: post, AuthorKey: author, SourceUrl: "https://example.org/" + name}
	}
	if err := w.Media(ctx, []*ttv1.Media{
		md("cover", ttv1.MediaKind_MEDIA_KIND_COVER, acct, proto.Int64(f.postAcct.PostId), nil),
		md("acct-avatar", ttv1.MediaKind_MEDIA_KIND_AVATAR, acct, nil, nil),
		md("commenter-avatar", ttv1.MediaKind_MEDIA_KIND_AVATAR, 0, nil, key(commenter)),
		md("other-cover", ttv1.MediaKind_MEDIA_KIND_COVER, other, proto.Int64(f.postOth.PostId), nil),
	}); err != nil {
		t.Fatal(err)
	}
	client, _ := uuid.NewV7()
	job, _ := uuid.NewV7()
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, 'c', 'p', '\x00', '{data:read}')`, []any{client}},
		{`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at) VALUES
			('u:acct_new', 7001, 9900, now()), ('id:7001', 7001, 9900, now()), ('u:other', 7002, 9900, now())`, nil},
		{`INSERT INTO account_registrations (api_client_id, account_ref) VALUES ($1, 'u:acct_new'), ($1, 'u:other')`, []any{client}},
		{`INSERT INTO jobs (job_id, kind, lane, mode, account_ref, params, status) VALUES
			($1, 'on_demand', 0, 'full', 'u:acct_old', '{"account": "@acct_old", "limits": {}}', 'completed')`, []any{job}},
		{`INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', 7001, 'complete'), ('account_list', 7002, 'complete')`, nil},
	} {
		if _, err := f.db.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *fixture) request(t *testing.T, subject string, authorKey []byte, user int64, input string) uuid.UUID {
	t.Helper()
	id, _ := uuid.NewV7()
	var uid *int64
	if user != 0 {
		uid = &user
	}
	if _, err := f.db.Exec(context.Background(), `INSERT INTO erasure_requests (id, subject_type, author_key, user_id, input, status)
		VALUES ($1,$2,$3,$4,$5,'pending')`, id, subject, authorKey, uid, input); err != nil {
		t.Fatal(err)
	}
	return id
}

func (f *fixture) count(t *testing.T, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := f.db.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func (f *fixture) done(t *testing.T, id uuid.UUID) (stats string, input string) {
	t.Helper()
	var status string
	var errText *string
	if err := f.db.QueryRow(context.Background(), `SELECT status, coalesce(stats::text, ''), input::text, error FROM erasure_requests WHERE id = $1`, id).
		Scan(&status, &stats, &input, &errText); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Fatalf("request %s: %s (%v)", id, status, errText)
	}
	return stats, input
}

// events reads n erasure events.
func (f *fixture) events(t *testing.T, n int) []*commonv1.ErasureEvent {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(f.brokers...), kgo.ConsumeTopics(f.prefix+topics.Erasure),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var out []*commonv1.ErasureEvent
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("%d erasure events, want %d", len(out), n)
		}
		fs.EachRecord(func(r *kgo.Record) {
			ev := &commonv1.ErasureEvent{}
			if err := proto.Unmarshal(r.Value, ev); err != nil {
				t.Fatal(err)
			}
			out = append(out, ev)
		})
	}
	return out
}

// An author's comments keep only their ids and key, their people row and
// avatar go, and every stored mention of them keeps only the key; the
// request forgets the name it was given; a second run finds nothing left.
func TestAuthorErasure(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	k := key(commenter)
	id := f.request(t, SubjectAuthor, k, 0, `{"username": "commenter"}`)
	if err := f.eraser.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	stats, input := f.done(t, id)
	if strings.Contains(input, "commenter") {
		t.Fatalf("the done request keeps the name: %s", input)
	}
	for _, s := range []string{`"comments_redacted": 1`, `"people_deleted": 1`, `"mentions_scrubbed": 2`} {
		if !strings.Contains(stats, s) {
			t.Errorf("stats %s lack %s", stats, s)
		}
	}
	if n := f.count(t, `SELECT count(*) FROM comments WHERE author_key = $1 AND (NOT redacted OR text IS NOT NULL OR author_user_id IS NOT NULL)`, k); n != 0 {
		t.Errorf("%d comments of the author not redacted", n)
	}
	if n := f.count(t, `SELECT count(*) FROM comments WHERE author_key = $1`, k); n != 1 {
		t.Errorf("the redacted comment is gone (%d rows): replies keep their parent", n)
	}
	if n := f.count(t, `SELECT count(*) FROM people WHERE author_key = $1`, k); n != 0 {
		t.Error("people row kept")
	}
	if n := f.count(t, `SELECT count(*) FROM media WHERE author_key = $1`, k); n != 0 {
		t.Error("avatar kept")
	}
	if n := f.count(t, `SELECT count(*) FROM post_mentions WHERE author_key = $1`, k); n != 0 {
		t.Error("mention row kept")
	}
	// The reply that tags them and the caption that mentions them keep the
	// mention's place and key, and nothing that names them.
	for _, q := range []string{`SELECT text_ranges::text FROM comments WHERE comment_id = 22`,
		`SELECT text_ranges::text FROM posts WHERE post_id = $1`} {
		var ranges string
		args := []any{}
		if strings.Contains(q, "posts") {
			args = append(args, f.postAcct.PostId)
		}
		if err := f.db.QueryRow(ctx, q, args...).Scan(&ranges); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(ranges, "commenter") || strings.Contains(ranges, "9001") || strings.Contains(ranges, "sec-") ||
			!strings.Contains(ranges, `"length"`) {
			t.Errorf("%s: %s", q, ranges)
		}
	}
	if n := f.count(t, `SELECT count(*) FROM comments WHERE comment_id = 22 AND text = '@commenter agreed' AND NOT redacted`); n != 1 {
		t.Error("the replier's own comment changed")
	}
	if n := f.count(t, `SELECT count(*) FROM suppression WHERE subject_type = 'author' AND subject_key = $1`, hex.EncodeToString(k)); n != 1 {
		t.Error("no suppression entry")
	}
	ev := f.events(t, 1)[0]
	if ev.SubjectType != commonv1.SubjectType_SUBJECT_TYPE_AUTHOR || string(ev.AuthorKey) != string(k) || ev.Platform != commonv1.Platform_PLATFORM_TIKTOK ||
		ev.RequestId != id.String() {
		t.Errorf("event %v", ev)
	}
	// Again: idempotent, nothing left to change.
	again := f.request(t, SubjectAuthor, k, 0, `{}`)
	if err := f.eraser.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if stats, _ := f.done(t, again); !strings.Contains(stats, `"comments_redacted": 0`) || !strings.Contains(stats, `"mentions_scrubbed": 0`) {
		t.Errorf("a second erasure changed things: %s", stats)
	}
}

// An account's erasure deletes everything stored of it, takes its names
// out of schedules and jobs, erases it as an author too (its comment on
// another account's post), and leaves the other account alone.
func TestAccountErasure(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	before := map[string]int64{}
	tables := []string{"posts", "post_index", "post_metrics", "comments", "media", "account_snapshots", "account_usernames", "accounts"}
	for _, tb := range tables {
		before[tb] = f.count(t, `SELECT count(*) FROM `+tb+` WHERE user_id = $1`, other)
	}
	id := f.request(t, SubjectAccount, nil, acct, `{"account": "u:acct_new"}`)
	if err := f.eraser.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	_, input := f.done(t, id)
	if strings.Contains(input, "acct") {
		t.Fatalf("the done request keeps the name: %s", input)
	}
	for _, tb := range tables {
		if n := f.count(t, `SELECT count(*) FROM `+tb+` WHERE user_id = $1`, acct); n != 0 {
			t.Errorf("%d rows of the account in %s", n, tb)
		}
		if n := f.count(t, `SELECT count(*) FROM `+tb+` WHERE user_id = $1`, other); n != before[tb] {
			t.Errorf("%s: %d rows of the other account, %d before", tb, n, before[tb])
		}
	}
	if n := f.count(t, `SELECT count(*) FROM post_mentions WHERE post_id = $1`, f.postAcct.PostId); n != 0 {
		t.Error("mention rows of the account's post kept")
	}
	if n := f.count(t, `SELECT count(*) FROM crawl_cursors WHERE scope_id = $1`, acct); n != 0 {
		t.Error("list cursor kept")
	}
	if n := f.count(t, `SELECT count(*) FROM comments WHERE comment_id = 23 AND redacted AND text IS NULL`); n != 1 {
		t.Error("the account's comment on another post is not redacted")
	}
	if n := f.count(t, `SELECT count(*) FROM account_schedule WHERE account_ref = 'u:acct_new'`); n != 0 {
		t.Error("the schedule under the username is kept")
	}
	if n := f.count(t, `SELECT count(*) FROM account_schedule WHERE account_ref = 'id:7001' AND NOT enabled`); n != 1 {
		t.Error("the schedule under the id is not disabled")
	}
	if n := f.count(t, `SELECT count(*) FROM account_schedule WHERE account_ref = 'u:other' AND enabled`); n != 1 {
		t.Error("the other account's schedule changed")
	}
	if n := f.count(t, `SELECT count(*) FROM jobs WHERE account_ref = 'id:7001' AND user_id = 7001 AND NOT params ? 'account' AND params ? 'limits'`); n != 1 {
		t.Error("the job still names the account")
	}
	for typ, k := range map[string]string{"account": "7001", "author": hex.EncodeToString(key(acct))} {
		if n := f.count(t, `SELECT count(*) FROM suppression WHERE subject_type = $1 AND subject_key = $2`, typ, k); n != 1 {
			t.Errorf("no %s suppression entry", typ)
		}
	}
	ev := f.events(t, 1)[0]
	if ev.SubjectType != commonv1.SubjectType_SUBJECT_TYPE_ACCOUNT || ev.AccountId != acct || string(ev.AuthorKey) != string(key(acct)) {
		t.Errorf("event %v", ev)
	}
	// What the crawler sends after the erasure is not written back.
	w := &ttpersist.Writer{DB: f.db}
	if err := w.Posts(ctx, []*ttv1.Post{f.postAcct}); err != nil {
		t.Fatal(err)
	}
	if n := f.count(t, `SELECT count(*) FROM posts WHERE user_id = $1`, acct); n != 0 {
		t.Error("a post of the erased account was written again")
	}
}

// A request that cannot be carried out fails; one that meets stored media
// without S3 is retried later, not done with the objects orphaned.
func TestInvalidAndRetried(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	bad := f.request(t, SubjectAuthor, nil, 0, `{}`)
	if _, err := f.db.Exec(ctx, `UPDATE media SET status = 'stored', s3_bucket = 'tks-media', s3_key = 'k' WHERE author_key = $1`, key(commenter)); err != nil {
		t.Fatal(err)
	}
	stored := f.request(t, SubjectAuthor, key(commenter), 0, `{}`)
	if err := f.eraser.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts int
	var next *time.Time
	if err := f.db.QueryRow(ctx, `SELECT status, attempts, next_attempt_at FROM erasure_requests WHERE id = $1`, bad).Scan(&status, &attempts, &next); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Errorf("an author erasure without a key: %s", status)
	}
	if err := f.db.QueryRow(ctx, `SELECT status, attempts, next_attempt_at FROM erasure_requests WHERE id = $1`, stored).Scan(&status, &attempts, &next); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempts != 1 || next == nil || time.Until(*next) < 30*time.Second {
		t.Errorf("stored media without S3: %s, %d attempts, next %v", status, attempts, next)
	}
	if n := f.count(t, `SELECT count(*) FROM media WHERE author_key = $1`, key(commenter)); n != 1 {
		t.Error("the stored media's row is gone")
	}
}
