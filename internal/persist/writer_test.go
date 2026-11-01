package persist

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/store"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

func obs(at time.Time, hash string) *fbv1.Observation {
	return &fbv1.Observation{ObservedAt: timestamppb.New(at), ContentHash: []byte(hash), EventId: hash}
}

func TestWriterUpsertsAndDedupes(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := store.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	w := &Writer{DB: db}
	created := now.Add(-48 * time.Hour)
	cc := int64(10)
	post := func(at time.Time, hash string, reactions int64) *fbv1.Post {
		return &fbv1.Post{Obs: obs(at, hash), PostId: 1, PageId: 9, CreatedAt: timestamppb.New(created), Type: fbv1.PostType_POST_TYPE_PHOTO,
			Text: "hello #x", Hashtags: []string{"x"}, Reactions: &fbv1.Reactions{Total: reactions, ByType: map[string]int64{"like": reactions}},
			CommentCount: &cc, MediaIds: []string{"m1"}, TextRanges: []*fbv1.TextRange{{Offset: 6, Length: 2, EntityType: "Hashtag"}}}
	}
	// Two versions of the same post in one batch: the later observation wins.
	if err := w.Posts(ctx, []*fbv1.Post{post(now.Add(-time.Minute), "h1", 5), post(now, "h2", 7)}); err != nil {
		t.Fatal(err)
	}
	var reactions int64
	if err := db.QueryRow(ctx, "SELECT reaction_count FROM posts WHERE post_id = 1").Scan(&reactions); err != nil || reactions != 7 {
		t.Fatalf("reactions = %d, %v", reactions, err)
	}
	// Unchanged content: no rewrite, no new metrics row.
	if err := w.Posts(ctx, []*fbv1.Post{post(now.Add(time.Minute), "h2", 7)}); err != nil {
		t.Fatal(err)
	}
	var metricsRows int
	_ = db.QueryRow(ctx, "SELECT count(*) FROM post_metrics WHERE post_id = 1").Scan(&metricsRows)
	if metricsRows != 1 {
		t.Fatalf("metrics rows = %d, want 1", metricsRows)
	}
	// Changed engagement: one more metrics row.
	if err := w.Posts(ctx, []*fbv1.Post{post(now.Add(2*time.Minute), "h3", 9)}); err != nil {
		t.Fatal(err)
	}
	_ = db.QueryRow(ctx, "SELECT count(*) FROM post_metrics WHERE post_id = 1").Scan(&metricsRows)
	if metricsRows != 2 {
		t.Fatalf("metrics rows = %d, want 2", metricsRows)
	}

	// Comments + authors; a redacted row is never overwritten.
	pid := int64(100)
	cm := func(hash, text string) *fbv1.Comment {
		return &fbv1.Comment{Obs: obs(now, hash), CommentId: 100, PostId: 1, PageId: 9, PostCreatedAt: timestamppb.New(created),
			Text: text, CreatedAt: timestamppb.New(now), Author: &fbv1.Author{Id: "a1", Name: "Ana", AuthorKey: []byte("k1")},
			Reactions: &fbv1.Reactions{Total: 1}}
	}
	reply := &fbv1.Comment{Obs: obs(now, "r"), CommentId: 101, PostId: 1, PageId: 9, PostCreatedAt: timestamppb.New(created),
		Depth: 1, ParentCommentId: &pid, RootCommentId: &pid, Text: "reply", Author: &fbv1.Author{Id: "a2", AuthorKey: []byte("k2")}}
	if err := w.Comments(ctx, []*fbv1.Comment{cm("c1", "first"), reply}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "UPDATE comments SET redacted = true, text = NULL WHERE comment_id = 100"); err != nil {
		t.Fatal(err)
	}
	if err := w.Comments(ctx, []*fbv1.Comment{cm("c2", "rescraped")}); err != nil {
		t.Fatal(err)
	}
	var text *string
	_ = db.QueryRow(ctx, "SELECT text FROM comments WHERE comment_id = 100").Scan(&text)
	if text != nil {
		t.Fatalf("redacted comment was overwritten: %q", *text)
	}
	var authors int
	_ = db.QueryRow(ctx, "SELECT count(*) FROM comment_authors").Scan(&authors)
	if authors != 2 {
		t.Fatalf("authors = %d", authors)
	}

	// Page + schedule resolution.
	if _, err := db.Exec(ctx, "INSERT INTO page_schedule (page_ref, interval_seconds, next_run_at) VALUES ('v:brand', 3600, now())"); err != nil {
		t.Fatal(err)
	}
	f := int64(1000)
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now, "p"), PageId: 9, RequestedRef: "v:brand", Name: "Brand", FollowersCount: &f}}); err != nil {
		t.Fatal(err)
	}
	var resolved *int64
	_ = db.QueryRow(ctx, "SELECT page_id FROM page_schedule WHERE page_ref = 'v:brand'").Scan(&resolved)
	if resolved == nil || *resolved != 9 {
		t.Fatalf("schedule not resolved: %v", resolved)
	}

	// Media metadata keeps download state.
	m := &fbv1.Media{Obs: obs(now, "m"), MediaId: "6f1c1f7e-8d9b-5c2a-9e0f-3b1f6a7c2d11", FbMediaId: "fb1", Variant: "original",
		Kind: fbv1.MediaKind_MEDIA_KIND_PHOTO, OwnerType: "post", OwnerId: 1, PostId: 1, PageId: 9, SourceUrl: "https://x/1?oe=1"}
	if err := w.Media(ctx, []*fbv1.Media{m}); err != nil {
		t.Fatal(err)
	}
	_, _ = db.Exec(ctx, "UPDATE media SET status = 'ready'")
	m.SourceUrl = "https://x/1?oe=2"
	if err := w.Media(ctx, []*fbv1.Media{m}); err != nil {
		t.Fatal(err)
	}
	var status, src string
	_ = db.QueryRow(ctx, "SELECT status, source_url FROM media").Scan(&status, &src)
	if status != "ready" || src != "https://x/1?oe=2" {
		t.Fatalf("media = %s %s", status, src)
	}

	// An old post creates its partition on demand.
	old := post(now, "old", 1)
	old.PostId, old.CreatedAt = 2, timestamppb.New(now.AddDate(-2, 0, 0))
	if err := w.Posts(ctx, []*fbv1.Post{old}); err != nil {
		t.Fatalf("on-demand partition: %v", err)
	}
}

// TestCommentEditsDeletionsAndRestores follows one comment through an edit,
// a deletion (also replayed out of order) and a later sighting.
func TestCommentEditsDeletionsAndRestores(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := store.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	w := &Writer{DB: db}
	created := timestamppb.New(now.Add(-72 * time.Hour))
	cm := func(at time.Time, hash, text string) *fbv1.Comment {
		return &fbv1.Comment{Obs: obs(at, hash), CommentId: 500, PostId: 5, PageId: 9, PostCreatedAt: created, Text: text,
			Author: &fbv1.Author{Id: "a1", AuthorKey: []byte("k1")}, Reactions: &fbv1.Reactions{Total: 1}}
	}
	deleted := func(at time.Time) *fbv1.Comment {
		return &fbv1.Comment{Obs: obs(at, "del"), CommentId: 500, PostId: 5, PageId: 9, PostCreatedAt: created,
			Status: fbv1.CommentStatus_COMMENT_STATUS_DELETED}
	}
	type row struct {
		Text      *string
		Status    string
		EditCount int
		EditedAt  *time.Time
		DeletedAt *time.Time
	}
	get := func() row {
		t.Helper()
		var r row
		if err := db.QueryRow(ctx, `SELECT text, status, edit_count, edited_at, deleted_at FROM comments WHERE comment_id = 500`).
			Scan(&r.Text, &r.Status, &r.EditCount, &r.EditedAt, &r.DeletedAt); err != nil {
			t.Fatal(err)
		}
		return r
	}
	t0 := now.Add(-10 * time.Hour)
	if err := w.Comments(ctx, []*fbv1.Comment{cm(t0, "v1", "first")}); err != nil {
		t.Fatal(err)
	}
	// Same text, more reactions: not an edit.
	again := cm(t0.Add(time.Hour), "v1b", "first")
	again.Reactions.Total = 3
	if err := w.Comments(ctx, []*fbv1.Comment{again}); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.EditCount != 0 || r.EditedAt != nil || r.Status != "visible" {
		t.Fatalf("after a reaction change: %+v", r)
	}
	// New text: an edit.
	t2 := t0.Add(2 * time.Hour)
	if err := w.Comments(ctx, []*fbv1.Comment{cm(t2, "v2", "first, edited")}); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.EditCount != 1 || r.EditedAt == nil || !r.EditedAt.Equal(t2) || *r.Text != "first, edited" {
		t.Fatalf("after an edit: %+v", r)
	}
	// An older observation replayed late changes nothing.
	if err := w.Comments(ctx, []*fbv1.Comment{cm(t0.Add(30*time.Minute), "v0", "stale text")}); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.EditCount != 1 || *r.Text != "first, edited" {
		t.Fatalf("a stale observation was applied: %+v", r)
	}

	// Deleted: the content is kept.
	t3 := t0.Add(5 * time.Hour)
	if err := w.Comments(ctx, []*fbv1.Comment{deleted(t3)}); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.Status != "deleted" || r.DeletedAt == nil || !r.DeletedAt.Equal(t3) || r.Text == nil {
		t.Fatalf("after the deletion: %+v", r)
	}
	// An observation from before the deletion does not restore it.
	if err := w.Comments(ctx, []*fbv1.Comment{cm(t3.Add(-time.Minute), "v2", "first, edited")}); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.Status != "deleted" {
		t.Fatalf("an older sighting restored the comment: %+v", r)
	}
	// Seen again later, unchanged: visible again.
	t4 := t3.Add(time.Hour)
	if err := w.Comments(ctx, []*fbv1.Comment{cm(t4, "v2", "first, edited")}); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.Status != "visible" || r.DeletedAt != nil || r.EditCount != 1 {
		t.Fatalf("after the re-sighting: %+v", r)
	}
	// The deletion replayed after the re-sighting changes nothing.
	if n, err := MarkDeleted(ctx, db, []*fbv1.Comment{deleted(t3)}); err != nil || n != 0 {
		t.Fatalf("replayed deletion: %d rows, %v", n, err)
	}
	if r := get(); r.Status != "visible" {
		t.Fatalf("a replayed deletion hid a newer sighting: %+v", r)
	}
	// A deletion and a later sighting in one batch: the later one wins.
	if err := w.Comments(ctx, []*fbv1.Comment{deleted(t4.Add(time.Hour)), cm(t4.Add(2*time.Hour), "v2", "first, edited")}); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.Status != "visible" {
		t.Fatalf("batch with a deletion and a later sighting: %+v", r)
	}
	// A redacted comment is never marked deleted.
	if _, err := db.Exec(ctx, "UPDATE comments SET redacted = true, text = NULL WHERE comment_id = 500"); err != nil {
		t.Fatal(err)
	}
	if n, err := MarkDeleted(ctx, db, []*fbv1.Comment{deleted(now)}); err != nil || n != 0 {
		t.Fatalf("redacted comment marked deleted: %d, %v", n, err)
	}
}

// TestRestoreBringsBackRepliesDeletedWithTheComment: a comment seen again
// after a deletion makes the replies deleted with it (same deleted_at)
// visible again, but not a reply deleted on its own.
func TestRestoreBringsBackRepliesDeletedWithTheComment(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := store.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	w := &Writer{DB: db}
	created := timestamppb.New(now.Add(-72 * time.Hour))
	root := int64(700)
	cm := func(id int64, depth int32, at time.Time, status fbv1.CommentStatus) *fbv1.Comment {
		c := &fbv1.Comment{Obs: obs(at, fmt.Sprintf("h%d", id)), CommentId: id, PostId: 7, PageId: 9, PostCreatedAt: created,
			Depth: depth, Text: fmt.Sprintf("comment %d", id), Status: status}
		if depth > 0 {
			c.ParentCommentId, c.RootCommentId = &root, &root
		}
		if status == fbv1.CommentStatus_COMMENT_STATUS_DELETED {
			c.Obs.ContentHash, c.Text = []byte("del"), ""
		}
		return c
	}
	t0 := now.Add(-5 * time.Hour)
	visible := fbv1.CommentStatus_COMMENT_STATUS_UNSPECIFIED
	deleted := fbv1.CommentStatus_COMMENT_STATUS_DELETED
	if err := w.Comments(ctx, []*fbv1.Comment{cm(700, 0, t0, visible), cm(701, 1, t0, visible), cm(702, 1, t0, visible), cm(703, 1, t0, visible)}); err != nil {
		t.Fatal(err)
	}
	// 703 was deleted on its own; then 700 with its replies.
	if _, err := MarkDeleted(ctx, db, []*fbv1.Comment{cm(703, 1, t0.Add(time.Hour), deleted)}); err != nil {
		t.Fatal(err)
	}
	t2 := t0.Add(2 * time.Hour)
	if _, err := MarkDeleted(ctx, db, []*fbv1.Comment{cm(700, 0, t2, deleted), cm(701, 1, t2, deleted), cm(702, 1, t2, deleted)}); err != nil {
		t.Fatal(err)
	}
	// 700 is seen again.
	if err := w.Comments(ctx, []*fbv1.Comment{cm(700, 0, t2.Add(time.Hour), visible)}); err != nil {
		t.Fatal(err)
	}
	want := map[int64]string{700: "visible", 701: "visible", 702: "visible", 703: "deleted"}
	for id, st := range want {
		var got string
		if err := db.QueryRow(ctx, `SELECT status FROM comments WHERE comment_id = $1`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != st {
			t.Fatalf("comment %d is %s, want %s", id, got, st)
		}
	}
}

// Data still in flight when an erasure ran never lands after it: posts,
// comments, media and profiles of a suppressed page are dropped, and a
// suppressed author's new comments are stored redacted, without an author
// row. Before M2 only the crawlers checked suppression (ported from fbs).
func TestSuppressedDataInFlight(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := store.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	w := &Writer{DB: db}
	created := timestamppb.New(now.Add(-time.Hour))
	erasedAuthor := []byte{0xab, 0xcd}
	f := int64(5)
	// Page 7 was erased: its row is left suppressed and stripped.
	if _, err := db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('page', '7'), ('author', 'abcd');
		INSERT INTO pages (page_id, status) VALUES (7, 'suppressed')`); err != nil {
		t.Fatal(err)
	}

	if err := w.Posts(ctx, []*fbv1.Post{
		{Obs: obs(now, "p7"), PostId: 70, PageId: 7, CreatedAt: created, Text: "erased page"},
		{Obs: obs(now, "p8"), PostId: 80, PageId: 8, CreatedAt: created, Text: "kept"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Comments(ctx, []*fbv1.Comment{
		{Obs: obs(now, "c7"), CommentId: 700, PostId: 70, PageId: 7, PostCreatedAt: created, Text: "on an erased page",
			Author: &fbv1.Author{Id: "x", AuthorKey: []byte{1}}},
		{Obs: obs(now, "c8"), CommentId: 800, PostId: 80, PageId: 8, PostCreatedAt: created, Text: "by an erased author", Url: "u",
			TextRanges: []*fbv1.TextRange{{EntityType: "User", Name: "n"}}, Author: &fbv1.Author{Id: "erased", Name: "E", AuthorKey: erasedAuthor}},
		{Obs: obs(now, "c9"), CommentId: 801, PostId: 80, PageId: 8, PostCreatedAt: created, Text: "kept",
			Author: &fbv1.Author{Id: "y", Name: "Y", AuthorKey: []byte{2}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Media(ctx, []*fbv1.Media{
		{Obs: obs(now, "m7"), MediaId: "00000000-0000-0000-0000-000000000007", Variant: "orig", OwnerType: "post", PostId: 70, PageId: 7, SourceUrl: "https://cdn.example/7"},
		{Obs: obs(now, "m8"), MediaId: "00000000-0000-0000-0000-000000000008", Variant: "orig", OwnerType: "post", PostId: 80, PageId: 8, SourceUrl: "https://cdn.example/8"},
		{Obs: obs(now, "m9"), MediaId: "00000000-0000-0000-0000-000000000009", Variant: "orig", OwnerType: "comment", PostId: 80, PageId: 8,
			SourceUrl: "https://cdn.example/9", AuthorKey: erasedAuthor},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now, "pg7"), PageId: 7, Name: "Erased", FollowersCount: &f}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now, "pg7u"), PageId: 7, Status: fbv1.PageStatus_PAGE_STATUS_UNAVAILABLE}}); err != nil {
		t.Fatal(err)
	}
	// Page 6 was erased before its profile was ever stored.
	if _, err := db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('page', '6')`); err != nil {
		t.Fatal(err)
	}
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now, "pg6"), PageId: 6, Name: "Never stored", FollowersCount: &f}}); err != nil {
		t.Fatal(err)
	}

	for q, want := range map[string]int{
		`SELECT count(*) FROM posts WHERE page_id = 7`:                           0,
		`SELECT count(*) FROM posts WHERE page_id = 8`:                           1,
		`SELECT count(*) FROM comments WHERE page_id = 7`:                        0,
		`SELECT count(*) FROM comments WHERE comment_id = 801 AND text = 'kept'`: 1,
		`SELECT count(*) FROM comments WHERE comment_id = 800 AND redacted AND text IS NULL AND url IS NULL
			AND text_ranges IS NULL AND author_id IS NULL AND author_key = '\xabcd'::bytea`: 1,
		`SELECT count(*) FROM comment_authors WHERE author_key = '\xabcd'::bytea`:                 0,
		`SELECT count(*) FROM comment_authors WHERE author_key = '\x02'::bytea`:                   1,
		`SELECT count(*) FROM media WHERE page_id = 7`:                                            0,
		`SELECT count(*) FROM media WHERE page_id = 8`:                                            1,
		`SELECT count(*) FROM pages WHERE page_id = 7 AND status = 'suppressed' AND name IS NULL`: 1,
		`SELECT count(*) FROM page_snapshots WHERE page_id = 7`:                                   0,
		`SELECT count(*) FROM pages WHERE page_id = 6`:                                            0,
		`SELECT count(*) FROM media WHERE media_id = '00000000-0000-0000-0000-000000000009'`:      0,
	} {
		var n int
		if err := db.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if n != want {
			t.Errorf("%s = %d, want %d", q, n, want)
		}
	}
}

// A NUL character (valid UTF-8, which Postgres can't store) and a missing
// metrics or snapshot partition never fail a batch; a record Postgres still
// refuses is skipped by itself, and the rest of its batch is written.
func TestBatchesThatPostgresWouldRefuse(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	// Posts and comments partitions only: no post_metrics nor page_snapshots.
	for _, table := range []string{"posts", "comments"} {
		if err := store.EnsurePartitionFor(ctx, db, table, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	w := &Writer{DB: db}
	created := timestamppb.New(now.Add(-time.Hour))
	cc := int64(3)
	nul := "bad\x00text"
	if err := w.Posts(ctx, []*fbv1.Post{{Obs: obs(now, "p1"), PostId: 1, PageId: 9, CreatedAt: created, Text: nul,
		Hashtags: []string{nul}, TextRanges: []*fbv1.TextRange{{EntityType: "User", Name: nul}}, CommentCount: &cc,
		Reactions: &fbv1.Reactions{Total: 2, ByType: map[string]int64{"like": 2}}, ExtraJson: `{"k":"a\u0000b"}`}}); err != nil {
		t.Fatalf("posts: %v", err)
	}
	if err := w.Comments(ctx, []*fbv1.Comment{{Obs: obs(now, "c1"), CommentId: 10, PostId: 1, PageId: 9, PostCreatedAt: created,
		Text: nul, Author: &fbv1.Author{Id: "a", Name: nul, AuthorKey: []byte{1}}}}); err != nil {
		t.Fatalf("comments: %v", err)
	}
	f := int64(5)
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now, "pg"), PageId: 9, Name: nul, FollowersCount: &f}}); err != nil {
		t.Fatalf("pages: %v", err)
	}
	for q, want := range map[string]int{
		`SELECT count(*) FROM posts WHERE text = 'badtext' AND hashtags = '{badtext}' AND extra->>'k' = 'ab'
			AND text_ranges->0->>'name' = 'badtext'`: 1,
		`SELECT count(*) FROM comments WHERE text = 'badtext'`:        1,
		`SELECT count(*) FROM comment_authors WHERE name = 'badtext'`: 1,
		`SELECT count(*) FROM pages WHERE name = 'badtext'`:           1,
	} {
		var n int
		if err := db.QueryRow(ctx, q).Scan(&n); err != nil || n != want {
			t.Errorf("%s = %d (%v), want %d", q, n, err, want)
		}
	}

	// A media id that isn't a UUID is dropped before writing, not refused
	// by Postgres (refused records are set aside by the consumer:
	// TestBadRecordsAreSetAside).
	if err := w.Media(ctx, []*fbv1.Media{
		{Obs: obs(now, "m"), MediaId: "not-a-uuid", Variant: "orig", OwnerType: "post", PostId: 1, PageId: 9},
		{Obs: obs(now, "m2"), MediaId: "00000000-0000-0000-0000-000000000002", Variant: "orig", OwnerType: "post", PostId: 1, PageId: 9},
	}); err != nil {
		t.Fatalf("media: %v", err)
	}
	var media int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM media`).Scan(&media)
	if media != 1 {
		t.Fatalf("%d media stored, want the one with a UUID", media)
	}
}

// Comments of a post older than comments retention are not written: their
// partition would be created only for retention to drop it again.
func TestCommentsPastRetentionAreNotWritten(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC()
	old, recent := now.AddDate(0, 0, -400), now.Add(-time.Hour)
	for _, at := range []time.Time{old, recent} {
		if err := store.EnsurePartitionFor(ctx, db, "comments", at); err != nil {
			t.Fatal(err)
		}
	}
	w := &Writer{DB: db, CommentsRetention: 365 * 24 * time.Hour}
	if err := w.Comments(ctx, []*fbv1.Comment{
		{Obs: obs(now, "a"), CommentId: 1, PostId: 1, PageId: 9, PostCreatedAt: timestamppb.New(old), Text: "old post"},
		{Obs: obs(now, "b"), CommentId: 2, PostId: 2, PageId: 9, PostCreatedAt: timestamppb.New(recent), Text: "recent post"},
	}); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	rows, _ := db.Query(ctx, `SELECT comment_id FROM comments`)
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("comments stored: %v, want only the recent post's", ids)
	}
}

// A vanity moves to the page that holds it now, instead of failing the
// unique index. (Records that break a constraint or don't decode are set
// aside by the consumer: TestBadRecordsAreSetAside.)
func TestVanityMovesToItsHolder(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if err := store.EnsurePartitionFor(ctx, db, "posts", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	w := &Writer{DB: db}
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now.Add(-time.Hour), "a"), PageId: 1, Vanity: "acme", Name: "A"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now, "b"), PageId: 2, Vanity: "Acme", Name: "B"}}); err != nil {
		t.Fatalf("a page taking another page's vanity: %v", err)
	}
	// A stale record of page 1 with the vanity (a redelivery) doesn't take
	// it back.
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now.Add(-2*time.Hour), "a0"), PageId: 1, Vanity: "acme", Name: "A"}}); err != nil {
		t.Fatalf("a stale record: %v", err)
	}
	var holder int64
	if err := db.QueryRow(ctx, `SELECT page_id FROM pages WHERE lower(vanity) = 'acme'`).Scan(&holder); err != nil || holder != 2 {
		t.Fatalf("vanity held by %d (%v), want page 2", holder, err)
	}
}

// Mentions of an erased author in comments and posts written after the
// erasure (crawlers learn of it up to a minute late) are cleared; other
// people's stay.
func TestErasedAuthorsMentionsAreNotWritten(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	for _, table := range []string{"posts", "comments"} {
		if err := store.EnsurePartitionFor(ctx, db, table, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	key := []byte("secret")
	if _, err := db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('author', $1)`,
		hex.EncodeToString(gdpr.AuthorKey(key, "100001"))); err != nil {
		t.Fatal(err)
	}
	mentions := func() []*fbv1.TextRange {
		return []*fbv1.TextRange{{EntityType: "User", EntityId: "100001", Name: "Erased", Url: "u1"},
			{EntityType: "User", EntityId: "100002", Name: "Kept", Url: "u2"}}
	}
	w := &Writer{DB: db, HMACKey: key}
	created := timestamppb.New(now.Add(-time.Hour))
	if err := w.Posts(ctx, []*fbv1.Post{{Obs: obs(now, "p"), PostId: 1, PageId: 9, CreatedAt: created, TextRanges: mentions()}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Comments(ctx, []*fbv1.Comment{{Obs: obs(now, "c"), CommentId: 2, PostId: 1, PageId: 9, PostCreatedAt: created,
		Text: "Erased Kept", TextRanges: mentions(), Author: &fbv1.Author{Id: "100003", AuthorKey: gdpr.AuthorKey(key, "100003")}}}); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"posts", "comments"} {
		var erased, kept string
		if err := db.QueryRow(ctx, `SELECT coalesce(text_ranges->0->>'entity_id', '') || coalesce(text_ranges->0->>'name', ''),
			text_ranges->1->>'entity_id' FROM `+table).Scan(&erased, &kept); err != nil {
			t.Fatal(err)
		}
		if erased != "" || kept != "100002" {
			t.Errorf("%s: erased mention %q, other %q", table, erased, kept)
		}
	}
}
