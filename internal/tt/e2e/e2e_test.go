package e2e

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
)

var (
	full        = commonv1.CrawlMode_CRAWL_MODE_FULL
	incremental = commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL
)

// round queues one check per account under ref and waits for all of them.
func round(t *testing.T, s *Stack, name string, mode commonv1.CrawlMode, refs ...string) {
	t.Helper()
	ids := make([]string, len(refs))
	for i, ref := range refs {
		ids[i] = fmt.Sprintf("%s-%d", name, i)
		s.Queue(t, ids[i], ref, mode)
	}
	for id, st := range s.Wait(t, 3*time.Minute, ids...) {
		if st.Counters["tasks_failed"] != 0 {
			t.Fatalf("job %s: %v", id, st.Counters)
		}
	}
}

// TestStoreEqualsGroundTruth runs tks's crawler and persister as the
// binaries wire them against fakett, and checks after every round that
// the store equals fakett's world (build brief §8, A6):
//  1. a backfill of every account, comments and replies included (each
//     comment and reply call signed, none of the list calls);
//  2. an incremental round after new posts, a rename and changed counts;
//  3. a round with nothing new: no row changes, no metrics row;
//  4. a round after new comments on a post, with one commenter and one
//     account erased in between: the account's data is not written again,
//     the commenter's comments are stored redacted, without a person.
func TestStoreEqualsGroundTruth(t *testing.T) {
	s := Start(t, Options{Spec: fakett.Spec{Seed: 5, Accounts: 3, PostsPerAccount: 24, MaxComments: 30, MaxReplies: 5,
		PostSpacing: 20 * time.Hour}})
	w := s.World
	accs := w.Accounts()
	refs := func() []string {
		out := make([]string, len(accs))
		for i, a := range accs {
			out[i] = "u:" + a.Username
		}
		return out
	}
	none := truth{}

	// 1. Backfill.
	round(t, s, "backfill", full, refs()...)
	for _, a := range accs {
		sameStore(t, s, a, none)
	}
	if t.Failed() {
		t.FailNow()
	}
	if count(t, s, `SELECT count(*) FROM music`) == 0 || count(t, s, `SELECT count(*) FROM media WHERE kind = 'cover'`) == 0 {
		t.Error("no music or cover rows")
	}
	if s.Signer.Calls.Load() == 0 {
		t.Error("no comment call was signed")
	}

	// 2. New posts with comments, a rename, changed counts.
	w.Advance(time.Hour)
	np := w.AddPost(accs[0], 12)
	old := accs[1].Username
	w.Rename(accs[1], "renamed_account")
	w.Mutate(func() {
		for _, p := range accs[2].Posts[:3] {
			p.Likes += 7
			p.Plays += 100
		}
	})
	metricsBefore := count(t, s, `SELECT count(*) FROM post_metrics`)
	round(t, s, "changes", incremental, "u:"+accs[0].Username, "u:"+old, "u:"+accs[2].Username)
	for _, a := range accs {
		sameStore(t, s, a, none)
	}
	if n := count(t, s, `SELECT count(*) FROM account_usernames WHERE user_id = $1`, accs[1].UID); n != 2 {
		t.Errorf("%d usernames for the renamed account, want 2", n)
	}
	// Metric rows only on change: the new post's and the three changed.
	if got := count(t, s, `SELECT count(*) FROM post_metrics`) - metricsBefore; got != 4 {
		t.Errorf("%d metrics rows written, want 4 (one new post, three changed)", got)
	}
	if t.Failed() {
		t.FailNow()
	}

	// 3. Nothing new.
	w.Advance(3 * time.Hour)
	metricsBefore = count(t, s, `SELECT count(*) FROM post_metrics`)
	commentsSeen := count(t, s, `SELECT count(*) FROM comments WHERE last_seen_at > now() - interval '1 year'`)
	round(t, s, "quiet", incremental, refs()...)
	if got := count(t, s, `SELECT count(*) FROM post_metrics`) - metricsBefore; got != 0 {
		t.Errorf("%d metrics rows written without any change", got)
	}
	if got := count(t, s, `SELECT count(*) FROM comments`); got != commentsSeen {
		t.Errorf("%d comments, %d before a quiet round", got, commentsSeen)
	}

	// 4. Erasures, then new comments on the new post (past its 6 h comment
	// window), one by the erased commenter. The eraser (M6) removes the
	// account's rows and redacts the commenter's; here the test does.
	ctx := context.Background()
	erased := np.Comments[0].Author
	key := emit.AuthorKey([]byte(s.Cfg.Privacy.AuthorHMACKey), erased.UID)
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO suppression (subject_type, subject_key) VALUES ('author', $1), ('account', $2)`,
			[]any{hex.EncodeToString(key), strconv.FormatInt(accs[2].UID, 10)}},
		{`UPDATE comments SET text = NULL, text_ranges = NULL, author_user_id = NULL, redacted = true WHERE author_key = $1`, []any{key}},
		{`DELETE FROM people WHERE author_key = $1`, []any{key}},
	} {
		if _, err := s.DB.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, tb := range []string{"comments", "post_metrics", "posts", "post_index", "media", "account_usernames", "accounts"} {
		if _, err := s.DB.Exec(ctx, `DELETE FROM `+tb+` WHERE user_id = $1`, accs[2].UID); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(3 * s.Cfg.Crawler.SuppressionRefresh) // the crawler's copy of the list
	w.Advance(7 * time.Hour)
	w.AddComments(np, 4)
	w.Mutate(func() { np.Comments[len(np.Comments)-1].Author = erased })
	w.AddReplies(np.Comments[1], 3)
	round(t, s, "erasure", incremental, refs()...)
	tr := truth{erasedAuthors: map[int64]bool{erased.UID: true}}
	sameStore(t, s, accs[0], tr)
	sameStore(t, s, accs[1], none)
	for _, tb := range []string{"accounts", "posts", "post_metrics", "comments", "media"} {
		if n := count(t, s, `SELECT count(*) FROM `+tb+` WHERE user_id = $1`, accs[2].UID); n != 0 {
			t.Errorf("%d rows of the erased account in %s after a re-scrape", n, tb)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM people WHERE author_key = $1`, key); n != 0 {
		t.Errorf("the erased commenter is in people again")
	}
	if n := count(t, s, `SELECT count(*) FROM comments WHERE author_key = $1 AND NOT redacted`, key); n != 0 {
		t.Errorf("%d comments of the erased commenter stored unredacted", n)
	}
}
