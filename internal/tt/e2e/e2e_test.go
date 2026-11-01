package e2e

import (
	"fmt"
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
//  4. a round after new comments and replies on a post, one by a commenter
//     seen before: everything is stored, nothing is redacted.
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

	// 4. New comments on the new post (past its 6 h comment window), one by
	// a commenter seen before, and new replies: everything is stored, the
	// commenter's earlier comments too (tks erases no one).
	again := np.Comments[0].Author
	w.Advance(7 * time.Hour)
	w.AddComments(np, 4)
	w.Mutate(func() { np.Comments[len(np.Comments)-1].Author = again })
	w.AddReplies(np.Comments[1], 3)
	round(t, s, "more comments", incremental, refs()...)
	for _, a := range accs {
		sameStore(t, s, a, none)
	}
	key := emit.AuthorKey([]byte(s.Cfg.Privacy.AuthorHMACKey), again.UID)
	if n := count(t, s, `SELECT count(*) FROM comments WHERE author_key = $1 AND text IS NOT NULL`, key); n < 2 {
		t.Errorf("%d comments of the returning commenter stored, want both", n)
	}
}
