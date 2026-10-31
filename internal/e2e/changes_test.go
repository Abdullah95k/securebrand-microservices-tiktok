package e2e

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// TestCommentEditsAndDeletions crawls a page, then deletes and edits
// comments on Facebook and crawls it in full again. The second job marks
// the deleted top-level comments, and their replies, deleted (store, API,
// fb.comment.v1) and counts the edit; a post that lost most of its comments
// is marked too, since what is left matches Facebook's count. Posts whose
// comments Facebook withholds (a partial or empty list under the full
// count) are left alone.
func TestCommentEditsAndDeletions(t *testing.T) {
	s := Start(t, Options{Spec: fakefb.Spec{Seed: 23, Pages: 1, PostsPerPage: 8, PostSpacing: 20 * time.Hour,
		MaxComments: 40, ReplyProbability: 0.3, MaxReplies: 3}})
	ctx := context.Background()
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	page := s.Fake.World.Pages()[0]

	first := s.runJob(t, key, page.Vanity, "full")
	if first.Status != "completed" {
		t.Fatalf("first job: %+v", first)
	}

	// Post A loses two top-level comments (one with replies) and has another
	// edited; B loses 4 in 5 of its comments; C has 4 in 5 withheld and D all.
	posts := append([]*fakefb.Post(nil), page.Posts...)
	sort.Slice(posts, func(i, j int) bool { return len(posts[i].Comments) > len(posts[j].Comments) })
	a, b, c, d := posts[0], posts[1], posts[2], posts[3]
	t.Logf("top-level comments: A %d, B %d, C %d, D %d", len(a.Comments), len(b.Comments), len(c.Comments), len(d.Comments))
	if len(a.Comments) < 6 || len(c.Comments) < 5 || len(d.Comments) < 1 {
		t.Fatal("posts too small for the test")
	}
	var gone []*fakefb.Comment
	for _, c := range a.Comments {
		if len(c.Replies) > 0 {
			gone = append(gone, c)
			break
		}
	}
	if len(gone) == 0 {
		t.Fatal("post A has no comment with replies")
	}
	for _, c := range a.Comments {
		if len(gone) < 2 && c.ID != gone[0].ID {
			gone = append(gone, c)
		}
	}
	var edited *fakefb.Comment
	for _, c := range a.Comments {
		if c.ID != gone[0].ID && c.ID != gone[1].ID {
			edited = c
			break
		}
	}
	gone = append(gone, b.Comments[:len(b.Comments)*4/5]...)
	wantDeleted := map[int64]bool{}
	var walk func(c *fakefb.Comment)
	walk = func(c *fakefb.Comment) {
		wantDeleted[c.ID] = true
		for _, r := range c.Replies {
			walk(r)
		}
	}
	for _, c := range gone {
		walk(c)
	}
	for _, c := range gone {
		if err := s.Fake.World.DeleteComment(c.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Fake.World.EditComment(edited.ID, "edited: still a fan"); err != nil {
		t.Fatal(err)
	}
	if err := s.Fake.World.WithholdComments(c.ID, len(c.Comments)*4/5); err != nil {
		t.Fatal(err)
	}
	if err := s.Fake.World.WithholdComments(d.ID, len(d.Comments)); err != nil {
		t.Fatal(err)
	}
	checks := func(outcome string) float64 {
		v, _, _ := sample("igs_deletion_checks_total", map[string]string{"outcome": outcome})
		return v
	}
	partialBefore := checks("skipped_partial")

	second := s.runJob(t, key, page.Vanity, "full")
	t.Logf("second job %s: counters %v, completeness %+v", second.Status, second.Counters, second.Completeness)
	if second.Status != "completed" {
		t.Fatalf("second job: %+v", second)
	}
	if got := second.Counters["comments_deleted"]; got != int64(len(wantDeleted)) {
		t.Fatalf("comments_deleted = %d, want %d", got, len(wantDeleted))
	}

	// Store: exactly the deleted comments and their replies.
	rows, err := s.DB.Query(ctx, `SELECT comment_id FROM comments WHERE page_id = $1 AND status = 'deleted' AND deleted_at IS NOT NULL`, page.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got[id] = true
	}
	rows.Close()
	if len(got) != len(wantDeleted) {
		t.Fatalf("deleted in the store: %v, want %v", got, wantDeleted)
	}
	for id := range wantDeleted {
		if !got[id] {
			t.Fatalf("comment %d not marked deleted (marked: %v)", id, got)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM comments WHERE post_id = ANY($1) AND status <> 'visible'`, []int64{c.ID, d.ID}); n != 0 {
		t.Fatalf("posts C and D: %d comments marked although their checks should have been skipped", n)
	}
	if partial := checks("skipped_partial"); partial != partialBefore+2 {
		t.Fatalf("partial reads skipped %v -> %v, want two more (C and D)", partialBefore, partial)
	}
	var text string
	var edits int
	if err := s.DB.QueryRow(ctx, `SELECT text, edit_count FROM comments WHERE comment_id = $1 AND edited_at IS NOT NULL`, edited.ID).
		Scan(&text, &edits); err != nil {
		t.Fatalf("edited comment: %v", err)
	}
	if text != "edited: still a fan" || edits != 1 {
		t.Fatalf("edited comment: %q, %d edits", text, edits)
	}
	if n := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1 AND edit_count > 0`, page.ID); n != 1 {
		t.Fatalf("%d comments counted as edited, want 1", n)
	}
	// Coverage counts what is still visible.
	truth, _ := s.Fake.World.TruthFor(page.ID, time.Time{})
	for _, p := range truth.Posts {
		if p.PostID != a.ID && p.PostID != b.ID {
			continue
		}
		var collected, replies int
		if err := s.DB.QueryRow(ctx, `SELECT comments_collected, replies_collected FROM posts WHERE post_id = $1`, p.PostID).
			Scan(&collected, &replies); err != nil {
			t.Fatal(err)
		}
		if collected+replies != p.TotalComments {
			t.Fatalf("post %d collected %d + %d, truth %d", p.PostID, collected, replies, p.TotalComments)
		}
	}

	// API.
	type commentsPage struct {
		Comments []api.Comment `json:"comments"`
	}
	var wantA int
	for _, g := range gone[:2] {
		wantA += 1 + len(g.Replies)
		for _, r := range g.Replies {
			wantA += len(r.Replies)
		}
	}
	del := decodeBody[commentsPage](t, s.Do(t, key, "GET", fmt.Sprintf("/v1/posts/%d/comments?status=deleted", a.ID), nil), http.StatusOK)
	if len(del.Comments) != wantA {
		t.Fatalf("API: %d deleted comments on post A, want %d", len(del.Comments), wantA)
	}
	for _, c := range del.Comments {
		if c.Status != "deleted" || c.DeletedAt == nil || c.Text == nil {
			t.Fatalf("API deleted comment %+v", c)
		}
	}
	all := decodeBody[commentsPage](t, s.Do(t, key, "GET", fmt.Sprintf("/v1/posts/%d/comments?depth=0", a.ID), nil), http.StatusOK)
	for _, c := range all.Comments {
		if c.CommentID == edited.ID && (c.EditCount != 1 || c.EditedAt == nil) {
			t.Fatalf("API edited comment %+v", c)
		}
	}

	// fb.comment.v1 carries a deletion record for each.
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Cfg.Kafka.Brokers...), kgo.ConsumeTopics(s.Producer.Topic(topics.Comment)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	published := map[int64]bool{}
	waitFor(t, "deletion records", 30*time.Second, func() bool {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		for _, r := range cl.PollFetches(pctx).Records() {
			var c fbv1.Comment
			if proto.Unmarshal(r.Value, &c) == nil && c.Status == fbv1.CommentStatus_COMMENT_STATUS_DELETED {
				if c.Text != "" || c.Obs.GetJobIds()[0] != second.ID || string(r.Key) != fmt.Sprint(c.PostId) {
					t.Fatalf("deletion record %v (key %s)", &c, r.Key)
				}
				published[c.CommentId] = true
			}
		}
		return len(published) >= len(wantDeleted)
	})
	for id := range wantDeleted {
		if !published[id] {
			t.Fatalf("no deletion record for %d", id)
		}
	}

	// A third full read finds nothing new to delete.
	third := s.runJob(t, key, page.Vanity, "full")
	if third.Status != "completed" || third.Counters["comments_deleted"] != 0 {
		t.Fatalf("third job: %+v", third)
	}

	// The page hides a comment with replies (gone from the lists and the
	// counts: a deletion as far as anyone can tell), then shows it again.
	var hidden *fakefb.Comment
	for _, p := range posts[4:] {
		for _, c := range p.Comments {
			if len(c.Replies) > 0 && hidden == nil {
				hidden = c
			}
		}
	}
	if hidden == nil {
		t.Fatal("no other comment with replies")
	}
	thread := map[int64]bool{}
	var mark func(c *fakefb.Comment)
	mark = func(c *fakefb.Comment) {
		thread[c.ID] = true
		for _, r := range c.Replies {
			mark(r)
		}
	}
	mark(hidden)
	statusOf := func() map[string]int {
		out := map[string]int{}
		for id := range thread {
			var st string
			if err := s.DB.QueryRow(ctx, `SELECT status FROM comments WHERE comment_id = $1`, id).Scan(&st); err != nil {
				t.Fatal(err)
			}
			out[st]++
		}
		return out
	}
	if err := s.Fake.World.HideComment(hidden.ID, true); err != nil {
		t.Fatal(err)
	}
	fourth := s.runJob(t, key, page.Vanity, "full")
	if got := statusOf(); fourth.Counters["comments_deleted"] != int64(len(thread)) || got["deleted"] != len(thread) {
		t.Fatalf("hidden thread of %d: job counted %d deleted, store %v", len(thread), fourth.Counters["comments_deleted"], got)
	}
	restoredBefore, _, _ := sample("igs_comment_changes_total", map[string]string{"kind": "restored"})
	if err := s.Fake.World.HideComment(hidden.ID, false); err != nil {
		t.Fatal(err)
	}
	fifth := s.runJob(t, key, page.Vanity, "full")
	if got := statusOf(); fifth.Status != "completed" || got["visible"] != len(thread) {
		t.Fatalf("shown again: store %v, want all %d visible", got, len(thread))
	}
	if v, _, _ := sample("igs_comment_changes_total", map[string]string{"kind": "restored"}); v < restoredBefore+float64(len(thread)) {
		t.Fatalf("restored %v -> %v, want %d more", restoredBefore, v, len(thread))
	}
	if n := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1 AND status = 'deleted'`, page.ID); n != len(wantDeleted) {
		t.Fatalf("%d comments deleted at the end, want the %d deleted on Facebook", n, len(wantDeleted))
	}
}
