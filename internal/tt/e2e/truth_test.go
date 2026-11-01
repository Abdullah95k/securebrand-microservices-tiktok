package e2e

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/emit"
)

// truth says what the store must hold beyond fakett's world: the authors
// whose comments are stored redacted (erased in flight).
type truth struct {
	erasedAuthors map[int64]bool
}

// sameStore compares what schema tt holds for an account with fakett's
// ground truth (build brief §8, A6): the account and its username history,
// every post with its latest counts (in posts and as the newest metrics
// row), every top-level comment and reply with its author, and no other
// visible comment (those TikTok stopped serving are marked deleted).
func sameStore(t *testing.T, s *Stack, acc *fakett.Account, tr truth) {
	t.Helper()
	ctx := context.Background()
	s.World.Mutate(func() {}) // nothing changes the world while it is read
	db := s.DB

	var (
		username, nickname, bio, status string
		link                            *string
		followers, following, videos    int64
	)
	if err := db.QueryRow(ctx, `SELECT username, nickname, bio, bio_link, follower_count, following_count, video_count, status
		FROM accounts WHERE user_id = $1`, acc.UID).Scan(&username, &nickname, &bio, &link, &followers, &following, &videos, &status); err != nil {
		t.Fatalf("account %s: %v", acc.Username, err)
	}
	if username != acc.Username || nickname != acc.Nickname || bio != acc.Bio || link == nil || *link != acc.BioLink ||
		followers != acc.Followers || following != acc.Following || status != "active" {
		t.Errorf("account %s: stored %s %q %q %v %d %d %s", acc.Username, username, nickname, bio, link, followers, following, status)
	}
	var named bool
	if err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM account_usernames WHERE user_id = $1 AND username = $2)`,
		acc.UID, acc.Username).Scan(&named); err != nil || !named {
		t.Errorf("account %s: not in the username history (%v)", acc.Username, err)
	}

	type postRow struct {
		caption, kind                         string
		pinned, ad                            *bool
		plays, likes, comments, shares, saves int64
		mPlays, mLikes, mComments             int64
	}
	rows, err := db.Query(ctx, `SELECT p.post_id, p.caption, p.kind, p.is_pinned, p.is_ad, p.play_count, p.like_count,
			p.comment_count, p.share_count, p.save_count, m.play_count, m.like_count, m.comment_count
		FROM posts p
		JOIN LATERAL (SELECT play_count, like_count, comment_count FROM post_metrics
			WHERE post_id = p.post_id ORDER BY captured_at DESC LIMIT 1) m ON true
		WHERE p.user_id = $1`, acc.UID)
	if err != nil {
		t.Fatal(err)
	}
	stored := map[int64]postRow{}
	for rows.Next() {
		var id int64
		var r postRow
		if err := rows.Scan(&id, &r.caption, &r.kind, &r.pinned, &r.ad, &r.plays, &r.likes, &r.comments, &r.shares, &r.saves,
			&r.mPlays, &r.mLikes, &r.mComments); err != nil {
			t.Fatal(err)
		}
		stored[id] = r
	}
	rows.Close()
	if len(stored) != len(acc.Posts) {
		t.Errorf("account %s: %d posts stored (with metrics), want %d", acc.Username, len(stored), len(acc.Posts))
	}
	for _, p := range acc.Posts {
		r, ok := stored[p.ID]
		switch {
		case !ok:
			t.Errorf("post %d missing", p.ID)
		case r.caption != p.Desc, r.kind != p.Kind, r.pinned == nil || *r.pinned != p.Pinned, r.ad == nil || *r.ad != p.Ad,
			r.plays != p.Plays, r.likes != p.Likes, r.comments != p.CommentCount(), r.shares != p.Shares, r.saves != p.Saves:
			t.Errorf("post %d: stored %+v", p.ID, r)
		case r.mPlays != p.Plays, r.mLikes != p.Likes, r.mComments != p.CommentCount():
			t.Errorf("post %d: newest metrics %d %d %d, want %d %d %d", p.ID, r.mPlays, r.mLikes, r.mComments, p.Plays, p.Likes, p.CommentCount())
		}
	}

	type commentRow struct {
		parent        *int64
		depth         int16
		text          *string
		likes         int64
		replies       *int64
		author        *int64
		status        string
		redacted      bool
		authorKeyHex  string
		hasPeopleData bool
	}
	rows, err = db.Query(ctx, `SELECT c.comment_id, c.parent_comment_id, c.depth, c.text, c.like_count, c.reply_count,
			c.author_user_id, c.status, c.redacted, encode(c.author_key, 'hex'), p.author_key IS NOT NULL
		FROM comments c LEFT JOIN people p ON p.author_key = c.author_key AND p.user_id IS NOT NULL
		WHERE c.user_id = $1 AND c.status = 'visible'`, acc.UID)
	if err != nil {
		t.Fatal(err)
	}
	comments := map[int64]commentRow{}
	for rows.Next() {
		var id int64
		var r commentRow
		if err := rows.Scan(&id, &r.parent, &r.depth, &r.text, &r.likes, &r.replies, &r.author, &r.status, &r.redacted,
			&r.authorKeyHex, &r.hasPeopleData); err != nil {
			t.Fatal(err)
		}
		comments[id] = r
	}
	rows.Close()
	key := []byte(s.Cfg.Privacy.AuthorHMACKey)
	want := 0
	check := func(p *fakett.Post, c *fakett.Comment, parent int64) {
		want++
		r, ok := comments[c.ID]
		if !ok {
			t.Errorf("post %d: comment %d (parent %d) missing", p.ID, c.ID, parent)
			return
		}
		if r.status != "visible" || r.authorKeyHex != hex.EncodeToString(emit.AuthorKey(key, c.Author.UID)) {
			t.Errorf("comment %d: status %s, author key %s", c.ID, r.status, r.authorKeyHex)
		}
		if tr.erasedAuthors[c.Author.UID] {
			if !r.redacted || r.text != nil || r.author != nil || r.hasPeopleData {
				t.Errorf("comment %d of an erased author: redacted %v text %v author %v person %v", c.ID, r.redacted, r.text, r.author, r.hasPeopleData)
			}
		} else if r.redacted || r.text == nil || *r.text != c.Text || r.likes != c.Likes || r.author == nil || *r.author != c.Author.UID || !r.hasPeopleData {
			t.Errorf("comment %d: stored %+v", c.ID, r)
		}
		switch {
		case parent == 0 && (r.depth != 0 || r.parent != nil || r.replies == nil || *r.replies != int64(len(c.Replies))):
			t.Errorf("comment %d: depth %d parent %v replies %v, want %d", c.ID, r.depth, r.parent, r.replies, len(c.Replies))
		case parent != 0 && (r.depth != 1 || r.parent == nil || *r.parent != parent):
			t.Errorf("reply %d: depth %d parent %v, want %d", c.ID, r.depth, r.parent, parent)
		}
	}
	for _, p := range acc.Posts {
		for _, c := range p.Comments {
			check(p, c, 0)
			for _, r := range c.Replies {
				check(p, r, c.ID)
			}
		}
	}
	if len(comments) != want {
		t.Errorf("account %s: %d comments and replies stored, want %d", acc.Username, len(comments), want)
	}
}

// count returns a count(*) query's result.
func count(t *testing.T, s *Stack, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := s.DB.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}
