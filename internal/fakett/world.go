// Package fakett is a synthetic TikTok for tests and the probe's dry run:
// a seeded world with ground truth (accounts, posts, comments, replies),
// served the way the owner's recordings show logged-out TikTok
// (docs/tiktok/har-findings-2026-10-31.md), with faults for the open
// questions the probe must detect (docs/tiktok/probe-spec.md §4). Its
// shapes are those of the recordings; nothing in it is real data.
package fakett

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Spec sizes a world.
type Spec struct {
	Seed            uint64
	Accounts        int
	PostsPerAccount int
	MaxComments     int // top-level comments per post, uniform in [0, MaxComments]
	MaxReplies      int // replies per comment, uniform in [0, MaxReplies]
	PostSpacing     time.Duration
	// Now is the world's present; zero means time.Now() at creation.
	Now time.Time
}

// DefaultSpec is a small world: 3 accounts of 60 posts, with comments and
// replies, one pinned post, one photo post and one ad per account.
func DefaultSpec() Spec {
	return Spec{Seed: 1, Accounts: 3, PostsPerAccount: 60, MaxComments: 45, MaxReplies: 5, PostSpacing: 20 * time.Hour}
}

// Person is a user seen as a comment author or a mention.
type Person struct {
	UID      int64
	SecUID   string
	Username string
	Nickname string
}

// Account is one monitored account.
type Account struct {
	Person
	Bio        string
	BioLink    string
	Verified   bool
	Private    bool
	Followers  int64
	Following  int64
	Hearts     int64
	Diggs      int64
	Friends    int64
	CreateTime time.Time
	// Posts are newest first by createTime; pinned posts are flagged.
	Posts []*Post
}

// Music is a post's sound.
type Music struct {
	ID       int64
	Title    string
	Author   string
	Original bool
	Duration int
}

// Post is a video or photo post.
type Post struct {
	ID       int64
	Created  time.Time
	Desc     string
	Hashtags []string
	Kind     string // "video" or "photo"
	Pinned   bool
	Ad       bool
	Duration int
	Music    Music
	Plays    int64
	Likes    int64
	Shares   int64
	Saves    int64
	Reposts  int64
	// Comments are top-level comments in TikTok's ranking order (not by
	// time), each with its replies.
	Comments []*Comment
	Owner    *Account
}

// CommentCount is what TikTok shows: top-level comments plus replies.
func (p *Post) CommentCount() int64 {
	n := int64(0)
	for _, c := range p.Comments {
		n += 1 + int64(len(c.Replies))
	}
	return n
}

// Comment is a top-level comment or a reply.
type Comment struct {
	ID      int64
	Text    string
	Created time.Time
	Likes   int64
	Author  Person
	Replies []*Comment
}

// World is the ground truth.
type World struct {
	Now      time.Time
	accounts []*Account
	byName   map[string]*Account
	bySec    map[string]*Account
	posts    map[int64]*Post
	comments map[int64]*Comment
}

// ItemID builds an id the way TikTok's carry their time: the Unix seconds
// in the top 32 bits (T8).
func ItemID(t time.Time, low uint32) int64 { return t.Unix()<<32 | int64(low) }

// NewWorld generates a world from s.
func NewWorld(s Spec) *World {
	now := s.Now
	if now.IsZero() {
		now = time.Now().UTC().Truncate(time.Second)
	}
	r := rand.New(rand.NewPCG(s.Seed, s.Seed^0x7ee))
	w := &World{Now: now, byName: map[string]*Account{}, bySec: map[string]*Account{}, posts: map[int64]*Post{}, comments: map[int64]*Comment{}}
	person := func(prefix string, i int) Person {
		created := now.Add(-time.Duration(1000+r.IntN(2000)) * 24 * time.Hour)
		return Person{UID: ItemID(created, r.Uint32()), SecUID: fmt.Sprintf("MS4wLjABAAAA%s%04d%08x", prefix, i, r.Uint32()),
			Username: fmt.Sprintf("%s_%d", prefix, i), Nickname: fmt.Sprintf("%s %d", prefix, i)}
	}
	for a := 0; a < s.Accounts; a++ {
		acc := &Account{Person: person("account", a), Bio: fmt.Sprintf("Synthetic account %d", a),
			BioLink: fmt.Sprintf("example.org/account%d", a), Verified: a%2 == 0,
			Followers: int64(1000 + r.IntN(5_000_000)), Following: int64(r.IntN(800)), Diggs: 0, Friends: int64(r.IntN(50)),
			CreateTime: now.Add(-time.Duration(1500+r.IntN(500)) * 24 * time.Hour)}
		t := now.Add(-time.Duration(r.IntN(3600)) * time.Second)
		for i := 0; i < s.PostsPerAccount; i++ {
			p := &Post{ID: ItemID(t, r.Uint32()), Created: t, Kind: "video", Duration: 10 + r.IntN(170), Owner: acc,
				Desc: fmt.Sprintf("Post %d of %s #news #iraq", i, acc.Username), Hashtags: []string{"news", "iraq"},
				Music: Music{ID: int64(7_000_000_000_000_000_000 + r.IntN(1_000_000)), Title: "original sound", Author: acc.Nickname, Original: true, Duration: 30},
				Plays: int64(r.IntN(2_000_000)), Likes: int64(r.IntN(100_000)), Shares: int64(r.IntN(5000)), Saves: int64(r.IntN(3000)), Reposts: int64(r.IntN(300))}
			switch i {
			case 3:
				p.Kind = "photo"
			case 5:
				p.Ad = true
			}
			nc := 0
			if s.MaxComments > 0 {
				nc = r.IntN(s.MaxComments + 1)
			}
			for c := 0; c < nc; c++ {
				ct := t.Add(time.Duration(1+r.IntN(48*3600)) * time.Second)
				cm := &Comment{ID: ItemID(ct, r.Uint32()), Created: ct, Likes: int64(r.IntN(500)),
					Text: fmt.Sprintf("comment %d on post %d", c, i), Author: person("commenter", c)}
				nr := 0
				if s.MaxReplies > 0 && r.IntN(3) == 0 {
					nr = r.IntN(s.MaxReplies + 1)
				}
				for k := 0; k < nr; k++ {
					rt := ct.Add(time.Duration(1+r.IntN(3600)) * time.Second)
					rp := &Comment{ID: ItemID(rt, r.Uint32()), Created: rt, Likes: int64(r.IntN(50)),
						Text: fmt.Sprintf("reply %d to comment %d", k, c), Author: person("replier", k)}
					cm.Replies = append(cm.Replies, rp)
					w.comments[rp.ID] = rp
				}
				p.Comments = append(p.Comments, cm)
				w.comments[cm.ID] = cm
			}
			// Ranking order, not time: a deterministic shuffle.
			r.Shuffle(len(p.Comments), func(x, y int) { p.Comments[x], p.Comments[y] = p.Comments[y], p.Comments[x] })
			acc.Posts = append(acc.Posts, p)
			w.posts[p.ID] = p
			t = t.Add(-s.PostSpacing - time.Duration(r.IntN(3600))*time.Second)
		}
		// An older post pinned to the top of the grid (T10).
		if len(acc.Posts) > 10 {
			acc.Posts[10].Pinned = true
		}
		for _, p := range acc.Posts {
			acc.Hearts += p.Likes
		}
		w.accounts = append(w.accounts, acc)
		w.byName[acc.Username] = acc
		w.bySec[acc.SecUID] = acc
	}
	return w
}

// Accounts returns every account.
func (w *World) Accounts() []*Account { return w.accounts }

// Account returns an account by username.
func (w *World) Account(username string) *Account { return w.byName[username] }

// AccountBySec returns an account by secUid.
func (w *World) AccountBySec(sec string) *Account { return w.bySec[sec] }

// Post returns a post by id.
func (w *World) Post(id int64) *Post { return w.posts[id] }

// Comment returns a comment or reply by id.
func (w *World) Comment(id int64) *Comment { return w.comments[id] }
