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
	"slices"
	"sync"
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
	// Gone marks a banned or deleted account: its page names nothing
	// (statusCode 10221) and its list answers "user not exist".
	Gone bool
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

// World is the ground truth. The server reads it under its lock; tests
// change it through Mutate and the helpers below (new posts, count changes,
// renames, deletions, private and gone accounts) between crawls.
type World struct {
	mu       sync.RWMutex
	rng      *rand.Rand
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
	w := &World{Now: now, rng: r, byName: map[string]*Account{}, bySec: map[string]*Account{}, posts: map[int64]*Post{}, comments: map[int64]*Comment{}}
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

// Mutate runs f with the world locked against the server.
func (w *World) Mutate(f func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f()
}

// Advance moves the world's present forward.
func (w *World) Advance(d time.Duration) { w.Mutate(func() { w.Now = w.Now.Add(d) }) }

// AddPost publishes a new post on acc at the world's present, with
// comments top-level comments (no replies), and returns it.
func (w *World) AddPost(acc *Account, comments int) *Post {
	var p *Post
	w.Mutate(func() {
		t := w.Now
		p = &Post{ID: ItemID(t, w.rng.Uint32()), Created: t, Kind: "video", Duration: 15, Owner: acc,
			Desc: "New post of " + acc.Username + " #news", Hashtags: []string{"news"},
			Music: Music{ID: int64(7_100_000_000_000_000_000 + w.rng.IntN(1_000_000)), Title: "original sound", Author: acc.Nickname, Original: true, Duration: 15},
			Plays: int64(w.rng.IntN(1000)), Likes: int64(w.rng.IntN(100))}
		for c := 0; c < comments; c++ {
			w.addComment(p, t)
		}
		acc.Posts = slices.Insert(acc.Posts, 0, p)
		w.posts[p.ID] = p
	})
	return p
}

// AddComments adds n top-level comments to p, at the world's present.
func (w *World) AddComments(p *Post, n int) {
	w.Mutate(func() {
		for range n {
			w.addComment(p, w.Now)
		}
	})
}

func (w *World) addComment(p *Post, t time.Time) {
	i := len(p.Comments)
	cm := &Comment{ID: ItemID(t, w.rng.Uint32()), Created: t, Likes: int64(w.rng.IntN(50)),
		Text: fmt.Sprintf("comment %d on post %d", i, p.ID), Author: Person{UID: ItemID(t.Add(-1000*24*time.Hour), w.rng.Uint32()),
			SecUID: fmt.Sprintf("MS4wLjABAAAAlate%06d%08x", i, w.rng.Uint32()), Username: fmt.Sprintf("late_%d", i), Nickname: fmt.Sprintf("late %d", i)}}
	p.Comments = append(p.Comments, cm)
	w.comments[cm.ID] = cm
}

// AddReplies adds n replies to the comment c, at the world's present.
func (w *World) AddReplies(c *Comment, n int) {
	w.Mutate(func() {
		for range n {
			i := len(c.Replies)
			rp := &Comment{ID: ItemID(w.Now, w.rng.Uint32()), Created: w.Now, Likes: int64(w.rng.IntN(20)),
				Text: fmt.Sprintf("late reply %d to comment %d", i, c.ID), Author: Person{UID: ItemID(w.Now.Add(-900*24*time.Hour), w.rng.Uint32()),
					SecUID: fmt.Sprintf("MS4wLjABAAAAlaterep%06d%08x", i, w.rng.Uint32()), Username: fmt.Sprintf("late_replier_%d", i), Nickname: fmt.Sprintf("late replier %d", i)}}
			c.Replies = append(c.Replies, rp)
			w.comments[rp.ID] = rp
		}
	})
}

// DeleteComment removes a top-level comment (with its replies) or a
// reply: it is no longer served.
func (w *World) DeleteComment(p *Post, c *Comment) {
	w.Mutate(func() {
		p.Comments = slices.DeleteFunc(p.Comments, func(x *Comment) bool { return x == c })
		for _, top := range p.Comments {
			top.Replies = slices.DeleteFunc(top.Replies, func(x *Comment) bool { return x == c })
		}
		delete(w.comments, c.ID)
		for _, r := range c.Replies {
			delete(w.comments, r.ID)
		}
	})
}

// DeletePost removes a post: it is no longer listed or served.
func (w *World) DeletePost(p *Post) {
	w.Mutate(func() {
		acc := p.Owner
		acc.Posts = slices.DeleteFunc(acc.Posts, func(x *Post) bool { return x == p })
		delete(w.posts, p.ID)
	})
}

// Rename changes an account's username: its old profile page names
// nothing, and its list (by secUid) carries the new name.
func (w *World) Rename(acc *Account, username string) {
	w.Mutate(func() {
		delete(w.byName, acc.Username)
		acc.Username = username
		w.byName[username] = acc
	})
}

