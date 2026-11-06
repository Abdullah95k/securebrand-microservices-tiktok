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

// AgeGroup is the age band TikTok infers and serves on some comment
// authors (predicted_age_group: 234 of 706 in the recordings). The fake
// serves a synthetic one, derived from the id, on about a third of them.
func (p Person) AgeGroup() string {
	if p.UID%3 != 0 {
		return ""
	}
	return []string{"band-a", "band-b", "band-c"}[(p.UID/3)%3]
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
	// Hidden are posts no longer public (HidePost): not listed, and their
	// pages answer their Unavailable statusCode. tks keeps them stored as
	// they were, marked unavailable (docs/tiktok/discovery-plan.md §17).
	Hidden []*Post
	// FollowingHidden: the profile hides the following list
	// (followingVisibility 2), which then answers 10222 (C6 run 1).
	FollowingHidden bool
	// FollowerList and FollowingList, once set (AddFollower,
	// AddFollowing), are the account's lists, newest first. Unset, the
	// account follows every other monitored account and is followed by the
	// people who commented on its posts.
	FollowerList  []*Account
	FollowingList []*Account
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
	// Unavailable is the statusCode a hidden post's page answers (HidePost).
	Unavailable int64
	// Region is locationCreated, where the post was made ("" serves none);
	// TextLanguage overrides textLanguage (default "ar").
	Region       string
	TextLanguage string
	// Mentions are the people the caption names (textExtra with their ids,
	// after the hashtags).
	Mentions []Person
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
	// Mentions are the people the comment names (text_extra with their
	// ids).
	Mentions []Person
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

// Clock returns the world's present, for a crawler whose clock follows it.
func (w *World) Clock() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.Now
}

// Advance moves the world's present forward.
func (w *World) Advance(d time.Duration) { w.Mutate(func() { w.Now = w.Now.Add(d) }) }

// AddAccount adds an account no monitored account mentions yet, as
// discovery finds them (docs/tiktok/discovery-plan.md §4): n posts with no
// comment, hashtag or region, the newest made newest before the present
// and each older one spacing before the last. A test sets what it scores
// (bio, posts' Region and Desc) and its state (Private, Gone) through
// Mutate.
func (w *World) AddAccount(username, bio string, n int, newest, spacing time.Duration) *Account {
	var acc *Account
	w.Mutate(func() {
		created := w.Now.Add(-1000 * 24 * time.Hour)
		i := len(w.accounts)
		acc = &Account{Person: Person{UID: ItemID(created, w.rng.Uint32()), SecUID: fmt.Sprintf("MS4wLjABAAAAfound%04d%08x", i, w.rng.Uint32()),
			Username: username, Nickname: username}, Bio: bio, CreateTime: created, Followers: int64(w.rng.IntN(10000))}
		t := w.Now.Add(-newest)
		for k := 0; k < n; k++ {
			p := &Post{ID: ItemID(t, w.rng.Uint32()), Created: t, Kind: "video", Duration: 15, Owner: acc,
				Desc: fmt.Sprintf("post %d", k), TextLanguage: "en",
				Music: Music{ID: int64(7_200_000_000_000_000_000 + w.rng.IntN(1_000_000)), Title: "original sound", Author: username, Original: true, Duration: 15},
				Plays: int64(w.rng.IntN(1000)), Likes: int64(w.rng.IntN(100))}
			acc.Posts = append(acc.Posts, p)
			w.posts[p.ID] = p
			t = t.Add(-spacing)
		}
		// Not in Accounts(): the world's ground truth is the monitored
		// accounts; a found one is served by its username and secUid.
		w.byName[username] = acc
		w.bySec[acc.SecUID] = acc
	})
	return acc
}

// AddFollower puts f at the top of acc's followers list (TikTok serves
// the newest first: T41).
func (w *World) AddFollower(acc, f *Account) {
	w.Mutate(func() { acc.FollowerList = append([]*Account{f}, acc.FollowerList...) })
}

// AddFollowing puts f at the top of acc's following list.
func (w *World) AddFollowing(acc, f *Account) {
	w.Mutate(func() { acc.FollowingList = append([]*Account{f}, acc.FollowingList...) })
}

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

// AddComment adds a top-level comment to p by author, naming mentions, at
// the world's present, and returns it.
func (w *World) AddComment(p *Post, author Person, mentions ...Person) *Comment {
	var cm *Comment
	w.Mutate(func() {
		cm = &Comment{ID: ItemID(w.Now, w.rng.Uint32()), Created: w.Now, Text: fmt.Sprintf("comment by %s", author.Username),
			Author: author, Mentions: mentions}
		p.Comments = append(p.Comments, cm)
		w.comments[cm.ID] = cm
	})
	return cm
}

// AddReply adds a reply to the comment c by author, at the world's
// present, and returns it.
func (w *World) AddReply(c *Comment, author Person) *Comment {
	var rp *Comment
	w.Mutate(func() {
		rp = &Comment{ID: ItemID(w.Now, w.rng.Uint32()), Created: w.Now, Text: fmt.Sprintf("reply by %s", author.Username), Author: author}
		c.Replies = append(c.Replies, rp)
		w.comments[rp.ID] = rp
	})
	return rp
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

// DeletePost removes a post: it is no longer listed, and its page answers
// statusCode 10204 "item not exist" (public sources'; unconfirmed: T34).
func (w *World) DeletePost(p *Post) {
	w.Mutate(func() {
		acc := p.Owner
		acc.Posts = slices.DeleteFunc(acc.Posts, func(x *Post) bool { return x == p })
		delete(w.posts, p.ID)
	})
}

// HidePost makes a post no longer public, as a post made private or
// removed: it is no longer listed, and its page answers code (10216 for a
// post made private: T6) without the post.
func (w *World) HidePost(p *Post, code int64) {
	w.Mutate(func() {
		acc := p.Owner
		acc.Posts = slices.DeleteFunc(acc.Posts, func(x *Post) bool { return x == p })
		acc.Hidden = append(acc.Hidden, p)
		p.Unavailable = code
	})
}

// ShowPost makes a hidden post public again, at its own date.
func (w *World) ShowPost(p *Post) {
	w.Mutate(func() {
		acc := p.Owner
		acc.Hidden = slices.DeleteFunc(acc.Hidden, func(x *Post) bool { return x == p })
		p.Unavailable = 0
		i, _ := slices.BinarySearchFunc(acc.Posts, p, func(x, t *Post) int { return t.Created.Compare(x.Created) })
		acc.Posts = slices.Insert(acc.Posts, i, p)
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
