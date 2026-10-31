// Package fakeig is a synthetic logged-out Instagram for tests: profile and
// post pages carrying tokens and cookies, and the five GraphQL operations of
// the owner's recordings (docs/instagram/har-findings-2026-10-30.md), with
// faults that model the open questions (U1 grid depth, U3 client-state
// fields, U5 identity-bound cursors) and the walls. Response shapes follow
// the findings' field lists; everything else about them is a stand-in. All
// data is generated: no real account, name or text.
package fakeig

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Spec sizes a world.
type Spec struct {
	Seed            uint64
	Profiles        int
	PostsPerProfile int
	MaxComments     int // per post, uniform in [0, MaxComments]
	Highlights      int // per profile
	PostSpacing     time.Duration
}

// DefaultSpec is a small world: 3 profiles of 40 posts.
func DefaultSpec() Spec {
	return Spec{Seed: 1, Profiles: 3, PostsPerProfile: 40, MaxComments: 25, Highlights: 2, PostSpacing: 18 * time.Hour}
}

// Profile is one account.
type Profile struct {
	PK int64
	// ID is the user object's "id", which the grid's _connection query
	// takes; it differs from the pk, as on Instagram.
	ID         string
	Username   string
	FullName   string
	Bio        string
	Followers  int64
	Following  int64
	Verified   bool
	Posts      []*Post // newest first
	Highlights []Highlight
}

// Post is one grid item.
type Post struct {
	PK          int64
	Code        string
	MediaType   int // 1 photo, 2 video, 8 carousel
	ProductType string
	Caption     string
	TakenAt     time.Time
	Likes       int64
	Comments    []Comment // newest first
	Owner       *Profile
}

// Comment is a top-level comment.
type Comment struct {
	PK        int64
	Text      string
	CreatedAt time.Time
	Likes     int64
	UserPK    int64
	Username  string
}

// Highlight is one highlights reel.
type Highlight struct {
	ID    int64
	Title string
	Items int
}

// World is the generated ground truth.
type World struct {
	profiles []*Profile
	byName   map[string]*Profile
	byPK     map[int64]*Profile
	byID     map[string]*Profile
	posts    map[int64]*Post
}

// Epoch is the newest post's time in every world.
var Epoch = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

// NewWorld generates a world from s.
func NewWorld(s Spec) *World {
	r := rand.New(rand.NewPCG(s.Seed, 0x1a))
	w := &World{byName: map[string]*Profile{}, byPK: map[int64]*Profile{}, byID: map[string]*Profile{}, posts: map[int64]*Post{}}
	nextPK := int64(3_000_000_000_000_000_000)
	pk := func() int64 { nextPK += 1 + r.Int64N(1000); return nextPK }
	for i := range s.Profiles {
		p := &Profile{PK: 50_000_000_000 + int64(i)*7919, Username: fmt.Sprintf("profile_%02d", i+1),
			FullName: fmt.Sprintf("Profile %02d", i+1), Bio: "synthetic biography",
			Followers: 1000 + r.Int64N(100000), Following: r.Int64N(500), Verified: i%2 == 0}
		p.ID = fmt.Sprintf("178414%011d", p.PK%100_000_000_000)
		for j := range s.PostsPerProfile {
			taken := Epoch.Add(-time.Duration(j) * s.PostSpacing)
			post := &Post{PK: MediaPK(taken, r.Int64N(1<<23)), MediaType: []int{1, 2, 8}[r.IntN(3)], Caption: fmt.Sprintf("synthetic caption %d", j+1),
				TakenAt: taken, Likes: r.Int64N(5000), Owner: p}
			post.Code = shortcode(post.PK)
			post.ProductType = map[int]string{1: "feed", 2: "clips", 8: "carousel_container"}[post.MediaType]
			n := 0
			if s.MaxComments > 0 {
				n = r.IntN(s.MaxComments + 1)
			}
			for k := range n {
				post.Comments = append(post.Comments, Comment{PK: pk(), Text: fmt.Sprintf("synthetic comment %d", n-k),
					CreatedAt: post.TakenAt.Add(time.Duration(n-k) * time.Minute), Likes: r.Int64N(50),
					UserPK: 60_000_000_000 + r.Int64N(1_000_000), Username: fmt.Sprintf("commenter_%04d", r.IntN(10000))})
			}
			p.Posts = append(p.Posts, post)
			w.posts[post.PK] = post
		}
		for h := range s.Highlights {
			p.Highlights = append(p.Highlights, Highlight{ID: 17_900_000_000_000_000 + int64(i*100+h), Title: fmt.Sprintf("Highlight %d", h+1), Items: 1 + r.IntN(5)})
		}
		w.profiles = append(w.profiles, p)
		w.byName[p.Username] = p
		w.byPK[p.PK] = p
		w.byID[p.ID] = p
	}
	return w
}

// Profiles returns every profile.
func (w *World) Profiles() []*Profile { return w.profiles }

// Profile returns the profile with username, or nil.
func (w *World) Profile(username string) *Profile { return w.byName[username] }

// IDEpochMs is the epoch of Instagram's ids: a media pk holds the
// milliseconds since it in its bits above the 23rd (checked against the
// post pages' taken_at in the owner's probe run, 2026-10-31).
const IDEpochMs = 1314220021721

// MediaPK builds a pk the way Instagram's encode their creation time.
func MediaPK(t time.Time, low int64) int64 {
	return (t.UnixMilli()-IDEpochMs)<<23 | low&(1<<23-1)
}

// shortcode encodes a media pk in Instagram's base64 alphabet.
func shortcode(pk int64) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	var b []byte
	for v := uint64(pk); v > 0; v /= 64 {
		b = append([]byte{alphabet[v%64]}, b...)
	}
	return string(b)
}
