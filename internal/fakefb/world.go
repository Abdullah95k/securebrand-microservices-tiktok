// Package fakefb is a synthetic Facebook used by tests, local development
// and load tests. It serves page HTML, the web GraphQL endpoint and an
// fbcdn-like media host from a deterministic world, and knows the ground
// truth so end-to-end tests can assert completeness. It can inject faults
// (rate limits, login walls, doc_id rotation, schema drift, latency).
package fakefb

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"
)

// Spec controls world generation.
type Spec struct {
	Seed             uint64
	Pages            int
	PostsPerPage     int
	PostSpacing      time.Duration
	MaxComments      int     // per post (heavy tailed)
	ReplyProbability float64 // per comment
	MaxReplies       int
	Depth2Prob       float64 // per reply: has its own replies
	AttachmentProb   float64 // per comment
	Now              time.Time
}

// DefaultSpec is small enough for unit tests.
func DefaultSpec() Spec {
	return Spec{Seed: 42, Pages: 2, PostsPerPage: 12, PostSpacing: 20 * time.Hour, MaxComments: 60,
		ReplyProbability: 0.25, MaxReplies: 14, Depth2Prob: 0.2, AttachmentProb: 0.1}
}

type Author struct {
	ID       int64
	Name     string
	Vanity   string
	Verified bool
}

type MediaItem struct {
	ID       int64
	Kind     string // photo | video | sticker | gif
	Width    int
	Height   int
	Duration float64
	Alt      string
}

type Comment struct {
	ID        int64
	PostID    int64
	Author    Author
	Text      string
	CreatedAt time.Time
	Reactions map[string]int64
	Replies   []*Comment
	Depth     int
	Parent    *Comment
	Media     *MediaItem
	Filtered  bool // hidden by the "most relevant" ordering
	// Withheld comments are left out of every ordering but still counted,
	// as when Facebook serves a logged-out visitor a partial list.
	Withheld bool
	// Hidden (top-level) comments, hidden by the page, are left out of the
	// lists, the counts and the truth, with their replies.
	Hidden bool
	IsOP   bool
	Edited bool
}

type Post struct {
	ID         int64
	PageID     int64
	CreatedAt  time.Time
	Type       string // status | photo | album | video | reel | link | share
	Text       string
	Media      []*MediaItem
	LinkURL    string
	LinkTitle  string
	SharedPost int64
	Reactions  map[string]int64
	Shares     int64
	Views      int64
	Pinned     bool
	Comments   []*Comment // top-level, oldest first
}

type Page struct {
	ID        int64
	Vanity    string
	Name      string
	Category  string
	Followers int64
	Likes     int64
	Verified  bool
	Intro     string
	Posts     []*Post // newest first
	// DelegateID, when set, is the id of the older Page object beside a
	// new-style page's profile (ID), as Facebook's new Pages experience
	// has it. Only the profile has a timeline.
	DelegateID int64
}

// delegate returns the id the page's Page object has.
func (p *Page) delegate() int64 {
	if p.DelegateID != 0 {
		return p.DelegateID
	}
	return p.ID
}

// World is the synthetic dataset. It is safe for concurrent use.
type World struct {
	mu       sync.RWMutex
	rng      *rand.Rand
	spec     Spec
	nextID   int64
	authors  []Author
	pages    map[int64]*Page
	byVanity map[string]*Page
	// byDelegate maps new-style pages' Page object ids to the page.
	byDelegate map[int64]*Page
	posts      map[int64]*Post
	comments   map[int64]*Comment
}

var reactionTypes = []string{"like", "love", "care", "haha", "wow", "sad", "angry"}

var words = strings.Fields(`launch new season drop collection store sale limited edition crew team
	support community event live tonight thanks everyone love this amazing quality price shipping
	size color restock fake scam counterfeit official original brand legit question order
	delivery review great bad awesome when where how why please help`)

// NewWorld generates a world from spec.
func NewWorld(spec Spec) *World {
	if spec.Now.IsZero() {
		spec.Now = time.Now().UTC()
	}
	w := &World{
		rng:        rand.New(rand.NewPCG(spec.Seed, spec.Seed^0x9e3779b97f4a7c15)),
		spec:       spec,
		nextID:     1_000_000,
		pages:      map[int64]*Page{},
		byVanity:   map[string]*Page{},
		byDelegate: map[int64]*Page{},
		posts:      map[int64]*Post{},
		comments:   map[int64]*Comment{},
	}
	for i := 0; i < 200; i++ {
		w.authors = append(w.authors, Author{
			ID:       100_000_000_000 + int64(i)*7919,
			Name:     fmt.Sprintf("%s %s", firstNames[i%len(firstNames)], lastNames[(i*7)%len(lastNames)]),
			Vanity:   fmt.Sprintf("user.%d", i),
			Verified: i%37 == 0,
		})
	}
	for i := 0; i < spec.Pages; i++ {
		p := &Page{
			ID:        100_064_000_000_000 + int64(i)*1_000_003,
			Vanity:    fmt.Sprintf("brand%d", i),
			Name:      fmt.Sprintf("Brand %d Official", i),
			Category:  []string{"Clothing (Brand)", "Sportswear Store", "Shopping & retail"}[i%3],
			Followers: 1_000 + w.rng.Int64N(5_000_000),
			Verified:  i%2 == 0,
			Intro:     "Official page. Questions? Message us.",
		}
		p.Likes = p.Followers * 8 / 10
		if i%2 == 1 { // every other page is a new-style one
			p.DelegateID = 104_000_000_000_000 + int64(i)
			w.byDelegate[p.DelegateID] = p
		}
		w.pages[p.ID] = p
		w.byVanity[p.Vanity] = p
		for j := 0; j < spec.PostsPerPage; j++ {
			created := spec.Now.Add(-time.Duration(j+1) * spec.PostSpacing).Add(-time.Duration(w.rng.IntN(3600)) * time.Second)
			w.addPostLocked(p, created, j == spec.PostsPerPage-1)
		}
	}
	return w
}

func (w *World) id() int64 {
	w.nextID += 1 + w.rng.Int64N(97)
	return 1_200_000_000_000 + w.nextID
}

func (w *World) sentence(n int) string {
	var b []string
	for i := 0; i < n; i++ {
		b = append(b, words[w.rng.IntN(len(words))])
	}
	return strings.Join(b, " ")
}

func (w *World) reactions(scale int64) map[string]int64 {
	m := map[string]int64{}
	if scale <= 0 {
		return m
	}
	total := w.rng.Int64N(scale) + 1
	m["like"] = total * 6 / 10
	rest := total - m["like"]
	for _, t := range reactionTypes[1:] {
		if rest <= 0 {
			break
		}
		v := w.rng.Int64N(rest + 1)
		if v > 0 {
			m[t] = v
			rest -= v
		}
	}
	m["like"] += rest
	return m
}

func (w *World) addPostLocked(p *Page, created time.Time, pinned bool) *Post {
	types := []string{"status", "status", "photo", "photo", "album", "video", "reel", "link", "link", "share"}
	post := &Post{
		ID: w.id(), PageID: p.ID, CreatedAt: created, Type: types[w.rng.IntN(len(types))],
		Reactions: w.reactions(5000), Shares: w.rng.Int64N(300), Pinned: pinned,
	}
	post.Text = w.sentence(6+w.rng.IntN(20)) + " #" + words[w.rng.IntN(len(words))] + " #brand"
	switch post.Type {
	case "photo":
		post.Media = []*MediaItem{w.media("photo")}
	case "album":
		for k := 0; k < 2+w.rng.IntN(4); k++ {
			post.Media = append(post.Media, w.media("photo"))
		}
	case "video", "reel":
		post.Media = []*MediaItem{w.media("video")}
		post.Views = w.rng.Int64N(500_000)
	case "link":
		post.LinkURL = fmt.Sprintf("https://shop.example.com/products/%d", w.rng.IntN(10000))
		post.LinkTitle = "Shop the " + w.sentence(3)
	case "share":
		// Share an older post of the same page when there is one.
		if len(p.Posts) > 0 {
			post.SharedPost = p.Posts[len(p.Posts)-1].ID
		} else {
			post.Type = "status"
		}
	}
	// Heavy-tailed comment counts: most posts few, some many.
	n := 0
	if w.spec.MaxComments > 0 {
		r := w.rng.Float64()
		n = int(float64(w.spec.MaxComments) * r * r * r)
	}
	at := created.Add(time.Minute)
	for k := 0; k < n; k++ {
		at = at.Add(time.Duration(1+w.rng.IntN(600)) * time.Second)
		c := w.newComment(post, nil, at)
		post.Comments = append(post.Comments, c)
	}
	p.Posts = append(p.Posts, post)
	sort.SliceStable(p.Posts, func(i, j int) bool { return p.Posts[i].CreatedAt.After(p.Posts[j].CreatedAt) })
	w.posts[post.ID] = post
	return post
}

func (w *World) media(kind string) *MediaItem {
	m := &MediaItem{ID: w.id(), Kind: kind, Width: 720 + w.rng.IntN(1200), Height: 720 + w.rng.IntN(800)}
	if kind == "video" {
		m.Duration = 5 + float64(w.rng.IntN(170))
	}
	if kind == "photo" {
		m.Alt = "May be an image of " + w.sentence(3)
	}
	return m
}

func (w *World) newComment(post *Post, parent *Comment, at time.Time) *Comment {
	a := w.authors[w.rng.IntN(len(w.authors))]
	c := &Comment{
		ID: w.id(), PostID: post.ID, Author: a, Text: w.sentence(3 + w.rng.IntN(25)),
		CreatedAt: at, Reactions: w.reactions(40), Parent: parent,
		Filtered: w.rng.Float64() < 0.3, Edited: w.rng.Float64() < 0.05,
	}
	if parent != nil {
		c.Depth = parent.Depth + 1
	}
	if w.rng.Float64() < 0.04 {
		c.Author = Author{ID: post.PageID, Name: w.pages[post.PageID].Name, Vanity: w.pages[post.PageID].Vanity}
		c.IsOP = true
	}
	if w.rng.Float64() < w.spec.AttachmentProb {
		c.Media = w.media([]string{"photo", "sticker", "gif"}[w.rng.IntN(3)])
	}
	w.comments[c.ID] = c
	prob := w.spec.ReplyProbability
	if c.Depth == 1 {
		prob = w.spec.Depth2Prob
	}
	if c.Depth < 2 && w.rng.Float64() < prob {
		n := 1 + w.rng.IntN(max(1, w.spec.MaxReplies))
		rat := at
		for k := 0; k < n; k++ {
			rat = rat.Add(time.Duration(1+w.rng.IntN(300)) * time.Second)
			c.Replies = append(c.Replies, w.newComment(post, c, rat))
		}
	}
	return c
}

var firstNames = strings.Fields("Ana Ben Chen Dara Eli Fatima Gio Hana Ivan Jade Kofi Lena Mo Nia Omar Priya Quinn Rosa Sam Tariq Uma Vik Wen Yara Zoe")
var lastNames = strings.Fields("Silva Okafor Nakamura Haddad Kowalski Mensah Rossi Duarte Novak Larsen Kaya Ibrahim Fischer Moreau")

// --- read helpers (callers hold no lock) ---------------------------------

func (w *World) PageByRef(ref string) *Page {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if p, ok := w.byVanity[strings.ToLower(ref)]; ok {
		return p
	}
	var id int64
	if _, err := fmt.Sscan(ref, &id); err == nil {
		return w.pages[id]
	}
	return nil
}

// Pages returns every page.
func (w *World) Pages() []*Page {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]*Page, 0, len(w.pages))
	for _, p := range w.pages {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Truth is the ground truth for one page.
type Truth struct {
	PageID int64       `json:"page_id"`
	Vanity string      `json:"vanity"`
	Posts  []PostTruth `json:"posts"`
}

type PostTruth struct {
	PostID        int64     `json:"post_id"`
	CreatedAt     time.Time `json:"created_at"`
	Type          string    `json:"type"`
	TotalComments int       `json:"total_comments"`
	TopLevel      int       `json:"top_level"`
	CommentIDs    []int64   `json:"comment_ids"`
	MediaCount    int       `json:"media_count"`
}

// TruthFor returns the ground truth for a page, restricted to posts created
// after since (zero = all).
func (w *World) TruthFor(pageID int64, since time.Time) (Truth, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	p, ok := w.pages[pageID]
	if !ok {
		return Truth{}, false
	}
	t := Truth{PageID: p.ID, Vanity: p.Vanity}
	for _, post := range p.Posts {
		if !since.IsZero() && post.CreatedAt.Before(since) {
			continue
		}
		pt := PostTruth{PostID: post.ID, CreatedAt: post.CreatedAt, Type: post.Type, TopLevel: len(post.Comments), MediaCount: len(post.Media)}
		var walk func(cs []*Comment)
		walk = func(cs []*Comment) {
			for _, c := range cs {
				if c.Hidden {
					continue
				}
				pt.CommentIDs = append(pt.CommentIDs, c.ID)
				walk(c.Replies)
			}
		}
		walk(post.Comments)
		pt.TotalComments = len(pt.CommentIDs)
		pt.TopLevel = 0
		for _, c := range post.Comments {
			if !c.Hidden {
				pt.TopLevel++
			}
		}
		t.Posts = append(t.Posts, pt)
	}
	return t, true
}

// Mutate adds new posts and comments "now" to a page (scheduled re-scrape
// tests).
func (w *World) Mutate(pageID int64, newPosts, newCommentsOnLatest int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.pages[pageID]
	if !ok {
		return fmt.Errorf("fakefb: no page %d", pageID)
	}
	now := time.Now().UTC()
	for i := 0; i < newPosts; i++ {
		w.addPostLocked(p, now.Add(-time.Duration(newPosts-i)*time.Minute), false)
	}
	if newCommentsOnLatest > 0 && len(p.Posts) > 0 {
		var target *Post
		for _, post := range p.Posts {
			if !post.Pinned {
				target = post
				break
			}
		}
		at := now
		for i := 0; i < newCommentsOnLatest; i++ {
			at = at.Add(time.Second)
			target.Comments = append(target.Comments, w.newComment(target, nil, at))
		}
	}
	return nil
}

// AddComments adds n new top-level comments to post postID, as if people
// kept commenting on an older post.
func (w *World) AddComments(postID int64, n int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range w.pages {
		for _, post := range p.Posts {
			if post.ID != postID {
				continue
			}
			at := time.Now().UTC()
			for i := 0; i < n; i++ {
				at = at.Add(time.Second)
				post.Comments = append(post.Comments, w.newComment(post, nil, at))
			}
			return nil
		}
	}
	return fmt.Errorf("fakefb: no post %d", postID)
}

// AddReplies adds n new replies (without replies of their own) to a
// comment, as when people answer an old comment.
func (w *World) AddReplies(commentID int64, n int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.comments[commentID]
	if c == nil {
		return fmt.Errorf("fakefb: no comment %d", commentID)
	}
	post := w.posts[c.PostID]
	at := time.Now().UTC()
	for i := 0; i < n; i++ {
		at = at.Add(time.Second)
		r := w.newComment(post, c, at)
		for _, nested := range r.Replies {
			delete(w.comments, nested.ID)
		}
		r.Replies = nil
		c.Replies = append(c.Replies, r)
	}
	return nil
}

// DeleteComment removes a comment (and its replies), as if its author
// deleted it.
func (w *World) DeleteComment(id int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	c, ok := w.comments[id]
	if !ok {
		return fmt.Errorf("fakefb: no comment %d", id)
	}
	drop := func(cs []*Comment) []*Comment {
		out := cs[:0]
		for _, x := range cs {
			if x.ID != id {
				out = append(out, x)
			}
		}
		return out
	}
	if c.Parent != nil {
		c.Parent.Replies = drop(c.Parent.Replies)
	} else {
		post := w.posts[c.PostID]
		post.Comments = drop(post.Comments)
	}
	var forget func(c *Comment)
	forget = func(c *Comment) {
		delete(w.comments, c.ID)
		for _, r := range c.Replies {
			forget(r)
		}
	}
	forget(c)
	return nil
}

// WithholdComments leaves the first n top-level comments of post postID out
// of the comment lists while still counting them.
func (w *World) WithholdComments(postID int64, n int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	post, ok := w.posts[postID]
	if !ok {
		return fmt.Errorf("fakefb: no post %d", postID)
	}
	for _, c := range post.Comments[:min(n, len(post.Comments))] {
		c.Withheld = true
	}
	return nil
}

// HideComment hides (or shows again) a top-level comment, as a page
// moderating its comments would.
func (w *World) HideComment(id int64, hidden bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	c, ok := w.comments[id]
	if !ok || c.Parent != nil {
		return fmt.Errorf("fakefb: no top-level comment %d", id)
	}
	c.Hidden = hidden
	return nil
}

// EditComment replaces a comment's text and flags it edited.
func (w *World) EditComment(id int64, text string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	c, ok := w.comments[id]
	if !ok {
		return fmt.Errorf("fakefb: no comment %d", id)
	}
	c.Text, c.Edited = text, true
	return nil
}

func countAll(cs []*Comment) int {
	n := 0
	for _, c := range cs {
		if !c.Hidden {
			n += 1 + countAll(c.Replies)
		}
	}
	return n
}
