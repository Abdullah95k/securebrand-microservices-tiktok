package fakefb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// WriteFixtures renders deterministic parser fixtures (timeline, comments,
// replies, page HTML) for a world into dir/<query>/<case>/. They are
// synthetic, so they contain no personal data.
func WriteFixtures(dir string, w *World, base string, at time.Time) error {
	rd := &renderer{base: base, mediaKey: []byte("fixture-key"), mediaTTL: 7 * 24 * time.Hour, topN: 3,
		inlineRepl: true, clock: func() time.Time { return at }}
	s := &Server{World: w}
	write := func(query, name string, body []byte, meta map[string]any) error {
		d := filepath.Join(dir, query, name)
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
		ext := "response.ndjson"
		if query == "page_html" {
			ext = "response.html"
		}
		if err := os.WriteFile(filepath.Join(d, ext), body, 0o644); err != nil {
			return err
		}
		meta["source"] = "fakefb"
		meta["__scrubbed"] = true
		mb, _ := json.MarshalIndent(meta, "", "  ")
		return os.WriteFile(filepath.Join(d, "meta.json"), append(mb, '\n'), 0o644)
	}
	pages := w.Pages()
	p := pages[0]
	w.mu.RLock()
	html := rd.pageHTML(p, "AVqFixtureLSD", "FixtureDatrValue123", false)
	w.mu.RUnlock()
	if err := write("page_html", "fakefb_basic", html, map[string]any{"page_id": p.ID}); err != nil {
		return err
	}
	tl := s.timeline(rd, map[string]any{"id": fmt.Sprint(p.ID), "count": float64(5)})
	if err := write("timeline", "fakefb_first_page", tl, map[string]any{"page_id": p.ID}); err != nil {
		return err
	}
	// The post with the most comments, and its busiest thread.
	w.mu.RLock()
	var post *Post
	for _, x := range p.Posts {
		if post == nil || len(x.Comments) > len(post.Comments) {
			post = x
		}
	}
	var thread *Comment
	for _, c := range post.Comments {
		if thread == nil || len(c.Replies) > len(thread.Replies) {
			thread = c
		}
	}
	w.mu.RUnlock()
	fid := b64(fmt.Sprintf("feedback:%d", post.ID))
	cm := s.comments(rd, map[string]any{"id": fid, "commentsIntentToken": "RANKED_UNFILTERED_CHRONOLOGICAL_REPLIES_INTENT_V1"})
	if err := write("comments", "fakefb_first_page", cm, map[string]any{"page_id": p.ID, "post_id": post.ID}); err != nil {
		return err
	}
	if thread != nil && len(thread.Replies) > 0 {
		rp := s.replies(rd, map[string]any{"id": b64(fmt.Sprintf("feedback:%d_%d", post.ID, thread.ID)),
			"expansionToken": b64(fmt.Sprintf("exp:%d", thread.ID))})
		if err := write("replies", "fakefb_first_page", rp, map[string]any{"page_id": p.ID, "post_id": post.ID,
			"parent_comment_id": thread.ID}); err != nil {
			return err
		}
	}
	return nil
}
