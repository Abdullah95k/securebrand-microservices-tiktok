package fakefb

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Faults configures injected failures. Counters are per identity (datr).
type Faults struct {
	RateLimitEvery int           `json:"rate_limit_every"` // every Nth GraphQL request -> rate-limit error
	LoginWallAfter int           `json:"login_wall_after"` // after N GraphQL requests -> login required
	Latency        time.Duration `json:"latency"`
	ConsentWall    bool          `json:"consent_wall"`
	Drift          bool          `json:"drift"` // rename story.message
	// DriftRequired renames the stories' creation_time: the timeline parser
	// misses a required field.
	DriftRequired bool `json:"drift_required"`
	// JSOnlyTokens serves pages whose tokens only a browser sees (a script
	// inserts them), so HTTP bootstraps fail and need the browser.
	JSOnlyTokens bool          `json:"js_only_tokens"`
	MediaTTL     time.Duration `json:"media_ttl"`
	// FieldErrors streams a partial "field_exception" error part after each
	// timeline page, as real Facebook does beside the posts.
	FieldErrors bool `json:"field_errors"`
	// NoPageID serves page HTML that names no page id (no userID, no
	// pageID, no fb:// link), as a few real pages do.
	NoPageID bool `json:"no_page_id"`
}

// Options configures a Server.
type Options struct {
	// Docs maps friendly names to accepted doc_ids.
	Docs   map[string][]string
	Faults Faults
	// TopReactions is how many reaction types Facebook exposes (default 3).
	TopReactions int
	InlineReply  bool
	// BundleOmits are friendly names the relay bundle does not declare
	// (as if lazily loaded from a bundle the scan does not reach).
	BundleOmits []string
}

// Server is the fake Facebook HTTP handler.
type Server struct {
	World *World

	mu          sync.Mutex
	opts        Options
	lsds        map[string]string // lsd -> datr
	perIdent    map[string]*identStats
	byQuery     map[string]int
	rateLimited int // rate-limit errors served
	mediaKey    []byte
}

type identStats struct {
	Requests    int       `json:"requests"`
	GraphQL     int       `json:"graphql"`
	Last        time.Time `json:"-"`
	MinInterval float64   `json:"min_interval_seconds"`
}

// NewServer wraps w.
func NewServer(w *World, o Options) *Server {
	if o.TopReactions == 0 {
		o.TopReactions = 3
	}
	if o.Faults.MediaTTL == 0 {
		o.Faults.MediaTTL = 7 * 24 * time.Hour
	}
	key := make([]byte, 16)
	_, _ = rand.Read(key)
	return &Server{World: w, opts: o, lsds: map[string]string{}, perIdent: map[string]*identStats{},
		byQuery: map[string]int{}, mediaKey: key}
}

// SetFaults replaces the fault configuration.
func (s *Server) SetFaults(f Faults) {
	s.mu.Lock()
	if f.MediaTTL == 0 {
		f.MediaTTL = s.opts.Faults.MediaTTL
	}
	s.opts.Faults = f
	s.mu.Unlock()
}

// AcceptDocs sets the accepted doc_ids of friendly; the bundle declares
// the first.
func (s *Server) AcceptDocs(friendly string, docIDs ...string) {
	s.mu.Lock()
	if s.opts.Docs == nil {
		s.opts.Docs = map[string][]string{}
	}
	s.opts.Docs[friendly] = append([]string(nil), docIDs...)
	s.mu.Unlock()
}

// OmitFromBundle stops the relay bundle declaring the friendly names.
func (s *Server) OmitFromBundle(friendly ...string) {
	s.mu.Lock()
	s.opts.BundleOmits = append(s.opts.BundleOmits, friendly...)
	s.mu.Unlock()
}

// RotateDoc makes docID the only accepted doc_id for friendly.
func (s *Server) RotateDoc(friendly, docID string) {
	s.mu.Lock()
	if s.opts.Docs == nil {
		s.opts.Docs = map[string][]string{}
	}
	s.opts.Docs[friendly] = []string{docID}
	s.mu.Unlock()
}

func (s *Server) renderer(r *http.Request) *renderer {
	s.mu.Lock()
	defer s.mu.Unlock()
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return &renderer{base: scheme + "://" + r.Host, mediaKey: s.mediaKey, mediaTTL: s.opts.Faults.MediaTTL,
		topN: s.opts.TopReactions, inlineRepl: s.opts.InlineReply, drift: s.opts.Faults.Drift, driftReq: s.opts.Faults.DriftRequired,
		jsOnly: s.opts.Faults.JSOnlyTokens, noID: s.opts.Faults.NoPageID, timeline: first(s.opts.Docs["ProfileCometTimelineFeedRefetchQuery"]),
		bundle: s.bundlePath()}
}

// bundlePath is the relay bundle's URL path. Like Facebook's, it is content
// addressed: it changes whenever the accepted doc_ids do. Callers hold s.mu.
func (s *Server) bundlePath() string {
	names := make([]string, 0, len(s.opts.Docs))
	for name := range s.opts.Docs {
		names = append(names, name)
	}
	sort.Strings(names)
	h := fnv.New64a()
	for _, name := range names {
		_, _ = fmt.Fprintf(h, "%s=%v;", name, s.opts.Docs[name])
	}
	_, _ = fmt.Fprintf(h, "omit=%v", s.opts.BundleOmits)
	return fmt.Sprintf("/rsrc.php/v3/%x/relay.js", h.Sum64())
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	lat := s.opts.Faults.Latency
	s.mu.Unlock()
	if lat > 0 {
		time.Sleep(lat/2 + time.Duration(mrand.Int64N(int64(lat))))
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/__fakefb/"):
		s.admin(w, r)
	case strings.HasPrefix(r.URL.Path, "/media/"):
		s.media(w, r)
	case strings.HasPrefix(r.URL.Path, "/rsrc.php/"):
		s.bundle(w)
	case r.URL.Path == "/api/graphql/" && r.Method == http.MethodPost:
		s.graphql(w, r)
	case r.URL.Path == "/cookie/consent/" && r.Method == http.MethodPost:
		http.SetCookie(w, &http.Cookie{Name: "fbs_consent", Value: "1", Path: "/"})
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `for (;;);{"payload":null}`)
	case r.Method == http.MethodGet:
		s.page(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) ident(r *http.Request) string {
	if c, err := r.Cookie("datr"); err == nil && c.Value != "" {
		return c.Value
	}
	return "anon"
}

func (s *Server) track(ident string, gql bool, query string) *identStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.perIdent[ident]
	if st == nil {
		st = &identStats{MinInterval: -1}
		s.perIdent[ident] = st
	}
	now := time.Now()
	if !st.Last.IsZero() {
		d := now.Sub(st.Last).Seconds()
		if st.MinInterval < 0 || d < st.MinInterval {
			st.MinInterval = d
		}
	}
	st.Last = now
	st.Requests++
	if gql {
		st.GraphQL++
		s.byQuery[query]++
	}
	return st
}

func randToken(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)[:n]
}

func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	ref := strings.Trim(r.URL.Path, "/")
	if i := strings.IndexByte(ref, '/'); i >= 0 {
		ref = ref[:i]
	}
	if ref == "profile.php" {
		ref = r.URL.Query().Get("id")
	}
	if strings.HasPrefix(ref, "login") {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, `<html><body><form id="login_form"></form></body></html>`)
		return
	}
	ident := s.ident(r)
	s.track(ident, false, "")
	var p *Page
	if ref != "" {
		p = s.World.PageByRef(ref)
		if p == nil {
			http.Error(w, "This content isn't available right now", http.StatusNotFound)
			return
		}
	} else {
		pages := s.World.Pages()
		if len(pages) == 0 {
			http.NotFound(w, r)
			return
		}
		p = pages[0]
	}
	datr := ident
	if datr == "anon" {
		datr = randToken(24)
	}
	lsd := randToken(11)
	s.mu.Lock()
	s.lsds[lsd] = datr
	consent := s.opts.Faults.ConsentWall
	s.mu.Unlock()
	if c, err := r.Cookie("fbs_consent"); err == nil && c.Value == "1" {
		consent = false
	}
	rd := s.renderer(r)
	s.World.mu.RLock()
	body := rd.pageHTML(p, lsd, datr, consent)
	s.World.mu.RUnlock()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(body)
}

// gqlContentType is what Facebook labels its GraphQL responses: JSON, sent
// as text/html.
const gqlContentType = `text/html; charset="utf-8"`

func gqlError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", gqlContentType)
	_ = json.NewEncoder(w).Encode(obj{"errors": []any{obj{"code": code, "message": msg, "severity": "CRITICAL", "summary": msg}}})
}

func (s *Server) graphql(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	friendly := r.PostForm.Get("fb_api_req_friendly_name")
	docID := r.PostForm.Get("doc_id")
	lsd := r.PostForm.Get("lsd")
	ident := s.ident(r)
	st := s.track(ident, true, friendly)

	s.mu.Lock()
	_, lsdOK := s.lsds[lsd]
	f := s.opts.Faults
	docs := s.opts.Docs[friendly]
	gqlCount := st.GraphQL
	s.mu.Unlock()

	if !lsdOK || r.Header.Get("x-fb-lsd") != lsd {
		w.Header().Set("Content-Type", gqlContentType)
		_, _ = io.WriteString(w, `for (;;);{"error":1357001,"errorSummary":"Sorry, something went wrong","errorDescription":"Please try again."}`)
		return
	}
	if r.Header.Get("x-fb-friendly-name") != friendly {
		gqlError(w, 1357004, "friendly name mismatch")
		return
	}
	if len(docs) > 0 && !contains(docs, docID) {
		gqlError(w, 1675012, "PersistedQueryNotFound: doc_id "+docID+" is not a persisted query")
		return
	}
	if f.RateLimitEvery > 0 && gqlCount%f.RateLimitEvery == 0 {
		s.mu.Lock()
		s.rateLimited++
		s.mu.Unlock()
		w.Header().Set("Content-Type", gqlContentType)
		_, _ = io.WriteString(w, `for (;;);{"error":1675004,"errorSummary":"Rate limit exceeded","errorDescription":"Please try again later."}`)
		return
	}
	if f.LoginWallAfter > 0 && gqlCount > f.LoginWallAfter {
		gqlError(w, 1357005, "Please log in to continue.")
		return
	}
	var vars map[string]any
	if err := json.Unmarshal([]byte(r.PostForm.Get("variables")), &vars); err != nil {
		gqlError(w, 1357004, "invalid variables")
		return
	}
	rd := s.renderer(r)
	var body []byte
	switch friendly {
	case "ProfileCometTimelineFeedRefetchQuery":
		body = s.timeline(rd, vars)
		if f.FieldErrors {
			body = append(body, "\n"+`{"label":"ProfileCometTimelineFeed_feedback$defer","path":["node","timeline_list_feed_units","edges",0],`+
				`"data":null,"errors":[{"message":"A server error field_exception occured. Check server logs for details.","severity":"ERROR",`+ //nolint:misspell // Facebook's own spelling
				`"path":["node","timeline_list_feed_units","edges",0,"node","feedback"]}]}`...)
		}
	case "CommentsListComponentsPaginationQuery":
		body = s.comments(rd, vars)
	case "Depth1CommentsListPaginationQuery":
		body = s.replies(rd, vars)
	default:
		gqlError(w, 1675012, "PersistedQueryNotFound: unknown operation "+friendly)
		return
	}
	w.Header().Set("Content-Type", gqlContentType)
	_, _ = w.Write(body)
}

func str(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}

func intOr(v any, d int) int {
	switch t := v.(type) {
	case float64:
		if t > 0 {
			return int(t)
		}
	case string:
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			return n
		}
	}
	return d
}

func (s *Server) timeline(rd *renderer, vars map[string]any) []byte {
	w := s.World
	w.mu.RLock()
	defer w.mu.RUnlock()
	pid, _ := strconv.ParseInt(str(vars["id"]), 10, 64)
	p := w.pages[pid]
	if p == nil && w.byDelegate[pid] != nil {
		// A new-style page's Page object: Facebook resolves the id and
		// returns no timeline at all.
		return ndjson(obj{"data": obj{"node": obj{"__typename": "Page", "id": str(vars["id"])}}, "extensions": obj{"is_final": true}})
	}
	if p == nil {
		return ndjson(obj{"data": obj{"node": nil}, "extensions": obj{"is_final": true}})
	}
	_, _, off, ok := parseCursor(str(vars["cursor"]))
	if !ok {
		return ndjson(obj{"errors": []any{obj{"code": 1357004, "message": "invalid cursor"}}})
	}
	count := intOr(vars["count"], 3)
	// Pinned post first, then newest first.
	ordered := make([]*Post, 0, len(p.Posts))
	for _, x := range p.Posts {
		if x.Pinned {
			ordered = append(ordered, x)
		}
	}
	for _, x := range p.Posts {
		if !x.Pinned {
			ordered = append(ordered, x)
		}
	}
	end := min(off+count, len(ordered))
	page := ordered[min(off, len(ordered)):end]
	hasNext := end < len(ordered)
	endCursor := cursor("t", pid, end)
	label := "ProfileCometTimelineFeed_user$stream$ProfileCometTimelineFeed_user_timeline_list_feed_units"
	var parts []obj
	if len(page) == 0 {
		parts = append(parts, obj{"data": obj{"node": obj{"__typename": "User", "id": str(vars["id"]),
			"timeline_list_feed_units": obj{"edges": []any{}, "page_info": obj{"has_next_page": false, "end_cursor": endCursor}}}}})
		return ndjson(parts...)
	}
	for i, post := range page {
		edge := obj{"node": rd.story(w, p, post, (off+i)%2 == 1), "cursor": cursor("t", pid, off+i+1)}
		if i == 0 {
			parts = append(parts, obj{"data": obj{"node": obj{"__typename": "User", "id": str(vars["id"]),
				"timeline_list_feed_units": obj{"edges": []any{edge}}}}, "extensions": obj{"is_final": false}})
			continue
		}
		parts = append(parts, obj{"label": label, "path": []any{"node", "timeline_list_feed_units", "edges", i},
			"data": edge, "extensions": obj{"is_final": false}})
	}
	parts = append(parts, obj{
		"label":      "ProfileCometTimelineFeed_user$defer$ProfileCometTimelineFeed_user_timeline_list_feed_units$page_info",
		"path":       []any{"node", "timeline_list_feed_units"},
		"data":       obj{"page_info": obj{"has_next_page": hasNext, "end_cursor": endCursor}},
		"extensions": obj{"is_final": true},
	})
	return ndjson(parts...)
}

func (s *Server) postByFeedback(id string) *Post {
	b, err := base64.StdEncoding.DecodeString(id)
	if err != nil || !strings.HasPrefix(string(b), "feedback:") {
		return nil
	}
	pid, err := strconv.ParseInt(strings.TrimPrefix(string(b), "feedback:"), 10, 64)
	if err != nil {
		return nil
	}
	return s.World.posts[pid]
}

func (s *Server) commentByFeedback(id string) *Comment {
	b, err := base64.StdEncoding.DecodeString(id)
	if err != nil {
		return nil
	}
	v := strings.TrimPrefix(string(b), "feedback:")
	i := strings.IndexByte(v, '_')
	if i < 0 {
		return nil
	}
	cid, err := strconv.ParseInt(v[i+1:], 10, 64)
	if err != nil {
		return nil
	}
	return s.World.comments[cid]
}

const commentsPageSize = 25
const repliesPageSize = 10

func (s *Server) comments(rd *renderer, vars map[string]any) []byte {
	w := s.World
	w.mu.RLock()
	defer w.mu.RUnlock()
	fid := str(vars["id"])
	post := s.postByFeedback(fid)
	if post == nil {
		return ndjson(obj{"data": obj{"node": nil}})
	}
	_, _, off, ok := parseCursor(str(vars["commentsAfterCursor"]))
	if !ok {
		return ndjson(obj{"errors": []any{obj{"code": 1357004, "message": "invalid cursor"}}})
	}
	var list []*Comment
	for _, c := range post.Comments {
		if !c.Withheld && !c.Hidden {
			list = append(list, c)
		}
	}
	switch str(vars["commentsIntentToken"]) {
	case "RECENT_ACTIVITY_INTENT_V1":
		sort.SliceStable(list, func(i, j int) bool { return list[i].CreatedAt.After(list[j].CreatedAt) })
	case "RANKED_FILTERED_INTENT_V1":
		kept := list[:0]
		for _, c := range list {
			if !c.Filtered {
				kept = append(kept, c)
			}
		}
		list = kept
	}
	end := min(off+commentsPageSize, len(list))
	var edges []any
	for i, c := range list[min(off, len(list)):end] {
		edges = append(edges, obj{"node": rd.comment(w, post, c, true), "cursor": cursor("c", post.ID, off+i+1)})
	}
	if edges == nil {
		edges = []any{}
	}
	return ndjson(obj{"data": obj{"node": obj{
		"__typename": "Feedback", "id": fid,
		"comment_rendering_instance_for_feed_location": obj{"comments": obj{
			"edges":       edges,
			"page_info":   obj{"has_next_page": end < len(list), "end_cursor": cursor("c", post.ID, end)},
			"total_count": countAll(post.Comments),
		}},
	}}, "extensions": obj{"is_final": true}})
}

func (s *Server) replies(rd *renderer, vars map[string]any) []byte {
	w := s.World
	w.mu.RLock()
	defer w.mu.RUnlock()
	fid := str(vars["id"])
	c := s.commentByFeedback(fid)
	if c == nil {
		return ndjson(obj{"data": obj{"node": nil}})
	}
	if str(vars["expansionToken"]) != b64(fmt.Sprintf("exp:%d", c.ID)) {
		return ndjson(obj{"errors": []any{obj{"code": 1357004, "message": "invalid expansion token"}}})
	}
	_, _, off, ok := parseCursor(str(vars["repliesAfterCursor"]))
	if !ok {
		return ndjson(obj{"errors": []any{obj{"code": 1357004, "message": "invalid cursor"}}})
	}
	post := w.posts[c.PostID]
	end := min(off+repliesPageSize, len(c.Replies))
	var edges []any
	for i, rpl := range c.Replies[min(off, len(c.Replies)):end] {
		edges = append(edges, obj{"node": rd.comment(w, post, rpl, false), "cursor": cursor("r", c.ID, off+i+1)})
	}
	if edges == nil {
		edges = []any{}
	}
	return ndjson(obj{"data": obj{"node": obj{
		"__typename": "Feedback", "id": fid,
		"replies_connection": obj{
			"edges":     edges,
			"page_info": obj{"has_next_page": end < len(c.Replies), "end_cursor": cursor("r", c.ID, end)},
		},
	}}, "extensions": obj{"is_final": true}})
}

func (s *Server) media(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/media/")
	oe, oh := r.URL.Query().Get("oe"), r.URL.Query().Get("oh")
	if oe != "" || oh != "" {
		rd := s.renderer(r)
		if oh != rd.sign(name, oe) {
			http.Error(w, "Bad URL hash", http.StatusForbidden)
			return
		}
		exp, err := strconv.ParseInt(oe, 16, 64)
		if err != nil || time.Now().Unix() > exp {
			http.Error(w, "URL signature expired", http.StatusForbidden)
			return
		}
	}
	sum := sha256.Sum256([]byte(name))
	size := 20_000 + int(binary.BigEndian.Uint32(sum[:4])%60_000)
	ct := "image/jpeg"
	switch {
	case strings.HasSuffix(name, ".mp4"):
		size = 150_000 + int(binary.BigEndian.Uint32(sum[4:8])%250_000)
		ct = "video/mp4"
	case strings.HasSuffix(name, ".gif"):
		ct = "image/gif"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", strconv.Itoa(size))
	rng := mrand.New(mrand.NewPCG(binary.BigEndian.Uint64(sum[8:16]), binary.BigEndian.Uint64(sum[16:24])))
	buf := make([]byte, 32<<10)
	for size > 0 {
		n := min(size, len(buf))
		for i := 0; i < n; i++ {
			buf[i] = byte(rng.UintN(256))
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return
		}
		size -= n
	}
}

// Stats is the admin snapshot.
type Stats struct {
	Identities  map[string]identStats `json:"identities"`
	ByQuery     map[string]int        `json:"by_query"`
	RateLimited int                   `json:"rate_limited"`
}

func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := Stats{Identities: map[string]identStats{}, ByQuery: map[string]int{}, RateLimited: s.rateLimited}
	for k, v := range s.perIdent {
		out.Identities[k] = *v
	}
	for k, v := range s.byQuery {
		out.ByQuery[k] = v
	}
	return out
}

func (s *Server) admin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/__fakefb/")
	switch {
	case path == "pages":
		var out []obj
		for _, p := range s.World.Pages() {
			out = append(out, obj{"page_id": p.ID, "vanity": p.Vanity, "name": p.Name, "posts": len(p.Posts)})
		}
		_ = json.NewEncoder(w).Encode(out)
	case strings.HasPrefix(path, "truth/"):
		id, _ := strconv.ParseInt(strings.TrimPrefix(path, "truth/"), 10, 64)
		var since time.Time
		if v := r.URL.Query().Get("since"); v != "" {
			since, _ = time.Parse(time.RFC3339, v)
		}
		t, ok := s.World.TruthFor(id, since)
		if !ok {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(t)
	case path == "mutate" && r.Method == http.MethodPost:
		var req struct {
			PageID      int64 `json:"page_id"`
			NewPosts    int   `json:"new_posts"`
			NewComments int   `json:"new_comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.World.Mutate(req.PageID, req.NewPosts, req.NewComments); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	case path == "faults" && r.Method == http.MethodPost:
		var f Faults
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.SetFaults(f)
		_, _ = io.WriteString(w, `{"ok":true}`)
	case path == "rotate" && r.Method == http.MethodPost:
		var req struct {
			FriendlyName string `json:"friendly_name"`
			DocID        string `json:"doc_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.RotateDoc(req.FriendlyName, req.DocID)
		_, _ = io.WriteString(w, `{"ok":true}`)
	case path == "stats":
		_ = json.NewEncoder(w).Encode(s.Stats())
	default:
		http.NotFound(w, r)
	}
}

func first(xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	return xs[0]
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// bundle serves a static JS bundle that declares the accepted doc_ids as
// Relay operation modules, the way Facebook's bundles do, so doc_id
// discovery (fbs-spike docids) can be exercised end to end.
func (s *Server) bundle(w http.ResponseWriter) {
	s.mu.Lock()
	names := make([]string, 0, len(s.opts.Docs))
	for name, ids := range s.opts.Docs {
		if len(ids) > 0 && !contains(s.opts.BundleOmits, name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "__d(\"%s_facebookRelayOperation\",[],(function(a,b,c,d,e,f){e.exports=\"%s\"}),null);\n",
			name, s.opts.Docs[name][0])
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/javascript")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	_, _ = io.WriteString(w, b.String())
}

// RotatedDocs returns docs with every doc_id replaced by a different,
// deterministic one, as after a Facebook deploy. Clients still using the
// original ids get PersistedQueryNotFound until they rediscover them.
func RotatedDocs(docs map[string][]string) map[string][]string {
	out := make(map[string][]string, len(docs))
	for name, ids := range docs {
		for _, id := range ids {
			r := "7316" + id
			if len(id) > 4 {
				r = "7316" + id[4:]
			}
			if !contains(out[name], r) {
				out[name] = append(out[name], r)
			}
		}
	}
	return out
}

// DocsFromSeed builds the accepted doc_id map from registry seed queries.
func DocsFromSeed(pairs map[string]string) map[string][]string {
	out := map[string][]string{}
	for friendly, doc := range pairs {
		out[friendly] = append(out[friendly], doc)
	}
	return out
}

var _ = hex.EncodeToString
