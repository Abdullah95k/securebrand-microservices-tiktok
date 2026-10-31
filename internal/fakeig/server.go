package fakeig

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// AppID is the app id the fake expects in x-ig-app-id.
const AppID = "936619743392459"

// Friendly names of the five operations.
const (
	OpProfile    = "PolarisLoggedOutDesktopWWWProfileRootContentQuery"
	OpGridFirst  = "PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery"
	OpGrid       = "PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery_connection"
	OpComments   = "PolarisLoggedOutDesktopWWWPostCommentsPaginationQuery"
	OpHighlights = "PolarisStoriesV3HighlightsLoggedOutPageQuery"
)

// GridPageSize and CommentPageSize are the recorded page sizes.
const (
	GridPageSize    = 12
	CommentPageSize = 10
)

// Faults model walls and the open questions.
type Faults struct {
	// RateLimitEvery answers every Nth GraphQL request with HTTP 429.
	RateLimitEvery int
	// LoginWallAfter walls an identity after N GraphQL requests: its
	// queries get a login page, its page loads a redirect to
	// /accounts/login/.
	LoginWallAfter int
	// Challenge redirects every page load to /challenge/.
	Challenge bool
	// GridWallAfter walls grid pages that start at or past this many posts
	// (U1: the browser stopped at 24).
	GridWallAfter int
	// RequireClientState fails queries sent without __dyn (U3).
	RequireClientState bool
	// CursorBound makes a cursor work only for the identity it was
	// issued to (U5).
	CursorBound bool
	// NoPKGrid fails the grid query sent without a cursor (U9).
	NoPKGrid bool
	// StreamGrid answers grid queries the way Relay streams a connection:
	// the first part holds the connection with no edges and a placeholder
	// page_info (has_next_page false), each edge follows in its own part
	// with a path, and the real page_info comes last in a deferred part. A
	// reader of the first part alone sees 0 posts and no next page.
	StreamGrid bool
}

// Options configure a server.
type Options struct {
	// Docs maps each friendly name to its accepted doc_ids; nil accepts any.
	Docs   map[string][]string
	Faults Faults
}

// Server serves a World.
type Server struct {
	World *World
	opts  Options

	mu     sync.Mutex
	idents map[string]*ident // by lsd
	byCSRF map[string]*ident
	minted uint64
	gql    atomic.Int64
	// Requests counts GraphQL requests by friendly name.
	Requests sync.Map
}

type ident struct {
	lsd, csrf string
	queries   int
}

// NewServer returns a server for w.
func NewServer(w *World, o Options) *Server {
	return &Server{World: w, opts: o, idents: map[string]*ident{}, byCSRF: map[string]*ident{}}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/accounts/login"):
		page(w, http.StatusOK, `<html><form action="/accounts/login/ajax/" method="post"></form></html>`)
	case strings.HasPrefix(r.URL.Path, "/challenge"):
		page(w, http.StatusOK, `<html><form action="/challenge/action/" method="post"></form></html>`)
	case strings.HasPrefix(r.URL.Path, "/fake-cdn/"):
		// A stand-in for the image CDN: a few bytes with a JPEG header.
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte("\xff\xd8\xff\xe0fake-jpeg" + r.URL.Path))
	case r.Method == http.MethodPost && (r.URL.Path == "/api/graphql" || r.URL.Path == "/graphql/query"):
		s.graphql(w, r)
	case r.Method == http.MethodGet:
		s.page(w, r)
	default:
		http.NotFound(w, r)
	}
}

// page serves a profile or post page: fresh tokens, cookies and embedded
// data, as a plain GET of any public page did in the recordings.
func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	if s.opts.Faults.Challenge {
		http.Redirect(w, r, "/challenge/?next="+r.URL.Path, http.StatusFound)
		return
	}
	if c, err := r.Cookie("csrftoken"); err == nil && s.walled(s.identByCSRF(c.Value)) {
		http.Redirect(w, r, "/accounts/login/?next="+r.URL.Path, http.StatusFound)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var data any
	switch {
	case len(parts) == 2 && (parts[0] == "p" || parts[0] == "reel"):
		post := s.postByCode(parts[1])
		if post == nil {
			http.NotFound(w, r)
			return
		}
		// The post page embeds the media object and its first comments.
		m := mediaJSON(post, true)
		m["comments_connection"] = s.commentPage(post, 0, "")
		data = map[string]any{"xig_polaris_media": m}
	case len(parts) == 1 && parts[0] != "":
		p := s.World.Profile(parts[0])
		if p == nil {
			http.NotFound(w, r)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/") {
			http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
			return
		}
		data = map[string]any{"xig_user_by_username": map[string]any{"pk": str(p.PK), "id": p.ID, "username": p.Username,
			"polaris_ordered_timeline_connection": s.gridPage(p, 0, "")}}
	default:
		http.NotFound(w, r)
		return
	}
	id := s.mint()
	for _, c := range []*http.Cookie{{Name: "csrftoken", Value: id.csrf}, {Name: "mid", Value: "fakemid" + id.lsd},
		{Name: "ig_did", Value: "fake-device-" + id.lsd}} {
		c.Path = "/"
		http.SetCookie(w, c)
	}
	blob, _ := json.Marshal(map[string]any{"result": map[string]any{"data": data}})
	page(w, http.StatusOK, fmt.Sprintf(`<html><head><script type="application/json" data-sjs>{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{"define":[`+
		`["LSD",[],{"token":"%s"},323],["InstagramSecurityConfig",[],{"csrf_token":"%s"},1],`+
		`["SiteData",[],{"server_revision":1029000000,"haste_session":"20400.HYP:instagram_web_pkg.2.1...0","hsi":"7560000000000000001",`+
		`"__spin_r":1029000000,"__spin_b":"trunk","__spin_t":1761800000},317],`+
		`["RelayAPIConfigDefaults",[],{"customHeaders":{"X-IG-App-ID":"%s"}},1],["PolarisSiteData",[],{"rollout_hash":"1029000000"},1],`+
		`["SprinkleConfig",[],{"jazoest":"22001"},1]]}}]]],"__comet_req":7}</script></head><body>`+
		`<script type="application/json" data-sjs>%s</script></body></html>`, id.lsd, id.csrf, AppID, blob))
}

func (s *Server) mint() *ident {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.minted++
	id := &ident{lsd: fmt.Sprintf("AVfake%06d", s.minted), csrf: fmt.Sprintf("csrffake%06d", s.minted)}
	s.idents[id.lsd] = id
	s.byCSRF[id.csrf] = id
	return id
}

func (s *Server) identByCSRF(csrf string) *ident {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byCSRF[csrf]
}

func (s *Server) walled(id *ident) bool {
	if id == nil || s.opts.Faults.LoginWallAfter <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return id.queries >= s.opts.Faults.LoginWallAfter
}

func (s *Server) graphql(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	f := r.PostForm
	friendly := f.Get("fb_api_req_friendly_name")
	n, _ := s.Requests.LoadOrStore(friendly, new(atomic.Int64))
	n.(*atomic.Int64).Add(1)
	if k := s.opts.Faults.RateLimitEvery; k > 0 && s.gql.Add(1)%int64(k) == 0 {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	s.mu.Lock()
	id := s.idents[f.Get("lsd")]
	over := false
	if id != nil {
		id.queries++
		over = s.opts.Faults.LoginWallAfter > 0 && id.queries > s.opts.Faults.LoginWallAfter
	}
	s.mu.Unlock()
	switch {
	case id == nil, r.Header.Get("x-fb-lsd") != id.lsd:
		softError(w, "unknown or mismatched lsd")
		return
	case r.Header.Get("x-csrftoken") != id.csrf:
		softError(w, "csrf token missing or wrong")
		return
	case r.Header.Get("x-ig-app-id") != AppID:
		softError(w, "app id missing or wrong")
		return
	case r.Header.Get("x-fb-friendly-name") != friendly:
		softError(w, "friendly name header and form differ")
		return
	case over:
		page(w, http.StatusOK, `<html><form action="/accounts/login/ajax/" method="post"></form></html>`)
		return
	case s.opts.Faults.RequireClientState && f.Get("__dyn") == "":
		softError(w, "client state missing")
		return
	}
	if docs, ok := s.opts.Docs[friendly]; s.opts.Docs != nil && (!ok || !slices.Contains(docs, f.Get("doc_id"))) {
		write(w, map[string]any{"errors": []any{map[string]any{"message": "PersistedQueryNotFound: doc_id " + f.Get("doc_id") + " is not a known persisted query"}}})
		return
	}
	if (friendly == OpHighlights) != (r.URL.Path == "/graphql/query") ||
		(friendly == OpHighlights && r.Header.Get("x-root-field-name") == "") {
		write(w, map[string]any{"errors": []any{map[string]any{"message": "unknown query on this endpoint"}}})
		return
	}
	var vars map[string]any
	dec := json.NewDecoder(strings.NewReader(f.Get("variables")))
	dec.UseNumber() // pks pass 2^53
	if err := dec.Decode(&vars); err != nil {
		softError(w, "bad variables")
		return
	}
	switch friendly {
	case OpProfile:
		p := s.World.Profile(sv(vars["username"]))
		if p == nil {
			write(w, data(map[string]any{"xig_user_by_username": nil}))
			return
		}
		write(w, data(map[string]any{"xig_user_by_username": profileJSON(p, "http://"+r.Host)}))
	case OpGridFirst:
		p := s.World.Profile(sv(vars["username"]))
		if p == nil {
			write(w, data(map[string]any{"xig_user_by_username": nil}))
			return
		}
		s.writeGrid(w, p, 0, id, func(conn any) any {
			return map[string]any{"xig_user_by_username": map[string]any{"pk": str(p.PK), "id": p.ID, "polaris_ordered_timeline_connection": conn}}
		})
	case OpGrid:
		// The connection query resolves the user's id, not its pk: a pk
		// answers a null node, as Instagram does.
		p := s.World.byID[sv(vars["id"])]
		if p == nil {
			write(w, data(map[string]any{"node": nil}))
			return
		}
		pk := p.PK
		offset := 0
		if c := sv(vars["after"]); c != "" {
			var ok bool
			if offset, ok = s.cursor(c, "g", pk, id); !ok {
				softError(w, "invalid cursor")
				return
			}
		} else if s.opts.Faults.NoPKGrid {
			softError(w, "missing cursor")
			return
		}
		s.writeGrid(w, p, offset, id, func(conn any) any {
			return map[string]any{"node": map[string]any{"__typename": "XDTUserDict", "polaris_ordered_timeline_connection": conn}}
		})
	case OpComments:
		pk, _ := strconv.ParseInt(sv(vars["media_id"]), 10, 64)
		post := s.World.posts[pk]
		if post == nil {
			write(w, data(map[string]any{"xig_polaris_media": nil}))
			return
		}
		if a, ok := vars["after"]; ok && a == "" {
			// An empty cursor fails the connection, as on Instagram.
			write(w, map[string]any{"data": map[string]any{"xig_polaris_media": map[string]any{"id": "POLARIS_" + str(pk),
				"comments_connection": map[string]any{"edges": []any{}, "page_info": nil}}},
				"errors": []any{map[string]any{"message": "A server error field_exception occurred. Check server logs for details.",
					"severity": "ERROR", "path": []any{"xig_polaris_media", "comments_connection", "edges"}}}})
			return
		}
		offset := 0
		if c := sv(vars["after"]); c != "" {
			var ok bool
			if offset, ok = s.cursor(c, "c", pk, id); !ok {
				softError(w, "invalid cursor")
				return
			}
		}
		write(w, data(map[string]any{"xig_polaris_media": map[string]any{"pk": str(pk),
			"comments_connection": s.commentPage(post, offset, id.lsd)}}))
	case OpHighlights:
		write(w, data(map[string]any{"xdt_api__v1__feed__reels_media__connection": s.highlights(vars["reel_ids"])}))
	default:
		write(w, map[string]any{"errors": []any{map[string]any{"message": "unknown persisted query " + friendly}}})
	}
}

func (s *Server) writeGrid(w http.ResponseWriter, p *Profile, offset int, id *ident, wrap func(any) any) {
	if k := s.opts.Faults.GridWallAfter; k > 0 && offset >= k {
		page(w, http.StatusOK, `<html><form action="/accounts/login/ajax/" method="post"></form></html>`)
		return
	}
	lsd := ""
	if s.opts.Faults.CursorBound {
		lsd = id.lsd
	}
	page := s.gridPage(p, offset, lsd)
	if !s.opts.Faults.StreamGrid {
		write(w, data(wrap(page)))
		return
	}
	edges, pi := page["edges"].([]any), page["page_info"]
	head := map[string]any{"edges": []any{}, "page_info": map[string]any{"has_next_page": false, "end_cursor": pi.(map[string]any)["end_cursor"]}}
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	enc := json.NewEncoder(w)
	_ = enc.Encode(map[string]any{"data": wrap(head), "extensions": map[string]any{"is_final": false}})
	for i, e := range edges {
		_ = enc.Encode(map[string]any{"data": e, "label": "PolarisProfilePostsTab$stream$" + gridConnKey,
			"path": []any{"xig_user_by_username", gridConnKey, "edges", i}, "extensions": map[string]any{"is_final": false}})
	}
	_ = enc.Encode(map[string]any{"data": map[string]any{"page_info": pi}, "label": "PolarisProfilePostsTab$defer$" + gridConnKey + "_page_info",
		"path": []any{"xig_user_by_username", gridConnKey}, "extensions": map[string]any{"is_final": true}})
}

const gridConnKey = "polaris_ordered_timeline_connection"

func (s *Server) gridPage(p *Profile, offset int, lsd string) map[string]any {
	end := min(offset+GridPageSize, len(p.Posts))
	edges := []any{}
	for _, post := range p.Posts[min(offset, end):end] {
		edges = append(edges, map[string]any{"node": mediaJSON(post, false)})
	}
	pi := map[string]any{"has_next_page": end < len(p.Posts), "end_cursor": nil}
	if end < len(p.Posts) {
		pi["end_cursor"] = encodeCursor("g", p.PK, end, lsd)
	}
	return map[string]any{"edges": edges, "page_info": pi}
}

func (s *Server) commentPage(post *Post, offset int, lsd string) map[string]any {
	if !s.opts.Faults.CursorBound {
		lsd = ""
	}
	end := min(offset+CommentPageSize, len(post.Comments))
	edges := []any{}
	for _, c := range post.Comments[min(offset, end):end] {
		edges = append(edges, map[string]any{"node": map[string]any{"pk": str(c.PK), "id": str(c.PK), "text": c.Text,
			"created_at": c.CreatedAt.Unix(), "comment_like_count": c.Likes, "parent_comment_id": nil, "child_comment_count": nil,
			"user": map[string]any{"pk": str(c.UserPK), "id": str(c.UserPK), "username": c.Username, "is_verified": false}}})
	}
	pi := map[string]any{"has_next_page": end < len(post.Comments), "end_cursor": nil}
	if end < len(post.Comments) {
		pi["end_cursor"] = encodeCursor("c", post.PK, end, lsd)
	}
	return map[string]any{"edges": edges, "page_info": pi}
}

func (s *Server) highlights(ids any) map[string]any {
	edges := []any{}
	list, _ := ids.([]any)
	for _, raw := range list {
		want := strings.TrimPrefix(sv(raw), "highlight:")
		for _, p := range s.World.profiles {
			for _, h := range p.Highlights {
				if str(h.ID) == want {
					items := []any{}
					for i := range h.Items {
						items = append(items, map[string]any{"pk": str(h.ID*10 + int64(i)), "media_type": 1})
					}
					edges = append(edges, map[string]any{"node": map[string]any{"id": "highlight:" + want, "title": h.Title, "items": items}})
				}
			}
		}
	}
	return map[string]any{"edges": edges}
}

func (s *Server) postByCode(code string) *Post {
	for _, post := range s.World.posts {
		if post.Code == code {
			return post
		}
	}
	return nil
}

// cursor decodes a cursor of kind for parent pk; a bound cursor must be
// the identity's own.
func (s *Server) cursor(c, kind string, pk int64, id *ident) (int, bool) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, false
	}
	f := strings.Split(string(b), ":")
	if len(f) != 4 || f[0] != kind || f[1] != str(pk) {
		return 0, false
	}
	if s.opts.Faults.CursorBound && f[3] != id.lsd {
		return 0, false
	}
	n, err := strconv.Atoi(f[2])
	return n, err == nil
}

func encodeCursor(kind string, pk int64, offset int, lsd string) string {
	// Real cursors are 90 (grid) and 142 (comments) characters; only the
	// opacity matters here.
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%d:%d:%s", kind, pk, offset, lsd)))
}

func profileJSON(p *Profile, host string) map[string]any {
	hl := []any{}
	for _, h := range p.Highlights {
		hl = append(hl, map[string]any{"node": map[string]any{"id": "highlight:" + str(h.ID), "title": h.Title}})
	}
	return map[string]any{"pk": str(p.PK), "id": p.ID, "username": p.Username, "full_name": p.FullName,
		"biography": p.Bio, "is_verified": p.Verified, "is_private": false, "is_memorialized": false,
		"follower_count": p.Followers, "following_count": p.Following, "all_media_count": nil, "has_any_clips": true,
		"profile_pic_url": host + "/fake-cdn/" + str(p.PK) + ".jpg", "linked_fb_info": nil,
		"bio_links":                 []any{map[string]any{"title": "Site", "url": "https://example.com/" + p.Username}},
		"lox_highlights_connection": map[string]any{"edges": hl}}
}

// mediaJSON is a grid item, or with detail the post page's media object.
func mediaJSON(post *Post, detail bool) map[string]any {
	m := map[string]any{"pk": str(post.PK), "id": str(post.PK) + "_" + str(post.Owner.PK), "code": post.Code,
		"media_type": post.MediaType, "product_type": post.ProductType, "caption": map[string]any{"text": post.Caption},
		"display_uri": "https://scontent.cdninstagram.example/v/" + post.Code + ".jpg",
		"owner":       map[string]any{"pk": str(post.Owner.PK), "id": post.Owner.ID},
		"user":        map[string]any{"username": post.Owner.Username, "id": post.Owner.ID}}
	if detail {
		m["taken_at"] = post.TakenAt.Unix()
		m["like_count"] = post.Likes
		m["comment_count"] = len(post.Comments)
	}
	return m
}

func data(v any) map[string]any {
	return map[string]any{"data": v, "extensions": map[string]any{"is_final": true}}
}

func write(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func softError(w http.ResponseWriter, msg string) {
	write(w, map[string]any{"errors": []any{map[string]any{"code": 1357004, "message": msg}}})
}

func page(w http.ResponseWriter, status int, html string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(html))
}

func str(v int64) string { return strconv.FormatInt(v, 10) }

func sv(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	}
	return ""
}
