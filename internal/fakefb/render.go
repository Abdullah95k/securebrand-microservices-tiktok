package fakefb

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"sort"
	"strconv"
	"strings"
	"time"
)

type obj = map[string]any

// Facebook reaction ids, as used in top_reactions.
var reactionNodeIDs = map[string]string{
	"like": "1635855486666999", "love": "1678524932434102", "care": "613557422527858",
	"haha": "115940658764963", "wow": "478547315650144", "sad": "908563459236466", "angry": "444813342392137",
}

type renderer struct {
	base       string // http://host:port
	mediaKey   []byte
	mediaTTL   time.Duration
	topN       int    // top_reactions entries exposed (Facebook shows 3)
	inlineRepl bool   // include one inline reply preview
	drift      bool   // schema drift: rename message -> msg_v2
	driftReq   bool   // schema drift: rename creation_time -> created_v2
	jsOnly     bool   // tokens are inserted by a script, not in the markup
	noID       bool   // the page HTML names no page id
	timeline   string // timeline doc_id the page script requests ("" = no script)
	bundle     string // relay bundle path; it changes when doc_ids rotate
	clock      func() time.Time
}

func (r *renderer) now() time.Time {
	if r.clock != nil {
		return r.clock()
	}
	return time.Now()
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func (r *renderer) mediaURL(m *MediaItem, variant string) string {
	ext := "jpg"
	if m.Kind == "video" && variant != "thumb" {
		ext = "mp4"
	}
	if m.Kind == "gif" {
		ext = "gif"
	}
	name := fmt.Sprintf("%d_%s.%s", m.ID, variant, ext)
	oe := strconv.FormatInt(r.now().Add(r.mediaTTL).Unix(), 16)
	return fmt.Sprintf("%s/media/%s?oe=%s&oh=%s", r.base, name, oe, r.sign(name, oe))
}

func (r *renderer) sign(name, oe string) string {
	h := hmac.New(sha256.New, r.mediaKey)
	h.Write([]byte(name + "|" + oe))
	return hex.EncodeToString(h.Sum(nil))[:24]
}

func (r *renderer) reactionsObj(m map[string]int64) (int64, []any) {
	var total int64
	type kv struct {
		k string
		v int64
	}
	var list []kv
	for k, v := range m {
		total += v
		list = append(list, kv{k, v})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].v != list[j].v {
			return list[i].v > list[j].v
		}
		return list[i].k < list[j].k
	})
	var edges []any
	for i, e := range list {
		if i >= r.topN {
			break
		}
		edges = append(edges, obj{
			"reaction_count": e.v,
			"node":           obj{"id": reactionNodeIDs[e.k], "localized_name": strings.ToUpper(e.k[:1]) + e.k[1:], "reaction_type": strings.ToUpper(e.k)},
		})
	}
	return total, edges
}

func (r *renderer) pageURL(p *Page) string { return r.base + "/" + p.Vanity }

func (r *renderer) postURL(p *Page, post *Post) string {
	if post.Type == "reel" {
		return fmt.Sprintf("%s/reel/%d", r.base, post.ID)
	}
	return fmt.Sprintf("%s/%s/posts/%d", r.base, p.Vanity, post.ID)
}

func (r *renderer) mediaNode(m *MediaItem) obj {
	switch m.Kind {
	case "video":
		return obj{
			"__typename": "Video", "id": strconv.FormatInt(m.ID, 10),
			"browser_native_sd_url": r.mediaURL(m, "sd"), "browser_native_hd_url": r.mediaURL(m, "hd"),
			"playable_url": r.mediaURL(m, "sd"), "playable_url_quality_hd": r.mediaURL(m, "hd"),
			"length_in_second": m.Duration, "width": m.Width, "height": m.Height,
			"preferred_thumbnail": obj{"image": obj{"uri": r.mediaURL(m, "thumb")}},
		}
	case "sticker":
		return obj{"__typename": "Sticker", "id": strconv.FormatInt(m.ID, 10),
			"image": obj{"uri": r.mediaURL(m, "orig"), "width": 240, "height": 240}}
	case "gif":
		return obj{"__typename": "GenericAttachmentMedia", "id": strconv.FormatInt(m.ID, 10),
			"animated_image": obj{"uri": r.mediaURL(m, "orig"), "width": 320, "height": 240}}
	default:
		return obj{"__typename": "Photo", "id": strconv.FormatInt(m.ID, 10),
			"accessibility_caption": m.Alt,
			"photo_image":           obj{"uri": r.mediaURL(m, "orig"), "width": m.Width, "height": m.Height}}
	}
}

// story renders a post as a Comet Story node.
func (r *renderer) story(w *World, p *Page, post *Post, alt bool) obj {
	total, edges := r.reactionsObj(post.Reactions)
	feedback := obj{
		"__typename":                 "Feedback",
		"id":                         b64("feedback:" + strconv.FormatInt(post.ID, 10)),
		"reaction_count":             obj{"count": total},
		"i18n_reaction_count":        strconv.FormatInt(total, 10),
		"top_reactions":              obj{"edges": edges},
		"share_count":                obj{"count": post.Shares},
		"comment_rendering_instance": obj{"comments": obj{"total_count": countAll(post.Comments)}},
	}
	if post.Views > 0 {
		feedback["video_view_count"] = post.Views
	}
	var atts []any
	switch post.Type {
	case "photo", "video", "reel":
		atts = append(atts, obj{"styles": obj{"attachment": obj{"media": r.mediaNode(post.Media[0])}}})
	case "album":
		var nodes []any
		for _, m := range post.Media {
			nodes = append(nodes, obj{"media": r.mediaNode(m)})
		}
		atts = append(atts, obj{"styles": obj{"attachment": obj{"all_subattachments": obj{"count": len(nodes), "nodes": nodes}}}})
	case "link":
		atts = append(atts, obj{"styles": obj{"attachment": obj{
			"url":                 "https://l.facebook.com/l.php?u=" + post.LinkURL + "&h=AT0",
			"title_with_entities": obj{"text": post.LinkTitle},
			"description":         obj{"text": "Free shipping on orders over $50"},
			"source":              obj{"text": "shop.example.com"},
			"media":               obj{"__typename": "GenericAttachmentMedia", "image": obj{"uri": r.base + "/media/link_" + strconv.FormatInt(post.ID, 10) + ".jpg"}},
		}}})
	}
	msgKey := "message"
	if r.drift {
		msgKey = "msg_v2"
	}
	content := obj{msgKey: obj{"text": post.Text, "ranges": hashtagRanges(post.Text)}}
	if len(atts) > 0 {
		content["attachments"] = atts
	}
	created := "creation_time"
	if r.driftReq {
		created = "created_v2"
	}
	s := obj{
		"__typename": "Story",
		"id":         b64(fmt.Sprintf("S:_I%d:%d:%d", p.ID, post.ID, post.ID)),
		"post_id":    strconv.FormatInt(post.ID, 10),
		"actors":     []any{obj{"__typename": "User", "id": strconv.FormatInt(p.ID, 10), "name": p.Name, "url": r.pageURL(p)}},
		"comet_sections": obj{
			"content": obj{"story": content},
			"context_layout": obj{"story": obj{"comet_sections": obj{"metadata": []any{
				obj{"__typename": "CometFeedStoryMinimizedTimestampStrategy", "story": obj{
					created: post.CreatedAt.Unix(), "url": r.postURL(p, post)}},
			}}}},
			"feedback": obj{"story": obj{"story_ufi_container": obj{"story": obj{"feedback_context": obj{
				"feedback_target_with_context": obj{"comet_ufi_summary_and_actions_renderer": obj{"feedback": feedback}},
			}}}}},
		},
	}
	if alt {
		// Some A/B shapes carry the timestamp at the top level.
		s[created] = post.CreatedAt.Unix()
		s["permalink_url"] = r.postURL(p, post)
	}
	if post.Pinned {
		s["is_pinned"] = true
	}
	if post.SharedPost != 0 {
		if sp := w.posts[post.SharedPost]; sp != nil {
			s["attached_story"] = obj{"__typename": "Story", "id": b64(fmt.Sprintf("S:_I%d:%d:%d", p.ID, sp.ID, sp.ID)),
				"post_id": strconv.FormatInt(sp.ID, 10), "url": r.postURL(p, sp)}
		}
	}
	return s
}

func hashtagRanges(text string) []any {
	var out []any
	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		if runes[i] != '#' {
			continue
		}
		j := i + 1
		for j < len(runes) && (runes[j] == '_' || isAlnum(runes[j])) {
			j++
		}
		if j > i+1 {
			tag := string(runes[i+1 : j])
			out = append(out, obj{"offset": i, "length": j - i,
				"entity": obj{"__typename": "Hashtag", "id": "ht_" + tag, "url": "https://www.facebook.com/hashtag/" + tag}})
		}
	}
	return out
}

func isAlnum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func (r *renderer) comment(w *World, post *Post, c *Comment, withInline bool) obj {
	total, edges := r.reactionsObj(c.Reactions)
	fb := obj{
		"__typename":     "Feedback",
		"id":             b64(fmt.Sprintf("feedback:%d_%d", post.ID, c.ID)),
		"reactors":       obj{"count": total},
		"top_reactions":  obj{"edges": edges},
		"replies_fields": obj{"total_count": countAll(c.Replies), "count": len(c.Replies)},
	}
	if len(c.Replies) > 0 {
		fb["expansion_info"] = obj{"expansion_token": b64(fmt.Sprintf("exp:%d", c.ID))}
		if withInline && r.inlineRepl {
			fb["replies_connection"] = obj{
				"edges":     []any{obj{"node": r.comment(w, post, c.Replies[0], false)}},
				"page_info": obj{"has_next_page": len(c.Replies) > 1, "end_cursor": cursor("r", c.ID, 1)},
			}
		}
	}
	author := obj{"__typename": "User", "id": strconv.FormatInt(c.Author.ID, 10), "name": c.Author.Name,
		"url": r.base + "/" + c.Author.Vanity, "is_verified": c.Author.Verified,
		"profile_picture_depth_0": obj{"uri": r.base + "/media/avatar_" + strconv.FormatInt(c.Author.ID, 10) + ".jpg"}}
	if c.IsOP {
		author["__typename"] = "Page"
	}
	n := obj{
		"__typename":                "Comment",
		"id":                        b64(fmt.Sprintf("comment:%d_%d", post.ID, c.ID)),
		"legacy_fbid":               strconv.FormatInt(c.ID, 10),
		"depth":                     c.Depth,
		"created_time":              c.CreatedAt.Unix(),
		"body":                      obj{"text": c.Text, "ranges": []any{}},
		"author":                    author,
		"is_author_original_poster": c.IsOP,
		"edit_history":              obj{"count": map[bool]int{true: 1, false: 0}[c.Edited]},
		"feedback":                  fb,
		"url":                       fmt.Sprintf("%s/%d?comment_id=%d", r.base, post.ID, c.ID),
	}
	if c.Parent != nil {
		n["comment_direct_parent"] = obj{"id": b64(fmt.Sprintf("comment:%d_%d", post.ID, c.Parent.ID)), "legacy_fbid": strconv.FormatInt(c.Parent.ID, 10)}
	}
	if c.Media != nil {
		n["attachments"] = []any{obj{"style_type_renderer": obj{"attachment": obj{"media": r.mediaNode(c.Media)}}}}
	} else {
		n["attachments"] = []any{}
	}
	return n
}

func cursor(kind string, scope int64, offset int) string {
	b, _ := json.Marshal(obj{"k": kind, "s": scope, "o": offset})
	return base64.RawURLEncoding.EncodeToString(b)
}

func parseCursor(c string) (kind string, scope int64, offset int, ok bool) {
	if c == "" {
		return "", 0, 0, true
	}
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return "", 0, 0, false
	}
	var v struct {
		K string `json:"k"`
		S int64  `json:"s"`
		O int    `json:"o"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return "", 0, 0, false
	}
	return v.K, v.S, v.O, true
}

// pageHTML renders a page's server HTML with tokens and the preloaded
// profile header.
func (r *renderer) pageHTML(p *Page, lsd, datr string, consent bool) []byte {
	user := obj{
		"__typename": "User", "id": strconv.FormatInt(p.ID, 10), "name": p.Name, "url": r.pageURL(p),
		"is_verified":     p.Verified,
		"profilePicLarge": obj{"uri": r.base + "/media/profile_" + strconv.FormatInt(p.ID, 10) + ".jpg"},
		"cover_photo":     obj{"photo": obj{"image": obj{"uri": r.base + "/media/cover_" + strconv.FormatInt(p.ID, 10) + ".jpg"}}},
		"profile_social_context": obj{"content": []any{
			obj{"text": obj{"text": humanCount(p.Likes) + " likes"}},
			obj{"text": obj{"text": humanCount(p.Followers) + " followers"}},
		}},
		"profile_intro_card": obj{"bio": obj{"text": p.Intro}},
	}
	if !r.noID {
		user["delegate_page"] = obj{"id": strconv.FormatInt(p.delegate(), 10), "category_name": p.Category}
	}
	header := obj{"require": []any{[]any{"RelayPrefetchedStreamCache", "next", []any{}, []any{
		"adp_ProfileCometHeaderQueryRelayPreloader_1",
		obj{"__bbox": obj{"complete": true, "result": obj{"data": obj{"user": user}}}},
	}}}}
	hb, _ := json.Marshal(header)
	site := fmt.Sprintf(`{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{"define":[["LSD",[],{"token":"%s"},323],["SiteData",[],{"server_revision":1017000000,"client_revision":1017000000,"haste_session":"20390.HYP:comet_loggedout_pkg.2.1...0","hsi":"7431000000000000001","__spin_r":1017000000,"__spin_b":"trunk","__spin_t":%d},317],["CurrentUserInitialData",[],{"USER_ID":"0"},270]]}}]],["CookieCoreConfig","set",null,[["_js_datr","%s",1800000]]]]}`,
		lsd, r.now().Unix(), datr)
	var b strings.Builder
	b.WriteString("<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">")
	fmt.Fprintf(&b, "<title>%s | Facebook</title>", html.EscapeString(p.Name))
	fmt.Fprintf(&b, `<meta property="og:title" content="%s"/>`, html.EscapeString(p.Name))
	fmt.Fprintf(&b, `<meta property="og:url" content="%s"/>`, r.pageURL(p))
	if !r.noID {
		fmt.Fprintf(&b, `<meta property="al:android:url" content="fb://page/%d"/>`, p.delegate())
	}
	bundle := r.bundle
	if bundle == "" {
		bundle = "/rsrc.php/v3/relay.js"
	}
	fmt.Fprintf(&b, `<script src="%s%s" async crossorigin="anonymous"></script>`, r.base, bundle)
	b.WriteString("</head><body>")
	if consent {
		b.WriteString(`<div data-cookiebanner="banner"><form action="/cookie/consent/" method="post"></form></div>`)
	}
	if r.jsOnly {
		// Like a page behind a JavaScript challenge: an HTTP client sees no
		// tokens; a browser runs the script that inserts them.
		fmt.Fprintf(&b, `<script>(function(){var s=document.createElement("script");s.type="application/json";`+
			`s.setAttribute("data-sjs","");s.textContent=atob("%s");document.body.appendChild(s);})();</script>`,
			base64.StdEncoding.EncodeToString([]byte(site)))
	} else {
		fmt.Fprintf(&b, `<script type="application/json" data-sjs>%s</script>`, site)
	}
	fmt.Fprintf(&b, `<script type="application/json" data-sjs>%s</script>`, hb)
	if !r.noID {
		fmt.Fprintf(&b, `<script>window.requireLazy&&requireLazy(["pageID"],function(){});var x={"pageID":"%d"};</script>`, p.delegate())
		// The profile route's props name the profile, the id with a timeline.
		fmt.Fprintf(&b, `<script type="application/json" data-sjs>{"require":[["CometProfileRoot","props",null,[{"userID":"%d"}]]]}</script>`, p.ID)
	}
	if r.timeline != "" {
		// The page's own first timeline request, as Facebook's client makes
		// it: what a render-and-intercept captures.
		vars, _ := json.Marshal(obj{"count": 3, "cursor": nil, "id": strconv.FormatInt(p.ID, 10)})
		fmt.Fprintf(&b, `<script>(function(){var lsd=atob("%s");var f=new URLSearchParams();`+
			`f.set("lsd",lsd);f.set("fb_api_caller_class","RelayModern");`+
			`f.set("fb_api_req_friendly_name","ProfileCometTimelineFeedRefetchQuery");f.set("doc_id","%s");`+
			`f.set("variables",atob("%s"));fetch("/api/graphql/",{method:"POST",credentials:"include",`+
			`headers:{"content-type":"application/x-www-form-urlencoded","x-fb-lsd":lsd,`+
			`"x-fb-friendly-name":"ProfileCometTimelineFeedRefetchQuery"},body:f.toString()});})();</script>`,
			base64.StdEncoding.EncodeToString([]byte(lsd)), r.timeline, base64.StdEncoding.EncodeToString(vars))
	}
	b.WriteString("</body></html>")
	return []byte(b.String())
}

func humanCount(n int64) string {
	switch {
	case n >= 1_000_000:
		return strconv.FormatFloat(float64(n)/1e6, 'f', 1, 64) + "M"
	case n >= 10_000:
		return strconv.FormatInt(n/1000, 10) + "K"
	default:
		return strconv.FormatInt(n, 10)
	}
}

func ndjson(parts ...obj) []byte {
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteString("\r\n")
		}
		x, _ := json.Marshal(p)
		b.Write(x)
	}
	return []byte(b.String())
}
