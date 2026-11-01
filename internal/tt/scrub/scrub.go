// Package scrub turns TikTok answers into fixtures that can be committed:
// every username, nickname, user id, secUid, post, comment and music id is
// pseudonymised consistently (so cross references survive), text people
// wrote is blanked keeping its length in UTF-16 units (TikTok's offsets),
// and signed media links lose their signatures. Pages are cut down to
// their rehydration document's account and post scopes. The build brief's
// rule is that no real username, name, id or comment text is ever
// committed (§7); the tests prove the scrubber leaves none.
package scrub

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
)

// Version is recorded in a fixture's meta.json. Raise it when Scrub learns
// a new rule, so older fixtures are scrubbed again.
const Version = 1

// Kind is what an answer is; it names the fixture directory.
type Kind string

// Kinds of answer, by the call or page that returned them.
const (
	ProfilePage Kind = "profile_page" // /@<username>
	PostPage    Kind = "post_page"    // /@<username>/video/<id>
	PostList    Kind = "post_list"    // /api/post/item_list/ (the web app's)
	CreatorList Kind = "creator_list" // /api/creator/item_list/
	UserDetail  Kind = "user_detail"  // /api/user/detail/
	Comments    Kind = "comments"     // /api/comment/list/
	Replies     Kind = "replies"      // /api/comment/list/reply/
)

// Kinds lists every kind, in the order fixtures are listed.
var Kinds = []Kind{ProfilePage, PostPage, PostList, CreatorList, UserDetail, Comments, Replies}

// IsPage reports whether answers of the kind are HTML.
func (k Kind) IsPage() bool { return k == ProfilePage || k == PostPage }

// File is the fixture file name an answer of the kind is stored under.
func (k Kind) File() string {
	if k.IsPage() {
		return "response.html"
	}
	return "response.json"
}

// Options shape a scrub.
type Options struct {
	// Items keeps at most this many list items or comments, chosen to
	// cover the shapes the parsers read (pinned, photo, ad, mentions,
	// hashtags, labels; replies, images, pins). 0 keeps all.
	Items int
}

var rehydration = regexp.MustCompile(`(?s)<script[^>]*\bid="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>`)

// Scrub returns the scrubbed answer of kind k.
func Scrub(b []byte, k Kind, o Options) ([]byte, error) {
	if k.IsPage() {
		return page(b, o)
	}
	v, err := decode(b)
	if err != nil {
		return nil, fmt.Errorf("scrub: %s is not JSON: %w", k, err)
	}
	trim(v, o.Items)
	s := newScrubber()
	s.collect(v, "")
	s.sortNames()
	return encode(s.transform(v, ""), false), nil
}

// appContext is what a fixture keeps of the page's webapp.app-context:
// the values internal/tt/htmlx reads, pseudonymised.
var appContext = map[string]bool{"language": true, "region": true, "appId": true, "appType": true, "wid": true,
	"webIdCreatedTime": true, "odinId": true, "botType": true, "clusterRegion": true, "csrfToken": true}

// pageScopes are the rehydration scopes a page fixture keeps.
var pageScopes = []string{"webapp.app-context", "webapp.user-detail", "webapp.video-detail"}

// page cuts a page down to a minimal document around its rehydration
// script, holding only the scopes the parsers read: the rest of the page
// (meta tags, titles, other scopes) names the account and is never kept.
func page(b []byte, o Options) ([]byte, error) {
	m := rehydration.FindSubmatch(b)
	if m == nil {
		return nil, errors.New("scrub: the page has no __UNIVERSAL_DATA_FOR_REHYDRATION__ document")
	}
	v, err := decode(bytes.TrimSpace(m[1]))
	if err != nil {
		return nil, fmt.Errorf("scrub: the page's document is not JSON: %w", err)
	}
	root, _ := v.(map[string]any)
	ds, _ := root["__DEFAULT_SCOPE__"].(map[string]any)
	if ds == nil {
		return nil, errors.New("scrub: the page's document has no __DEFAULT_SCOPE__")
	}
	kept := map[string]any{}
	for _, name := range pageScopes {
		if sc, ok := ds[name]; ok {
			kept[name] = sc
		}
	}
	if ac, ok := kept["webapp.app-context"].(map[string]any); ok {
		for k := range ac {
			if !appContext[k] {
				delete(ac, k)
			}
		}
	}
	doc := map[string]any{"__DEFAULT_SCOPE__": kept}
	trim(doc, o.Items)
	s := newScrubber()
	s.collect(doc, "")
	s.sortNames()
	out := encode(s.transform(doc, ""), true) // escaped: it sits in a <script>
	var sb bytes.Buffer
	sb.WriteString("<!DOCTYPE html>\n<html><head><meta charset=\"utf-8\"><title>scrubbed</title></head><body>\n")
	sb.WriteString(`<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">`)
	sb.Write(out)
	sb.WriteString("</script>\n</body></html>\n")
	return sb.Bytes(), nil
}

// Keys by what they hold. A key's rule applies wherever it appears.
var (
	// Numeric ids of people and of their content (posts, comments, music,
	// hashtags, playlists) and of the browser that recorded them.
	idKeys = keys("id", "uid", "userId", "user_id", "cid", "aweme_id", "awemeId", "item_id", "reply_id",
		"reply_to_reply_id", "comment_id", "thread_id", "roomId", "room_id", "playlistId", "group_id", "creator_uid",
		"shortId", "short_id", "wid", "odinId", "device_id", "hashtag_id", "set_id", "product_id", "music_id",
		"author_user_id", "videoID", "diversificationId")
	secUIDKeys   = keys("secUid", "sec_uid")
	usernameKeys = keys("uniqueId", "unique_id", "userUniqueId", "author_unique_id")
	nicknameKeys = keys("nickname", "nickName", "nick_name", "authorName", "author_name", "ownerNickname")
	// Text people wrote or that names them: captions, comments, bios,
	// titles (music titles name an original sound's creator), hashtags.
	textKeys = keys("desc", "text", "signature", "title", "hashtagName", "hashtag_name", "keyword", "description",
		"album", "name", "group_name", "product_title", "shareTitle", "shareDesc", "share_title", "share_desc",
		"challengeName", "recommend_reason", "link_text", "stickerText", "subtitle", "caption")
	// Links that are not media: bio links, share links, deep links.
	linkKeys = keys("link", "url", "schema", "product_schema", "shareUrl", "share_url", "canonical")
	// Tokens and file handles that identify a session or a file.
	opaqueKeys = keys("csrfToken", "nonce", "requestId", "encryptedWebid", "impr_id", "Uri", "uri", "UrlKey",
		"FileHash", "FileCs", "url_prefix", "encodeUserTag", "backendSourceEventTracking", "predicted_age_group",
		"msToken", "ttwid", "verify_fp", "x-tt-logid")
)

func keys(ks ...string) map[string]bool {
	m := make(map[string]bool, len(ks))
	for _, k := range ks {
		m[k] = true
	}
	return m
}

type scrubber struct {
	ids                           map[string]string // real id -> pseudonym
	secUIDs, usernames, nicknames map[string]string
	order                         []string // secUids, usernames and nicknames, longest first
}

func newScrubber() *scrubber {
	return &scrubber{ids: map[string]string{}, secUIDs: map[string]string{}, usernames: map[string]string{},
		nicknames: map[string]string{}}
}

// collect records every id and name in v, so they are replaced wherever
// else they appear (links, share text, nested JSON).
func (s *scrubber) collect(v any, key string) {
	switch v := v.(type) {
	case map[string]any:
		for k, x := range v {
			s.collect(x, k)
		}
	case []any:
		for _, x := range v {
			s.collect(x, key)
		}
	case json.Number:
		if idKeys[key] || len(v.String()) >= longID {
			s.id(v.String())
		}
	case string:
		for _, d := range longDigits.FindAllString(v, -1) {
			s.id(d)
		}
		switch {
		case idKeys[key]:
			s.id(v)
		case secUIDKeys[key] && v != "":
			s.secUIDs[v] = pseudoSecUID(v)
		case usernameKeys[key] && v != "":
			s.usernames[v] = pseudoUsername(v)
		case nicknameKeys[key] && v != "":
			s.nicknames[v] = "Person " + hash(v, "nick")[:6]
		default:
			if inner, ok := serialized(v); ok {
				s.collect(inner, key)
			}
		}
	}
}

// sortNames orders the collected names longest first, so a name inside a
// longer one is replaced after it.
func (s *scrubber) sortNames() {
	s.order = s.order[:0]
	for _, m := range []map[string]string{s.secUIDs, s.usernames, s.nicknames} {
		for k := range m {
			s.order = append(s.order, k)
		}
	}
	sort.Slice(s.order, func(i, j int) bool {
		if len(s.order[i]) != len(s.order[j]) {
			return len(s.order[i]) > len(s.order[j])
		}
		return s.order[i] < s.order[j]
	})
}

// longID is the length from which any number is taken for an id, under
// whatever key and inside any string (subtitle, song and log ids; ids in
// links): no count or time has 15 digits.
const longID = 15

var longDigits = regexp.MustCompile(`\d{15,}`)

// id records a numeric id (6 digits or more: counts and flags are not ids).
func (s *scrubber) id(v string) {
	if len(v) < 6 || strings.Trim(v, "0123456789") != "" {
		return
	}
	if _, ok := s.ids[v]; !ok {
		s.ids[v] = PseudoID(v)
	}
}

// transform scrubs v, which sits under key.
func (s *scrubber) transform(v any, key string) any {
	switch v := v.(type) {
	case map[string]any:
		for k, x := range v {
			v[k] = s.transform(x, k)
		}
		return v
	case []any:
		for i, x := range v {
			v[i] = s.transform(x, key)
		}
		return v
	case json.Number:
		if p, ok := s.ids[v.String()]; ok {
			return json.Number(p)
		}
		return v
	case string:
		return s.str(v, key)
	}
	return v
}

func (s *scrubber) str(v, key string) string {
	if v == "" {
		return v
	}
	switch {
	case idKeys[key]:
		if p, ok := s.ids[v]; ok {
			return p
		}
		if strings.Trim(v, "0123456789") == "" {
			return v // a short number, not an id
		}
		return "scrubbed"
	case secUIDKeys[key]:
		return s.secUIDs[v]
	case usernameKeys[key]:
		return s.usernames[v]
	case nicknameKeys[key]:
		return s.nicknames[v]
	case opaqueKeys[key]:
		return "scrubbed"
	}
	if inner, ok := serialized(v); ok {
		return string(encode(s.transform(inner, key), false))
	}
	switch {
	case textKeys[key]:
		return Blank(s.blankNames(v))
	case linkKeys[key] && (isURL(v) || strings.Contains(v, ".")):
		if mediaLink(v) {
			return s.signed(v)
		}
		if pageLink(v) {
			return s.replace(v)
		}
		return "https://example.invalid/scrubbed"
	}
	return s.replace(v)
}

// blankNames replaces collected nicknames in a text with as many x as
// they have UTF-16 units, so names Blank would keep (emoji, symbols) go
// too and the text keeps its length.
func (s *scrubber) blankNames(v string) string {
	for _, k := range s.order {
		if _, ok := s.nicknames[k]; ok && strings.Contains(v, k) {
			v = strings.ReplaceAll(v, k, strings.Repeat("x", len(utf16.Encode([]rune(k)))))
		}
	}
	return v
}

var (
	urlRe   = regexp.MustCompile(`https?://[^\s"'<>\\]+`)
	digitRe = regexp.MustCompile(`\d+`)
)

// replace scrubs a free string: media links, then names and ids wherever
// they appear in it (share links carry the username and the post id).
func (s *scrubber) replace(v string) string {
	v = urlRe.ReplaceAllStringFunc(v, func(u string) string {
		switch {
		case mediaLink(u):
			return s.signed(u)
		case pageLink(u):
			return u // its username and ids are replaced below
		}
		return "https://example.invalid/scrubbed" // another site's link: a person's picture, a tracker
	})
	for _, k := range s.order {
		if p, ok := s.usernames[k]; ok {
			v = replaceWord(v, k, p)
			continue
		}
		if p, ok := s.secUIDs[k]; ok {
			v = strings.ReplaceAll(v, k, p)
			continue
		}
		if p, ok := s.nicknames[k]; ok && len([]rune(k)) >= 3 {
			v = strings.ReplaceAll(v, k, p)
		}
	}
	// A profile link names its account even where no username field does
	// (a comment's share link names the post's account).
	v = profilePath.ReplaceAllStringFunc(v, func(m string) string {
		name := profilePath.FindStringSubmatch(m)[1]
		if strings.HasPrefix(name, "user") && len(name) == 12 && s.isPseudonym(name) {
			return m
		}
		return "tiktok.com/@" + pseudoUsername(name)
	})
	return digitRe.ReplaceAllStringFunc(v, func(d string) string {
		if p, ok := s.ids[d]; ok {
			return p
		}
		return d
	})
}

var profilePath = regexp.MustCompile(`tiktok\.com/@([A-Za-z0-9._]+)`)

// pseudoUsername is a username's pseudonym: the same in every answer.
func pseudoUsername(v string) string { return "user" + hash(v, "user")[:8] }

// isPseudonym reports a username this scrub already replaced.
func (s *scrubber) isPseudonym(name string) bool {
	for _, p := range s.usernames {
		if p == name {
			return true
		}
	}
	return false
}

// replaceWord replaces old in s where it is a whole username: not inside a
// longer one (usernames are letters, digits, '.' and '_').
func replaceWord(s, old, nw string) string {
	var sb strings.Builder
	for {
		i := strings.Index(s, old)
		if i < 0 {
			sb.WriteString(s)
			return sb.String()
		}
		end := i + len(old)
		if (i == 0 || !usernameByte(s[i-1])) && (end == len(s) || !usernameByte(s[end])) {
			sb.WriteString(s[:i])
			sb.WriteString(nw)
		} else {
			sb.WriteString(s[:end])
		}
		s = s[end:]
	}
}

func usernameByte(c byte) bool {
	return c == '.' || c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isURL(v string) bool { return strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://") }

// pageLink reports a link to one of TikTok's own pages (a profile, a
// post), which keeps its shape with its username and ids pseudonymised.
func pageLink(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return false
	}
	h := u.Hostname()
	return h == "tiktok.com" || h == "www.tiktok.com" || h == "m.tiktok.com" || h == "vm.tiktok.com"
}

// cdnSuffixes are TikTok's media hosts: images (tiktokcdn*), video files
// (*-webapp-prime.tiktok.com and tiktokv*), and the byte* hosts some
// stickers use (har-findings §6, c2-findings).
var cdnSuffixes = []string{".tiktokcdn.com", ".tiktokcdn-us.com", ".tiktokcdn-eu.com", ".tiktokv.com", ".tiktokv.us",
	".tiktokv.eu", ".ibyteimg.com", ".byteoversea.com", ".muscdn.com", ".ttwstatic.com"}

// mediaLink reports a signed media link: on a media host, or TikTok's own
// video redirect (www.tiktok.com/aweme/v1/play/, which carries
// signature= and tk=tt_chain_token).
func mediaLink(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if strings.HasPrefix(u.Path, "/aweme/") && strings.HasSuffix(u.Hostname(), "tiktok.com") {
		return true
	}
	h := u.Hostname()
	for _, s := range cdnSuffixes {
		if strings.HasSuffix(h, s) {
			return true
		}
	}
	return strings.HasSuffix(h, ".tiktok.com") && strings.Contains(h, "webapp")
}

// signed replaces a signed media link with one that names only its host,
// a stable file name and its expiry (which the parsers read).
func (s *scrubber) signed(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "https://p16-sign.tiktokcdn.com/scrubbed"
	}
	ext := path.Ext(u.Path)
	if i := strings.IndexAny(ext, "~:"); i >= 0 {
		ext = ext[:i]
	}
	if len(ext) > 6 {
		ext = ""
	}
	out := "https://" + u.Host + "/scrubbed/" + hash(raw, "url")[:12] + ext
	q := url.Values{}
	for _, k := range []string{"x-expires", "expire"} {
		if v := u.Query().Get(k); v != "" {
			q.Set(k, v)
		}
	}
	if len(q) > 0 {
		out += "?" + q.Encode()
	}
	return out
}

func hash(s, salt string) string {
	h := sha256.Sum256([]byte("tks-scrub|" + salt + "|" + s))
	return hex.EncodeToString(h[:])
}

// PseudoID returns the pseudonym of a numeric id. An id that carries a
// time in its top 32 bits (T8: posts, comments, newer accounts and music)
// keeps that time and gets new low 32 bits, so a fixture's id times and
// its createTimes still agree; any other id gets new digits, as many as it
// had. Both stay in int64's range.
func PseudoID(id string) string {
	h := sha256.Sum256([]byte("tks-scrub|id|" + id))
	r := binary.BigEndian.Uint64(h[:8])
	if v, err := strconv.ParseUint(id, 10, 64); err == nil && v <= 1<<63-1 {
		if t := v >> 32; t >= 1_400_000_000 && t <= 2_100_000_000 {
			p := v&^0xffffffff | r&0xffffffff
			if p == v {
				p ^= 1
			}
			return strconv.FormatUint(p, 10)
		}
	}
	digits := strconv.FormatUint(r, 10)
	for len(digits) < len(id) {
		digits += strconv.FormatUint(binary.BigEndian.Uint64(h[8:16]), 10)
	}
	out := []byte(digits[:len(id)])
	out[0] = '1' + out[0]%8 // 1..8: never a leading zero, and 19 digits stay under int64's limit
	if string(out) == id {
		out[len(out)-1] = '0' + (out[len(out)-1]-'0'+1)%10
	}
	return string(out)
}

func pseudoSecUID(v string) string {
	const prefix = "MS4wLjABAAAA"
	h := sha256.Sum256([]byte("tks-scrub|sec|" + v))
	body := base64.RawURLEncoding.EncodeToString(append(h[:], h[:]...))
	n := len(v) - len(prefix)
	for len(body) < n {
		body += body
	}
	if n < 8 {
		n = 8
	}
	return prefix + body[:n]
}

// Blank replaces letters, digits and marks with x, keeping whitespace,
// punctuation, '#', '@' and emoji, and the length in UTF-16 units (the
// unit of TikTok's text offsets): "@Ali #tag 1" becomes "@xxx #xxx x".
func Blank(t string) string {
	var sb strings.Builder
	for _, r := range t {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r) {
			sb.WriteString(strings.Repeat("x", utf16.RuneLen(r)))
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// serialized decodes a string that holds a JSON object or array.
func serialized(s string) (any, bool) {
	if t := strings.TrimSpace(s); t == "" || (t[0] != '{' && t[0] != '[') {
		return nil, false
	}
	v, err := decode([]byte(s))
	if err != nil {
		return nil, false
	}
	return v, true
}

func decode(b []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	if d.More() {
		return nil, errors.New("more than one JSON document")
	}
	return v, nil
}

// encode writes v as JSON (keys sorted); escapeHTML escapes <, > and &.
func encode(v any, escapeHTML bool) []byte {
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.SetEscapeHTML(escapeHTML)
	_ = e.Encode(v)
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}
