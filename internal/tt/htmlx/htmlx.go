// Package htmlx reads TikTok's pages: the rehydration document a profile
// or post page embeds (<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">)
// and, from its app context, the values the web client sends with every
// call. The owner's recordings showed that the calls' device_id, odinId and
// WebIdLastTime are the page's wid, odinId and webIdCreatedTime, and their
// region and language the page's (docs/tiktok/har-findings-2026-10-31.md).
package htmlx

import (
	"bytes"
	"regexp"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
)

// Scope names inside the rehydration document's __DEFAULT_SCOPE__.
const (
	ScopeAppContext  = "webapp.app-context"
	ScopeUserDetail  = "webapp.user-detail"
	ScopeVideoDetail = "webapp.video-detail"
)

var rehydration = regexp.MustCompile(`(?s)<script[^>]*\bid="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>`)

// Rehydration returns the page's __DEFAULT_SCOPE__ object, and whether the
// page carried one. A client-rendered shell or a challenge page carries none.
func Rehydration(html []byte) (gjson.Result, bool) {
	m := rehydration.FindSubmatch(html)
	if m == nil {
		return gjson.Result{}, false
	}
	body := bytes.TrimSpace(m[1])
	if !gjson.ValidBytes(body) {
		return gjson.Result{}, false
	}
	scope := gjson.GetBytes(body, "__DEFAULT_SCOPE__")
	return scope, scope.IsObject()
}

// Scope returns one scope of the document, e.g. ScopeUserDetail.
func Scope(html []byte, name string) (gjson.Result, bool) {
	ds, ok := Rehydration(html)
	if !ok {
		return gjson.Result{}, false
	}
	s := ds.Get(gjson.Escape(name))
	return s, s.Exists()
}

// tokenFields maps the app context's fields to the query parameters they
// are sent as.
var tokenFields = [][2]string{
	{"wid", "device_id"},
	{"odinId", "odinId"},
	{"webIdCreatedTime", "WebIdLastTime"},
	{"region", "region"},
	{"language", "language"},
	{"csrfToken", "csrfToken"},
	{"botType", "botType"},
}

// Extract returns the values a page hands its web client, keyed by the
// query parameter each is sent as, and whether they are enough to send
// calls (a device id).
func Extract(html []byte) (session.Tokens, bool) {
	ac, ok := Scope(html, ScopeAppContext)
	if !ok {
		return session.Tokens{}, false
	}
	tok := session.Tokens{}
	for _, f := range tokenFields {
		if v := ac.Get(f[0]); v.Exists() && v.String() != "" {
			tok[f[1]] = v.String()
		}
	}
	return tok, tok["device_id"] != ""
}

// ChallengeMarkers mark TikTok's JavaScript challenge page ("Please
// wait...", a proof of work answered with the _wafchallengeid cookie) and
// its captcha, as public sources describe them (T6, T28).
var ChallengeMarkers = [][]byte{[]byte("_wafchallengeid"), []byte("secsdk-captcha"), []byte("captcha_container")}

// IsChallenge reports whether a page is a challenge rather than content.
func IsChallenge(html []byte) bool {
	head := html
	if len(head) > 64<<10 {
		head = head[:64<<10]
	}
	for _, m := range ChallengeMarkers {
		if bytes.Contains(head, m) {
			return true
		}
	}
	return false
}
