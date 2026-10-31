// Package htmlx extracts session tokens and the page id from Facebook's
// server-rendered HTML. The pattern engine and the embedded JSON reader are
// platform-neutral and live in internal/scrape/htmlx.
package htmlx

import (
	"bytes"
	"regexp"
	"strconv"

	scrapehtml "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/htmlx"
)

// Valid reports whether tokens hold the minimum for GraphQL calls (an lsd).
func Valid(tok map[string]string) bool { return tok["lsd"] != "" }

// Jazoest computes Facebook's jazoest checksum: "2" followed by the sum of
// the character codes of the CSRF token (fb_dtsg when present, else lsd).
// Verify against real traffic in Phase 0.
func Jazoest(tok map[string]string) string {
	src := tok["fb_dtsg"]
	if src == "" {
		src = tok["lsd"]
	}
	sum := 0
	for _, r := range src {
		sum += int(r)
	}
	return "2" + strconv.Itoa(sum)
}

// Several candidate patterns per token, tried in order, keyed by the form
// field each token is sent as. Facebook moves these around; unknown layouts
// show up as missing tokens in bootstrap metrics.
var patterns = []scrapehtml.Pattern{
	{Key: "lsd", Re: regexp.MustCompile(`\["LSD",\[\],\{"token":"([^"]+)"`)},
	{Key: "lsd", Re: regexp.MustCompile(`"LSD"\s*:\s*\{\s*"token"\s*:\s*"([^"]+)"`)},
	{Key: "lsd", Re: regexp.MustCompile(`name="lsd" value="([^"]+)"`)},
	{Key: "fb_dtsg", Re: regexp.MustCompile(`\["DTSGInitialData",\[\],\{"token":"([^"]+)"`)},
	{Key: "js_datr", Re: regexp.MustCompile(`"_js_datr"\s*,\s*"([^"]+)"`)},
	{Key: "js_datr", Re: regexp.MustCompile(`"_js_datr"\s*:\s*\{\s*"value"\s*:\s*"([^"]+)"`)},
	{Key: "__spin_r", Re: regexp.MustCompile(`"__spin_r"\s*:\s*(\d+)`)},
	{Key: "__spin_b", Re: regexp.MustCompile(`"__spin_b"\s*:\s*"([^"]+)"`)},
	{Key: "__spin_t", Re: regexp.MustCompile(`"__spin_t"\s*:\s*(\d+)`)},
	{Key: "__hsi", Re: regexp.MustCompile(`"hsi"\s*:\s*"(\d+)"`)},
	{Key: "__rev", Re: regexp.MustCompile(`"server_revision"\s*:\s*(\d+)`)},
	{Key: "__rev", Re: regexp.MustCompile(`"client_revision"\s*:\s*(\d+)`), Fallback: true},
	{Key: "__hs", Re: regexp.MustCompile(`"haste_session"\s*:\s*"([^"]+)"`)},
	{Key: "__comet_req", Re: regexp.MustCompile(`"__comet_req"\s*:\s*(\d+)`)},
}

var consentMarkers = [][]byte{[]byte(`"cookie_consent_dialog"`), []byte(`data-cookiebanner=`), []byte(`/cookie/consent/`)}

// ExtractTokens scans html for every known token pattern and returns the
// tokens found, keyed by the form field each is sent as (js_datr is the
// value of the datr cookie).
func ExtractTokens(html []byte) map[string]string {
	return scrapehtml.Extract(html, patterns)
}

// ConsentWall reports whether html shows the cookie-consent wall.
func ConsentWall(html []byte) bool { return scrapehtml.ContainsAny(html, consentMarkers) }

// The page's id, the profile's first. A page made in the new Pages
// experience is a profile (typed User) beside an older Page object with an
// id of its own (delegate_page, pageID). Only the profile has a timeline:
// given the Page's id, the timeline query returns a bare Page node with no
// posts.
var (
	// profileUserID is the profile route's userID; a logged-out viewer's
	// own is "0".
	profileUserID  = regexp.MustCompile(`"userID"\s*:\s*"(\d{5,})"`)
	pageIDPatterns = []idPattern{
		{"fb://profile/", regexp.MustCompile(`fb://profile/(\d+)`)},
		{"pageID", regexp.MustCompile(`"pageID"\s*:\s*"(\d+)"`)},
		{"page_id", regexp.MustCompile(`"page_id"\s*:\s*"(\d+)"`)},
		{"delegate_page", regexp.MustCompile(`"delegate_page"\s*:\s*\{\s*"id"\s*:\s*"(\d+)"`)},
		{"fb://page/?id=", regexp.MustCompile(`fb://page/\?id=(\d+)`)},
		{"fb://page/", regexp.MustCompile(`fb://page/(\d+)`)},
	}
	// hintPatterns are keys that may name the page but are not trusted to;
	// PageIDHints counts them to show what a page the extractor missed holds.
	hintPatterns = []idPattern{
		{"profile_owner", regexp.MustCompile(`"profile_owner"\s*:\s*\{\s*"id"\s*:\s*"(\d+)"`)},
		{"owning_profile_id", regexp.MustCompile(`"owning_profile_id"\s*:\s*"(\d+)"`)},
		{"selectedID", regexp.MustCompile(`"selectedID"\s*:\s*"(\d+)"`)},
		{"entity_id", regexp.MustCompile(`"entity_id"\s*:\s*"(\d+)"`)},
		{"actorID", regexp.MustCompile(`"actorID"\s*:\s*"(\d+)"`)},
		{"al:android:url", regexp.MustCompile(`<meta property="al:android:url" content="fb://[a-z]+/\??(?:id=)?(\d+)`)},
	}
)

type idPattern struct {
	name string
	re   *regexp.Regexp
}

// PageID returns the numeric id of the page/profile an HTML document is
// about, or 0. The userID counts only when it is the document's one: with
// several, some belong to other people.
func PageID(html []byte) int64 {
	var only []byte
	for _, m := range profileUserID.FindAllSubmatch(html, -1) {
		if only != nil && !bytes.Equal(only, m[1]) {
			only = nil
			break
		}
		only = m[1]
	}
	if id, err := strconv.ParseInt(string(only), 10, 64); err == nil && id > 0 {
		return id
	}
	for _, p := range pageIDPatterns {
		if m := p.re.FindSubmatch(html); m != nil {
			id, err := strconv.ParseInt(string(m[1]), 10, 64)
			if err == nil && id > 0 {
				return id
			}
		}
	}
	return 0
}

// PageIDHints counts, for each key PageID reads and a few it does not, the
// distinct non-zero ids an HTML document holds under it. Only the counts
// leave: they show why a page's id was not found without carrying the ids.
func PageIDHints(html []byte) map[string]int {
	out := map[string]int{}
	count := func(name string, re *regexp.Regexp) {
		seen := map[string]bool{}
		for _, m := range re.FindAllSubmatch(html, -1) {
			if id, err := strconv.ParseInt(string(m[1]), 10, 64); err == nil && id > 0 {
				seen[string(m[1])] = true
			}
		}
		out[name] = len(seen)
	}
	count("userID", profileUserID)
	for _, p := range pageIDPatterns {
		count(p.name, p.re)
	}
	for _, p := range hintPatterns {
		count(p.name, p.re)
	}
	return out
}
