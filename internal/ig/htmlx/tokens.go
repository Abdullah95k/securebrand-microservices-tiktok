// Package htmlx reads the tokens logged-out Instagram's pages hand their web
// client (har-findings §2): every 200 profile, post or reel page carried an
// LSD, a csrf_token, the app id, server_revision/__spin_r, haste_session,
// hsi, rollout_hash, __comet_req and jazoest, and no DTSG.
//
// The findings name the tokens but not the exact text around them, so each
// token has several candidate patterns, as Facebook's do. They are
// hypotheses until the probe (C2) reports which tokens real pages carry;
// a token no pattern finds shows up in its report by name, never by value.
package htmlx

import (
	"regexp"
	"strconv"

	scrapehtml "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/htmlx"
)

// AppID is the web app id the logged-out client sent on 2026-10-30
// (x-ig-app-id); a page that names another one wins.
const AppID = "936619743392459"

// Keys are the tokens ExtractTokens looks for, by the form field or header
// each is sent as.
var Keys = []string{"lsd", "csrf_token", "app_id", "jazoest", "__spin_r", "__spin_b", "__spin_t",
	"__rev", "__hs", "__hsi", "__comet_req", "rollout_hash"}

// patterns are tried in order; a later match replaces an earlier one unless
// it is a fallback (internal/scrape/htmlx).
var patterns = []scrapehtml.Pattern{
	{Key: "lsd", Re: regexp.MustCompile(`\["LSD",\[\],\{"token":"([^"]+)"`)},
	{Key: "lsd", Re: regexp.MustCompile(`"LSD"\s*:\s*\{\s*"token"\s*:\s*"([^"]+)"`)},
	{Key: "csrf_token", Re: regexp.MustCompile(`"csrf_token"\s*:\s*"([^"]+)"`)},
	{Key: "app_id", Re: regexp.MustCompile(`"appId"\s*:\s*"(\d+)"`)},
	{Key: "app_id", Re: regexp.MustCompile(`"app_id"\s*:\s*"(\d+)"`)},
	{Key: "app_id", Re: regexp.MustCompile(`"X-IG-App-ID"\s*:\s*"(\d+)"`)},
	{Key: "jazoest", Re: regexp.MustCompile(`"jazoest"\s*:\s*"?(\d+)`)},
	{Key: "jazoest", Re: regexp.MustCompile(`jazoest=(\d+)`), Fallback: true},
	{Key: "__spin_r", Re: regexp.MustCompile(`"__spin_r"\s*:\s*(\d+)`)},
	{Key: "__spin_b", Re: regexp.MustCompile(`"__spin_b"\s*:\s*"([^"]+)"`)},
	{Key: "__spin_t", Re: regexp.MustCompile(`"__spin_t"\s*:\s*(\d+)`)},
	{Key: "__rev", Re: regexp.MustCompile(`"server_revision"\s*:\s*(\d+)`)},
	{Key: "__rev", Re: regexp.MustCompile(`"client_revision"\s*:\s*(\d+)`), Fallback: true},
	{Key: "__hs", Re: regexp.MustCompile(`"haste_session"\s*:\s*"([^"]+)"`)},
	{Key: "__hsi", Re: regexp.MustCompile(`"hsi"\s*:\s*"(\d+)"`)},
	{Key: "__comet_req", Re: regexp.MustCompile(`"__comet_req"\s*:\s*(\d+)`)},
	{Key: "rollout_hash", Re: regexp.MustCompile(`"rollout_hash"\s*:\s*"([^"]+)"`)},
}

// ExtractTokens returns the tokens html carries, keyed as in Keys.
func ExtractTokens(html []byte) map[string]string {
	return scrapehtml.Extract(html, patterns)
}

// Valid reports whether tokens are enough to send queries: an LSD. The CSRF
// token may come from the csrftoken cookie instead (U8), and the app id
// has a default.
func Valid(tok map[string]string) bool { return tok["lsd"] != "" }

// Found reports, for each of Keys, whether tok holds it: the probe's view,
// with no values.
func Found(tok map[string]string) map[string]bool {
	out := make(map[string]bool, len(Keys))
	for _, k := range Keys {
		out[k] = tok[k] != ""
	}
	return out
}

// Jazoest is the page's jazoest, or else Facebook's checksum over the LSD
// ("2" and the sum of its character codes), a hypothesis for pages that
// carry none.
func Jazoest(tok map[string]string) string {
	if j := tok["jazoest"]; j != "" {
		return j
	}
	sum := 0
	for _, r := range tok["lsd"] {
		sum += int(r)
	}
	return "2" + strconv.Itoa(sum)
}
