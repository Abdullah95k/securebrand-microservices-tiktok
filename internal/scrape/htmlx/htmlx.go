// Package htmlx reads server-rendered HTML: the JSON payloads a page embeds
// and values found by an ordered table of patterns. A platform supplies its
// own table (Facebook's tokens are in internal/fb/htmlx).
package htmlx

import (
	"bytes"
	"regexp"

	"github.com/tidwall/gjson"
)

// Pattern finds one value in a document: the first group of Re's first
// match, stored under Key (usually the form field it is sent as).
type Pattern struct {
	Key string
	Re  *regexp.Regexp
	// Fallback patterns set Key only when no earlier pattern did, for a
	// second-best source (Facebook's client_revision for __rev).
	Fallback bool
}

// Extract applies patterns in order and returns the values found, by key.
// A platform lists several candidate patterns per key because pages move
// these around: a later match replaces an earlier one, unless the later
// pattern is a fallback. Keys with no match are absent.
func Extract(doc []byte, patterns []Pattern) map[string]string {
	out := map[string]string{}
	for _, p := range patterns {
		if p.Fallback && out[p.Key] != "" {
			continue
		}
		if m := p.Re.FindSubmatch(doc); m != nil {
			out[p.Key] = string(m[1])
		}
	}
	return out
}

// ContainsAny reports whether doc holds any of the markers.
func ContainsAny(doc []byte, markers [][]byte) bool {
	for _, m := range markers {
		if bytes.Contains(doc, m) {
			return true
		}
	}
	return false
}

var jsonScript = regexp.MustCompile(`(?s)<script type="application/json"[^>]*>(.*?)</script>`)

// JSONBlobs returns every embedded <script type="application/json"> payload
// that parses as JSON. Facebook and Instagram preload GraphQL results this
// way (Instagram's carry a data-sjs attribute).
func JSONBlobs(html []byte) []gjson.Result {
	var out []gjson.Result
	for _, m := range jsonScript.FindAllSubmatch(html, -1) {
		if gjson.ValidBytes(m[1]) {
			out = append(out, gjson.ParseBytes(m[1]))
		}
	}
	return out
}
