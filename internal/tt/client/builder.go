package client

import (
	"net/url"
	"sort"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
)

// pageTokens are the identity's page values sent with every call.
var pageTokens = []string{"device_id", "odinId", "WebIdLastTime", "region", "language"}

// Builder writes logged-out TikTok's GET calls: every value in the query
// string, sorted by name as the recorded browser sends them (capitals
// first), with Chrome's standard headers only. The web client's standard
// parameters come from the registry's client params, the identity's page
// values from its tokens, the call's own from the operation's variables;
// browser_language, browser_platform, browser_version and os follow the
// identity's fingerprint. An msToken cookie, when the identity has one,
// goes in the query as the browser sends it. No signature is added: calls
// that need one go through the browser signer (docs/tiktok ADR 0006 §4).
type Builder struct {
	AcceptLanguage string
	// NoMsToken leaves msToken out even when the identity has one (the
	// probe's T2 check).
	NoMsToken bool
}

// Build implements graphql.RequestBuilder.
func (b Builder) Build(id *session.Identity, q registry.Query, vars []byte, params map[string]string, referer string) graphql.Built {
	v := map[string]string{}
	for k, val := range params {
		v[k] = val
	}
	v["browser_language"] = firstLanguage(b.AcceptLanguage)
	ua := id.FP.UserAgent
	v["browser_version"] = strings.TrimPrefix(ua, "Mozilla/")
	v["browser_platform"], v["os"] = platform(id.FP.SecChUaPlatform)
	tok := id.Tokens()
	for _, k := range pageTokens {
		if tok[k] != "" {
			v[k] = tok[k]
		}
	}
	v["referer"] = referer
	v["root_referer"] = referer
	gjson.ParseBytes(vars).ForEach(func(k, val gjson.Result) bool {
		if val.Type != gjson.Null {
			v[k.String()] = val.String()
		}
		return true
	})
	if !b.NoMsToken {
		if ms := id.Doer.Cookie(id.Base, "msToken"); ms != "" {
			v["msToken"] = ms
		}
	}
	method := q.Method
	if method == "" {
		method = "GET"
	}
	hdr := [][2]string{
		{"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"user-agent", ua},
		{"sec-ch-ua", id.FP.SecChUa},
		{"sec-ch-ua-mobile", id.FP.SecChUaMobile},
		{"accept", "*/*"},
		{"sec-fetch-site", "same-origin"},
		{"sec-fetch-mode", "cors"},
		{"sec-fetch-dest", "empty"},
		{"referer", referer},
		{"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", b.AcceptLanguage},
		{"priority", "u=1, i"},
	}
	return graphql.Built{Method: method, Path: q.Endpoint, Query: Encode(v), Header: hdr}
}

// Encode writes the query string sorted by name, as the recorded browser
// orders it (byte order puts capitals first: WebIdLastTime before aid),
// keeping empty values (priority_region= is sent empty).
func Encode(v map[string]string) string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte('&')
		}
		sb.WriteString(url.QueryEscape(k))
		sb.WriteByte('=')
		sb.WriteString(url.QueryEscape(v[k]))
	}
	return sb.String()
}

func firstLanguage(acceptLanguage string) string {
	l, _, _ := strings.Cut(acceptLanguage, ",")
	l, _, _ = strings.Cut(l, ";")
	if l == "" {
		return "en-US"
	}
	return strings.TrimSpace(l)
}

// platform maps sec-ch-ua-platform to navigator.platform and the os
// parameter (MacIntel and mac in the recordings).
func platform(secChUaPlatform string) (navigator, os string) {
	switch strings.Trim(secChUaPlatform, `"`) {
	case "macOS":
		return "MacIntel", "mac"
	case "Linux":
		return "Linux x86_64", "linux"
	default:
		return "Win32", "windows"
	}
}
