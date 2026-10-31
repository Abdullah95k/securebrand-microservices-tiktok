package client

import (
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// DefaultEndpoint is where four of the five operations go; highlights names
// /graphql/query in the registry.
const DefaultEndpoint = "/api/graphql"

// clientState are the browser's client-state fields (__dyn, __csr, ...). A
// script computes them, so no page carries them; they are sent only when the
// client params set them. Whether requests work without them is U3, which
// the probe answers.
var clientState = []string{"__dyn", "__csr", "__hblp", "__hsdp", "__sjsp"}

// Builder writes logged-out Instagram's GraphQL requests: the form family of
// Facebook's plus __d=www, and the headers x-csrftoken, x-ig-app-id,
// x-ig-max-touch-points beside x-fb-lsd, x-fb-friendly-name and x-asbd-id.
//
// The recordings give the form fields in browser order and the headers only
// alphabetically (a HAR does not keep wire order), so the header order below
// follows Chrome's for Facebook's same-family requests: a hypothesis.
type Builder struct {
	AcceptLanguage string
}

// Build implements graphql.RequestBuilder.
func (b Builder) Build(id *session.Identity, q registry.Query, vars []byte, params map[string]string, referer string) graphql.Built {
	tok := id.Tokens()
	csrf := tok["csrf_token"]
	if csrf == "" {
		csrf = id.Doer.Cookie(id.Base, "csrftoken")
	}
	appID := tok["app_id"]
	if appID == "" {
		appID = params["app_id"]
	}
	if appID == "" {
		appID = htmlx.AppID
	}
	hdr := [][2]string{
		{"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"x-ig-app-id", appID},
		{"x-fb-lsd", tok["lsd"]},
		{"sec-ch-ua", id.FP.SecChUa},
		{"sec-ch-ua-mobile", id.FP.SecChUaMobile},
		{"x-fb-friendly-name", q.FriendlyName},
		{"x-asbd-id", params["x_asbd_id"]},
		{"x-csrftoken", csrf},
		{"x-ig-max-touch-points", "0"},
		{"user-agent", id.FP.UserAgent},
		{"content-type", "application/x-www-form-urlencoded"},
		{"accept", "*/*"},
		{"origin", session.Origin(id.Base)},
		{"sec-fetch-site", "same-origin"},
		{"sec-fetch-mode", "cors"},
		{"sec-fetch-dest", "empty"},
		{"referer", referer},
		{"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", b.AcceptLanguage},
		{"priority", "u=1, i"},
	}
	return graphql.Built{Path: DefaultEndpoint, Header: hdr, Body: graphql.EncodeForm(form(id, tok, params, q, vars))}
}

// form builds the POST body in the recorded browser order. Empty values are
// left out (graphql.EncodeForm).
func form(id *session.Identity, tok session.Tokens, params map[string]string, q registry.Query, vars []byte) []graphql.Field {
	cometReq := tok["__comet_req"]
	if cometReq == "" {
		cometReq = params["__comet_req"]
	}
	d := params["__d"]
	if d == "" {
		d = "www"
	}
	f := []graphql.Field{
		{K: "av", V: "0"},
		{K: "__d", V: d},
		{K: "__user", V: "0"},
		{K: "__a", V: params["__a"]},
		{K: "__req", V: id.NextReq()},
		{K: "__hs", V: tok["__hs"]},
		{K: "dpr", V: params["dpr"]},
		{K: "__ccg", V: params["__ccg"]},
		{K: "__rev", V: tok["__rev"]},
		{K: "__s", V: id.SessionParam()},
		{K: "__hsi", V: tok["__hsi"]},
	}
	for _, k := range clientState {
		f = append(f, graphql.Field{K: k, V: params[k]})
	}
	return append(f,
		graphql.Field{K: "__comet_req", V: cometReq},
		graphql.Field{K: "lsd", V: tok["lsd"]},
		graphql.Field{K: "jazoest", V: htmlx.Jazoest(tok)},
		graphql.Field{K: "__spin_r", V: tok["__spin_r"]},
		graphql.Field{K: "__spin_b", V: tok["__spin_b"]},
		graphql.Field{K: "__spin_t", V: tok["__spin_t"]},
		graphql.Field{K: "__crn", V: params["__crn"]},
		graphql.Field{K: "fb_api_caller_class", V: params["fb_api_caller_class"]},
		graphql.Field{K: "fb_api_req_friendly_name", V: q.FriendlyName},
		graphql.Field{K: "variables", V: string(vars)},
		graphql.Field{K: "server_timestamps", V: params["server_timestamps"]},
		graphql.Field{K: "doc_id", V: q.DocID},
	)
}
