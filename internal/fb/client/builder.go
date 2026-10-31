package client

import (
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// DefaultEndpoint is the path of Facebook's persisted queries.
const DefaultEndpoint = "/api/graphql/"

// Builder writes logged-out Facebook's GraphQL requests.
type Builder struct {
	AcceptLanguage string
	Locale         string
}

// Build implements graphql.RequestBuilder.
func (b Builder) Build(id *session.Identity, q registry.Query, vars []byte, params map[string]string, referer string) graphql.Built {
	tok := id.Tokens()
	hdr := [][2]string{
		{"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"x-fb-lsd", tok["lsd"]},
		{"sec-ch-ua", id.FP.SecChUa},
		{"sec-ch-ua-mobile", id.FP.SecChUaMobile},
		{"x-fb-friendly-name", q.FriendlyName},
		{"x-asbd-id", params["x_asbd_id"]},
		{"user-agent", id.FP.UserAgent},
		{"content-type", "application/x-www-form-urlencoded"},
		{"accept", "*/*"},
		{"origin", id.Base.Scheme + "://" + id.Base.Host},
		{"sec-fetch-site", "same-origin"},
		{"sec-fetch-mode", "cors"},
		{"sec-fetch-dest", "empty"},
		{"referer", referer},
		{"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", b.AcceptLanguage},
		{"priority", "u=1, i"},
	}
	return graphql.Built{Path: DefaultEndpoint, Header: hdr, Body: graphql.EncodeForm(b.form(id, tok, params, q, vars))}
}

// form builds the POST body in the order a browser sends it.
func (b Builder) form(id *session.Identity, tok session.Tokens, params map[string]string, q registry.Query, vars []byte) []graphql.Field {
	cometReq := params["__comet_req"]
	if tok["__comet_req"] != "" {
		cometReq = tok["__comet_req"]
	}
	f := []graphql.Field{
		{K: "av", V: "0"},
		{K: "__aaid", V: "0"},
		{K: "__user", V: "0"},
		{K: "__a", V: params["__a"]},
		{K: "__req", V: id.NextReq()},
		{K: "__hs", V: tok["__hs"]},
		{K: "dpr", V: params["dpr"]},
		{K: "__ccg", V: params["__ccg"]},
		{K: "__rev", V: tok["__rev"]},
		{K: "__s", V: id.SessionParam()},
		{K: "__hsi", V: tok["__hsi"]},
		{K: "__comet_req", V: cometReq},
	}
	if b.Locale != "" {
		f = append(f, graphql.Field{K: "locale", V: b.Locale})
	}
	if tok["fb_dtsg"] != "" {
		f = append(f, graphql.Field{K: "fb_dtsg", V: tok["fb_dtsg"]})
	}
	f = append(f,
		graphql.Field{K: "lsd", V: tok["lsd"]},
		graphql.Field{K: "jazoest", V: htmlx.Jazoest(tok)},
		graphql.Field{K: "__spin_r", V: tok["__spin_r"]},
		graphql.Field{K: "__spin_b", V: tok["__spin_b"]},
		graphql.Field{K: "__spin_t", V: tok["__spin_t"]},
		graphql.Field{K: "fb_api_caller_class", V: params["fb_api_caller_class"]},
		graphql.Field{K: "fb_api_req_friendly_name", V: q.FriendlyName},
		graphql.Field{K: "variables", V: string(vars)},
		graphql.Field{K: "server_timestamps", V: params["server_timestamps"]},
		graphql.Field{K: "doc_id", V: q.DocID},
	)
	return f
}
