package graphql

import (
	"net/url"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
)

// RequestBuilder writes one request the way a platform's web client sends
// it: the method, the endpoint path, the query string, the headers in the
// browser's wire order and the body (Facebook's and Instagram's GraphQL
// form POST; TikTok's GET with every value in the query string). The
// client adds the query's registry endpoint and headers on top, and owns
// pacing, the transport and the classification.
type RequestBuilder interface {
	Build(id *session.Identity, q registry.Query, vars []byte, params map[string]string, referer string) Built
}

// Built is a request ready for the wire.
type Built struct {
	// Method is POST when empty.
	Method string
	Path   string // on the identity's host; a registry endpoint replaces it
	// Query is the encoded query string, without the "?"; empty for none.
	Query  string
	Header [][2]string
	Body   []byte
}

// Decoder turns a response body into its parts, its errors and whether it
// carries data. The default reads GraphQL (SplitParts, Errors, HasData); a
// platform with another envelope (TikTok's statusCode) passes its own.
type Decoder func(body []byte) (parts []gjson.Result, errs []detect.GQLError, hasData bool)

func decodeGraphQL(body []byte) ([]gjson.Result, []detect.GQLError, bool) {
	parts := SplitParts(body)
	return parts, Errors(parts), HasData(parts)
}

// Field is one form field.
type Field struct{ K, V string }

// EncodeForm URL-encodes fields in order, leaving out empty values (except
// variables, which is always sent).
func EncodeForm(f []Field) []byte {
	var b strings.Builder
	for i, p := range f {
		if p.V == "" && p.K != "variables" {
			continue
		}
		if i > 0 && b.Len() > 0 {
			b.WriteByte('&')
		}
		b.WriteString(url.QueryEscape(p.K))
		b.WriteByte('=')
		b.WriteString(url.QueryEscape(p.V))
	}
	return []byte(b.String())
}
