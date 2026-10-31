package graphql

import (
	"net/url"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// RequestBuilder writes one GraphQL request the way a platform's web client
// sends it: the endpoint path, the headers in the browser's wire order and
// the form body. The client adds the query's registry endpoint and headers
// on top, and owns pacing, the transport and the classification.
type RequestBuilder interface {
	Build(id *session.Identity, q registry.Query, vars []byte, params map[string]string, referer string) Built
}

// Built is a request ready for the wire.
type Built struct {
	Path   string // on the identity's host; a registry endpoint replaces it
	Header [][2]string
	Body   []byte
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
