package client

import (
	"context"
	"net/url"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
)

// signedSuffix marks an operation variant that needs TikTok's signature:
// its registry label (doc_id) ends in it ("web-signed"). The other
// variants go out as built (the creator list, the profile page: C2, C3).
const signedSuffix = "-signed"

// Signed reports whether an operation variant needs a signature.
func Signed(q registry.Query) bool { return strings.HasSuffix(q.DocID, signedSuffix) }

// URLSigner signs one call's URL in a TikTok page (internal/tt/sign): it
// returns the URL the page's own script made of it, for the identity to
// send by plain HTTP. referer is the page the call is made from.
type URLSigner interface {
	SignURL(ctx context.Context, id *session.Identity, query, rawURL, referer string) (string, error)
}

// SignerFor returns the client's signer for tt.signer.mode: none sends
// every call as built (a variant that needs a signature is then refused,
// and the client falls back to the next variant); browser has s sign the
// variants that need it and sends the rest as built.
func SignerFor(mode string, s URLSigner) graphql.Signer {
	if mode != config.SignerBrowser || s == nil {
		return nil
	}
	return variantSigner{s}
}

type variantSigner struct{ s URLSigner }

func (v variantSigner) Sign(ctx context.Context, id *session.Identity, q registry.Query, rawURL string, hdr [][2]string) (string, [][2]string, error) {
	if !Signed(q) {
		return rawURL, hdr, nil
	}
	referer := ""
	for _, h := range hdr {
		if h[0] == "referer" {
			referer = h[1]
		}
	}
	signed, err := v.s.SignURL(ctx, id, q.Name, rawURL, referer)
	if err != nil {
		return "", nil, err
	}
	// The page signs on its own host; the call goes to the identity's.
	if su, err := url.Parse(signed); err == nil {
		if ru, err := url.Parse(rawURL); err == nil {
			su.Scheme, su.Host = ru.Scheme, ru.Host
			signed = su.String()
		}
	}
	return signed, hdr, nil
}
