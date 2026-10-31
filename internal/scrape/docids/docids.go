// Package docids finds Facebook's current Relay operation ids
// (friendly_name -> doc_id) in page HTML and the JS bundles it references.
// Facebook rotates doc_ids with its deploys; the fbs-spike and the
// registry-sync service use this to follow them.
package docids

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

var (
	// Bundle URLs appear in script tags and, JSON-escaped, in inline data.
	scriptSrc   = regexp.MustCompile(`(?:https?:)?(?:\\/\\/|//)[A-Za-z0-9.\-:]+(?:\\/|/)rsrc\.php(?:\\/|/)[^"'\s<>]+?\.js[^"'\s<>]*`)
	relayOp     = regexp.MustCompile(`__d\("([A-Za-z0-9_]+)_facebookRelayOperation"[^{]*\{[^}]*?exports\s*=\s*"(\d+)"`)
	relayOpLoad = regexp.MustCompile(`"([A-Za-z0-9_]+Query)"\s*,\s*\{?\s*"?(?:queryID|id)"?\s*:\s*"(\d{8,})"`)
)

// Scan adds the friendly_name -> doc_id pairs declared in b to out.
func Scan(b []byte, out map[string]string) {
	for _, re := range []*regexp.Regexp{relayOp, relayOpLoad} {
		for _, m := range re.FindAllSubmatch(b, -1) {
			out[string(m[1])] = string(m[2])
		}
	}
}

// Bundles returns the JS bundle URLs html references that may be fetched:
// those on Facebook's CDN or on the page's own host, in page order and
// without duplicates.
func Bundles(html []byte, pageURL *url.URL) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range scriptSrc.FindAll(html, -1) {
		src := strings.ReplaceAll(string(m), `\/`, "/")
		if strings.HasPrefix(src, "//") {
			src = pageURL.Scheme + ":" + src
		}
		u, err := url.Parse(src)
		if err != nil || seen[src] {
			continue
		}
		if h := u.Hostname(); h != "fbcdn.net" && !strings.HasSuffix(h, ".fbcdn.net") && u.Host != pageURL.Host {
			continue
		}
		seen[src] = true
		out = append(out, src)
	}
	return out
}

// Fetcher downloads one bundle.
type Fetcher func(ctx context.Context, src string) ([]byte, error)

// MaxBundleBytes caps a bundle download.
const MaxBundleBytes = 20 << 20

// HTTPFetcher downloads bundles with hc. A bundle larger than
// MaxBundleBytes is an error rather than silently truncated.
func HTTPFetcher(hc *http.Client) Fetcher {
	return func(ctx context.Context, src string) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
		if err != nil {
			return nil, err
		}
		resp, err := hc.Do(req)
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("docids: %s: status %d", src, resp.StatusCode)
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, MaxBundleBytes+1))
		if err != nil {
			return nil, err
		}
		if len(b) > MaxBundleBytes {
			return nil, fmt.Errorf("docids: %s: larger than %d bytes", src, MaxBundleBytes)
		}
		return b, nil
	}
}

// Scanner scans pages and their bundles. Bundle URLs are content-addressed
// and immutable, so each bundle's ids are remembered and it is fetched
// once per Scanner.
type Scanner struct {
	Fetch Fetcher
	// MaxBundles caps the bundles fetched per Page call.
	MaxBundles int
	// MaxCached caps the remembered bundles; 0 means 4096.
	MaxCached int

	cache map[string]map[string]string
}

// Page scans html and up to MaxBundles of the bundles it references into
// out. It returns the number of bundles fetched; bundles that fail to
// download are skipped and fetched again on the next call.
func (s *Scanner) Page(ctx context.Context, html []byte, pageURL *url.URL, out map[string]string) int {
	Scan(html, out)
	if s.cache == nil {
		s.cache = map[string]map[string]string{}
	}
	fetched := 0
	for _, src := range Bundles(html, pageURL) {
		if ids, ok := s.cache[src]; ok {
			for k, v := range ids {
				out[k] = v
			}
			continue
		}
		if fetched >= s.MaxBundles {
			continue
		}
		fetched++
		b, err := s.Fetch(ctx, src)
		if err != nil {
			continue
		}
		ids := map[string]string{}
		Scan(b, ids)
		for k, v := range ids {
			out[k] = v
		}
		limit := s.MaxCached
		if limit <= 0 {
			limit = 4096
		}
		if len(s.cache) >= limit {
			clear(s.cache)
		}
		s.cache[src] = ids
	}
	return fetched
}
