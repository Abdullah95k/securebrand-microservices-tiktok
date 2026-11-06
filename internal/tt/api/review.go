package api

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// The discovery review page (docs/tiktok/discovery-plan.md §11, ADR 0007
// §22): three static files under /review/ that hold no data. The page asks
// for an API key, keeps it in the tab's session storage, and calls /v1 with
// it like any client, so the API's keys and scopes decide what it shows and
// does: usernames only with pii:read, changes only with discovery:write or
// accounts:write. The API stays the source of truth.
//
// Forgery: the key travels in the Authorization header, which a browser
// never adds by itself and another site's script cannot set (the API sends
// no CORS headers), so no other site can act with it; sameOrigin also
// refuses changes a browser marks as sent from another site. The page's
// Content-Security-Policy runs only its own script and lets nothing frame
// it.

//go:embed review
var reviewFiles embed.FS

// reviewCSP allows the page's own script, style and calls, and nothing
// else: no inline script, no other origin, no framing, no form posts.
const reviewCSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; " +
	"base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// reviewHandler serves the page's files with headers that keep it to
// itself.
func reviewHandler() http.Handler {
	sub, _ := fs.Sub(reviewFiles, "review")
	files := http.StripPrefix("/review", http.FileServerFS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			problem(w, http.StatusMethodNotAllowed, "method not allowed", "the review page is read-only: it calls /v1")
			return
		}
		if r.URL.Path == "" || r.URL.Path == "/review" {
			http.Redirect(w, r, "/review/", http.StatusMovedPermanently)
			return
		}
		h := w.Header()
		h.Set("Content-Security-Policy", reviewCSP)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}

// sameOrigin refuses a change (any method but GET, HEAD and OPTIONS) that a
// browser says another site sent (Sec-Fetch-Site cross-site or same-site).
// Programs other than browsers send no such header and are not affected.
func sameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
		default:
			if site := strings.ToLower(r.Header.Get("Sec-Fetch-Site")); site == "cross-site" || site == "same-site" {
				problem(w, http.StatusForbidden, "cross-site request", "changes are accepted only from this API's own page or a program")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
