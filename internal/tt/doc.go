// Package tt is the root of tks's TikTok code: htmlx (the page's
// rehydration document and the values the web client sends), detect
// (TikTok's status codes, empty answers, challenge pages), client (page
// hooks, the GET request builder, the decoder, the operation seed), and
// later sign, parse, stage and probe. It builds on internal/scrape only and
// never imports Facebook code; boundary_test.go enforces it.
package tt
