// Package detect holds logged-out Instagram's classification rules: the
// shared engine (internal/scrape/detect) with Instagram's wall paths.
package detect

import scrapedetect "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"

// Rules returns Instagram's rules. Its GraphQL backend is Facebook's, so the
// error codes and messages start as Facebook's; the probe and Phase 0
// confirm them. Walls (brief §6, M4): a redirect to /accounts/login is a
// login wall and one to /challenge a checkpoint. An HTML login page where
// GraphQL JSON was expected names /accounts/login/.
//
// No page markers yet: the recordings showed no walled page load (only the
// dismissible login prompt drawn by JavaScript), so any marker would be a
// guess that could misread a normal profile page.
func Rules() scrapedetect.Rules {
	r := scrapedetect.DefaultRules()
	r.CheckpointPaths = []string{"/challenge"}
	r.LoginPaths = []string{"/accounts/login"}
	r.LoginMarkers = append(r.LoginMarkers, []byte(`/accounts/login/`))
	r.CheckpointPageMarkers = nil
	r.LoginPageMarkers = nil
	return r
}
