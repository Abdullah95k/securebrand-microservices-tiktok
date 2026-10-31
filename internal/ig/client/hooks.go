// Package client makes the shared client layer talk like logged-out
// Instagram's web client: its page hooks, its GraphQL request builder and
// its query seed. Request shapes follow the owner's recordings
// (docs/instagram/har-findings-2026-10-30.md §1-2).
package client

import (
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// BaseURL is where the logged-out web client lives; instagram.com answers
// 301 to it.
const BaseURL = "https://www.instagram.com"

// Hooks are Instagram's page hooks: its tokens, and /accounts/login and
// /challenge as walls. There is no interstitial to answer: the login prompt
// the recordings showed is drawn by JavaScript over a page that loaded.
func Hooks() session.Hooks {
	return session.Hooks{
		Extract: func(html []byte) (session.Tokens, bool) {
			tok := htmlx.ExtractTokens(html)
			return tok, htmlx.Valid(tok)
		},
		IsWall: IsWall,
	}
}

// IsWall reports a login or challenge path.
func IsWall(path string) bool {
	return strings.HasPrefix(path, "/accounts/login") || strings.HasPrefix(path, "/challenge")
}

// Seed returns the embedded registry seed: the five logged-out operations
// with the doc_ids recorded on 2026-10-30 (they rotate).
func Seed() (registry.Seed, error) {
	b, err := configs.FS.ReadFile("ig/queries.seed.yaml")
	if err != nil {
		return registry.Seed{}, err
	}
	return registry.ParseSeed(b)
}
