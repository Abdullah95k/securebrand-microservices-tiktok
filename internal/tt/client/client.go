// Package client makes the shared client layer talk like logged-out
// TikTok's web client: its page hooks, its GET request builder, its
// statusCode decoder and its operation seed. Request shapes follow the
// owner's recordings (docs/tiktok/har-findings-2026-10-31.md §2).
package client

import (
	"strings"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/htmlx"
)

// BaseURL is where the logged-out web client lives.
const BaseURL = "https://www.tiktok.com"

// Hooks are TikTok's page hooks: the page's app-context values, and /login
// and the captcha and verify paths as walls (T6).
func Hooks() session.Hooks {
	return session.Hooks{Extract: htmlx.Extract, IsWall: IsWall}
}

// IsWall reports a login, captcha or verify path.
func IsWall(path string) bool {
	return strings.HasPrefix(path, "/login") || strings.HasPrefix(path, "/captcha") || strings.HasPrefix(path, "/verify")
}

// Seed returns the embedded operation seed (configs/tt/operations.seed.yaml).
func Seed() (registry.Seed, error) {
	b, err := configs.FS.ReadFile("tt/operations.seed.yaml")
	if err != nil {
		return registry.Seed{}, err
	}
	return registry.ParseSeed(b)
}

// Decode reads TikTok's envelope: one JSON object whose statusCode (or
// status_code) is an error unless it is 0. An empty body has no parts; the
// rules class it RegistryStale (detect.Rules.EmptyIsStale).
func Decode(body []byte) ([]gjson.Result, []detect.GQLError, bool) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || !gjson.Valid(trimmed) {
		return nil, nil, false
	}
	r := gjson.Parse(trimmed)
	code := r.Get("statusCode")
	if !code.Exists() {
		code = r.Get("status_code")
	}
	msg := r.Get("statusMsg").String()
	if msg == "" {
		msg = r.Get("status_msg").String()
	}
	if c := code.Int(); c != 0 {
		return []gjson.Result{r}, []detect.GQLError{{Code: c, Message: msg}}, false
	}
	return []gjson.Result{r}, nil, true
}
