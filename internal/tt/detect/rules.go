// Package detect holds TikTok's detect rules for the neutral classifier
// (internal/scrape/detect). Every value is a hypothesis from public sources
// or the owner's recordings until the probe confirms it
// (docs/tiktok/open-questions.md).
package detect

import (
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/htmlx"
)

// Status codes TikTok's web API answers in its statusCode field.
const (
	// StatusCaptcha comes with a verify_event: the caller must solve a
	// captcha, which tks never does (T6).
	StatusCaptcha = 10000
	// StatusPrivatePost and StatusPrivateAccount are yt-dlp's (T6).
	StatusPrivatePost    = 10216
	StatusPrivateAccount = 10222
	// StatusUserNotFound and StatusUserBanned are reported by public
	// sources for unknown and banned accounts (T6).
	StatusUserNotFound = 10202
	StatusUserBanned   = 10221
)

// Rules returns TikTok's rules: an empty 200 is a refused request (no
// device id, no or a bad signature: T2, T4) and falls back to the next
// variant; a captcha is a checkpoint; private, unknown and banned accounts
// are content the viewer cannot see; a redirect to /login is a wall; the
// JavaScript challenge and captcha pages are checkpoints on page loads (T28).
func Rules() detect.Rules {
	return detect.Rules{
		EmptyIsStale:          true,
		CheckpointCodes:       []int64{StatusCaptcha},
		UnavailableCodes:      []int64{StatusPrivatePost, StatusPrivateAccount, StatusUserNotFound, StatusUserBanned},
		LoginPaths:            []string{"/login"},
		CheckpointPaths:       []string{"/captcha", "/verify"},
		CheckpointPageMarkers: htmlx.ChallengeMarkers,
		BlockMarkers:          [][]byte{[]byte("Access Denied"), []byte("too many requests")},
	}
}
