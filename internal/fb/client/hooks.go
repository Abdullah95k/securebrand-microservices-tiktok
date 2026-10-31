// Package client holds what makes the shared client layer talk like
// logged-out Facebook's web client: the GraphQL request builder and the page
// hooks the loader and the browser bootstrapper run.
package client

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// Hooks are Facebook's page hooks. acceptLanguage is sent with the consent
// form.
func Hooks(acceptLanguage string) session.Hooks {
	return session.Hooks{
		Extract: func(html []byte) (session.Tokens, bool) {
			tok := htmlx.ExtractTokens(html)
			return tok, htmlx.Valid(tok)
		},
		IsWall: IsWall,
		AfterLoad: func(ctx context.Context, id *session.Identity, page string, html []byte, tok session.Tokens, canReload bool) bool {
			if tok["js_datr"] != "" && id.Doer.Cookie(id.Base, "datr") == "" {
				id.Doer.SetCookie(id.Base, "datr", tok["js_datr"])
			}
			if canReload && tok["lsd"] != "" && htmlx.ConsentWall(html) {
				return acceptConsent(ctx, id, tok, page, acceptLanguage) == nil
			}
			return false
		},
	}
}

// IsWall reports a login or checkpoint path.
func IsWall(path string) bool {
	return strings.HasPrefix(path, "/login") || strings.HasPrefix(path, "/checkpoint")
}

// acceptConsent answers the cookie-consent wall with essential cookies only.
func acceptConsent(ctx context.Context, id *session.Identity, tok session.Tokens, referer, acceptLanguage string) error {
	form := url.Values{}
	form.Set("accept_only_essential", "true")
	form.Set("lsd", tok["lsd"])
	form.Set("jazoest", htmlx.Jazoest(tok))
	u, _ := id.Base.Parse("/cookie/consent/")
	hdr := [][2]string{
		{"sec-ch-ua", id.FP.SecChUa}, {"sec-ch-ua-mobile", id.FP.SecChUaMobile}, {"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"user-agent", id.FP.UserAgent}, {"content-type", "application/x-www-form-urlencoded"}, {"accept", "*/*"},
		{"origin", session.Origin(id.Base)}, {"sec-fetch-site", "same-origin"}, {"sec-fetch-mode", "cors"},
		{"sec-fetch-dest", "empty"}, {"referer", referer}, {"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", acceptLanguage},
	}
	if err := id.Wait(ctx); err != nil {
		return err
	}
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: "POST", URL: u.String(), Header: hdr, Body: []byte(form.Encode())})
	if err != nil {
		return err
	}
	if resp.Status >= 400 {
		return fmt.Errorf("consent: http %d", resp.Status)
	}
	metrics.Bootstraps.WithLabelValues("http", "consent").Inc()
	return nil
}
