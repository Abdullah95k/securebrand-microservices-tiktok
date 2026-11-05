// Command fakett serves a synthetic TikTok (internal/fakett) for local
// development, the Compose stack and the chart's smoke run. Point
// tt.base_url at it (TKS_TT__BASE_URL). It never reaches TikTok.
//
//	fakett [--addr :8090] [--accounts 3] [--posts 60] [--require-signature]
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
)

func main() {
	spec := fakett.DefaultSpec()
	addr := flag.String("addr", ":8090", "listen address")
	seed := flag.Uint64("seed", spec.Seed, "world seed")
	accounts := flag.Int("accounts", spec.Accounts, "number of accounts (account_0, account_1, ...)")
	posts := flag.Int("posts", spec.PostsPerAccount, "posts per account")
	comments := flag.Int("max-comments", spec.MaxComments, "max top-level comments per post")
	replies := flag.Int("max-replies", spec.MaxReplies, "max replies per comment")
	spacing := flag.Duration("post-spacing", spec.PostSpacing, "time between an account's posts")
	signed := flag.Bool("require-signature", false, "refuse unsigned comment and reply calls (empty 200), as TikTok does; "+
		"pages then carry a script that signs them, for the browser signer")
	rateLimitEvery := flag.Int("rate-limit-every", 0, "answer every Nth API call with HTTP 429 (0 = off)")
	flag.Parse()

	spec.Seed, spec.Accounts, spec.PostsPerAccount = *seed, *accounts, *posts
	spec.MaxComments, spec.MaxReplies, spec.PostSpacing = *comments, *replies, *spacing
	w := fakett.NewWorld(spec)
	f := fakett.Faults{RateLimitEvery: *rateLimitEvery}
	if *signed {
		f.RequireSignature = true
		f.SignedPaths = []string{"/api/comment/list/", "/api/comment/list/reply/"}
	}
	for _, a := range w.Accounts() {
		slog.Info("account", "username", a.Username, "posts", len(a.Posts))
	}
	slog.Info("fakett listening", "addr", *addr, "require_signature", *signed)
	hs := &http.Server{Addr: *addr, Handler: fakett.NewServer(w, fakett.Options{Faults: f}), ReadHeaderTimeout: 10 * time.Second}
	if err := hs.ListenAndServe(); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
