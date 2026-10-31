// Command fakefb serves a synthetic Facebook for local development, tests
// and load tests. Point fb.base_url at it (IGS_FB__BASE_URL).
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

func main() {
	addr := flag.String("addr", ":8090", "listen address")
	seed := flag.Uint64("seed", 42, "world seed")
	pages := flag.Int("pages", 5, "number of pages")
	posts := flag.Int("posts", 30, "posts per page")
	comments := flag.Int("max-comments", 120, "max comments per post")
	rateLimitEvery := flag.Int("rate-limit-every", 0, "every Nth GraphQL request per identity is rate limited (0 = off)")
	loginWallAfter := flag.Int("login-wall-after", 0, "GraphQL requests per identity before a login wall (0 = off)")
	latency := flag.Duration("latency", 0, "mean added latency")
	consent := flag.Bool("consent-wall", false, "show the cookie consent wall")
	docIDs := flag.String("docids", "seed", "accepted doc_ids: seed (the embedded registry seed) or rotated (different ids, discoverable from /rsrc.php bundles)")
	flag.Parse()

	spec := fakefb.DefaultSpec()
	spec.Seed, spec.Pages, spec.PostsPerPage, spec.MaxComments = *seed, *pages, *posts, *comments
	w := fakefb.NewWorld(spec)

	s, err := registry.ParseSeed(mustRead("queries.seed.yaml"))
	if err != nil {
		slog.Error("seed", "err", err)
		os.Exit(1)
	}
	docs := map[string][]string{}
	for _, q := range s.Queries {
		docs[q.FriendlyName] = append(docs[q.FriendlyName], q.DocID)
	}
	switch *docIDs {
	case "seed":
	case "rotated":
		docs = fakefb.RotatedDocs(docs)
	default:
		slog.Error("unknown --docids", "value", *docIDs)
		os.Exit(2)
	}
	for name, ids := range docs {
		slog.Info("doc_id", "friendly_name", name, "doc_id", ids[0])
	}
	srv := fakefb.NewServer(w, fakefb.Options{Docs: docs, InlineReply: true, Faults: fakefb.Faults{
		RateLimitEvery: *rateLimitEvery, LoginWallAfter: *loginWallAfter, Latency: *latency, ConsentWall: *consent,
	}})
	for _, p := range w.Pages() {
		slog.Info("page", "id", p.ID, "vanity", p.Vanity, "posts", len(p.Posts))
	}
	slog.Info("fakefb listening", "addr", *addr)
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 10 * time.Second}
	if err := hs.ListenAndServe(); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}

func mustRead(name string) []byte {
	b, err := configs.FS.ReadFile(name)
	if err != nil {
		panic(err)
	}
	return b
}
