package main

import (
	"context"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/replay"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

// replayRaw parses captured raw responses again with the current parsers.
// It exits 3 when some still miss required fields.
func replayRaw(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	prefix := fs.String("prefix", "raw-failures", "raw-failures (drift and failures) or raw (everything, capture_raw: all)")
	queries := fs.String("query", "", "query names, comma separated (default: all)")
	since := fs.Duration("since", 7*24*time.Hour, "only objects written within this long")
	maxObjects := fs.Int("max-objects", 500, "newest objects read per query (0 = all)")
	fixturesDir := fs.String("fixtures-dir", "", "write still-drifting responses here as scrubbed parser fixtures")
	name := fs.String("name", "replay", "fixture case name")
	samples := fs.Int("samples", 3, "fixtures per query")
	_ = fs.Parse(args)

	parsers, err := queryParsers(ctx, cfg)
	if err != nil {
		return err
	}
	s3, err := s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, cfg.S3.UseSSL)
	if err != nil {
		return err
	}
	var qs []string
	for _, q := range strings.Split(*queries, ",") {
		if q = strings.TrimSpace(q); q != "" {
			qs = append(qs, q)
		}
	}
	res, err := replay.Run(ctx, s3, replay.Options{Bucket: cfg.S3.RawBucket, Prefix: *prefix, Parsers: parsers, Queries: qs,
		Since: time.Now().Add(-*since), MaxObjects: *maxObjects, FixturesDir: *fixturesDir, Name: *name, Samples: *samples})
	printJSON(res)
	if err != nil {
		return err
	}
	if res.Drifting() {
		os.Exit(3)
	}
	return nil
}

// queryParsers maps query names to parsers from the live registry, or from
// the seed when the database is not reachable.
func queryParsers(ctx context.Context, cfg *config.Config) (map[string]string, error) {
	parsers := map[string]string{}
	seedBytes, err := configs.FS.ReadFile(cfg.FB.RegistrySeed)
	if err != nil {
		return nil, err
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, err
	}
	for _, q := range seed.Queries {
		parsers[q.Name] = q.Parser
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if db, err := pg.Open(cctx, cfg.Postgres.DSN, 2); err == nil {
		defer db.Close()
		if qs, err := registry.List(cctx, db); err == nil {
			for _, q := range qs {
				parsers[q.Name] = q.Parser
			}
		}
	}
	return parsers, nil
}
