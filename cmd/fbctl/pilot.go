package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/pilot"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

// registryImport makes every query of a registry file (a Phase 0
// registry.yaml) the active entry; the entries it replaces stay as
// fallbacks. With --keep-promoted it only replaces seed entries, so running
// it again (the pilot does at every start) never undoes a rotation
// registry-sync or an operator promoted since.
func registryImport(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("registry import", flag.ExitOnError)
	file := fs.String("file", "", "registry YAML (fbs-spike docids/har --write-registry, or phase0/out/registry.yaml)")
	keep := fs.Bool("keep-promoted", false, "leave queries whose active entry is not the embedded seed's")
	_ = fs.Parse(args)
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	seed, err := registry.ParseSeed(b)
	if err != nil {
		return err
	}
	if q := registry.PlaceholderQueries(seed); len(q) > 0 {
		return fmt.Errorf("%s still has placeholder doc_ids for %v: run Phase 0's docids or har step first", *file, q)
	}
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, q := range seed.Queries {
		if *keep {
			var source, doc string
			err := db.QueryRow(ctx, `SELECT source, doc_id FROM fb_queries WHERE name = $1 AND status = 'active'
				ORDER BY promoted_at DESC NULLS LAST LIMIT 1`, q.Name).Scan(&source, &doc)
			if err == nil && source != "seed" {
				fmt.Printf("%-20s %s kept (active from %s)\n", q.Name, doc, source)
				continue
			}
		}
		q.Status, q.Source = registry.StatusActive, "import"
		if err := registry.Upsert(ctx, db, q); err != nil {
			return fmt.Errorf("%s: %w", q.Name, err)
		}
		fmt.Printf("%-20s %s active\n", q.Name, q.DocID)
	}
	return nil
}

// reportLoad prints what the pipeline's jobs cost per page per day.
func reportLoad(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("report load", flag.ExitOnError)
	since := fs.Duration("since", 24*time.Hour, "measure jobs created this long ago and after; start after the backfills")
	pages := fs.Int("pages", 0, "project the steady state to this many pages")
	cost := fs.Float64("cost-per-gb", 0, "project the cost on a per-GB proxy tier at this price")
	headroom := fs.Float64("headroom", 0.5, "IPs beyond the average need, for peaks")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	rep, err := pilot.Load(ctx, db, time.Now().Add(-*since))
	if err != nil {
		return err
	}
	if *asJSON {
		printJSON(rep)
		return nil
	}
	fmt.Print(pilot.Format(rep, pilot.Projection{Pages: *pages, IdentityRPS: cfg.FB.IdentityRPS, CostPerGB: *cost, Headroom: *headroom}))
	return nil
}
