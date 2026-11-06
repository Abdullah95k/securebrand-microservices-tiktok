package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// discoveryKeywords runs `tksctl discovery keywords seed|import|list`
// (fbs's fbctl command on schema tt):
//
//	seed                 load the Iraq keywords (configs/tt/discovery/iraq.yaml)
//	import --file F      load a seed file (YAML, same format) or a CSV of
//	                     term,language[,group[,enabled]]
//	list [--language L] [--group G]
//
// Loading adds only keywords not listed yet; listed ones keep their state,
// so a keyword someone turned off stays off.
func discoveryKeywords(ctx context.Context, cfg *config.Config, action string, args []string) (any, error) {
	fs := flag.NewFlagSet("discovery keywords "+action, flag.ExitOnError)
	file := fs.String("file", "", "seed YAML or CSV (term,language,group,enabled)")
	lang := fs.String("language", "", "list: only this language (en, ar, ckb, kmr)")
	group := fs.String("group", "", "list: only this group")
	_ = fs.Parse(args)
	var seed []discovery.SeedKeyword
	var err error
	switch action {
	case "seed":
		seed, err = discovery.IraqSeed()
	case "import":
		if *file == "" {
			return nil, errors.New("discovery keywords import: --file is required")
		}
		seed, err = readKeywordFile(*file)
	case "list":
	default:
		return nil, fmt.Errorf("discovery keywords: unknown action %q (seed, import, list)", action)
	}
	if err != nil {
		return nil, err
	}
	db, err := ttstore.OpenConfig(ctx, cfg.Postgres, 4)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if action == "list" {
		kws, err := discovery.ListKeywords(ctx, db, discovery.KeywordFilter{Language: *lang, Group: *group, Limit: 100000})
		if err != nil {
			return nil, err
		}
		return map[string]any{"keywords": kws}, nil
	}
	added, err := discovery.ImportSeed(ctx, db, seed)
	if err != nil {
		return nil, err
	}
	return map[string]any{"keywords": len(seed), "added": added, "already_listed": len(seed) - added}, nil
}

func readKeywordFile(path string) ([]discovery.SeedKeyword, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the operator's own file
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(strings.ToLower(path), ".csv") {
		return discovery.ParseKeywordCSV(bytes.NewReader(b))
	}
	return discovery.ParseSeed(b)
}
