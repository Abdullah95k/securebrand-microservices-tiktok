package main

import (
	"context"
	"errors"
	"flag"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	ttapi "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/api"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// apikeyCreate makes an API client in schema tt and prints its key once.
func apikeyCreate(ctx context.Context, cfg *config.Config, args []string) (any, error) {
	fs := flag.NewFlagSet("apikey create", flag.ExitOnError)
	name := fs.String("name", "", "client name")
	scopes := fs.String("scopes", "jobs:write,accounts:write,data:read", "comma-separated scopes: "+strings.Join(ttapi.Scopes, ", "))
	rate := fs.Int("rate", 0, "requests per minute (0 = api.default_rate_per_minute)")
	_ = fs.Parse(args)
	if *name == "" {
		return nil, errors.New("--name is required")
	}
	db, err := ttstore.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	key, id, err := ttapi.CreateClient(ctx, db, *name, strings.Split(*scopes, ","), *rate)
	if err != nil {
		return nil, err
	}
	return map[string]any{"client_id": id, "name": *name, "api_key": key, "note": "store this key now; it cannot be shown again"}, nil
}

// apikeyRevoke revokes a key by its prefix (the part after tks_).
func apikeyRevoke(ctx context.Context, cfg *config.Config, args []string) (any, error) {
	fs := flag.NewFlagSet("apikey revoke", flag.ExitOnError)
	prefix := fs.String("prefix", "", "key prefix (the part after tks_)")
	_ = fs.Parse(args)
	if *prefix == "" {
		return nil, errors.New("--prefix is required")
	}
	db, err := ttstore.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	ok, err := ttapi.RevokeClient(ctx, db, *prefix)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("no live key has this prefix")
	}
	return map[string]any{"revoked": *prefix}, nil
}
