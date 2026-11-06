package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/accounts"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// accountsImport runs `tksctl accounts import` (docs/tiktok/
// discovery-plan.md §6; runbook docs/runbooks/tks-import.md):
//
//	accounts import FILE --client NAME [--label L]... [--dry-run]
//	accounts import FILE --client NAME --disable-missing [--confirm] [--force]
//	accounts import --report BATCH --client NAME [--problems]
//
// FILE holds one account a line (username, @username, profile or video
// link, secUid, id:<user id>) or a CSV with an account column. Running it
// again is safe: lines done already come back "already monitored".
// --disable-missing removes the client's other registrations: without
// --confirm it only shows what it would remove.
func accountsImport(ctx context.Context, cfg *config.Config, args []string) (any, error) {
	file := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		file, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet("accounts import", flag.ExitOnError)
	client := fs.String("client", "", "the API client the accounts are registered to (tksctl apikey create --name)")
	var labels multiFlag
	fs.Var(&labels, "label", "label every account (repeatable, or comma-separated)")
	dry := fs.Bool("dry-run", false, "report what the import would do and change nothing")
	disable := fs.Bool("disable-missing", false, "remove the client's registrations of accounts not in the file (data stays)")
	confirm := fs.Bool("confirm", false, "with --disable-missing: apply it (without, only the dry run is shown)")
	force := fs.Bool("force", false, "with --disable-missing: allow removing more than 10 % of the list")
	report := fs.String("report", "", "list an earlier import's accounts and what tks found since (its batch id)")
	problems := fs.Bool("problems", false, "with --report: only accounts that turned out private, inactive or missing")
	_ = fs.Parse(args)
	if file == "" && fs.NArg() > 0 {
		file = fs.Arg(0)
	}
	if *client == "" {
		return nil, errors.New("--client is required: the API client the accounts belong to")
	}
	db, err := ttstore.OpenConfig(ctx, cfg.Postgres, 4)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	cid, err := clientByName(ctx, db, *client)
	if err != nil {
		return nil, err
	}
	if *report != "" {
		batch, err := uuid.Parse(*report)
		if err != nil {
			return nil, fmt.Errorf("--report: %w", err)
		}
		out, err := accounts.BatchReport(ctx, db, cid, false, batch, *problems, time.Now())
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("no import of this client has this batch id")
		}
		return map[string]any{"batch": batch, "accounts": out}, err
	}
	if file == "" {
		return nil, errors.New("give the file to import (or --report BATCH)")
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	lines, err := accounts.Parse(f)
	if err != nil {
		return nil, err
	}
	o := accounts.ImportOptions{ClientID: cid, Labels: labels, DryRun: *dry || (*disable && !*confirm), DisableMissing: *disable,
		Force: *force, Budget: budgetState(ctx, cfg)}
	rep, err := accounts.Import(ctx, db, cfg, lines, o)
	if rep != nil && *disable && !*confirm && !*dry {
		fmt.Fprintln(os.Stderr, "tksctl: this was the dry run of --disable-missing: nothing changed; add --confirm to apply it")
	}
	return rep, err
}

// clientByName finds a live API client by name.
func clientByName(ctx context.Context, db *pgxpool.Pool, name string) (uuid.UUID, error) {
	rows, err := db.Query(ctx, `SELECT id FROM api_clients WHERE name = $1 AND revoked_at IS NULL`, name)
	if err != nil {
		return uuid.Nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	switch {
	case err != nil:
		return uuid.Nil, err
	case len(ids) == 0:
		return uuid.Nil, fmt.Errorf("no live API client is named %q (tksctl apikey create --name %s)", name, name)
	case len(ids) > 1:
		return uuid.Nil, fmt.Errorf("%d live API clients are named %q: revoke the extra keys", len(ids), name)
	}
	return ids[0], nil
}

// budgetState reads the budget controller's last state from Valkey, for
// the estimated finish; nil without a request budget or Valkey.
func budgetState(ctx context.Context, cfg *config.Config) *ratelimit.BudgetState {
	b := cfg.Scheduler.Budget
	if !b.Enabled || b.RequestsPerDay <= 0 || cfg.Redis.Addr == "" {
		return nil
	}
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return nil
	}
	defer func() { _ = rdb.Close() }()
	st, err := ratelimit.LoadBudgetState(ctx, rdb, cfg.Kafka.TopicPrefix)
	if err != nil {
		return nil
	}
	return &st
}

// multiFlag is a repeatable, comma-separable string flag.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(v string) error {
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			*m = append(*m, s)
		}
	}
	return nil
}
