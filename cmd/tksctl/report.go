package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/cost"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/halt"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/report"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// exitStop is report --check's exit code when a stop rule fired.
const exitStop = 3

// parseIraq reads "2006-01-02 15:04" (or with T) in Iraq time.
func parseIraq(s string) (time.Time, error) {
	s = strings.Replace(strings.TrimSpace(s), "T", " ", 1)
	for _, f := range []string{"2006-01-02 15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(f, s, report.Iraq); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("%q: want YYYY-MM-DD HH:MM, Iraq time", s)
}

// runReport is `tksctl report`: the first-run report and the stop rules
// (docs/runbooks/tks-production.md).
func runReport(ctx context.Context, cfg *config.Config, args []string) (any, error) {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	from := fs.String("from", "", "start, Iraq time (YYYY-MM-DD HH:MM); default: --hours before --to")
	to := fs.String("to", "", "end, Iraq time; default: now")
	hours := fs.Float64("hours", 24, "window length when --from is not given")
	prom := fs.String("prometheus", os.Getenv("TKS_PROMETHEUS_URL"), "Prometheus base URL (requests, signatures, blocks, proxy data, containers)")
	promSel := fs.String("prometheus-selector", `namespace="tks"`, "label selector added to every Prometheus query")
	asJSON := fs.Bool("json", false, "print JSON instead of text")
	check := fs.Bool("check", false, "exit with code 3 when a stop rule fires")
	ipMonth := fs.Float64("ip-month-usd", 3, "price of one proxy IP a month (owner, 2026-11-05)")
	browserHour := fs.Float64("browser-hour-usd", 0.017, "price of a signer page-hour")
	compute := fs.Float64("compute-usd", 0.0002, "compute per account a day")
	storage := fs.Float64("storage-gb-month-usd", 0.10, "storage per GB-month")
	r := report.DefaultRules
	fs.Float64Var(&r.MaxBlockRate, "max-block-rate", r.MaxBlockRate, "stop above this share of blocked requests")
	fs.Float64Var(&r.MaxChallengesPer1k, "max-challenges-per-1000", r.MaxChallengesPer1k, "stop above this many captchas or challenges per 1,000 requests")
	fs.Float64Var(&r.MaxSignRefused, "max-sign-refused", r.MaxSignRefused, "stop above this share of signed calls TikTok refused")
	fs.Float64Var(&r.MaxRestarts, "max-restarts", r.MaxRestarts, "stop above this many restarts of one container")
	_ = fs.Parse(args)

	w := report.Window{To: time.Now()}
	var err error
	if *to != "" {
		if w.To, err = parseIraq(*to); err != nil {
			return nil, err
		}
	}
	if *from != "" {
		if w.From, err = parseIraq(*from); err != nil {
			return nil, err
		}
	} else {
		w.From = w.To.Add(-time.Duration(*hours * float64(time.Hour)))
	}
	if !w.From.Before(w.To) {
		return nil, errors.New("report: --from must be before --to")
	}
	db, err := ttstore.OpenConfig(ctx, cfg.Postgres, 2)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdb.Close() }()

	rep := report.Report{Window: w}
	if *prom != "" {
		if rep.Traffic, err = report.ReadTraffic(ctx, report.Prometheus{URL: *prom, Labels: *promSel}, w); err != nil {
			return nil, err
		}
	}
	if rep.Store, err = report.ReadStore(ctx, db, w); err != nil {
		return nil, err
	}
	pc := *cfg
	pc.TT.Cost.IPMonthUSD, pc.TT.Cost.BrowserHourUSD = *ipMonth, *browserHour
	pc.TT.Cost.ComputeUSDPerAccount, pc.TT.Cost.StorageGBMonthUSD = *compute, *storage
	est, err := (&cost.Meter{DB: db, RDB: rdb, NS: cfg.Kafka.TopicPrefix, Cfg: &pc}).Measure(ctx)
	if err != nil {
		return nil, err
	}
	rep.PerAccount, rep.Per1000 = est.PerAccountUSD, 1000*est.PerAccountUSD
	rep.CostNote = fmt.Sprintf("from %d accounts, %d IPs, %.2f proxy GB, %.1f signer hours and %.2f GB stored over the last 24 hours",
		est.Accounts, est.IPs, est.ProxyGBDay, est.BrowserHoursDay, est.StoredGB)
	rep.Projections = report.Project(est.PerAccountUSD, 1000, 10000, 100000, 300000)
	r.BudgetPerAccount = cfg.TT.Cost.BudgetPerAccountUSD
	rep.Verdicts = report.Check(r, rep.Traffic, rep.Store, est.PerAccountUSD)
	if st, err := halt.Get(ctx, rdb, cfg.Kafka.TopicPrefix); err == nil && st.Stopped {
		rep.Verdicts = append(rep.Verdicts, report.Verdict{Level: "WARN",
			Rule: fmt.Sprintf("tks is stopped since %s (%s): tksctl start resumes it", st.At.In(report.Iraq).Format("2006-01-02 15:04"), st.Reason)})
	}
	if *asJSON {
		printJSON(rep)
	} else {
		if err := rep.Write(os.Stdout); err != nil {
			return nil, err
		}
	}
	if *check && rep.Stop() {
		os.Exit(exitStop)
	}
	return nil, nil
}

// runStop is `tksctl stop --reason R`: no new task starts and no request
// goes to TikTok until `tksctl start`.
func runStop(ctx context.Context, cfg *config.Config, args []string) (any, error) {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	reason := fs.String("reason", "", "why (shown by tksctl report and in the logs)")
	_ = fs.Parse(args)
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdb.Close() }()
	return halt.Stop(ctx, rdb, cfg.Kafka.TopicPrefix, *reason, time.Now())
}

// runStart is `tksctl start`: clears the stop switch.
func runStart(ctx context.Context, cfg *config.Config) (any, error) {
	rdb, err := app.OpenRedis(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdb.Close() }()
	if err := halt.Start(ctx, rdb, cfg.Kafka.TopicPrefix); err != nil {
		return nil, err
	}
	return halt.Get(ctx, rdb, cfg.Kafka.TopicPrefix)
}
