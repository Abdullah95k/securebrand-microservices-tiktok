// Command tksctl is the tks operations CLI. It grows with the service; M3
// gives it the contracts' lifecycle:
//
//	tksctl migrate up|down|status        migrate schema tt, seed the operation registry, create partitions
//	tksctl kafka apply                   create/grow tks's topics from configs/tt/topics.yaml
//	tksctl schemas apply|check           register the tt.v1 and common.v1 schemas (BACKWARD_TRANSITIVE) / dry run
//	tksctl fixtures add|scrub            scrub a real answer into a golden fixture (M4) / scrub old fixtures again
//	tksctl apikey create|revoke          make a tks-api client and print its key once / revoke a key (M6b)
//	tksctl accounts import               register a list of accounts for a client, with a report (M10a)
//	tksctl discovery keywords seed|import|list   discovery keywords (seed: the Iraq list; M10a)
//	tksctl dlq replay                    send dead letters back: tasks to run again, set-aside records as they were (M9c)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	plog "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/log"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: tksctl [--config file] <command> [args]
commands:
  migrate up|down|status
  kafka apply [--partitions-cap N] [--replicas N]
  schemas apply | schemas check
  fixtures add --from <har|answer> --name <case> [--kind K] [--entry N] [--items N]
  fixtures scrub [--root testdata/tt]
  apikey create --name NAME [--scopes s1,s2] [--rate N]
  apikey revoke --prefix PREFIX
  accounts import FILE --client NAME [--label L]... [--dry-run] [--disable-missing [--confirm] [--force]]
  accounts import --report BATCH --client NAME [--problems]
  discovery keywords seed | discovery keywords import --file F | discovery keywords list [--language L] [--group G]
  dlq replay [--topic tks.task.dlq|tks.persist.dlq] [--reason R] [--max N] [--idle D]
  report [--from "YYYY-MM-DD HH:MM"] [--to ...] [--hours 24] [--prometheus URL] [--check] [--json]   (Iraq time)
  stop --reason R | start`)
	os.Exit(2)
}

func main() {
	global := flag.NewFlagSet("tksctl", flag.ExitOnError)
	cfgPath := global.String("config", os.Getenv("TKS_CONFIG"), "config file")
	_ = global.Parse(os.Args[1:])
	args := global.Args()
	if len(args) < 1 {
		usage()
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fail(err)
	}
	plog.Setup("tksctl", cfg.Log.Level, "text")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cmd, rest := args[0], args[1:]
	sub := ""
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		sub, rest = rest[0], rest[1:]
	}
	var out any
	switch cmd + " " + sub {
	case "migrate up", "migrate down", "migrate status":
		out, err = migrate(ctx, cfg, sub)
	case "kafka apply":
		out, err = kafkaApply(ctx, cfg, rest)
	case "schemas apply", "schemas check":
		out, err = schemasApply(ctx, cfg, sub == "check")
	case "fixtures add":
		out, err = fixturesAdd(rest)
	case "fixtures scrub":
		out, err = fixturesScrub(rest)
	case "apikey create":
		out, err = apikeyCreate(ctx, cfg, rest)
	case "apikey revoke":
		out, err = apikeyRevoke(ctx, cfg, rest)
	case "accounts import":
		out, err = accountsImport(ctx, cfg, rest)
	case "discovery keywords":
		if len(rest) == 0 || strings.HasPrefix(rest[0], "-") {
			usage()
		}
		out, err = discoveryKeywords(ctx, cfg, rest[0], rest[1:])
	case "dlq replay":
		out, err = dlqReplay(ctx, cfg, rest)
	case "report ":
		out, err = runReport(ctx, cfg, rest)
	case "stop ":
		out, err = runStop(ctx, cfg, rest)
	case "start ":
		out, err = runStart(ctx, cfg)
	default:
		usage()
	}
	if out != nil {
		printJSON(out)
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tksctl:", err)
	os.Exit(1)
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
