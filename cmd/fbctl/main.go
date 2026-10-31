// Command fbctl is the fbs operations CLI.
//
//	fbctl migrate up|down|status        apply schema migrations, registry seed and partitions
//	fbctl kafka apply                    create/grow topics from configs/topics.yaml
//	fbctl schemas apply|check            register the fb.v1 schemas (BACKWARD_TRANSITIVE) / dry run
//	fbctl apikey create --name N --scopes jobs:write,data:read
//	fbctl apikey revoke --prefix P
//	fbctl registry list
//	fbctl registry set --file query.json
//	fbctl registry import --file phase0/out/registry.yaml   make every query of a registry file active
//	fbctl report load [--since 24h] [--pages N] [--cost-per-gb P]   requests, bytes and blocks per page per day
//	fbctl dlq replay [--reason R] [--max N]
//	fbctl replay [--query Q] [--since 24h] [--fixtures-dir DIR]   re-parse captured raw responses; exit 3 while some drift
//	fbctl fixtures scrub --dir testdata/fb/<query>/<case>
//	fbctl media ack --consumer analysis [--status processed]   (dev: simulate analysis-service acks)
//	fbctl pages import --api URL --key KEY FILE   register many pages through the API (safe to re-run)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: fbctl [--config file] <command> [args]
commands:
  migrate up|down|status
  kafka apply [--partitions-cap N] [--replicas N]
  schemas apply | schemas check
  apikey create --name NAME --scopes s1,s2 [--rate N]
  apikey revoke --prefix PREFIX
  registry list | registry set --file query.json | registry import --file registry.yaml
  report load [--since 24h] [--pages N] [--cost-per-gb P] [--json]
  dlq replay [--reason R] [--max N]
  replay [--prefix raw-failures] [--query Q,..] [--since 168h] [--fixtures-dir DIR --name N --samples 3]
  fixtures scrub --dir DIR
  media ack --consumer NAME [--status processed|failed_permanent|failed_retryable] [--max N]
  pages import --api URL --key KEY [--interval-hours N] [--rps 5] [--backfill=false] FILE`)
	os.Exit(2)
}

func main() {
	global := flag.NewFlagSet("fbctl", flag.ExitOnError)
	cfgPath := global.String("config", os.Getenv("IGS_CONFIG"), "config file")
	_ = global.Parse(os.Args[1:])
	args := global.Args()
	if len(args) < 1 {
		usage()
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fail(err)
	}
	plog.Setup("fbctl", cfg.Log.Level, "text")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cmd, rest := args[0], args[1:]
	sub := ""
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		sub, rest = rest[0], rest[1:]
	}
	switch cmd + " " + sub {
	case "migrate up", "migrate down", "migrate status":
		err = migrate(ctx, cfg, sub)
	case "kafka apply":
		err = kafkaApply(ctx, cfg, rest)
	case "schemas apply", "schemas check":
		err = schemasApply(ctx, cfg, sub == "check")
	case "apikey create":
		err = apikeyCreate(ctx, cfg, rest)
	case "apikey revoke":
		err = apikeyRevoke(ctx, cfg, rest)
	case "registry list":
		err = registryList(ctx, cfg)
	case "registry set":
		err = registrySet(ctx, cfg, rest)
	case "registry import":
		err = registryImport(ctx, cfg, rest)
	case "report load":
		err = reportLoad(ctx, cfg, rest)
	case "replay ":
		err = replayRaw(ctx, cfg, rest)
	case "dlq replay":
		err = dlqReplay(ctx, cfg, rest)
	case "fixtures scrub":
		err = fixturesScrub(rest)
	case "media ack":
		err = mediaAck(ctx, cfg, rest)
	case "pages import":
		// Long imports outlive the 30-minute command timeout; Ctrl-C stops them.
		ictx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		err = pagesImport(ictx, rest)
		stop()
	default:
		usage()
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "fbctl:", err)
	os.Exit(1)
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
