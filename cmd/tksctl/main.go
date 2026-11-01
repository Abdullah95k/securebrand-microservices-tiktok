// Command tksctl is the tks operations CLI. It grows with the service; M3
// gives it the contracts' lifecycle:
//
//	tksctl migrate up|down|status        migrate schema tt, seed the operation registry, create partitions
//	tksctl kafka apply                   create/grow tks's topics from configs/tt/topics.yaml
//	tksctl schemas apply|check           register the tt.v1 and common.v1 schemas (BACKWARD_TRANSITIVE) / dry run
//	tksctl fixtures add|scrub            scrub a real answer into a golden fixture (M4) / scrub old fixtures again
//
// The copied Facebook pipeline keeps fbctl until M9 removes it.
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
  fixtures scrub [--root testdata/tt]`)
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
