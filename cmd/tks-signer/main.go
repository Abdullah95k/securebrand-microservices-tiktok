// Command tks-signer is the browser signer in a service of its own (build
// brief D11, M9d; docs/runbooks/tks-deploy.md#the-signer-service): a few
// TikTok pages in headless Chromium, without its automation flag, whose
// own script signs the comment and reply calls of the crawlers and the
// canary (tt.signer.mode remote). Each page opens on the asking identity's
// exit, named in the request and found in this process's own proxy file;
// the caller sends the signed URL by plain HTTP. Its pod is capped (memory
// request = limit), and a page past tt.signer.kill_page_mb is closed even
// while it signs, so a runaway page takes down only this pod.
//
//	tks-signer [--config file]
//
// It serves POST /v1/sign and /v1/outcome on http.addr, and /metrics,
// /healthz and /readyz on http.ops_addr.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/connect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/httpserver"
	plog "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/cost"
)

func main() {
	fs := flag.NewFlagSet("tks-signer", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("tks-signer", cfg.Log.Level, cfg.Log.Format)
	if err := serve(context.Background(), cfg); err != nil {
		slog.Error("tks-signer exited", "err", err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, cfg *config.Config) error {
	// Chromium must be there before the pod takes calls: without it every
	// page fails to open (M9b2 found a deployed signer that never signed).
	if _, err := browser.FindBinary(cfg.TT.Browser.Binary); err != nil {
		return err
	}
	s, err := app.NewSigner(cfg, app.SignerOptions{Self: selfURL(cfg.HTTP.Addr)})
	if err != nil {
		return err
	}
	defer s.Close()
	rdb, err := connect.Redis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	slog.Info("tks-signer: pages", "max", cfg.TT.Signer.Pages, "kill_page_mb", cfg.TT.Signer.KillPageMB, "self", s.Server.Self)
	return run.Group(ctx, ops.Run,
		func(ctx context.Context) error {
			return httpserver.Serve(ctx, cfg.HTTP.Addr, s.Server.Handler(), httpserver.WithReadTimeout(cfg.HTTP.ReadTimeout))
		},
		func(ctx context.Context) error {
			// The pages close with the process.
			<-ctx.Done()
			s.Close()
			return nil
		},
		func(ctx context.Context) error {
			// The pages' browser time, priced by tks's cost (A5).
			return cost.RunBrowserTime(ctx, rdb, cfg.Kafka.TopicPrefix, s.Pool, time.Minute)
		})
}

// selfURL is this replica's own address from $POD_IP (the chart sets it
// from the pod's status), so the callers report TikTok's answers to the
// replica that signed; "" without it (one replica: the service's address
// serves).
func selfURL(addr string) string {
	ip := os.Getenv("POD_IP")
	if ip == "" {
		return ""
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		port = "8080"
	}
	return "http://" + net.JoinHostPort(ip, port)
}
