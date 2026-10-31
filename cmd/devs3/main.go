// Command devs3 is an in-memory S3-compatible server for local development
// (Docker Compose). Production uses AWS S3 or another S3-compatible store.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	flag.Parse()
	srv := &http.Server{Addr: *addr, Handler: gofakes3.New(s3mem.New()).Server(), ReadHeaderTimeout: 10 * time.Second}
	slog.Info("devs3 listening (in-memory, data is lost on restart)", "addr", *addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("devs3", "err", err)
		os.Exit(1)
	}
}
