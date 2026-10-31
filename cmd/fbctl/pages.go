package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/spike"
)

// pagesImport registers the pages of a file through the API: one page per
// line (URL, vanity name or id), or CSV with a "page" column. A page
// registered before keeps its schedule and is not backfilled again, so an
// interrupted import is simply run again.
func pagesImport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pages import", flag.ExitOnError)
	apiURL := fs.String("api", os.Getenv("IGS_API_URL"), "fbs API base URL (default $IGS_API_URL)")
	key := fs.String("key", os.Getenv("IGS_API_KEY"), "API key with pages:write (default $IGS_API_KEY)")
	interval := fs.Int("interval-hours", 0, "fixed re-scrape interval in hours (0 = adaptive tiers)")
	rps := fs.Float64("rps", 5, "registrations per second")
	backfill := fs.Bool("backfill", true, "backfill pages not registered before")
	_ = fs.Parse(args)
	if fs.NArg() != 1 || *apiURL == "" || *key == "" || *rps <= 0 || *rps > 1000 {
		return errors.New("usage: fbctl pages import --api URL --key KEY [--interval-hours N] [--rps 5 (at most 1000)] [--backfill=false] FILE")
	}
	pages, err := spike.ReadPagesFile(fs.Arg(0))
	if err != nil {
		return err
	}
	im := importer{url: strings.TrimRight(*apiURL, "/") + "/v1/pages", key: *key, http: &http.Client{Timeout: 30 * time.Second}}
	tick := time.NewTicker(time.Duration(float64(time.Second) / *rps))
	defer tick.Stop()
	var res importResult
	for i, p := range pages {
		select {
		case <-ctx.Done():
			printJSON(res) // what was done, and which pages failed, before the interruption
			return fmt.Errorf("interrupted after %d of %d pages: %w", i, len(pages), ctx.Err())
		case <-tick.C:
		}
		body := map[string]any{"page": p.Ref}
		if *interval > 0 {
			body["schedule"] = map[string]any{"interval_hours": *interval}
		}
		if !*backfill {
			body["backfill"] = false
		}
		created, err := im.register(ctx, body)
		switch {
		case err != nil:
			res.Failed = append(res.Failed, importFailure{Page: p.Ref, Error: err.Error()})
		case created:
			res.Created++
		default:
			res.Existing++
		}
		if (i+1)%100 == 0 {
			slog.Info("pages import", "done", i+1, "of", len(pages), "created", res.Created, "existing", res.Existing, "failed", len(res.Failed))
		}
	}
	printJSON(res)
	if len(res.Failed) > 0 {
		return fmt.Errorf("%d of %d pages failed", len(res.Failed), len(pages))
	}
	return nil
}

type importResult struct {
	Created  int             `json:"created"`
	Existing int             `json:"existing"`
	Failed   []importFailure `json:"failed,omitempty"`
}

type importFailure struct {
	Page  string `json:"page"`
	Error string `json:"error"`
}

type importer struct {
	url, key string
	http     *http.Client
	sleep    func(time.Duration) // tests
}

// register posts one page; rate limits, server errors and network errors
// are retried with backoff (Retry-After when given), other refusals are not.
func (im importer) register(ctx context.Context, body map[string]any) (created bool, err error) {
	b, _ := json.Marshal(body)
	backoff := time.Second
	for attempt := 1; ; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, im.url, bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+im.key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := im.http.Do(req)
		var status int
		var msg []byte
		wait := backoff
		if err == nil {
			status = resp.StatusCode
			msg, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<16))
			_ = resp.Body.Close()
			if s, perr := strconv.Atoi(resp.Header.Get("Retry-After")); perr == nil && s > 0 {
				wait = time.Duration(s) * time.Second
			}
		}
		switch {
		case err == nil && status == http.StatusCreated:
			var r struct {
				Created bool `json:"created"`
			}
			_ = json.Unmarshal(msg, &r)
			return r.Created, nil
		case err == nil && status != http.StatusTooManyRequests && status < 500:
			return false, fmt.Errorf("%d: %s", status, bytes.TrimSpace(msg))
		case attempt >= 5:
			if err != nil {
				return false, err
			}
			return false, fmt.Errorf("%d after %d attempts: %s", status, attempt, bytes.TrimSpace(msg))
		}
		if im.sleep != nil {
			im.sleep(wait)
		} else {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(wait):
			}
		}
		backoff *= 2
	}
}
