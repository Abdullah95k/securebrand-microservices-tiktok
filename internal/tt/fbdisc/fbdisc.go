// Package fbdisc sends the facebook.com links found in monitored accounts'
// bios to the Facebook collector, once each (owner D-065, 2026-11-13: "activate
// Facebook discovery"), so it can check the page and schedule it. It runs on
// the leader scheduler; off unless tt.facebook_discovery is enabled with an
// address and a key.
package fbdisc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/content"
)

// linkRE finds Facebook links in free text: facebook.com (any www., m.,
// web. or country subdomain), fb.com and fb.me, with or without a scheme.
var linkRE = regexp.MustCompile(`(?i)(?:https?://)?(?:[a-z0-9-]+\.)?(?:facebook\.com|fb\.com|fb\.me)/[^\s<>"'()\[\]{}]+`)

// Links returns the Facebook links in texts, as found (trailing punctuation
// trimmed), each once.
func Links(texts ...string) []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range texts {
		for _, m := range linkRE.FindAllString(t, -1) {
			m = strings.TrimRight(m, ".,;:!?")
			if m == "" || seen[m] {
				continue
			}
			seen[m] = true
			out = append(out, m)
		}
	}
	return out
}

// Sender finds new links and sends them.
type Sender struct {
	DB   *pgxpool.Pool
	Cfg  config.FacebookDiscovery
	HTTP *http.Client
	Now  func() time.Time
}

func (s *Sender) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Retry is how soon a send that got no answer, or a 503 (the collector's
// mentions are off), is tried again.
const Retry = 30 * time.Minute

// Tick records the links in monitored accounts' bios not seen before, then
// sends unsent ones up to the day's cap. A 503 stops the tick: the rest
// wait for the next.
func (s *Sender) Tick(ctx context.Context) error {
	if !s.Cfg.Enabled || s.Cfg.APIURL == "" || s.Cfg.APIKey == "" {
		return nil
	}
	if err := s.find(ctx); err != nil {
		return err
	}
	var sentToday int
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM fb_candidates WHERE sent_at >= date_trunc('day', $1::timestamptz)`,
		s.now()).Scan(&sentToday); err != nil {
		return err
	}
	room := s.Cfg.MaxPerDay - sentToday
	if room <= 0 {
		return nil
	}
	rows, err := s.DB.Query(ctx, `SELECT f.ref, coalesce(a.username, '') FROM fb_candidates f
		LEFT JOIN accounts a ON a.user_id = f.user_id
		WHERE f.sent_at IS NULL AND f.next_try_at <= $1 ORDER BY f.found_at LIMIT $2`, s.now(), room)
	if err != nil {
		return err
	}
	type cand struct{ ref, username string }
	todo, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (cand, error) {
		var c cand
		return c, r.Scan(&c.ref, &c.username)
	})
	if err != nil {
		return err
	}
	for _, c := range todo {
		result, retry := s.send(ctx, c.ref, c.username)
		metrics.FBCandidates.WithLabelValues(result).Inc()
		if retry {
			if _, err := s.DB.Exec(ctx, `UPDATE fb_candidates SET attempts = attempts + 1, result = $2,
				next_try_at = $3 WHERE ref = $1`, c.ref, result, s.now().Add(Retry)); err != nil {
				return err
			}
			if result == "unavailable" {
				// The collector's mentions are off: every unsent link waits.
				_, err := s.DB.Exec(ctx, `UPDATE fb_candidates SET next_try_at = greatest(next_try_at, $1)
					WHERE sent_at IS NULL`, s.now().Add(Retry))
				return err
			}
			continue
		}
		if _, err := s.DB.Exec(ctx, `UPDATE fb_candidates SET attempts = attempts + 1, result = $2, sent_at = $3
			WHERE ref = $1`, c.ref, result, s.now()); err != nil {
			return err
		}
	}
	return nil
}

// find records the Facebook links of enabled monitored accounts' bios and
// bio links that are not recorded yet. A link stays recorded for good, so
// it is sent once; a new link in a changed bio is new.
func (s *Sender) find(ctx context.Context) error {
	rows, err := s.DB.Query(ctx, `SELECT DISTINCT a.user_id, coalesce(a.bio, ''), coalesce(a.bio_link, '')
		FROM accounts a JOIN account_schedule sc ON sc.user_id = a.user_id
		WHERE sc.enabled AND (a.bio ~* '(facebook\.com|fb\.com|fb\.me)/' OR a.bio_link ~* '(facebook\.com|fb\.com|fb\.me)/')`)
	if err != nil {
		return err
	}
	type found struct {
		uid       int64
		bio, link string
	}
	fs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (found, error) {
		var f found
		return f, r.Scan(&f.uid, &f.bio, &f.link)
	})
	if err != nil {
		return err
	}
	for _, f := range fs {
		for _, ref := range Links(f.link, f.bio) {
			if _, err := s.DB.Exec(ctx, `INSERT INTO fb_candidates (ref, user_id, found_at, next_try_at) VALUES ($1, $2, $3, $3)
				ON CONFLICT (ref) DO NOTHING`, ref, f.uid, s.now()); err != nil {
				return err
			}
		}
	}
	return nil
}

// send posts one candidate. It returns the collector's result, or what
// went wrong, and whether to try again later. Every account sent is a
// monitored one, a publisher, so the profile goes as evidence.
func (s *Sender) send(ctx context.Context, ref, username string) (string, bool) {
	body := map[string]string{"ref": ref, "source": "tks"}
	if username != "" {
		if u := content.AccountURL(username); len(u) <= 2048 {
			body["evidence_url"] = u
		}
	}
	b, _ := json.Marshal(body)
	timeout := s.Cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, strings.TrimRight(s.Cfg.APIURL, "/")+"/v1/discovery/candidates", bytes.NewReader(b))
	if err != nil {
		return "error", true
	}
	req.Header.Set("Authorization", "Bearer "+s.Cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")
	cl := s.HTTP
	if cl == nil {
		cl = http.DefaultClient
	}
	resp, err := cl.Do(req)
	if err != nil {
		// Never the request itself in the log: it carries the key.
		slog.Warn("fbdisc: send failed", "err", fmt.Sprintf("%T", err))
		return "error", true
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK:
		var r struct {
			Result string `json:"result"`
		}
		if json.Unmarshal(raw, &r) == nil && validResult[r.Result] {
			return r.Result, false
		}
		return "accepted", false
	case resp.StatusCode == http.StatusServiceUnavailable:
		return "unavailable", true
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
		return "error", true
	default:
		// Refused for what it is (a bad link, a bad key): not again.
		slog.Warn("fbdisc: candidate refused", "status", resp.StatusCode)
		return "refused", false
	}
}

// validResult are the collector's answers, kept as the metric's labels.
var validResult = map[string]bool{"queued": true, "on_list": true, "added": true, "public_waiting": true,
	"checked_recently": true, "ignored": true}
