package sign

// tks-signer (M9d): the signer's pages in their own capped service. The
// crawlers and the canary send it the URL to sign and the identity's
// browser fingerprint and exit by name; it signs in a page opened on that
// exit (its own copy of the proxy file holds the credentials, which never
// cross the network) and answers the signed URL, which the caller sends by
// plain HTTP as before. TikTok's answer goes back to the replica that
// signed, so a refused call is signed once more on another page
// (graphql.SignRetry) and pages going bad are replaced (max_refusals), as
// in one process.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
)

// The service's paths.
const (
	SignPath    = "/v1/sign"
	OutcomePath = "/v1/outcome"
)

// SignRequest is one call to sign. The identity travels by its browser
// fingerprint (the signed URL carries the browser's version, so the page
// must present the identity's) and its exit by name.
type SignRequest struct {
	Query   string `json:"query"`
	URL     string `json:"url"`
	Referer string `json:"referer,omitempty"`

	UserAgent       string `json:"user_agent"`
	SecChUa         string `json:"sec_ch_ua,omitempty"`
	SecChUaMobile   string `json:"sec_ch_ua_mobile,omitempty"`
	SecChUaPlatform string `json:"sec_ch_ua_platform,omitempty"`

	// Tier and Exit (a static exit) or Session (a gateway's sticky
	// session) name the identity's exit in the proxy file.
	Tier    string `json:"tier"`
	Exit    string `json:"exit,omitempty"`
	Session string `json:"session,omitempty"`

	// Avoid is the page a retry avoids (graphql.SignRetry).
	Avoid PageID `json:"avoid,omitempty"`
}

// SignResponse is the signed URL, or why there is none. By is the address
// of the replica that signed, for the outcome ("" when it has none: the
// caller then reports to the service's address).
type SignResponse struct {
	URL   string `json:"url,omitempty"`
	By    string `json:"by,omitempty"`
	Error string `json:"error,omitempty"`
}

// OutcomeRequest is TikTok's answer to a signed call: detect.Class's name.
type OutcomeRequest struct {
	URL   string `json:"url"`
	Class string `json:"class"`
}

// OutcomeResponse names the page the call's retry must avoid, if any.
type OutcomeResponse struct {
	Avoid PageID `json:"avoid,omitempty"`
}

// Server is tks-signer's HTTP handler: it signs with Signer (a Pool) on
// exits rebuilt from Proxies, on the site Base.
type Server struct {
	Signer  ttclient.URLSigner
	Proxies *proxy.Pool
	// Base is tt.base_url: the site a new page opens on when the call has
	// no referer there.
	Base *url.URL
	// Self is this replica's own address, answered as SignResponse.By
	// ("" when the service has one replica, or none is known).
	Self string
}

// Handler serves SignPath and OutcomePath.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+SignPath, s.sign)
	mux.HandleFunc("POST "+OutcomePath, s.outcome)
	return mux
}

// maxBody bounds a request: a URL, a referer and a fingerprint.
const maxBody = 64 << 10

func (s *Server) sign(w http.ResponseWriter, r *http.Request) {
	var req SignRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil || req.URL == "" || req.Query == "" {
		answer(w, http.StatusBadRequest, SignResponse{Error: "want a JSON body with query and url"})
		return
	}
	ep, ok := s.Proxies.Endpoint(req.Tier, req.Exit, req.Session)
	if !ok {
		// The callers' proxy file and this one differ: the page would open
		// on an exit the identity does not use.
		answer(w, http.StatusUnprocessableEntity, SignResponse{Error: fmt.Sprintf("exit %q/%q of tier %q is not in tks-signer's proxy file", req.Exit, req.Session, req.Tier)})
		return
	}
	id := &session.Identity{Endpoint: ep, Base: s.Base, FP: session.Fingerprint{UserAgent: req.UserAgent, SecChUa: req.SecChUa,
		SecChUaMobile: req.SecChUaMobile, SecChUaPlatform: req.SecChUaPlatform}}
	ctx := r.Context()
	if req.Avoid != "" {
		ctx = graphql.WithSignRetry(ctx, &graphql.SignRetry{Avoid: req.Avoid})
	}
	signed, err := s.Signer.SignURL(ctx, id, req.Query, req.URL, req.Referer)
	switch {
	case err == nil:
		answer(w, http.StatusOK, SignResponse{URL: signed, By: s.Self})
	case errors.Is(err, ErrBusy):
		answer(w, http.StatusServiceUnavailable, SignResponse{Error: err.Error()})
	case errors.Is(err, ErrClosed) || r.Context().Err() != nil:
		answer(w, http.StatusServiceUnavailable, SignResponse{Error: "tks-signer is stopping"})
	default:
		answer(w, http.StatusBadGateway, SignResponse{Error: err.Error()})
	}
}

func (s *Server) outcome(w http.ResponseWriter, r *http.Request) {
	var req OutcomeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil || req.URL == "" {
		answer(w, http.StatusBadRequest, OutcomeResponse{})
		return
	}
	cls, err := detect.ParseClass(req.Class)
	if err != nil {
		answer(w, http.StatusBadRequest, OutcomeResponse{})
		return
	}
	note := &graphql.SignRetry{}
	if rep, ok := s.Signer.(graphql.SignReporter); ok {
		rep.SignOutcome(graphql.WithSignRetry(r.Context(), note), req.URL, cls)
	}
	answer(w, http.StatusOK, OutcomeResponse{Avoid: Noted(note)})
}

func answer(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Remote is the crawlers' and the canary's signer in mode remote: a
// client.URLSigner and graphql.SignReporter that has tks-signer sign.
type Remote struct {
	// URL is tks-signer's address (tt.signer.remote_url).
	URL string
	// HTTP sends the requests; its timeout must cover tks-signer's wait
	// for a page, opening one and signing (NewRemote sets it).
	HTTP *http.Client

	mu sync.Mutex
	// by remembers the replica that signed each recent call (by path and
	// query), until its outcome is reported.
	by map[string]string
}

// NewRemote returns a Remote for tks-signer at rawURL whose requests wait
// at most what tks-signer itself may take: a page's sign timeout to start,
// a page's opening and a signature (tt.signer's sign_timeout and
// page_timeout), and a little more.
func NewRemote(rawURL string, signTimeout, pageTimeout time.Duration) *Remote {
	return &Remote{URL: strings.TrimRight(rawURL, "/"), HTTP: &http.Client{Timeout: 2*signTimeout + pageTimeout + 10*time.Second}}
}

// Why a call to tks-signer failed (tks_signer_remote_calls_total).
const (
	RemoteOK          = "ok"
	RemoteBusy        = "busy"        // every page busy for the sign timeout
	RemoteError       = "error"       // a page did not open or sign
	RemoteUnreachable = "unreachable" // no answer: tks-signer down or not reachable
	RemoteRefused     = "refused"     // tks-signer refused the request (a bad exit, a bad request)
	RemoteCancelled   = "cancelled"   // the caller gave up first (its context ended)
)

// SignURL implements client.URLSigner.
func (c *Remote) SignURL(ctx context.Context, id *session.Identity, query, rawURL, referer string) (string, error) {
	req := SignRequest{Query: query, URL: rawURL, Referer: referer,
		UserAgent: id.FP.UserAgent, SecChUa: id.FP.SecChUa, SecChUaMobile: id.FP.SecChUaMobile, SecChUaPlatform: id.FP.SecChUaPlatform,
		Tier: id.Endpoint.Tier, Exit: id.Endpoint.Exit, Session: id.Endpoint.SessionID,
		Avoid: Noted(graphql.SignRetryFrom(ctx))}
	if req.Exit != "" {
		req.Session = "" // a static exit is named by its exit alone
	}
	var resp SignResponse
	status, err := c.post(ctx, c.URL+SignPath, req, &resp)
	result := RemoteOK
	switch {
	case err != nil && ctx.Err() != nil:
		result = RemoteCancelled
		err = ctx.Err()
	case err != nil:
		result = RemoteUnreachable
		err = fmt.Errorf("sign: tks-signer: %w", err)
	case status == http.StatusOK && resp.URL != "":
	case status == http.StatusServiceUnavailable:
		result, err = RemoteBusy, fmt.Errorf("%w (tks-signer: %s)", ErrBusy, resp.Error)
	case status == http.StatusBadGateway:
		result, err = RemoteError, fmt.Errorf("sign: tks-signer: %s", resp.Error)
	default:
		result, err = RemoteRefused, fmt.Errorf("sign: tks-signer answered %d: %s", status, resp.Error)
	}
	metrics.SignerRemoteCalls.WithLabelValues("sign", result).Inc()
	if err != nil {
		return "", err
	}
	if by := replicaURL(resp.By); by != "" {
		c.mu.Lock()
		if c.by == nil || len(c.by) >= maxSignedBy {
			c.by = map[string]string{} // outcomes never reported: forget them
		}
		c.by[callKey(resp.URL)] = by
		c.mu.Unlock()
	}
	return resp.URL, nil
}

// SignOutcome implements graphql.SignReporter: the answer goes to the
// replica that signed, and the page it names is the one the call's retry
// avoids.
func (c *Remote) SignOutcome(ctx context.Context, sentURL string, cls detect.Class) {
	key := callKey(sentURL)
	c.mu.Lock()
	by := c.by[key]
	delete(c.by, key)
	c.mu.Unlock()
	if by == "" {
		by = c.URL
	}
	var resp OutcomeResponse
	status, err := c.post(ctx, by+OutcomePath, OutcomeRequest{URL: sentURL, Class: cls.String()}, &resp)
	result := RemoteOK
	switch {
	case err != nil && ctx.Err() != nil:
		result = RemoteCancelled
	case err != nil:
		result = RemoteUnreachable
	case status != http.StatusOK:
		result = RemoteRefused
	}
	metrics.SignerRemoteCalls.WithLabelValues("outcome", result).Inc()
	if result != RemoteOK {
		// The page is not told: at worst a bad page signs a little longer.
		slog.Debug("sign: outcome not reported to tks-signer", "status", status, "err", err)
		return
	}
	if r := graphql.SignRetryFrom(ctx); r != nil && r.Avoid == nil && resp.Avoid != "" {
		r.Avoid = resp.Avoid
	}
}

// replicaURL is a replica's address as tks-signer answers it (SignResponse.By):
// http:// and a pod's IP and port, nothing else, so an answer can only send
// the outcome to another tks-signer pod; "" otherwise.
func replicaURL(by string) string {
	u, err := url.Parse(by)
	if err != nil || u.Scheme != "http" || u.Port() == "" || net.ParseIP(u.Hostname()) == nil || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return ""
	}
	return "http://" + u.Host
}

// post sends v as JSON and decodes the JSON answer into out.
func (c *Remote) post(ctx context.Context, u string, v, out any) (int, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	// u is tt.signer.remote_url, or a replica's pod address (replicaURL).
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(b)) //nolint:gosec // see above
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req) //nolint:gosec // tks-signer's own addresses only (above)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	// An answer that is not ours (a proxy's error page) decodes to nothing.
	_ = json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(out)
	return resp.StatusCode, nil
}
