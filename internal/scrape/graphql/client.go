package graphql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
)

// RawSink receives raw responses for replay and drift debugging, with the
// request's template values (page_id, feedback_id, ...) so they can be
// parsed again with their context.
type RawSink interface {
	Capture(ctx context.Context, query string, cls detect.Class, body []byte, vars map[string]any)
}

// Request is one logical GraphQL call.
type Request struct {
	Query   string         // registry name
	Vars    map[string]any // template values
	PageID  int64          // fleet-wide per-page limit key; 0 = none
	Fast    bool           // p0 per-page rate
	Referer string
	Lane    string // p0 | p1 | p2, for the traffic budget
}

// Response is the decoded result.
type Response struct {
	Query  registry.Query
	Parts  []gjson.Result
	Body   []byte
	Status int
	// ContentType is the response's header (Facebook labels JSON text/html).
	ContentType string
	Class       detect.Class
	Reason      string
	Errors      []detect.GQLError
	Elapsed     time.Duration
	// Vars are the request's template values.
	Vars map[string]any
	// Wire bytes (compressed, as billed by proxies).
	BytesIn  int64
	BytesOut int64
	// SignRetried marks a signed call TikTok refused once and answered
	// signed again on another page (SignRetry).
	SignRetried bool
}

// Client sends GraphQL and JSON web-API requests on behalf of identities.
// The package keeps its name from fbs (so igs's later changes cherry-pick
// cleanly), but it is the persisted-query and JSON web-API client: a
// platform's RequestBuilder picks the method and query string, its Decoder
// reads the answer (docs/tiktok ADR 0006 §3).
type Client struct {
	Registry    registry.Registry
	Rules       detect.Rules
	Limiter     ratelimit.Limiter
	PageRPS     int
	PageRPSFast int
	GlobalRPS   int
	// Builder writes the platform's request (internal/fb/client.Builder
	// for fbs).
	Builder RequestBuilder
	// Decode reads a response body; nil reads GraphQL.
	Decode     Decoder
	Raw        RawSink
	CaptureAll bool
	// Ceiling, when set, replaces GlobalRPS with the fleet ceiling AIMD
	// publishes; Outcomes receives every response (class, lane, wire
	// bytes) for AIMD and the traffic budget.
	Ceiling  interface{ RPS() int }
	Outcomes interface {
		Observe(c detect.Class, lane string, wireBytes int64)
	}
	// Breakers short-circuit a (query, doc_id) whose responses keep being
	// rejected or failing to parse, so a Facebook change does not turn into
	// a wave of wasted requests. nil disables them.
	Breakers *breaker.Set
	// Signer, when set, gives each built request its final URL and headers
	// just before it is sent (TikTok's calls that need a signature: docs/
	// tiktok ADR 0006 §4). nil sends every request as built (fbs, igs).
	Signer Signer
}

// Signer signs a built request: it returns the URL and headers to send
// instead of rawURL and hdr, or the same when the query needs nothing. It
// runs after the identity's pacing, so a signature is as fresh as it can
// be when the request goes out.
type Signer interface {
	Sign(ctx context.Context, id *session.Identity, q registry.Query, rawURL string, hdr [][2]string) (string, [][2]string, error)
}

// SignReporter is a Signer that hears how each request it signed was
// answered: TikTok's signer replaces a page whose signatures keep being
// refused (C4, 2026-11-03: a page went bad within minutes and stayed bad).
type SignReporter interface {
	SignOutcome(ctx context.Context, sentURL string, cls detect.Class)
}

// SignRetry is what a signer notes about a call it signed that TikTok
// refused: the client then signs and sends the call once more, with the
// same SignRetry in its context, and the signer avoids what it noted (the
// page that signed it). In C4's third attempt (2026-11-04) every refused
// comment call had been signed on a page opened for another account, and
// those were answered 89 % of the time, the account's own page's 100 %.
type SignRetry struct {
	// Avoid is the signer's note; nil when it noted nothing, and the call
	// is not retried.
	Avoid any
}

type signRetryKey struct{}

// WithSignRetry returns ctx carrying r, as Client.Do sends a call.
func WithSignRetry(ctx context.Context, r *SignRetry) context.Context {
	return context.WithValue(ctx, signRetryKey{}, r)
}

// SignRetryFrom returns the call's SignRetry, nil outside Client.Do.
func SignRetryFrom(ctx context.Context) *SignRetry {
	r, _ := ctx.Value(signRetryKey{}).(*SignRetry)
	return r
}

// ErrSign wraps a signer's failure: no request was sent, and the identity
// and its exit are not to blame.
var ErrSign = errors.New("graphql: sign")

func breakerKey(q registry.Query) string { return q.Name + "|" + q.DocID }

// Parsed reports whether a response of resp's query parsed with every
// required field. Stages call it after parsing; it feeds the breakers, and
// a drifted response is captured (the transport classed it OK, so the
// capture of failures did not see it).
func (c *Client) Parsed(resp *Response, ok bool) {
	if resp == nil {
		return
	}
	if c.Breakers != nil && resp.Query.DocID != "" {
		c.Breakers.Observe(breakerKey(resp.Query), !ok)
	}
	// Also with CaptureAll: that copy is under raw/ as OK, and the drifted
	// ones belong under raw-failures/ where replay looks first.
	if !ok && c.Raw != nil && resp.Class == detect.OK {
		c.Raw.Capture(context.Background(), resp.Query.Name, detect.SchemaDrift, resp.Body, resp.Vars)
	}
}

// withHeaders applies a query's headers to the client's list: a name already
// in the list takes the query's value in place, so the browser's header
// order holds; a new name is appended.
func withHeaders(hdr [][2]string, extra []registry.Header) [][2]string {
	for _, h := range extra {
		found := false
		for i := range hdr {
			if hdr[i][0] == h.Name {
				hdr[i][1], found = h.Value, true
				break
			}
		}
		if !found {
			hdr = append(hdr, [2]string{h.Name, h.Value})
		}
	}
	return hdr
}

// ErrTransport wraps network/proxy failures.
var ErrTransport = errors.New("graphql: transport")

// Do runs req with identity id. Non-OK classes are reported in Response.Class
// (not as errors); only transport failures return an error. When the active
// doc_id is rejected, Do retries once with a fallback registry entry.
func (c *Client) Do(ctx context.Context, id *session.Identity, req Request) (*Response, error) {
	q, err := c.Registry.Get(req.Query)
	if err != nil {
		return nil, err
	}
	if c.Breakers != nil && !c.Breakers.Allow(breakerKey(q)) {
		fb, ok := c.Registry.Fallback(req.Query, q.DocID)
		if !ok || !c.Breakers.Allow(breakerKey(fb)) {
			metrics.QueryShortCircuit.WithLabelValues(q.Name).Inc()
			return &Response{Query: q, Class: detect.SchemaDrift,
				Reason: "breaker open for " + q.Name + " doc_id " + q.DocID + ": its responses keep failing; waiting for a registry or parser update"}, nil
		}
		q = fb
	}
	note := &SignRetry{}
	ctx = WithSignRetry(ctx, note)
	resp, err := c.do(ctx, id, q, req)
	if err != nil {
		return nil, err
	}
	if resp.Class == detect.RegistryStale && note.Avoid != nil {
		// A signed call refused: once more, signed on another page.
		metrics.SignRetries.Inc()
		retried, err := c.do(ctx, id, q, req)
		if err != nil {
			return nil, err
		}
		retried.SignRetried = true
		retried.BytesIn += resp.BytesIn
		retried.BytesOut += resp.BytesOut
		resp = retried
	}
	if resp.Class == detect.RegistryStale {
		if c.Breakers != nil {
			c.Breakers.Observe(breakerKey(q), true)
		}
		if fb, ok := c.Registry.Fallback(req.Query, q.DocID); ok {
			return c.do(ctx, id, fb, req)
		}
	}
	return resp, nil
}

func (c *Client) do(ctx context.Context, id *session.Identity, q registry.Query, req Request) (*Response, error) {
	vars, err := registry.Render(q.Variables, req.Vars)
	if err != nil {
		return nil, err
	}
	if err := c.pace(ctx, id, req); err != nil {
		return nil, err
	}
	referer := req.Referer
	if referer == "" {
		referer = id.Base.String()
	}
	b := c.Builder.Build(id, q, vars, c.Registry.ClientParams(), referer)
	path := b.Path
	if q.Endpoint != "" {
		path = q.Endpoint
	}
	endpoint, err := id.Base.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("graphql: %s endpoint: %w", q.Name, err)
	}
	if b.Query != "" {
		endpoint.RawQuery = b.Query
	}
	method := b.Method
	if method == "" {
		method = "POST"
	}
	body := b.Body
	hdr := withHeaders(b.Header, q.Headers)
	target := endpoint.String()
	if c.Signer != nil {
		if target, hdr, err = c.Signer.Sign(ctx, id, q, target, hdr); err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrSign, q.Name, err)
		}
	}
	start := time.Now()
	tr, err := id.Doer.Do(ctx, &transport.Request{Method: method, URL: target, Header: hdr, Body: body})
	elapsed := time.Since(start)
	tier := id.Endpoint.Tier
	metrics.FBRequestSecs.WithLabelValues(q.Name, tier).Observe(elapsed.Seconds())
	if err != nil {
		if ctx.Err() == nil { // a cancelled request says nothing about the exit
			session.Outcome(id, detect.Transport)
		}
		if c.Outcomes != nil {
			c.Outcomes.Observe(detect.Transport, req.Lane, int64(len(body))) // the request went out
		}
		metrics.FBRequests.WithLabelValues(q.Name, detect.Transport.String(), tier).Inc()
		return nil, fmt.Errorf("%w: %w", ErrTransport, err)
	}
	metrics.ProxyBytes.WithLabelValues(tier, "in").Add(float64(tr.BytesIn))
	metrics.ProxyBytes.WithLabelValues(tier, "out").Add(float64(tr.BytesOut))
	metrics.FBResponseBytes.WithLabelValues(q.Name).Observe(float64(len(tr.Body)))

	decode := c.Decode
	if decode == nil {
		decode = decodeGraphQL
	}
	parts, errs, hasData := decode(tr.Body)
	ct := tr.Header.Get("Content-Type")
	cls, reason := c.Rules.Classify(detect.Input{Status: tr.Status, Location: tr.Location,
		ContentType: ct, Body: tr.Body, Errors: errs, HasData: hasData, Expect: "graphql"})
	if cls == detect.OK && len(parts) == 0 {
		cls, reason = detect.SoftError, "empty body"
		if c.Rules.EmptyIsStale {
			cls, reason = detect.RegistryStale, "empty answer: request refused"
		}
	}
	session.Outcome(id, cls)
	if r, ok := c.Signer.(SignReporter); ok {
		r.SignOutcome(ctx, target, cls)
	}
	if c.Outcomes != nil {
		c.Outcomes.Observe(cls, req.Lane, tr.BytesIn+tr.BytesOut)
	}
	metrics.FBRequests.WithLabelValues(q.Name, cls.String(), tier).Inc()
	if c.Raw != nil && (c.CaptureAll || cls != detect.OK) {
		c.Raw.Capture(ctx, q.Name, cls, tr.Body, req.Vars)
	}
	return &Response{Query: q, Parts: parts, Body: tr.Body, Status: tr.Status, ContentType: ct, Class: cls,
		Reason: reason, Errors: errs, Elapsed: elapsed, Vars: req.Vars, BytesIn: tr.BytesIn, BytesOut: tr.BytesOut}, nil
}

// pace applies identity pacing, then the per-page and global fleet limits.
func (c *Client) pace(ctx context.Context, id *session.Identity, req Request) error {
	if err := id.Wait(ctx); err != nil {
		return err
	}
	if c.Limiter == nil {
		return nil
	}
	if req.PageID != 0 {
		rps := c.PageRPS
		if req.Fast && c.PageRPSFast > 0 {
			rps = c.PageRPSFast
		}
		if err := c.Limiter.Wait(ctx, "page:"+strconv.FormatInt(req.PageID, 10), rps); err != nil {
			return err
		}
	}
	global := c.GlobalRPS
	if c.Ceiling != nil {
		if v := c.Ceiling.RPS(); v > 0 {
			global = v
		}
	}
	return c.Limiter.Wait(ctx, "global", global)
}
