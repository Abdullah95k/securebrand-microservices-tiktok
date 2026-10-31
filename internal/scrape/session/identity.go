// Package session manages logged-out identities: a proxy exit, a browser
// fingerprint, cookies and the tokens bootstrapped from page HTML.
package session

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// Tokens are the values a page hands its web client for later requests,
// keyed by the form field each is sent as (Facebook: lsd, fb_dtsg,
// __spin_r, ...; js_datr is the datr cookie's value).
type Tokens map[string]string

// Identity is used by one task at a time (see Pool.Acquire).
type Identity struct {
	ID       string
	Endpoint proxy.Endpoint
	FP       Fingerprint
	Doer     transport.Doer
	Base     *url.URL

	mu           sync.Mutex
	tokens       Tokens
	bootstrapped time.Time
	health       float64
	consecBlocks int
	// consecFails counts transport failures in a row: an exit that
	// stopped answering.
	consecFails int
	requests    int
	maxRequests int
	born        time.Time
	cooldown    time.Time
	retired     string

	req       atomic.Uint64
	limiter   *rate.Limiter
	sessionID string // __s

	// observe reports outcomes to the exit's breaker (set by the pool).
	observe func(detect.Class)
}

func newIdentity(id string, ep proxy.Endpoint, fp Fingerprint, d transport.Doer, base *url.URL, rps float64, burst, maxReq int) *Identity {
	return &Identity{
		ID: id, Endpoint: ep, FP: fp, Doer: d, Base: base,
		health: 1, maxRequests: maxReq, born: time.Now(),
		limiter:   rate.NewLimiter(rate.Limit(rps), burst),
		sessionID: randAlnum(6) + ":" + randAlnum(6) + ":" + randAlnum(6),
	}
}

// Tokens returns a copy of the current token bundle.
func (id *Identity) Tokens() Tokens {
	id.mu.Lock()
	defer id.mu.Unlock()
	return maps.Clone(id.tokens)
}

// SetTokens installs tokens the platform's Extract hook accepted.
func (id *Identity) SetTokens(t Tokens) {
	id.mu.Lock()
	id.tokens = maps.Clone(t)
	id.bootstrapped = time.Now()
	id.mu.Unlock()
}

// Bootstrapped reports whether tokens are installed.
func (id *Identity) Bootstrapped() bool {
	id.mu.Lock()
	defer id.mu.Unlock()
	return len(id.tokens) > 0
}

// NextReq returns the next __req value (base36 counter).
func (id *Identity) NextReq() string { return strconv.FormatUint(id.req.Add(1), 36) }

// SessionParam is the per-identity __s value.
func (id *Identity) SessionParam() string { return id.sessionID }

// Wait paces this identity (1 request every few seconds plus jitter).
func (id *Identity) Wait(ctx context.Context) error {
	if err := id.limiter.Wait(ctx); err != nil {
		return err
	}
	// ±40% think-time jitter on top of the token bucket.
	j := jitter(time.Duration(float64(time.Second) / float64(id.limiter.Limit()) * 0.4))
	if j <= 0 {
		return nil
	}
	t := time.NewTimer(j)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// MaxTransportFailures is how many transport failures in a row (timeouts,
// refused or reset connections) retire an identity: its exit has stopped
// answering, and a fresh identity gets another.
const MaxTransportFailures = 3

// Record updates health with one request outcome and returns whether the
// identity must be retired.
func (id *Identity) Record(c detect.Class) (retire bool, reason string) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.requests++
	score := 1.0
	switch {
	case c.Blocking():
		score = 0
		id.consecBlocks++
	case c == detect.Transport:
		score = 0.5
	default:
		id.consecBlocks = 0
	}
	id.health = 0.9*id.health + 0.1*score
	if c == detect.Transport {
		id.consecFails++
	} else {
		id.consecFails = 0 // the exit answered
	}
	switch {
	case c == detect.Checkpoint:
		id.retired = "checkpoint"
	case c == detect.LoginWall:
		id.retired = "login_wall"
	case id.consecBlocks >= 2:
		id.retired = "blocked"
	case id.consecFails >= MaxTransportFailures:
		// Health alone never retires a dead exit: transport failures
		// score 0.5, which health approaches but never falls below.
		id.retired = "transport"
	case c == detect.RateLimited:
		backoff := time.Duration(30<<min(id.consecBlocks, 5)) * time.Second
		if backoff > 15*time.Minute {
			backoff = 15 * time.Minute
		}
		id.cooldown = time.Now().Add(backoff)
	case id.health < 0.5:
		id.retired = "unhealthy"
	case id.maxRequests > 0 && id.requests >= id.maxRequests:
		id.retired = "max_requests"
	case !id.Endpoint.ExpiresAt.IsZero() && time.Until(id.Endpoint.ExpiresAt) < time.Minute:
		id.retired = "sticky_expiry"
	}
	return id.retired != "", id.retired
}

// RetireReason is non-empty once the identity must not be reused.
func (id *Identity) RetireReason() string {
	id.mu.Lock()
	defer id.mu.Unlock()
	return id.retired
}

func (id *Identity) coolingDown() bool {
	id.mu.Lock()
	defer id.mu.Unlock()
	return time.Now().Before(id.cooldown)
}

// Snapshot serialises the identity so another pod can rebuild it on the
// same sticky exit (used if cursors turn out to be session-bound).
type Snapshot struct {
	ID        string            `json:"id"`
	Tier      string            `json:"tier"`
	ProxyURL  string            `json:"proxy_url"`
	SessionID string            `json:"session_id"`
	Expires   time.Time         `json:"expires"`
	Profile   string            `json:"profile"`
	Cookies   map[string]string `json:"cookies"`
	Tokens    Tokens            `json:"tokens"`
}

func (id *Identity) Snapshot() ([]byte, error) {
	s := Snapshot{
		ID: id.ID, Tier: id.Endpoint.Tier, ProxyURL: id.Endpoint.URL, SessionID: id.Endpoint.SessionID,
		Expires: id.Endpoint.ExpiresAt, Profile: id.FP.Profile, Cookies: id.Doer.Cookies(id.Base), Tokens: id.Tokens(),
	}
	return json.Marshal(s)
}

func randAlnum(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		v, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		b[i] = alphabet[v.Int64()]
	}
	return string(b)
}

func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(max)))
	return time.Duration(v.Int64())
}

func randRange(lo, hi int) int {
	if hi <= lo {
		return lo
	}
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(hi-lo)))
	return lo + int(v.Int64())
}

func newID() string { return fmt.Sprintf("id-%s", randAlnum(10)) }
