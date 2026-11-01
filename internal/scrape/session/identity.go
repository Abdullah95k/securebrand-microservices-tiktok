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

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
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
	// consecSoft counts soft errors in a row (transport failures, which
	// say nothing about the tokens, neither count nor reset it).
	consecSoft  int
	requests    int
	maxRequests int
	born        time.Time
	cooldown    time.Time
	retired     string

	req       atomic.Uint64
	limiter   *rate.Limiter
	sessionID string // __s

	// observe reports outcomes to the exit's breaker or, for page loads, its
	// page-load gate (set by the pool).
	observe func(c detect.Class, pageLoad bool)
	// reserved is the page load the pool reserved on the exit's gate when it
	// created the identity; the first page load uses it.
	reserved bool
}

// reservationTTL is how long the page load reserved for a new identity
// stays good: the slot was its exit's at creation, and another identity
// may take the exit's next slot once the interval passes.
const reservationTTL = 30 * time.Second

// TakeReservation reports whether id still holds the page load reserved
// when it was created (and is young enough to use it), and uses it up.
// Any other page load takes a slot from its exit's gate.
func (id *Identity) TakeReservation() bool {
	id.mu.Lock()
	defer id.mu.Unlock()
	r := id.reserved && time.Since(id.born) < reservationTTL
	id.reserved = false
	return r
}

// markRetired marks id for retirement when it is released.
func (id *Identity) markRetired(reason string) {
	id.mu.Lock()
	if id.retired == "" {
		id.retired = reason
	}
	id.mu.Unlock()
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

// RateLimitStreak is how many rate limits in a row (no other answer
// between them) retire an identity. Each one before it sends the identity
// to a longer cooldown (RateLimitCooldown): 1, 2 and 4 minutes.
const RateLimitStreak = 4

// RateLimitCooldown is how long an identity rests after its n-th rate
// limit in a row (n >= 1): a minute, doubling with each one, at most 15
// minutes.
func RateLimitCooldown(n int) time.Duration {
	return min(time.Duration(30<<min(max(n, 1), 5))*time.Second, 15*time.Minute)
}

// Soft errors in a row (server errors, HTML instead of GraphQL, an error
// payload without data): the second drops the identity's tokens, so its next
// use loads a page for fresh ones (stale tokens fail every request alike),
// and MaxSoftErrors retire it, fresh tokens or not: only a request that
// works ends the run. A soft error scores as a success otherwise, so health
// alone would let one identity fail hundreds of requests, and the pool hands
// the identity released last out first.
const (
	softRefreshAfter = 2
	MaxSoftErrors    = 4
)

// Record updates health with one request outcome and returns whether the
// identity must be retired.
func (id *Identity) Record(c detect.Class) (retire bool, reason string) {
	return id.record(c, false)
}

func (id *Identity) record(c detect.Class, pageLoad bool) (retire bool, reason string) {
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
	case c == detect.SoftError:
		id.consecSoft++
		if id.consecSoft == softRefreshAfter { // once per run of soft errors
			id.tokens = nil
		}
	case c == detect.Transport, pageLoad && c == detect.OK:
		// Neither says the requests work: a transport failure got no
		// answer, and a page load only brings tokens yet untested.
	default:
		id.consecSoft = 0
	}
	switch {
	case c == detect.Checkpoint:
		id.retired = "checkpoint"
	case c == detect.LoginWall:
		id.retired = "login_wall"
	case c == detect.RateLimited && id.consecBlocks >= RateLimitStreak:
		id.retired = "blocked"
	case c == detect.RateLimited:
		// Checkpoints and login walls retire at once, so every block in a
		// row here is a rate limit: each one doubles the cooldown.
		id.cooldown = time.Now().Add(RateLimitCooldown(id.consecBlocks))
	case id.consecFails >= MaxTransportFailures:
		// Health alone never retires a dead exit: transport failures
		// score 0.5, which health approaches but never falls below.
		id.retired = "transport"
	case id.consecSoft >= MaxSoftErrors:
		id.retired = "soft_errors"
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
