package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// Scopes.
const (
	ScopeJobsWrite  = "jobs:write"
	ScopePagesWrite = "pages:write"
	ScopeDataRead   = "data:read"
	ScopePIIRead    = "pii:read"
	ScopeAdmin      = "admin"
)

// Client is an authenticated API client.
type Client struct {
	ID            uuid.UUID
	Name          string
	Scopes        []string
	RatePerMinute int
	hash          []byte
	fetched       time.Time
}

// Has reports whether the client holds scope (admin holds all).
func (c *Client) Has(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope || s == ScopeAdmin {
			return true
		}
	}
	return false
}

// GenerateKey returns a new API key "fbs_<prefix>_<secret>", its prefix and
// the hash to store.
func GenerateKey() (key, prefix string, hash []byte) {
	b := make([]byte, 30)
	_, _ = rand.Read(b)
	s := base64.RawURLEncoding.EncodeToString(b)
	prefix, secret := strings.ToLower(strings.NewReplacer("-", "a", "_", "b").Replace(s[:8])), s[8:]
	h := sha256.Sum256([]byte(secret))
	return "igs_" + prefix + "_" + secret, prefix, h[:]
}

// CreateClient stores a new API client and returns its key.
func CreateClient(ctx context.Context, db *pgxpool.Pool, name string, scopes []string, ratePerMinute int) (string, uuid.UUID, error) {
	key, prefix, hash := GenerateKey()
	id, _ := uuid.NewV7()
	var rate *int
	if ratePerMinute > 0 {
		rate = &ratePerMinute
	}
	_, err := db.Exec(ctx, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes, rate_per_minute)
		VALUES ($1,$2,$3,$4,$5,$6)`, id, name, prefix, hash, scopes, rate)
	return key, id, err
}

type authenticator struct {
	db          *pgxpool.Pool
	limiter     *redis_rate.Limiter
	defaultRate int
	mu          sync.Mutex
	cache       map[string]*Client
}

var errUnauthorized = errors.New("missing or invalid API key")

func (a *authenticator) lookup(ctx context.Context, key string) (*Client, error) {
	parts := strings.SplitN(strings.TrimPrefix(key, "igs_"), "_", 2)
	if !strings.HasPrefix(key, "igs_") || len(parts) != 2 {
		return nil, errUnauthorized
	}
	prefix, secret := parts[0], parts[1]
	a.mu.Lock()
	c := a.cache[prefix]
	a.mu.Unlock()
	if c == nil || time.Since(c.fetched) > time.Minute {
		c = &Client{}
		var rate *int
		err := a.db.QueryRow(ctx, `SELECT id, name, scopes, rate_per_minute, key_hash FROM api_clients
			WHERE key_prefix = $1 AND revoked_at IS NULL`, prefix).Scan(&c.ID, &c.Name, &c.Scopes, &rate, &c.hash)
		if err != nil {
			return nil, errUnauthorized
		}
		c.RatePerMinute = a.defaultRate
		if rate != nil {
			c.RatePerMinute = *rate
		}
		c.fetched = time.Now()
		a.mu.Lock()
		a.cache[prefix] = c
		a.mu.Unlock()
	}
	h := sha256.Sum256([]byte(secret))
	if subtle.ConstantTimeCompare(h[:], c.hash) != 1 {
		return nil, errUnauthorized
	}
	return c, nil
}

type clientKey struct{}

func clientFrom(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey{}).(*Client)
	return c
}

func (a *authenticator) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		c, err := a.lookup(r.Context(), key)
		if err != nil {
			problem(w, http.StatusUnauthorized, "unauthorized", err.Error())
			return
		}
		if a.limiter != nil && c.RatePerMinute > 0 {
			res, err := a.limiter.Allow(r.Context(), redisx.KeyPrefix+"rl:api:"+c.ID.String(), redis_rate.PerMinute(c.RatePerMinute))
			if err == nil && res.Allowed == 0 {
				w.Header().Set("Retry-After", strconvItoa(int(res.RetryAfter.Seconds())+1))
				problem(w, http.StatusTooManyRequests, "rate limited", "API rate limit exceeded")
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientKey{}, c)))
	})
}

func require(scope string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if c := clientFrom(r.Context()); c == nil || !c.Has(scope) {
			problem(w, http.StatusForbidden, "forbidden", "this API key lacks the "+scope+" scope")
			return
		}
		h(w, r)
	}
}
