// Package registry resolves logical query names (timeline, comments, ...) to
// the platform's current doc_id, friendly name, variables template and, where
// a platform has more than one GraphQL endpoint, the endpoint and extra
// headers. doc_ids rotate, so the registry is data, reloaded at runtime.
package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/net/http/httpguts"
	"gopkg.in/yaml.v3"
)

// Status values of a registry entry.
const (
	StatusCandidate = "candidate"
	StatusActive    = "active"
	StatusFallback  = "fallback"
	StatusRetired   = "retired"
)

// Query is one registry entry.
type Query struct {
	Name          string          `json:"name"`
	DocID         string          `json:"doc_id"`
	FriendlyName  string          `json:"friendly_name"`
	Variables     json.RawMessage `json:"variables"`
	Parser        string          `json:"parser"`
	ParserVersion int             `json:"parser_version"`
	Status        string          `json:"status"`
	Priority      int             `json:"priority"`
	Source        string          `json:"source"`
	// Endpoint is the request path on the identity's host, for platforms
	// with more than one GraphQL endpoint (Instagram: /api/graphql and
	// /graphql/query). Empty means the client's default endpoint.
	Endpoint string `json:"endpoint,omitempty"`
	// Headers are sent with this query only, in this order: a name the
	// client already sends takes this value in the client's position, a new
	// name is added. Names are lower case. Empty means none.
	Headers []Header `json:"headers,omitempty"`
	// Check state recorded by fbs-registry-sync (read only; List fills it).
	LastCheck     string     `json:"last_check,omitempty"`
	LastCheckedAt *time.Time `json:"last_checked_at,omitempty"`
	LastOKAt      *time.Time `json:"last_ok_at,omitempty"`
}

// Header is one request header of a query.
type Header struct {
	Name  string `json:"name" yaml:"name"`
	Value string `json:"value" yaml:"value"`
}

// endpointPath is the shape of an endpoint: a plain path, with no host,
// query string or escapes, so a registry row cannot send an identity's
// cookies and tokens anywhere but its own host.
var endpointPath = regexp.MustCompile(`^/[A-Za-z0-9/_.~-]*$`)

// reservedHeaders belong to the transport (framing, cookies) and cannot be
// set per query.
var reservedHeaders = map[string]bool{
	"host": true, "content-length": true, "transfer-encoding": true, "connection": true,
	"keep-alive": true, "proxy-connection": true, "upgrade": true, "te": true, "cookie": true,
}

// normalizeRoute checks a query's endpoint and headers and returns the
// headers with lower-case names.
func normalizeRoute(name, endpoint string, headers []Header) ([]Header, error) {
	if endpoint != "" {
		clean := path.Clean(endpoint)
		if !endpointPath.MatchString(endpoint) || (endpoint != clean && endpoint != clean+"/") {
			return nil, fmt.Errorf("registry: %s: endpoint %q must be a plain path on the platform's host, like /api/graphql/", name, endpoint)
		}
	}
	if len(headers) == 0 {
		return nil, nil
	}
	out := make([]Header, 0, len(headers))
	seen := map[string]bool{}
	for _, h := range headers {
		n := strings.ToLower(h.Name)
		switch {
		case !httpguts.ValidHeaderFieldName(n):
			return nil, fmt.Errorf("registry: %s: invalid header name %q", name, h.Name)
		case reservedHeaders[n]:
			return nil, fmt.Errorf("registry: %s: header %q is set by the transport, not per query", name, n)
		case seen[n]:
			return nil, fmt.Errorf("registry: %s: header %q given twice", name, n)
		case !httpguts.ValidHeaderFieldValue(h.Value):
			return nil, fmt.Errorf("registry: %s: invalid value for header %q", name, n)
		}
		seen[n] = true
		out = append(out, Header{Name: n, Value: h.Value})
	}
	return out, nil
}

// headersJSON encodes headers for the jsonb column ([] when none).
func headersJSON(h []Header) []byte {
	if len(h) == 0 {
		return []byte("[]")
	}
	b, _ := json.Marshal(h)
	return b
}

// ErrNotFound is returned for unknown query names.
var ErrNotFound = errors.New("registry: query not found")

// Registry resolves queries.
type Registry interface {
	// Get returns the active entry with the highest priority.
	Get(name string) (Query, error)
	// Fallback returns the best fallback entry whose doc_id differs from
	// failedDocID.
	Fallback(name, failedDocID string) (Query, bool)
	// ClientParams are global form parameters (__a, __comet_req, ...).
	ClientParams() map[string]string
	All() []Query
}

// Seed is the parsed queries.seed.yaml.
type Seed struct {
	ClientParams map[string]string
	Queries      []Query
}

type seedYAML struct {
	ClientParams map[string]string `yaml:"client_params"`
	Queries      []seedQueryYAML   `yaml:"queries"`
}

type seedQueryYAML struct {
	Name          string         `yaml:"name"`
	FriendlyName  string         `yaml:"friendly_name"`
	DocID         string         `yaml:"doc_id"`
	Parser        string         `yaml:"parser"`
	ParserVersion int            `yaml:"parser_version"`
	Priority      int            `yaml:"priority"`
	Status        string         `yaml:"status,omitempty"`
	Endpoint      string         `yaml:"endpoint,omitempty"`
	Headers       []Header       `yaml:"headers,omitempty"`
	Variables     map[string]any `yaml:"variables"`
}

// ParseSeed parses a seed YAML document.
func ParseSeed(b []byte) (Seed, error) {
	var y seedYAML
	if err := yaml.Unmarshal(b, &y); err != nil {
		return Seed{}, fmt.Errorf("registry: seed: %w", err)
	}
	s := Seed{ClientParams: y.ClientParams}
	for _, q := range y.Queries {
		vars, err := json.Marshal(q.Variables)
		if err != nil {
			return Seed{}, fmt.Errorf("registry: seed %s: %w", q.Name, err)
		}
		st := q.Status
		if st == "" {
			st = StatusActive
		}
		hdr, err := normalizeRoute(q.Name, q.Endpoint, q.Headers)
		if err != nil {
			return Seed{}, fmt.Errorf("registry: seed: %w", err)
		}
		s.Queries = append(s.Queries, Query{
			Name: q.Name, DocID: q.DocID, FriendlyName: q.FriendlyName, Variables: vars,
			Parser: q.Parser, ParserVersion: q.ParserVersion, Status: st, Priority: q.Priority, Source: "seed",
			Endpoint: q.Endpoint, Headers: hdr,
		})
	}
	return s, nil
}

// placeholderDocID matches the doc_ids shipped in configs/queries.seed.yaml.
// They only work against fakefb; real Facebook needs discovered ids.
var placeholderDocID = regexp.MustCompile(`^9000000000000\d{3}$`)

// IsPlaceholderDocID reports whether id is one of the seed placeholders.
func IsPlaceholderDocID(id string) bool { return placeholderDocID.MatchString(id) }

// PlaceholderQueries returns the names of seed queries that still carry a
// placeholder doc_id.
func PlaceholderQueries(s Seed) []string {
	var out []string
	for _, q := range s.Queries {
		if IsPlaceholderDocID(q.DocID) {
			out = append(out, q.Name)
		}
	}
	return out
}

// MarshalSeed renders s in the seed YAML format read by ParseSeed.
func MarshalSeed(s Seed) ([]byte, error) {
	var y seedYAML
	y.ClientParams = s.ClientParams
	for _, q := range s.Queries {
		var vars map[string]any
		if len(q.Variables) > 0 {
			if err := json.Unmarshal(q.Variables, &vars); err != nil {
				return nil, fmt.Errorf("registry: seed %s variables: %w", q.Name, err)
			}
		}
		st := q.Status
		if st == StatusActive {
			st = ""
		}
		y.Queries = append(y.Queries, seedQueryYAML{Name: q.Name, FriendlyName: q.FriendlyName, DocID: q.DocID,
			Parser: q.Parser, ParserVersion: q.ParserVersion, Priority: q.Priority, Status: st,
			Endpoint: q.Endpoint, Headers: q.Headers, Variables: vars})
	}
	var b bytes.Buffer
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(y); err != nil {
		return nil, fmt.Errorf("registry: seed: %w", err)
	}
	_ = enc.Close()
	return b.Bytes(), nil
}

// snapshot is an immutable view used by Static and DB.
type snapshot struct {
	byName map[string][]Query // sorted: active first, then priority desc
	params map[string]string
	all    []Query
}

func newSnapshot(qs []Query, params map[string]string) *snapshot {
	s := &snapshot{byName: map[string][]Query{}, params: params, all: qs}
	for _, q := range qs {
		if q.Status == StatusRetired || q.Status == StatusCandidate {
			continue
		}
		s.byName[q.Name] = append(s.byName[q.Name], q)
	}
	for _, list := range s.byName {
		sort.SliceStable(list, func(i, j int) bool {
			ai, aj := list[i].Status == StatusActive, list[j].Status == StatusActive
			if ai != aj {
				return ai
			}
			return list[i].Priority > list[j].Priority
		})
	}
	if s.params == nil {
		s.params = map[string]string{}
	}
	return s
}

func (s *snapshot) get(name string) (Query, error) {
	list := s.byName[name]
	if len(list) == 0 {
		return Query{}, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return list[0], nil
}

func (s *snapshot) fallback(name, failed string) (Query, bool) {
	for _, q := range s.byName[name] {
		if q.DocID != failed {
			return q, true
		}
	}
	return Query{}, false
}

// Static is an in-memory registry (spike, tests, offline tools).
type Static struct{ s *snapshot }

func NewStatic(seed Seed) *Static { return &Static{s: newSnapshot(seed.Queries, seed.ClientParams)} }

func (r *Static) Get(name string) (Query, error)             { return r.s.get(name) }
func (r *Static) Fallback(name, failed string) (Query, bool) { return r.s.fallback(name, failed) }
func (r *Static) ClientParams() map[string]string            { return r.s.params }
func (r *Static) All() []Query                               { return r.s.all }

// DB is a Postgres-backed registry refreshed in the background.
type DB struct {
	pool *pgxpool.Pool
	mu   sync.RWMutex
	s    *snapshot
	seed Seed
}

// NewDB loads the registry once; call Run to keep it fresh.
func NewDB(ctx context.Context, pool *pgxpool.Pool, seed Seed) (*DB, error) {
	r := &DB{pool: pool, seed: seed}
	if err := r.Reload(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *DB) Get(name string) (Query, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.s.get(name)
}

func (r *DB) Fallback(name, failed string) (Query, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.s.fallback(name, failed)
}

func (r *DB) ClientParams() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.s.params
}

func (r *DB) All() []Query {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.s.all
}

// Reload reads fb_queries and fb_client_params. Seed client params fill in
// keys the table does not override.
func (r *DB) Reload(ctx context.Context) error {
	qs, err := List(ctx, r.pool)
	if err != nil {
		return err
	}
	rows, err := r.pool.Query(ctx, "SELECT key, value FROM fb_client_params")
	if err != nil {
		return fmt.Errorf("registry: client params: %w", err)
	}
	params := map[string]string{}
	for k, v := range r.seed.ClientParams {
		params[k] = v
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return err
		}
		params[k] = v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	s := newSnapshot(qs, params)
	r.mu.Lock()
	r.s = s
	r.mu.Unlock()
	return nil
}

// Run reloads every interval until ctx is done.
func (r *DB) Run(ctx context.Context, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := r.Reload(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("registry: reload failed", "err", err)
			}
		}
	}
}

// List returns every row of fb_queries.
func List(ctx context.Context, pool *pgxpool.Pool) ([]Query, error) {
	rows, err := pool.Query(ctx, `SELECT name, doc_id, friendly_name, variables_template, parser,
		parser_version, status, priority, source, endpoint, headers,
		coalesce(last_check, ''), last_checked_at, last_ok_at
		FROM fb_queries ORDER BY name, priority DESC`)
	if err != nil {
		return nil, fmt.Errorf("registry: list: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Query, error) {
		var q Query
		var hdr []byte
		err := row.Scan(&q.Name, &q.DocID, &q.FriendlyName, &q.Variables, &q.Parser,
			&q.ParserVersion, &q.Status, &q.Priority, &q.Source, &q.Endpoint, &hdr,
			&q.LastCheck, &q.LastCheckedAt, &q.LastOKAt)
		if err == nil {
			q.Headers, err = ScanHeaders(hdr)
		}
		return q, err
	})
}

// ScanHeaders decodes the headers column.
func ScanHeaders(b []byte) ([]Header, error) {
	var h []Header
	if err := json.Unmarshal(b, &h); err != nil {
		return nil, fmt.Errorf("registry: headers: %w", err)
	}
	if len(h) == 0 {
		return nil, nil
	}
	return h, nil
}

// ApplySeed upserts seed rows. Existing rows from other sources win: a seed
// never overrides a doc_id discovered or set manually.
func ApplySeed(ctx context.Context, pool *pgxpool.Pool, seed Seed) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		for _, q := range seed.Queries {
			_, err := tx.Exec(ctx, `
				INSERT INTO fb_queries (name, doc_id, friendly_name, variables_template, parser,
					parser_version, status, priority, endpoint, headers, source, promoted_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'seed', now())
				ON CONFLICT (name, doc_id) DO UPDATE SET
					friendly_name = EXCLUDED.friendly_name,
					variables_template = EXCLUDED.variables_template,
					parser = EXCLUDED.parser,
					parser_version = EXCLUDED.parser_version,
					priority = EXCLUDED.priority,
					endpoint = EXCLUDED.endpoint,
					headers = EXCLUDED.headers,
					updated_at = now()
				WHERE fb_queries.source = 'seed'`,
				q.Name, q.DocID, q.FriendlyName, q.Variables, q.Parser, q.ParserVersion, q.Status, q.Priority,
				q.Endpoint, headersJSON(q.Headers))
			if err != nil {
				return fmt.Errorf("registry: seed %s: %w", q.Name, err)
			}
		}
		return nil
	})
}

// Upsert sets an entry (admin API, registry-sync). When status is active,
// other active entries of the same name are demoted to fallback.
func Upsert(ctx context.Context, pool *pgxpool.Pool, q Query) error {
	if q.Status == "" {
		q.Status = StatusActive
	}
	if q.Source == "" {
		q.Source = "manual"
	}
	if !json.Valid(q.Variables) {
		return errors.New("registry: variables must be valid JSON")
	}
	hdr, err := normalizeRoute(q.Name, q.Endpoint, q.Headers)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if q.Status == StatusActive {
			if _, err := tx.Exec(ctx, `UPDATE fb_queries SET status = 'fallback', updated_at = now()
				WHERE name = $1 AND doc_id <> $2 AND status = 'active'`, q.Name, q.DocID); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO fb_queries (name, doc_id, friendly_name, variables_template, parser,
				parser_version, status, priority, source, endpoint, headers, promoted_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, CASE WHEN $7 = 'active' THEN now() END)
			ON CONFLICT (name, doc_id) DO UPDATE SET
				friendly_name = EXCLUDED.friendly_name, variables_template = EXCLUDED.variables_template,
				parser = EXCLUDED.parser, parser_version = EXCLUDED.parser_version,
				status = EXCLUDED.status, priority = EXCLUDED.priority, source = EXCLUDED.source,
				endpoint = EXCLUDED.endpoint, headers = EXCLUDED.headers,
				promoted_at = COALESCE(EXCLUDED.promoted_at, fb_queries.promoted_at), updated_at = now()`,
			q.Name, q.DocID, q.FriendlyName, q.Variables, q.Parser, q.ParserVersion, q.Status, q.Priority, q.Source,
			q.Endpoint, headersJSON(hdr))
		return err
	})
}

var placeholder = regexp.MustCompile(`^\{\{(\w+)\}\}$`)

// Render fills the variables template. Placeholders without a value become
// JSON null.
func Render(tmpl json.RawMessage, vars map[string]any) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(tmpl))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("registry: template: %w", err)
	}
	v = fill(v, vars)
	return json.Marshal(v)
}

func fill(v any, vars map[string]any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			t[k] = fill(x, vars)
		}
		return t
	case []any:
		for i, x := range t {
			t[i] = fill(x, vars)
		}
		return t
	case string:
		if m := placeholder.FindStringSubmatch(t); m != nil {
			val, ok := vars[m[1]]
			if !ok {
				return nil
			}
			return val
		}
		return t
	default:
		return v
	}
}
