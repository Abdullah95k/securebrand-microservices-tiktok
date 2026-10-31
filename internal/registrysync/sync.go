// Package registrysync follows Facebook's doc_id rotations without a
// deploy. Every run it loads the canary pages and scans them and their JS
// bundles for Relay operation ids, records ids it does not know as
// candidate entries of the queries sharing that friendly name, checks the
// active entry and the candidates with a real request on a canary page
// (timeline, then comments, then replies, each feeding the next its
// input), and promotes a candidate whose response parses. The former active
// entry stays as a fallback the GraphQL client switches to on a stale
// doc_id. Crawlers pick up promotions on their next registry reload.
package registrysync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
	fbclient "github.com/abdullah95k/securebrand-microservices-igs/internal/fb/client"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// Verdict is the outcome of one check.
type Verdict string

const (
	// OK: the response parsed with every required field and had data.
	OK Verdict = "ok"
	// Stale: Facebook no longer accepts the doc_id (or its variables).
	Stale Verdict = "stale"
	// Drift: accepted, but the parser misses required fields.
	Drift Verdict = "drift"
	// Inconclusive: blocked, rate limited, no data to judge, transport
	// failure. Says nothing about the entry.
	Inconclusive Verdict = "inconclusive"
)

func (v Verdict) conclusive() bool { return v == OK || v == Stale || v == Drift }

// Run statuses.
const (
	StatusOK       = "ok"       // every query has a working active entry
	StatusDegraded = "degraded" // some query failed with nothing to promote, or could not be checked
	StatusError    = "error"    // no canary page could be loaded, or the database failed
	StatusSkipped  = "skipped"  // another instance holds the run lock
)

// Check is the verdict on one entry.
type Check struct {
	Query   string  `json:"query"`
	DocID   string  `json:"doc_id"`
	Status  string  `json:"status"` // the entry's registry status when checked
	Verdict Verdict `json:"verdict"`
	Reason  string  `json:"reason,omitempty"`
	Page    string  `json:"page,omitempty"`
}

// Promotion is a doc_id made active.
type Promotion struct {
	Query string `json:"query"`
	From  string `json:"from,omitempty"`
	To    string `json:"to"`
	Why   string `json:"why"`
}

// PageResult is one canary page load.
type PageResult struct {
	Page   string `json:"page"`
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

// Report describes one run.
type Report struct {
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt time.Time    `json:"finished_at"`
	Status     string       `json:"status"`
	Error      string       `json:"error,omitempty"`
	Pages      []PageResult `json:"pages,omitempty"`
	// Discovered maps the registry's friendly names found in the bundles to
	// their doc_id; Operations counts every operation the bundles declared.
	Discovered map[string]string `json:"discovered,omitempty"`
	Operations int               `json:"operations"`
	// NotFound are friendly names of registry queries no bundle declared:
	// Facebook may have renamed the operation.
	NotFound []string `json:"not_found,omitempty"`
	// Rendered are doc_ids read from the requests a rendered canary page
	// made, for friendly names the bundles did not declare.
	Rendered   map[string]string `json:"rendered,omitempty"`
	Candidates []string          `json:"candidates,omitempty"` // name/doc_id recorded this run
	Checks     []Check           `json:"checks,omitempty"`
	Promoted   []Promotion       `json:"promoted,omitempty"`
	// Proposed are the promotions a dry run (promote: false) would make.
	Proposed []Promotion `json:"proposed,omitempty"`
	Retired  []string    `json:"retired,omitempty"` // name/doc_id
	// Failing are queries whose active entry is stale or drifted with no
	// working replacement: the parser or the registry needs a person.
	Failing []string `json:"failing,omitempty"`
	// Unchecked are queries no canary page gave a verdict on.
	Unchecked []string `json:"unchecked,omitempty"`
}

// Checked reports whether the run gave every query a verdict. Failing
// queries count (igs_registry_query_healthy reports them); unchecked ones
// do not: a run whose probes are all blocked validates nothing.
func (r Report) Checked() bool {
	return (r.Status == StatusOK || r.Status == StatusDegraded) && len(r.Unchecked) == 0
}

// Sync runs registry syncs.
type Sync struct {
	DB     *pgxpool.Pool
	Cfg    config.RegistrySync
	Pool   *session.Pool
	Loader *session.Loader
	Rules  detect.Rules
	// Params returns the global GraphQL form parameters.
	Params         func() map[string]string
	AcceptLanguage string
	Locale         string
	// Fetch downloads a bundle over direct egress; with
	// Cfg.BundlesViaProxy the canary page's identity is used instead.
	Fetch docids.Fetcher
	// Render, when set (fb.browser), renders a canary page in a browser
	// and returns the GraphQL requests the page made; it is used when the
	// bundles do not declare some registry query's friendly name.
	Render func(ctx context.Context, id *session.Identity, rawURL string) ([]browser.Exchange, error)

	scanner *docids.Scanner
	now     func() time.Time
}

type entry struct {
	registry.Query
	failures   int
	promotedAt *time.Time
	checked    *time.Time
}

func key(name, docID string) string { return name + "/" + docID }

// canary is a loaded canary page.
type canary struct {
	ref    string
	url    *url.URL
	html   []byte
	pageID int64
	id     *session.Identity
}

// InitMetrics sets the last-success gauge from the stored runs, so a
// restart does not look like a sync that stopped working.
func (s *Sync) InitMetrics(ctx context.Context) error {
	var last *time.Time
	if err := s.DB.QueryRow(ctx, `SELECT max(finished_at) FROM registry_sync_runs
		WHERE status IN ('ok', 'degraded') AND coalesce(jsonb_array_length(report->'unchecked'), 0) = 0`).Scan(&last); err != nil {
		return err
	}
	if last != nil {
		metrics.RegistrySyncLastSuccess.Set(float64(last.Unix()))
	}
	return nil
}

// lockKey serialises runs across replicas and rollouts.
const lockKey = "igs:registry-sync"

// Run performs one sync. The report is returned (and stored in
// registry_sync_runs) even when the run fails.
func (s *Sync) Run(ctx context.Context) (rep Report, err error) {
	if s.now == nil {
		s.now = time.Now
	}
	rep.StartedAt = s.now().UTC()
	conn, err := s.DB.Acquire(ctx)
	if err != nil {
		return s.finish(ctx, rep, err)
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, lockKey).Scan(&locked); err != nil {
		return s.finish(ctx, rep, err)
	}
	if !locked {
		rep.Status = StatusSkipped
		rep.FinishedAt = s.now().UTC()
		return rep, nil
	}
	defer func() {
		// The lock is per connection; release it before the connection
		// goes back to the pool, even when ctx is done.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, lockKey)
	}()
	err = s.run(ctx, &rep)
	return s.finish(ctx, rep, err)
}

func (s *Sync) finish(ctx context.Context, rep Report, err error) (Report, error) {
	rep.FinishedAt = s.now().UTC()
	if err != nil {
		rep.Status, rep.Error = StatusError, err.Error()
	}
	metrics.RegistrySyncRuns.WithLabelValues(rep.Status).Inc()
	if rep.Checked() {
		metrics.RegistrySyncLastSuccess.Set(float64(rep.FinishedAt.Unix()))
	}
	js, _ := json.Marshal(rep)
	if _, dbErr := s.DB.Exec(context.WithoutCancel(ctx), `INSERT INTO registry_sync_runs (started_at, finished_at, status, report)
		VALUES ($1, $2, $3, $4)`, rep.StartedAt, rep.FinishedAt, rep.Status, js); dbErr != nil && err == nil {
		err = dbErr
	}
	lvl := slog.LevelInfo
	if rep.Status != StatusOK {
		lvl = slog.LevelWarn
	}
	slog.Log(ctx, lvl, "registry-sync: run finished", "status", rep.Status, "err", rep.Error,
		"operations", rep.Operations, "discovered", len(rep.Discovered), "candidates", rep.Candidates, "promoted", len(rep.Promoted),
		"failing", rep.Failing, "unchecked", rep.Unchecked, "not_found", rep.NotFound)
	return rep, err
}

func (s *Sync) run(ctx context.Context, rep *Report) error {
	if len(s.Cfg.CanaryPages) == 0 {
		return errors.New("registry_sync.canary_pages is empty: configure public pages with regular posts, comments and replies")
	}
	pages := s.loadCanaries(ctx, rep)
	defer func() {
		for _, c := range pages {
			s.Pool.Release(c.id)
		}
	}()
	if len(pages) == 0 {
		return errors.New("no canary page could be loaded")
	}

	// Discovery.
	if s.scanner == nil {
		s.scanner = &docids.Scanner{}
	}
	found := map[string]string{}
	for _, c := range pages {
		s.scanner.MaxBundles = s.Cfg.MaxBundles
		s.scanner.Fetch = s.fetcher(c.id)
		s.scanner.Page(ctx, c.html, c.url, found)
	}
	rep.Operations = len(found)
	metrics.RegistryDiscovered.Set(float64(len(found)))

	entries, err := s.entries(ctx)
	if err != nil {
		return err
	}
	s.renderMissing(ctx, pages, entries, found, rep)
	rep.Discovered = map[string]string{}
	for _, e := range entries {
		if d, ok := found[e.FriendlyName]; ok {
			rep.Discovered[e.FriendlyName] = d
		}
	}
	if err := s.recordCandidates(ctx, entries, found, rep); err != nil {
		return err
	}
	if entries, err = s.entries(ctx); err != nil {
		return err
	}

	// Checks.
	verdicts := s.check(ctx, pages, entries)
	for _, e := range sortedEntries(entries) {
		if c, ok := verdicts[key(e.Name, e.DocID)]; ok {
			rep.Checks = append(rep.Checks, c)
		}
	}
	if err := s.recordChecks(ctx, entries, verdicts, rep); err != nil {
		return err
	}
	if err := s.decide(ctx, entries, verdicts, found, rep); err != nil {
		return err
	}
	rep.Status = StatusOK
	if len(rep.Failing) > 0 || len(rep.Unchecked) > 0 {
		rep.Status = StatusDegraded
	}
	return nil
}

// renderMissing renders canary pages in a browser, while some registry
// friendly name is still missing, and takes the doc_ids from the GraphQL
// requests the page made. A failed render only leaves names missing.
func (s *Sync) renderMissing(ctx context.Context, pages []*canary, entries []*entry, found map[string]string, rep *Report) {
	if s.Render == nil {
		return
	}
	missing := map[string]bool{}
	for _, e := range entries {
		if _, ok := found[e.FriendlyName]; !ok && e.Status != registry.StatusRetired {
			missing[e.FriendlyName] = true
		}
	}
	for _, c := range pages {
		if len(missing) == 0 {
			return
		}
		xs, err := s.Render(ctx, c.id, c.url.String())
		if err != nil {
			slog.Warn("registry-sync: render", "page", c.ref, "err", err)
			continue
		}
		for _, x := range xs {
			if x.DocID == "" || !missing[x.FriendlyName] {
				continue
			}
			if rep.Rendered == nil {
				rep.Rendered = map[string]string{}
			}
			found[x.FriendlyName] = x.DocID
			rep.Rendered[x.FriendlyName] = x.DocID
			delete(missing, x.FriendlyName)
		}
	}
}

// loadCanaries loads every canary page through its own identity.
func (s *Sync) loadCanaries(ctx context.Context, rep *Report) []*canary {
	var out []*canary
	for _, p := range s.Cfg.CanaryPages {
		c, err := s.loadCanary(ctx, p)
		if err != nil {
			rep.Pages = append(rep.Pages, PageResult{Page: p, Reason: err.Error()})
			continue
		}
		rep.Pages = append(rep.Pages, PageResult{Page: c.ref, OK: true})
		out = append(out, c)
	}
	return out
}

func (s *Sync) loadCanary(ctx context.Context, page string) (*canary, error) {
	ref, err := task.NormalizeRef(page)
	if err != nil {
		return nil, err
	}
	id, err := s.Pool.Acquire(ctx, s.Cfg.Tier)
	if err != nil {
		return nil, err
	}
	u, _ := id.Base.Parse(task.RefPath(ref))
	pl, err := s.Loader.Bootstrap(ctx, id, u.String())
	if err != nil {
		s.Pool.Release(id)
		return nil, err
	}
	if pl.Class != detect.OK {
		s.Pool.Release(id)
		return nil, fmt.Errorf("page load: %s %s", pl.Class, pl.Reason)
	}
	pg, _ := parse.Page(pl.HTML, 0, task.RefID(ref))
	if pg == nil || pg.PageId == 0 {
		s.Pool.Release(id)
		return nil, errors.New("no page id in the page html")
	}
	return &canary{ref: ref, url: u, html: pl.HTML, pageID: pg.PageId, id: id}, nil
}

// fetcher downloads bundles directly, or through id's proxy.
func (s *Sync) fetcher(id *session.Identity) docids.Fetcher {
	if s.Fetch != nil && (!s.Cfg.BundlesViaProxy || id == nil) {
		return s.Fetch
	}
	return func(ctx context.Context, src string) ([]byte, error) {
		resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: src,
			Header: [][2]string{{"user-agent", id.FP.UserAgent}, {"accept", "*/*"}, {"accept-encoding", "gzip, deflate, br, zstd"}}})
		if err != nil {
			return nil, err
		}
		if resp.Status != 200 {
			return nil, fmt.Errorf("registry-sync: %s: status %d", src, resp.Status)
		}
		return resp.Body, nil
	}
}

// entries reads fb_queries with the check state.
func (s *Sync) entries(ctx context.Context) ([]*entry, error) {
	rows, err := s.DB.Query(ctx, `SELECT name, doc_id, friendly_name, variables_template, parser, parser_version,
			status, priority, source, endpoint, headers, check_failures, promoted_at, last_checked_at
		FROM fb_queries ORDER BY name, doc_id`)
	if err != nil {
		return nil, fmt.Errorf("registry-sync: entries: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*entry, error) {
		e := &entry{}
		var hdr []byte
		err := row.Scan(&e.Name, &e.DocID, &e.FriendlyName, &e.Variables, &e.Parser, &e.ParserVersion,
			&e.Status, &e.Priority, &e.Source, &e.Endpoint, &hdr, &e.failures, &e.promotedAt, &e.checked)
		if err == nil {
			e.Headers, err = registry.ScanHeaders(hdr)
		}
		return e, err
	})
}

// byName groups entries per query, ordered active, candidates (highest
// priority first), fallbacks (most recently promoted first), retired.
func byName(entries []*entry) map[string][]*entry {
	out := map[string][]*entry{}
	for _, e := range entries {
		out[e.Name] = append(out[e.Name], e)
	}
	rank := map[string]int{registry.StatusActive: 0, registry.StatusCandidate: 1, registry.StatusFallback: 2, registry.StatusRetired: 3}
	for _, list := range out {
		sort.SliceStable(list, func(i, j int) bool {
			a, b := list[i], list[j]
			if rank[a.Status] != rank[b.Status] {
				return rank[a.Status] < rank[b.Status]
			}
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}
			at, bt := time.Time{}, time.Time{}
			if a.promotedAt != nil {
				at = *a.promotedAt
			}
			if b.promotedAt != nil {
				bt = *b.promotedAt
			}
			return at.After(bt)
		})
	}
	return out
}

func sortedEntries(entries []*entry) []*entry {
	var out []*entry
	groups := byName(entries)
	names := make([]string, 0, len(groups))
	for n := range groups {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		out = append(out, groups[n]...)
	}
	return out
}

// template is the entry new doc_ids of name copy their variables, parser,
// endpoint and headers from.
func template(list []*entry) *entry {
	for _, e := range list {
		if e.Status != registry.StatusRetired {
			return e
		}
	}
	if len(list) > 0 {
		return list[0]
	}
	return nil
}

// recordCandidates inserts the discovered doc_ids every query of that
// friendly name does not know yet. A retired candidate discovered again is
// re-checked once RecheckRetired has passed (a parser fix may have made it
// usable).
func (s *Sync) recordCandidates(ctx context.Context, entries []*entry, found map[string]string, rep *Report) error {
	groups := byName(entries)
	names := make([]string, 0, len(groups))
	for n := range groups {
		names = append(names, n)
	}
	sort.Strings(names)
	friendly := map[string]bool{}
	for _, n := range names {
		t := template(groups[n])
		if t == nil {
			continue
		}
		friendly[t.FriendlyName] = true
		doc, ok := found[t.FriendlyName]
		if !ok {
			continue
		}
		var added bool
		err := s.DB.QueryRow(ctx, `
			INSERT INTO fb_queries (name, doc_id, friendly_name, variables_template, parser, parser_version,
				status, priority, endpoint, headers, source)
			SELECT name, $2, friendly_name, variables_template, parser, parser_version, 'candidate', priority,
				endpoint, headers, 'bundle'
			FROM fb_queries WHERE name = $1 AND doc_id = $3
			ON CONFLICT (name, doc_id) DO UPDATE SET status = 'candidate', check_failures = 0, updated_at = now()
				WHERE fb_queries.status = 'retired' AND fb_queries.source = 'bundle'
					AND (fb_queries.last_checked_at IS NULL OR fb_queries.last_checked_at < $4)
			RETURNING true`, n, doc, t.DocID, s.now().Add(-s.Cfg.RecheckRetired)).Scan(&added)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("registry-sync: candidate %s: %w", key(n, doc), err)
		}
		rep.Candidates = append(rep.Candidates, key(n, doc))
	}
	for f := range friendly {
		if _, ok := found[f]; !ok {
			rep.NotFound = append(rep.NotFound, f)
		}
	}
	sort.Strings(rep.NotFound)
	return nil
}

// Probe kinds by parser, in the order their inputs chain.
var kinds = []string{"timeline", "comments", "replies"}

type post struct {
	id, pageID int64
	feedback   string
}

// check runs the checks page by page until every entry that needs one has
// a conclusive verdict. Per query it checks the active entry and the
// candidates; fallbacks only when the active one failed and no candidate
// passed.
func (s *Sync) check(ctx context.Context, pages []*canary, entries []*entry) map[string]Check {
	groups := byName(entries)
	byKind := map[string][]string{}
	for name, list := range groups {
		t := template(list)
		if t == nil {
			continue
		}
		byKind[t.Parser] = append(byKind[t.Parser], name)
	}
	for _, names := range byKind {
		sort.Strings(names)
	}
	out := map[string]Check{}
	verdict := func(e *entry) Verdict { return out[key(e.Name, e.DocID)].Verdict }
	set := func(e *entry, c Check) {
		k := key(e.Name, e.DocID)
		if prev, ok := out[k]; ok && prev.Verdict.conclusive() {
			return
		}
		out[k] = c
		metrics.RegistryChecks.WithLabelValues(e.Name, e.Status, string(c.Verdict)).Inc()
	}
	// want reports whether e should be checked now: needed is set when the
	// caller needs a passing entry's output as input for the next kind.
	want := func(name string, e *entry, needed bool) bool {
		v := verdict(e)
		if v == Stale || v == Drift {
			return false
		}
		switch e.Status {
		case registry.StatusActive, registry.StatusCandidate:
		case registry.StatusFallback:
			// Only as a replacement for a failed active entry.
			activeBad, candidateOK := true, false
			for _, o := range groups[name] {
				switch {
				case o.Status == registry.StatusActive && verdict(o) != Stale && verdict(o) != Drift:
					activeBad = false
				case o.Status == registry.StatusCandidate && verdict(o) == OK:
					candidateOK = true
				}
			}
			if !activeBad || candidateOK {
				return false
			}
		default:
			return false
		}
		return !v.conclusive() || (needed && v == OK)
	}
	pending := func(kind string) bool {
		for _, n := range byKind[kind] {
			for _, e := range groups[n] {
				if want(n, e, false) {
					return true
				}
			}
		}
		return false
	}
	done := func() bool {
		for _, kind := range kinds {
			if pending(kind) {
				return false
			}
		}
		return true
	}
	for _, c := range pages {
		if done() || ctx.Err() != nil {
			break
		}
		var posts []post
		var threads []parse.ThreadRef
		storedTried := false
		for _, kind := range kinds {
			for _, name := range byKind[kind] {
				for _, e := range groups[name] {
					// A passing entry is checked again on this page only when
					// its output is the input a later kind still needs.
					var needed bool
					switch kind {
					case "timeline":
						needed = posts == nil && (pending("comments") || pending("replies"))
					case "comments":
						needed = threads == nil && pending("replies")
					}
					if !want(name, e, needed) {
						continue
					}
					var chk Check
					switch kind {
					case "timeline":
						var ps []post
						chk, ps = s.probeTimeline(ctx, c, e)
						if chk.Verdict == OK && posts == nil {
							posts = ps
						}
					case "comments":
						if len(posts) == 0 && !storedTried {
							// No timeline check passed here: read posts already stored.
							storedTried = true
							posts = s.storedPosts(ctx, c.pageID)
						}
						if len(posts) == 0 {
							chk = Check{Verdict: Inconclusive, Reason: "no post with comments to read"}
							break
						}
						var ths []parse.ThreadRef
						for i, p := range posts {
							if i >= 2 {
								break
							}
							chk, ths = s.probeComments(ctx, c, e, p)
							if chk.Verdict.conclusive() {
								break
							}
						}
						if chk.Verdict == OK && threads == nil {
							threads = ths
						}
					case "replies":
						if len(threads) == 0 {
							chk = Check{Verdict: Inconclusive, Reason: "no reply thread to read"}
							break
						}
						chk = s.probeReplies(ctx, c, e, threads[0])
					}
					chk.Query, chk.DocID, chk.Status, chk.Page = e.Name, e.DocID, e.Status, c.ref
					set(e, chk)
				}
			}
		}
	}
	return out
}

// ProbeQueryPrefix prefixes the query name of the sync's own requests in
// the request metrics, so its probes of stale candidates do not count as
// crawl traffic.
const ProbeQueryPrefix = "registry_sync:"

// client returns a GraphQL client whose registry holds only e, so a
// failure is e's own and not hidden by a fallback.
func (s *Sync) client(e *entry) *graphql.Client {
	q := e.Query
	q.Name = ProbeQueryPrefix + e.Name
	q.Status = registry.StatusActive
	var params map[string]string
	if s.Params != nil {
		params = s.Params()
	}
	return &graphql.Client{Registry: registry.NewStatic(registry.Seed{ClientParams: params, Queries: []registry.Query{q}}),
		Rules: s.Rules, Builder: fbclient.Builder{AcceptLanguage: s.AcceptLanguage, Locale: s.Locale}}
}

func (s *Sync) do(ctx context.Context, c *canary, e *entry, vars map[string]any) (*graphql.Response, Check) {
	resp, err := s.client(e).Do(ctx, c.id, graphql.Request{Query: ProbeQueryPrefix + e.Name, Vars: vars, Referer: c.url.String()})
	if err != nil {
		return nil, Check{Verdict: Inconclusive, Reason: err.Error()}
	}
	switch resp.Class {
	case detect.OK:
		return resp, Check{Verdict: OK}
	case detect.RegistryStale:
		return nil, Check{Verdict: Stale, Reason: resp.Reason}
	case detect.SchemaDrift:
		return nil, Check{Verdict: Drift, Reason: resp.Reason}
	default:
		return nil, Check{Verdict: Inconclusive, Reason: resp.Class.String() + ": " + resp.Reason}
	}
}

func drift(rep parse.Report) Check {
	return Check{Verdict: Drift, Reason: "missing required fields: " + strings.Join(rep.Missing, ", ")}
}

func (s *Sync) probeTimeline(ctx context.Context, c *canary, e *entry) (Check, []post) {
	resp, chk := s.do(ctx, c, e, map[string]any{"page_id": strconv.FormatInt(c.pageID, 10), "count": 5})
	if resp == nil {
		return chk, nil
	}
	tl, rep := parse.Timeline(resp.Parts, c.pageID, c.id.Base.String())
	if !rep.OK() {
		return drift(rep), nil
	}
	if len(tl.Posts) == 0 {
		return Check{Verdict: Inconclusive, Reason: "no posts"}, nil
	}
	var posts []post
	type ranked struct {
		post
		n int64
	}
	var rs []ranked
	for _, b := range tl.Posts {
		p := b.Post
		if p.GetCommentCount() <= 0 {
			continue
		}
		fb := p.FeedbackId
		if fb == "" {
			fb = parse.FeedbackID(p.PostId)
		}
		rs = append(rs, ranked{post{id: p.PostId, pageID: c.pageID, feedback: fb}, p.GetCommentCount()})
	}
	// Busy posts first: they are the likeliest to have reply threads.
	sort.SliceStable(rs, func(i, j int) bool { return rs[i].n > rs[j].n })
	for _, r := range rs {
		posts = append(posts, r.post)
	}
	return Check{Verdict: OK}, posts
}

// storedPosts returns recent stored posts of pageID with comments, so the
// comment queries can be checked when the timeline cannot.
func (s *Sync) storedPosts(ctx context.Context, pageID int64) []post {
	rows, err := s.DB.Query(ctx, `SELECT post_id, coalesce(feedback_id, '') FROM posts
		WHERE page_id = $1 AND comment_count_total > 0 AND post_created_at > now() - interval '30 days'
		ORDER BY comment_count_total DESC LIMIT 2`, pageID)
	if err != nil {
		slog.Warn("registry-sync: stored posts", "page_id", pageID, "err", err)
		return nil
	}
	defer rows.Close()
	var out []post
	for rows.Next() {
		p := post{pageID: pageID}
		if err := rows.Scan(&p.id, &p.feedback); err != nil {
			slog.Warn("registry-sync: stored posts", "page_id", pageID, "err", err)
			return nil
		}
		if p.feedback == "" {
			p.feedback = parse.FeedbackID(p.id)
		}
		out = append(out, p)
	}
	return out
}

func (s *Sync) probeComments(ctx context.Context, c *canary, e *entry, p post) (Check, []parse.ThreadRef) {
	resp, chk := s.do(ctx, c, e, map[string]any{"feedback_id": p.feedback})
	if resp == nil {
		return chk, nil
	}
	res, rep := parse.Comments(resp.Parts, parse.CommentContext{PostID: p.id, PageID: p.pageID,
		PageAuthorID: strconv.FormatInt(p.pageID, 10)})
	if !rep.OK() {
		return drift(rep), nil
	}
	if len(res.Comments) == 0 {
		return Check{Verdict: Inconclusive, Reason: "no comments"}, nil
	}
	var threads []parse.ThreadRef
	for _, t := range res.Threads {
		if t.FeedbackID != "" && t.ReplyCount > t.InlineReplies {
			threads = append(threads, t)
		}
	}
	for _, t := range res.Threads {
		if t.FeedbackID != "" && t.ReplyCount <= t.InlineReplies {
			threads = append(threads, t)
		}
	}
	return Check{Verdict: OK}, threads
}

func (s *Sync) probeReplies(ctx context.Context, c *canary, e *entry, t parse.ThreadRef) Check {
	resp, chk := s.do(ctx, c, e, map[string]any{"feedback_id": t.FeedbackID, "expansion_token": t.ExpansionToken})
	if resp == nil {
		return chk
	}
	res, rep := parse.Comments(resp.Parts, parse.CommentContext{PageID: c.pageID, PageAuthorID: strconv.FormatInt(c.pageID, 10),
		BaseDepth: t.Depth + 1, ParentCommentID: t.CommentID, RootCommentID: t.RootCommentID, Replies: true})
	if !rep.OK() {
		return drift(rep)
	}
	if len(res.Comments) == 0 {
		return Check{Verdict: Inconclusive, Reason: "no replies"}
	}
	return Check{Verdict: OK}
}

// recordChecks stores each verdict. A candidate or fallback that failed
// RetireAfter checks in a row is retired; the active entry never is (it is
// only demoted by a promotion).
func (s *Sync) recordChecks(ctx context.Context, entries []*entry, verdicts map[string]Check, rep *Report) error {
	batch := &pgx.Batch{}
	for _, e := range entries {
		c, ok := verdicts[key(e.Name, e.DocID)]
		if !ok {
			continue
		}
		note := string(c.Verdict)
		if c.Reason != "" {
			note += ": " + truncate(c.Reason, 200)
		}
		switch c.Verdict {
		case OK:
			e.failures = 0
			batch.Queue(`UPDATE fb_queries SET last_checked_at = now(), last_check = $3, check_failures = 0,
				last_ok_at = now() WHERE name = $1 AND doc_id = $2`, e.Name, e.DocID, note)
		case Stale, Drift:
			e.failures++
			retire := e.Status != registry.StatusActive && s.Cfg.RetireAfter > 0 && e.failures >= s.Cfg.RetireAfter
			if retire {
				rep.Retired = append(rep.Retired, key(e.Name, e.DocID))
				e.Status = registry.StatusRetired
			}
			batch.Queue(`UPDATE fb_queries SET last_checked_at = now(), last_check = $3, check_failures = $4,
				status = CASE WHEN $5 THEN 'retired' ELSE status END, updated_at = now()
				WHERE name = $1 AND doc_id = $2`, e.Name, e.DocID, note, e.failures, retire)
		default:
			batch.Queue(`UPDATE fb_queries SET last_checked_at = now(), last_check = $3 WHERE name = $1 AND doc_id = $2`,
				e.Name, e.DocID, note)
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	return s.DB.SendBatch(ctx, batch).Close()
}

// decide promotes per query: a passing candidate Facebook's bundles
// currently declare, else, when the active entry failed, any passing
// candidate or fallback. It then trims the fallbacks and sets the health
// gauges.
func (s *Sync) decide(ctx context.Context, entries []*entry, verdicts map[string]Check, found map[string]string, rep *Report) error {
	groups := byName(entries)
	names := make([]string, 0, len(groups))
	for n := range groups {
		names = append(names, n)
	}
	sort.Strings(names)
	v := func(e *entry) Verdict {
		if e == nil {
			return ""
		}
		return verdicts[key(e.Name, e.DocID)].Verdict
	}
	for _, name := range names {
		list := groups[name]
		var active *entry
		for _, e := range list {
			if e.Status == registry.StatusActive {
				active = e
				break
			}
		}
		activeBad := active == nil || v(active) == Stale || v(active) == Drift
		var pick *entry
		why := ""
		for _, e := range list {
			if e.Status == registry.StatusCandidate && v(e) == OK && found[e.FriendlyName] == e.DocID {
				pick, why = e, "declared by Facebook's current bundles and parsed on a canary page"
				break
			}
		}
		if pick == nil && activeBad {
			for _, e := range list {
				if (e.Status == registry.StatusCandidate || e.Status == registry.StatusFallback) && v(e) == OK {
					pick, why = e, "the active doc_id failed; this one parsed on a canary page"
					if active == nil {
						why = "no active doc_id; this one parsed on a canary page"
					}
					break
				}
			}
		}
		from := ""
		if active != nil {
			from = active.DocID
		}
		switch {
		case pick != nil && s.Cfg.Promote:
			if err := s.promote(ctx, name, pick.DocID); err != nil {
				return err
			}
			rep.Promoted = append(rep.Promoted, Promotion{Query: name, From: from, To: pick.DocID, Why: why})
			metrics.RegistryPromotions.WithLabelValues(name).Inc()
			metrics.RegistryQueryHealthy.WithLabelValues(name).Set(1)
			slog.Warn("registry-sync: promoted", "query", name, "from", from, "to", pick.DocID, "why", why)
		case active != nil && v(active) == OK:
			metrics.RegistryQueryHealthy.WithLabelValues(name).Set(1)
		case active == nil || v(active).conclusive():
			rep.Failing = append(rep.Failing, name)
			metrics.RegistryQueryHealthy.WithLabelValues(name).Set(0)
		default:
			// Inconclusive: the gauge keeps its last value.
			rep.Unchecked = append(rep.Unchecked, name)
		}
		if pick != nil && !s.Cfg.Promote {
			rep.Proposed = append(rep.Proposed, Promotion{Query: name, From: from, To: pick.DocID, Why: why})
		}
		if !s.Cfg.Promote {
			continue // dry run: statuses stay as they are
		}
		// A passing candidate that is no longer current stays as a spare.
		for _, e := range list {
			if e.Status == registry.StatusCandidate && v(e) == OK && (pick == nil || e.DocID != pick.DocID) {
				if _, err := s.DB.Exec(ctx, `UPDATE fb_queries SET status = 'fallback', updated_at = now()
					WHERE name = $1 AND doc_id = $2 AND status = 'candidate'`, name, e.DocID); err != nil {
					return err
				}
			}
		}
		retired, err := s.trimFallbacks(ctx, name)
		if err != nil {
			return err
		}
		for _, d := range retired {
			rep.Retired = append(rep.Retired, key(name, d))
		}
	}
	return nil
}

func (s *Sync) promote(ctx context.Context, name, docID string) error {
	return pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE fb_queries SET status = 'fallback', updated_at = now()
			WHERE name = $1 AND doc_id <> $2 AND status = 'active'`, name, docID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE fb_queries SET status = 'active', promoted_at = now(), updated_at = now()
			WHERE name = $1 AND doc_id = $2`, name, docID)
		return err
	})
}

// trimFallbacks retires all but the KeepFallbacks fallbacks of name most
// recently known to work: the last passing check, else the promotion (a
// spare candidate kept as a fallback was never promoted).
func (s *Sync) trimFallbacks(ctx context.Context, name string) ([]string, error) {
	rows, err := s.DB.Query(ctx, `UPDATE fb_queries SET status = 'retired', updated_at = now()
		WHERE name = $1 AND status = 'fallback' AND doc_id NOT IN (
			SELECT doc_id FROM fb_queries WHERE name = $1 AND status = 'fallback'
			ORDER BY coalesce(last_ok_at, promoted_at) DESC NULLS LAST, updated_at DESC LIMIT $2)
		RETURNING doc_id`, name, max(s.Cfg.KeepFallbacks, 0))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
