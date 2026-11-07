// Package canary is tks-canary (build brief M8; docs/tiktok reuse-plan §3):
// it follows TikTok's changes to the web calls the crawler makes (T25)
// without a deploy. TikTok has no doc_ids to discover, so where fbs's
// registry sync scans bundles, the canary checks: every run it loads the
// canary accounts' profile pages and calls each variant of the crawler's
// operations on them, the post list first, then comments on a post it
// returned, then replies on a comment that has some, each feeding the
// next its input. It adds each variant's signed or unsigned twin as a
// candidate (TikTok changes which calls it accepts unsigned: T4), and
// promotes a working variant when the active one stops answering, or when
// an unsigned one answers where the active one needs a signature (a
// signature costs browser time: build brief D11). The former active
// variant stays as a fallback the client switches to on an empty answer.
// Crawlers pick up promotions on their next registry reload.
//
// Its skeleton (the run lock, the run records, the check columns, the
// promotion and the trimming of fallbacks) is internal/registrysync's,
// on schema tt's operations and canary_runs.
package canary

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	ttv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/leader"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/parse"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/task"
)

// Verdict is the outcome of one check.
type Verdict string

const (
	// OK: the answer parsed with every required field and had data.
	OK Verdict = "ok"
	// Stale: TikTok refused the variant (an empty answer) on
	// tt.canary.confirm accounts in the run.
	Stale Verdict = "stale"
	// Drift: answered, but the parser misses required fields.
	Drift Verdict = "drift"
	// Inconclusive: blocked, rate limited, not signed, nothing to judge,
	// refused on fewer accounts than confirm. Says nothing about the
	// variant.
	Inconclusive Verdict = "inconclusive"
)

func (v Verdict) conclusive() bool { return v == OK || v == Stale || v == Drift }

// Run statuses.
const (
	StatusOK       = "ok"       // every operation has a working active variant
	StatusDegraded = "degraded" // some operation failed with nothing to promote, or could not be checked
	StatusError    = "error"    // no canary account could be loaded, or the database failed
	StatusSkipped  = "skipped"  // another instance holds the run lock
)

// Why a variant was promoted.
const (
	WhyFailed   = "failed"   // the active variant stopped answering
	WhyUnsigned = "unsigned" // it answers without the signature the active one needs
)

// Check is the verdict on one variant. Accounts are named by their
// position in tt.canary.accounts, never by username.
type Check struct {
	Operation string  `json:"operation"`
	Variant   string  `json:"variant"`
	Status    string  `json:"status"` // the variant's registry status when checked
	Signed    bool    `json:"signed"`
	Verdict   Verdict `json:"verdict"`
	Reason    string  `json:"reason,omitempty"`
	Account   int     `json:"account,omitempty"`
	// SignError: the signer could not sign the call (inconclusive).
	SignError bool `json:"sign_error,omitempty"`
	// called: a call was made (not "nothing to read").
	called bool
}

// Promotion is a variant made active.
type Promotion struct {
	Operation string `json:"operation"`
	From      string `json:"from,omitempty"`
	To        string `json:"to"`
	Why       string `json:"why"`
}

// AccountResult is one canary account's profile page.
type AccountResult struct {
	Account int    `json:"account"`
	OK      bool   `json:"ok"`
	Reason  string `json:"reason,omitempty"`
}

// SignerResult counts the run's checks of signed variants.
type SignerResult struct {
	// Checks were sent to be signed; Errors are those the signer could not
	// sign (no page, a page that failed, every page busy).
	Checks int `json:"checks"`
	Errors int `json:"errors"`
}

// Report describes one run.
type Report struct {
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt time.Time       `json:"finished_at"`
	Status     string          `json:"status"`
	Error      string          `json:"error,omitempty"`
	Accounts   []AccountResult `json:"accounts,omitempty"`
	// Twins are the operation/variant candidates added this run.
	Twins    []string    `json:"twins,omitempty"`
	Checks   []Check     `json:"checks,omitempty"`
	Promoted []Promotion `json:"promoted,omitempty"`
	// Proposed are the promotions a dry run (promote: false) would make.
	Proposed []Promotion `json:"proposed,omitempty"`
	Retired  []string    `json:"retired,omitempty"` // operation/variant
	// Failing are operations whose active variant is stale or drifted with
	// no working replacement: the registry or the parser needs a person.
	Failing []string `json:"failing,omitempty"`
	// Unchecked are operations no canary account gave a verdict on.
	Unchecked []string     `json:"unchecked,omitempty"`
	Signer    SignerResult `json:"signer"`
}

// Checked reports whether the run gave every operation a verdict.
func (r Report) Checked() bool {
	return (r.Status == StatusOK || r.Status == StatusDegraded) && len(r.Unchecked) == 0
}

// Canary runs canary checks.
type Canary struct {
	// DB is a pool on schema tt (ttstore.Open).
	DB  *pgxpool.Pool
	Cfg config.TTCanary
	// Tier is the proxy tier (tt.canary.tier, or the first one).
	Tier   string
	Pool   *session.Pool
	Loader *session.Loader
	Rules  detect.Rules
	// Builder and Decode are the crawler's (ttclient).
	Builder graphql.RequestBuilder
	Decode  graphql.Decoder
	// Params returns the client parameters (the registry's).
	Params func() map[string]string
	// Signer signs the variants that need it (ttclient.SignerFor); nil
	// sends every call as built, so signed variants are refused.
	Signer graphql.Signer
	// ListPageSize, CommentsPageSize and RepliesPageSize are tt's.
	ListPageSize, CommentsPageSize, RepliesPageSize int

	now func() time.Time
}

type entry struct {
	registry.Query
	failures   int
	promotedAt *time.Time
}

func key(name, variant string) string { return name + "/" + variant }

// Operation kinds, in the order their inputs chain.
const (
	kindList     = "list"
	kindComments = "comments"
	kindReplies  = "replies"
)

var kinds = []string{kindList, kindComments, kindReplies}

// kindOf is the check an operation gets: by its parser, and replies (the
// comments parser's too) by name.
func kindOf(q registry.Query) string {
	switch {
	case q.Parser == "post_list":
		return kindList
	case q.Name == "replies":
		return kindReplies
	case q.Parser == "comments":
		return kindComments
	}
	return ""
}

// InitMetrics sets the last-success gauge from the stored runs, so a
// restart does not look like a canary that stopped working.
func (c *Canary) InitMetrics(ctx context.Context) error {
	var last *time.Time
	if err := c.DB.QueryRow(ctx, `SELECT max(finished_at) FROM canary_runs
		WHERE status IN ('ok', 'degraded') AND coalesce(jsonb_array_length(report->'unchecked'), 0) = 0`).Scan(&last); err != nil {
		return err
	}
	if last != nil {
		metrics.CanaryLastSuccess.Set(float64(last.Unix()))
	}
	return nil
}

// lockKey serialises runs across replicas and rollouts.
const lockKey = "tks:canary"

// Run performs one canary run. The report is returned (and stored in
// canary_runs) even when the run fails.
func (c *Canary) Run(ctx context.Context) (rep Report, err error) {
	if c.now == nil {
		c.now = time.Now
	}
	rep.StartedAt = c.now().UTC()
	// One run at a time across replicas and rollouts: a lease, not a
	// session advisory lock, which PgBouncer's transaction mode would share
	// between clients (leader.Hold).
	ran, err := leader.Hold(ctx, c.DB, lockKey, func(ctx context.Context) error {
		return c.run(ctx, &rep)
	})
	if !ran && err == nil {
		rep.Status = StatusSkipped
		rep.FinishedAt = c.now().UTC()
		metrics.CanaryRuns.WithLabelValues(rep.Status).Inc()
		return rep, nil
	}
	return c.finish(ctx, rep, err)
}

func (c *Canary) finish(ctx context.Context, rep Report, err error) (Report, error) {
	rep.FinishedAt = c.now().UTC()
	if err != nil {
		rep.Status, rep.Error = StatusError, err.Error()
	}
	metrics.CanaryRuns.WithLabelValues(rep.Status).Inc()
	if rep.Checked() {
		metrics.CanaryLastSuccess.Set(float64(rep.FinishedAt.Unix()))
	}
	switch {
	case rep.Signer.Checks > 0 && rep.Signer.Errors == rep.Signer.Checks:
		metrics.CanarySigner.Set(0)
	case rep.Signer.Checks > 0:
		metrics.CanarySigner.Set(1)
	}
	js, _ := json.Marshal(rep)
	if _, dbErr := c.DB.Exec(context.WithoutCancel(ctx), `INSERT INTO canary_runs (started_at, finished_at, status, report)
		VALUES ($1, $2, $3, $4)`, rep.StartedAt, rep.FinishedAt, rep.Status, js); dbErr != nil && err == nil {
		err = dbErr
	}
	lvl := slog.LevelInfo
	if rep.Status != StatusOK {
		lvl = slog.LevelWarn
	}
	slog.Log(ctx, lvl, "canary: run finished", "status", rep.Status, "err", rep.Error, "twins", rep.Twins,
		"promoted", len(rep.Promoted), "failing", rep.Failing, "unchecked", rep.Unchecked,
		"signed_checks", rep.Signer.Checks, "sign_errors", rep.Signer.Errors)
	return rep, err
}

func (c *Canary) run(ctx context.Context, rep *Report) error {
	if len(c.Cfg.Accounts) == 0 {
		return errors.New("tt.canary.accounts is empty: configure public accounts with posts, comments and replies")
	}
	entries, err := c.entries(ctx)
	if err != nil {
		return err
	}
	if c.Cfg.Twins {
		if err := c.recordTwins(ctx, entries, rep); err != nil {
			return err
		}
		if entries, err = c.entries(ctx); err != nil {
			return err
		}
	}
	verdicts, loaded := c.check(ctx, entries, rep)
	if loaded == 0 {
		return errors.New("no canary account could be loaded")
	}
	for _, e := range sortedEntries(entries) {
		if v, ok := verdicts[key(e.Name, e.DocID)]; ok {
			rep.Checks = append(rep.Checks, v)
		}
	}
	if err := c.recordChecks(ctx, entries, verdicts, rep); err != nil {
		return err
	}
	if err := c.decide(ctx, entries, verdicts, rep); err != nil {
		return err
	}
	rep.Status = StatusOK
	if len(rep.Failing) > 0 || len(rep.Unchecked) > 0 {
		rep.Status = StatusDegraded
	}
	return nil
}

// entries reads the configured operations' variants with their check
// state.
func (c *Canary) entries(ctx context.Context) ([]*entry, error) {
	rows, err := c.DB.Query(ctx, `SELECT name, doc_id, friendly_name, variables_template, parser, parser_version,
			status, priority, source, method, endpoint, headers, check_failures, promoted_at
		FROM operations WHERE name = ANY($1) ORDER BY name, doc_id`, c.Cfg.Operations)
	if err != nil {
		return nil, fmt.Errorf("canary: entries: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*entry, error) {
		e := &entry{}
		var hdr []byte
		err := row.Scan(&e.Name, &e.DocID, &e.FriendlyName, &e.Variables, &e.Parser, &e.ParserVersion,
			&e.Status, &e.Priority, &e.Source, &e.Method, &e.Endpoint, &hdr, &e.failures, &e.promotedAt)
		if err == nil {
			e.Headers, err = registry.ScanHeaders(hdr)
		}
		return e, err
	})
}

// byName groups entries per operation, ordered active, candidates
// (highest priority first), fallbacks (most recently promoted first),
// retired.
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

func sortedNames(groups map[string][]*entry) []string {
	names := make([]string, 0, len(groups))
	for n := range groups {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func sortedEntries(entries []*entry) []*entry {
	var out []*entry
	groups := byName(entries)
	for _, n := range sortedNames(groups) {
		out = append(out, groups[n]...)
	}
	return out
}

// recordTwins adds, for every variant in use, its signed or unsigned twin
// as a candidate when the operation does not have it. A twin the canary
// retired is checked again once tt.canary.recheck_retired has passed
// (TikTok may have changed what it accepts again).
func (c *Canary) recordTwins(ctx context.Context, entries []*entry, rep *Report) error {
	groups := byName(entries)
	for _, n := range sortedNames(groups) {
		have := map[string]*entry{}
		for _, e := range groups[n] {
			have[e.DocID] = e
		}
		for _, e := range groups[n] {
			if e.Status == registry.StatusRetired {
				continue
			}
			twin := ttclient.Twin(e.Query).DocID
			if t, ok := have[twin]; ok && t.Status != registry.StatusRetired {
				continue
			}
			var added bool
			err := c.DB.QueryRow(ctx, `
				INSERT INTO operations (name, doc_id, friendly_name, variables_template, parser, parser_version,
					status, priority, method, endpoint, headers, source)
				SELECT name, $2, friendly_name, variables_template, parser, parser_version, 'candidate', priority,
					method, endpoint, headers, 'canary'
				FROM operations WHERE name = $1 AND doc_id = $3
				ON CONFLICT (name, doc_id) DO UPDATE SET status = 'candidate', check_failures = 0, updated_at = now()
					WHERE operations.status = 'retired' AND operations.source = 'canary'
						AND (operations.last_checked_at IS NULL OR operations.last_checked_at < $4)
				RETURNING true`, n, twin, e.DocID, c.now().Add(-c.Cfg.RecheckRetired)).Scan(&added)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return fmt.Errorf("canary: twin %s: %w", key(n, twin), err)
			}
			rep.Twins = append(rep.Twins, key(n, twin))
			have[twin] = &entry{Query: registry.Query{Name: n, DocID: twin, Status: registry.StatusCandidate}}
		}
	}
	return nil
}

// account is a loaded canary account.
type account struct {
	pos  int // 1-based position in tt.canary.accounts
	name string
	sec  string
	id   *session.Identity
}

// post is a post with comments, for the comment check.
type post struct {
	id, userID int64
	kind       ttv1.PostKind
	created    *timestamppb.Timestamp
	n          int64 // its comment count
}

// check runs the checks account by account until every variant that
// needs one has a conclusive verdict. Per operation it checks the active
// variant and the candidates; fallbacks when the active one failed, and
// unsigned ones also when the active one needs a signature. It returns the
// verdicts and how many accounts loaded.
func (c *Canary) check(ctx context.Context, entries []*entry, rep *Report) (map[string]Check, int) {
	groups := byName(entries)
	byKind := map[string][]string{}
	for _, name := range sortedNames(groups) {
		k := ""
		for _, e := range groups[name] {
			if k = kindOf(e.Query); k != "" {
				break
			}
		}
		if k == "" {
			slog.Warn("canary: operation has no check", "operation", name)
			continue
		}
		byKind[k] = append(byKind[k], name)
	}
	confirm := max(1, min(c.Cfg.Confirm, len(c.Cfg.Accounts)))
	out := map[string]Check{}
	refused := map[string]int{}
	verdict := func(e *entry) Verdict { return out[key(e.Name, e.DocID)].Verdict }
	set := func(e *entry, chk Check) {
		k := key(e.Name, e.DocID)
		prev, ok := out[k]
		if ok && prev.Verdict.conclusive() {
			return
		}
		chk.called = chk.called || prev.called
		chk.SignError = chk.SignError || (prev.SignError && chk.Verdict == Inconclusive)
		if chk.Verdict == Stale {
			// One refusal may be the identity's (a burnt device, a bad
			// signer page): a variant is stale once confirm accounts
			// refused it.
			refused[k]++
			if refused[k] < confirm {
				chk.Verdict = Inconclusive
				chk.Reason = fmt.Sprintf("refused on %d of the %d accounts needed: %s", refused[k], confirm, chk.Reason)
			}
		}
		out[k] = chk
		metrics.CanaryChecks.WithLabelValues(e.Name, e.Status, string(chk.Verdict)).Inc()
	}
	failed := func(e *entry) bool { v := verdict(e); return v == Stale || v == Drift }
	active := func(name string) *entry {
		for _, e := range groups[name] {
			if e.Status == registry.StatusActive {
				return e
			}
		}
		return nil
	}
	// want reports whether e should be checked now: needed is set when the
	// caller needs a passing variant's output as input for the next kind.
	want := func(name string, e *entry, needed bool) bool {
		if failed(e) {
			return false
		}
		switch e.Status {
		case registry.StatusActive, registry.StatusCandidate:
		case registry.StatusFallback:
			a := active(name)
			activeBad := a == nil || failed(a)
			cheaper := a != nil && ttclient.Signed(a.Query) && !ttclient.Signed(e.Query)
			if !activeBad && !cheaper {
				return false
			}
			if activeBad && !cheaper {
				// Only while no candidate replaces the active one.
				for _, o := range groups[name] {
					if o.Status == registry.StatusCandidate && verdict(o) == OK {
						return false
					}
				}
			}
		default:
			return false
		}
		return !verdict(e).conclusive() || (needed && verdict(e) == OK)
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
		for _, k := range kinds {
			if pending(k) {
				return false
			}
		}
		return true
	}
	loaded := 0
	for i, name := range c.Cfg.Accounts {
		if done() || ctx.Err() != nil {
			break
		}
		a, err := c.load(ctx, i+1, name)
		if err != nil {
			rep.Accounts = append(rep.Accounts, AccountResult{Account: i + 1, Reason: err.Error()})
			continue
		}
		rep.Accounts = append(rep.Accounts, AccountResult{Account: i + 1, OK: true})
		loaded++
		var posts []post
		var threads []parse.ThreadRef
		var threadPost post
		for _, kind := range kinds {
			for _, name := range byKind[kind] {
				for _, e := range groups[name] {
					var needed bool
					switch kind {
					case kindList:
						needed = posts == nil && (pending(kindComments) || pending(kindReplies))
					case kindComments:
						needed = threads == nil && pending(kindReplies)
					}
					if !want(name, e, needed) {
						continue
					}
					var chk Check
					switch kind {
					case kindList:
						var ps []post
						chk, ps = c.checkList(ctx, a, e)
						chk.called = true
						if chk.Verdict == OK && posts == nil {
							posts = ps
						}
					case kindComments:
						if len(posts) == 0 {
							chk = Check{Verdict: Inconclusive, Reason: "no post with comments to read"}
							break
						}
						var ths []parse.ThreadRef
						for j, p := range posts {
							if j >= 2 {
								break
							}
							chk, ths = c.checkComments(ctx, a, e, p)
							chk.called = true
							if chk.Verdict.conclusive() {
								if chk.Verdict == OK && threads == nil && len(ths) > 0 {
									threads, threadPost = ths, p
								}
								break
							}
						}
					case kindReplies:
						if len(threads) == 0 {
							chk = Check{Verdict: Inconclusive, Reason: "no comment with replies to read"}
							break
						}
						chk = c.checkReplies(ctx, a, e, threadPost, threads[0])
						chk.called = true
					}
					chk.Operation, chk.Variant, chk.Status, chk.Account = e.Name, e.DocID, e.Status, a.pos
					chk.Signed = ttclient.Signed(e.Query)
					set(e, chk)
				}
			}
		}
		c.Pool.Release(a.id)
	}
	return out, loaded
}

// load reads a canary account's profile page through its own identity
// (its bootstrap) for the account's secUid.
func (c *Canary) load(ctx context.Context, pos int, name string) (*account, error) {
	name = strings.TrimPrefix(strings.TrimSpace(name), "@")
	if name == "" {
		return nil, errors.New("empty username")
	}
	id, err := c.Pool.Acquire(ctx, c.Tier)
	if err != nil {
		return nil, err
	}
	pl, err := c.Loader.Load(ctx, id, profileURL(id, name))
	if err != nil || pl == nil {
		c.Pool.Release(id)
		if err == nil {
			err = errors.New("no page")
		}
		return nil, fmt.Errorf("page load: %w", err)
	}
	if pl.Class != detect.OK {
		c.Pool.Release(id)
		return nil, fmt.Errorf("page load: %s %s", pl.Class, pl.Reason)
	}
	res, _ := parse.ProfilePage(pl.HTML, parse.Options{Query: "canary:profile_page"})
	switch acc := res.Account; {
	case acc == nil && res.Status != ttv1.AccountStatus_ACCOUNT_STATUS_UNSPECIFIED:
		c.Pool.Release(id)
		return nil, fmt.Errorf("account %s", strings.ToLower(strings.TrimPrefix(res.Status.String(), "ACCOUNT_STATUS_")))
	case acc == nil || acc.GetSecUid() == "":
		c.Pool.Release(id)
		return nil, errors.New("no account in the profile page")
	case acc.GetPrivate():
		c.Pool.Release(id)
		return nil, errors.New("account private")
	default:
		return &account{pos: pos, name: name, sec: acc.GetSecUid(), id: id}, nil
	}
}

func profileURL(id *session.Identity, name string) string {
	u, _ := id.Base.Parse(task.ProfilePath(name))
	return u.String()
}

func postURL(id *session.Identity, name string, p post) string {
	kind := "/video/"
	if p.kind == ttv1.PostKind_POST_KIND_PHOTO {
		kind = "/photo/"
	}
	u, _ := id.Base.Parse(task.ProfilePath(name) + kind + strconv.FormatInt(p.id, 10))
	return u.String()
}

// ProbeQueryPrefix prefixes the operation name of the canary's own calls
// in the request metrics, so its checks of refused variants do not count
// as crawl traffic.
const ProbeQueryPrefix = "canary:"

// client returns a client whose registry holds only e, so a failure is
// e's own and not hidden by a fallback.
func (c *Canary) client(e *entry) *graphql.Client {
	q := e.Query
	q.Name = ProbeQueryPrefix + e.Name
	q.Status = registry.StatusActive
	var params map[string]string
	if c.Params != nil {
		params = c.Params()
	}
	return &graphql.Client{Registry: registry.NewStatic(registry.Seed{ClientParams: params, Queries: []registry.Query{q}}),
		Rules: c.Rules, Builder: c.Builder, Decode: c.Decode, Signer: c.Signer}
}

func (c *Canary) do(ctx context.Context, a *account, e *entry, vars map[string]any, referer string) (*graphql.Response, Check) {
	resp, err := c.client(e).Do(ctx, a.id, graphql.Request{Query: ProbeQueryPrefix + e.Name, Vars: vars, Referer: referer})
	if err != nil {
		if errors.Is(err, graphql.ErrSign) {
			return nil, Check{Verdict: Inconclusive, Reason: err.Error(), SignError: true}
		}
		return nil, Check{Verdict: Inconclusive, Reason: err.Error()}
	}
	switch resp.Class {
	case detect.OK:
		return resp, Check{Verdict: OK}
	case detect.RegistryStale:
		return nil, Check{Verdict: Stale, Reason: first(resp.Reason, "empty answer")}
	case detect.SchemaDrift:
		return nil, Check{Verdict: Drift, Reason: resp.Reason}
	default:
		return nil, Check{Verdict: Inconclusive, Reason: resp.Class.String() + ": " + resp.Reason}
	}
}

func drift(rep parse.Report) Check {
	return Check{Verdict: Drift, Reason: "missing required fields: " + strings.Join(rep.Missing, ", ")}
}

func (c *Canary) checkList(ctx context.Context, a *account, e *entry) (Check, []post) {
	resp, chk := c.do(ctx, a, e, map[string]any{"sec_uid": a.sec, "count": c.ListPageSize,
		"cursor": (c.now().Unix() + 1) * 1000}, profileURL(a.id, a.name))
	if resp == nil {
		return chk, nil
	}
	res, rep := parse.PostList(resp.Body, parse.Options{Query: e.Name, Variant: e.DocID})
	if !rep.OK() {
		return drift(rep), nil
	}
	if len(res.Posts) == 0 {
		return Check{Verdict: Inconclusive, Reason: "no posts"}, nil
	}
	var posts []post
	for _, b := range res.Posts {
		p := b.Post
		if p.GetCounts().GetComments() <= 0 || p.GetCommentsDisabled() {
			continue
		}
		posts = append(posts, post{id: p.GetPostId(), userID: p.GetUserId(), kind: p.GetKind(), created: p.GetCreatedAt(), n: p.GetCounts().GetComments()})
	}
	// Busy posts first: they are the likeliest to have reply threads.
	sort.SliceStable(posts, func(i, j int) bool { return posts[i].n > posts[j].n })
	return Check{Verdict: OK}, posts
}

func (c *Canary) checkComments(ctx context.Context, a *account, e *entry, p post) (Check, []parse.ThreadRef) {
	resp, chk := c.do(ctx, a, e, map[string]any{"item_id": p.id, "count": c.CommentsPageSize, "cursor": 0}, postURL(a.id, a.name, p))
	if resp == nil {
		return chk, nil
	}
	cc := parse.CommentContext{PostID: p.id, UserID: p.userID, PostCreatedAt: p.created}
	res, rep := parse.Comments(resp.Body, cc, parse.Options{Query: e.Name, Variant: e.DocID})
	if !rep.OK() {
		return drift(rep), nil
	}
	if len(res.Comments) == 0 {
		return Check{Verdict: Inconclusive, Reason: "no comments"}, nil
	}
	var threads []parse.ThreadRef
	for _, t := range res.Threads {
		if t.ReplyCount > 0 {
			threads = append(threads, t)
		}
	}
	sort.SliceStable(threads, func(i, j int) bool { return threads[i].ReplyCount > threads[j].ReplyCount })
	return Check{Verdict: OK}, threads
}

func (c *Canary) checkReplies(ctx context.Context, a *account, e *entry, p post, t parse.ThreadRef) Check {
	resp, chk := c.do(ctx, a, e, map[string]any{"item_id": p.id, "comment_id": t.CommentID, "count": c.RepliesPageSize, "cursor": 0},
		postURL(a.id, a.name, p))
	if resp == nil {
		return chk
	}
	cc := parse.CommentContext{PostID: p.id, UserID: p.userID, PostCreatedAt: p.created, Replies: true, ParentCommentID: t.CommentID}
	res, rep := parse.Comments(resp.Body, cc, parse.Options{Query: e.Name, Variant: e.DocID})
	if !rep.OK() {
		return drift(rep)
	}
	if len(res.Comments) == 0 {
		return Check{Verdict: Inconclusive, Reason: "no replies"}
	}
	return Check{Verdict: OK}
}

// recordChecks stores each verdict. A candidate or fallback that failed
// tt.canary.retire_after checks in a row is retired; the active variant
// never is (it is only demoted by a promotion).
func (c *Canary) recordChecks(ctx context.Context, entries []*entry, verdicts map[string]Check, rep *Report) error {
	batch := &pgx.Batch{}
	for _, e := range entries {
		v, ok := verdicts[key(e.Name, e.DocID)]
		if !ok {
			continue
		}
		if v.Signed && v.called {
			rep.Signer.Checks++
			if v.SignError {
				rep.Signer.Errors++
			}
		}
		note := string(v.Verdict)
		if v.Reason != "" {
			note += ": " + truncate(v.Reason, 200)
		}
		switch v.Verdict {
		case OK:
			e.failures = 0
			batch.Queue(`UPDATE operations SET last_checked_at = now(), last_check = $3, check_failures = 0,
				last_ok_at = now() WHERE name = $1 AND doc_id = $2`, e.Name, e.DocID, note)
		case Stale, Drift:
			e.failures++
			retire := e.Status != registry.StatusActive && c.Cfg.RetireAfter > 0 && e.failures >= c.Cfg.RetireAfter
			if retire && c.Cfg.Promote {
				rep.Retired = append(rep.Retired, key(e.Name, e.DocID))
				e.Status = registry.StatusRetired
			} else {
				retire = false
			}
			batch.Queue(`UPDATE operations SET last_checked_at = now(), last_check = $3, check_failures = $4,
				status = CASE WHEN $5 THEN 'retired' ELSE status END, updated_at = now()
				WHERE name = $1 AND doc_id = $2`, e.Name, e.DocID, note, e.failures, retire)
		default:
			batch.Queue(`UPDATE operations SET last_checked_at = now(), last_check = $3 WHERE name = $1 AND doc_id = $2`,
				e.Name, e.DocID, note)
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	return c.DB.SendBatch(ctx, batch).Close()
}

// decide promotes per operation: when the active variant failed, a
// passing candidate or fallback (unsigned ones first); when the active
// variant needs a signature, a passing unsigned one. It then keeps the
// other passing candidates as fallbacks, trims the fallbacks and sets the
// health gauges.
func (c *Canary) decide(ctx context.Context, entries []*entry, verdicts map[string]Check, rep *Report) error {
	groups := byName(entries)
	v := func(e *entry) Verdict { return verdicts[key(e.Name, e.DocID)].Verdict }
	for _, name := range sortedNames(groups) {
		list := groups[name]
		var active *entry
		for _, e := range list {
			if e.Status == registry.StatusActive {
				active = e
				break
			}
		}
		activeBad := active == nil || v(active) == Stale || v(active) == Drift
		passing := func(unsignedOnly bool) *entry {
			var signed *entry
			for _, e := range list {
				if (e.Status != registry.StatusCandidate && e.Status != registry.StatusFallback) || v(e) != OK {
					continue
				}
				if !ttclient.Signed(e.Query) {
					return e
				}
				if signed == nil && !unsignedOnly {
					signed = e
				}
			}
			return signed
		}
		var pick *entry
		why := ""
		switch {
		case activeBad:
			pick, why = passing(false), WhyFailed
		case v(active) == OK && ttclient.Signed(active.Query):
			pick, why = passing(true), WhyUnsigned
		}
		from := ""
		if active != nil {
			from = active.DocID
		}
		switch {
		case pick != nil && c.Cfg.Promote:
			if err := c.promote(ctx, name, pick.DocID); err != nil {
				return err
			}
			rep.Promoted = append(rep.Promoted, Promotion{Operation: name, From: from, To: pick.DocID, Why: why})
			metrics.CanaryPromotions.WithLabelValues(name, why).Inc()
			metrics.CanaryHealthy.WithLabelValues(name).Set(1)
			slog.Warn("canary: promoted", "operation", name, "from", from, "to", pick.DocID, "why", why)
		case active != nil && v(active) == OK:
			metrics.CanaryHealthy.WithLabelValues(name).Set(1)
		case active == nil || v(active).conclusive():
			rep.Failing = append(rep.Failing, name)
			metrics.CanaryHealthy.WithLabelValues(name).Set(0)
		default:
			// Inconclusive: the gauge keeps its last value.
			rep.Unchecked = append(rep.Unchecked, name)
		}
		if pick != nil && !c.Cfg.Promote {
			rep.Proposed = append(rep.Proposed, Promotion{Operation: name, From: from, To: pick.DocID, Why: why})
		}
		if !c.Cfg.Promote {
			continue // dry run: statuses stay as they are
		}
		// A passing candidate not promoted stays as a spare the client
		// falls back to on an empty answer.
		for _, e := range list {
			if e.Status == registry.StatusCandidate && v(e) == OK && (pick == nil || e.DocID != pick.DocID) {
				if _, err := c.DB.Exec(ctx, `UPDATE operations SET status = 'fallback', updated_at = now()
					WHERE name = $1 AND doc_id = $2 AND status = 'candidate'`, name, e.DocID); err != nil {
					return err
				}
			}
		}
		retired, err := c.trimFallbacks(ctx, name)
		if err != nil {
			return err
		}
		for _, d := range retired {
			rep.Retired = append(rep.Retired, key(name, d))
		}
	}
	return nil
}

func (c *Canary) promote(ctx context.Context, name, variant string) error {
	return pgx.BeginFunc(ctx, c.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE operations SET status = 'fallback', updated_at = now()
			WHERE name = $1 AND doc_id <> $2 AND status = 'active'`, name, variant); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE operations SET status = 'active', promoted_at = now(), updated_at = now()
			WHERE name = $1 AND doc_id = $2`, name, variant)
		return err
	})
}

// trimFallbacks retires all but the tt.canary.keep_fallbacks fallbacks of
// name most recently known to work.
func (c *Canary) trimFallbacks(ctx context.Context, name string) ([]string, error) {
	rows, err := c.DB.Query(ctx, `UPDATE operations SET status = 'retired', updated_at = now()
		WHERE name = $1 AND status = 'fallback' AND doc_id NOT IN (
			SELECT doc_id FROM operations WHERE name = $1 AND status = 'fallback'
			ORDER BY coalesce(last_ok_at, promoted_at) DESC NULLS LAST, updated_at DESC LIMIT $2)
		RETURNING doc_id`, name, max(c.Cfg.KeepFallbacks, 0))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func first(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
