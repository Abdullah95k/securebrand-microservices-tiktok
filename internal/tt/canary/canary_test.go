package canary_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/transport"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/app"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/canary"
	ttclient "github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/client"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// signer adds the parameter fakett checks, as TikTok's script adds its
// signature; with fail set it cannot sign, as a signer with no page.
type signer struct{ fail atomic.Bool }

func (s *signer) SignURL(_ context.Context, _ *session.Identity, _, raw, _ string) (string, error) {
	if s.fail.Load() {
		return "", errors.New("no signer page")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("X-Gnarly", "canary-test")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

type env struct {
	c      *app.Canary
	fake   *fakett.Server
	signer *signer
	// wrap, when set, answers instead of fakett.
	wrap atomic.Pointer[func(w http.ResponseWriter, r *http.Request) bool]
}

func start(t *testing.T, accounts int, cfg func(*config.TTCanary)) *env {
	t.Helper()
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	seed, err := ttclient.Seed()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.ApplySeedTo(ctx, db, ttstore.Registry, seed); err != nil {
		t.Fatal(err)
	}
	w := fakett.NewWorld(fakett.Spec{Seed: 3, Accounts: accounts, PostsPerAccount: 6, MaxComments: 30, MaxReplies: 5, PostSpacing: 20 * time.Hour})
	e := &env{signer: &signer{}}
	e.fake = fakett.NewServer(w, fakett.Options{Faults: fakett.Faults{RequireSignature: true,
		SignedPaths: []string{"/api/comment/list/", "/api/comment/list/reply/"}}})
	hs := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if f := e.wrap.Load(); f != nil && (*f)(rw, r) {
			return
		}
		e.fake.ServeHTTP(rw, r)
	}))
	t.Cleanup(hs.Close)
	c, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	c.TT.BaseURL = hs.URL
	c.TT.IdentityRPS, c.TT.IdentityBurst = 500, 20
	c.TT.Signer.Mode = config.SignerBrowser
	for _, a := range w.Accounts() {
		c.TT.Canary.Accounts = append(c.TT.Canary.Accounts, a.Username)
	}
	if cfg != nil {
		cfg(&c.TT.Canary)
	}
	e.c, err = app.NewCanary(ctx, c, db, app.CanaryOptions{Signer: e.signer,
		DoerFactory: func(transport.Options) (transport.Doer, error) { return transport.NewStd(10 * time.Second), nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.c.Close)
	return e
}

func (e *env) run(t *testing.T) canary.Report {
	t.Helper()
	ctx := context.Background()
	if err := e.c.Registry.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	rep, err := e.c.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return rep
}

func (e *env) status(t *testing.T, name, variant string) string {
	t.Helper()
	var st string
	if err := e.c.DB.QueryRow(context.Background(), `SELECT status FROM operations WHERE name = $1 AND doc_id = $2`,
		name, variant).Scan(&st); err != nil {
		t.Fatalf("%s/%s: %v", name, variant, err)
	}
	return st
}

func check(rep canary.Report, op, variant string) canary.Check {
	for _, c := range rep.Checks {
		if c.Operation == op && c.Variant == variant {
			return c
		}
	}
	return canary.Check{}
}

func gauge(op string) float64 {
	var m dto.Metric
	_ = metrics.CanaryHealthy.WithLabelValues(op).Write(&m)
	return m.GetGauge().GetValue()
}

// One account refusing a variant is not enough: it may be the identity.
func TestOneRefusalIsNotStale(t *testing.T) {
	e := start(t, 3, nil)
	var refused atomic.Int64
	f := func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/api/creator/item_list/" && refused.Add(1) == 1 {
			w.WriteHeader(http.StatusOK) // an empty answer, once
			return true
		}
		return false
	}
	e.wrap.Store(&f)
	rep := e.run(t)
	if rep.Status != canary.StatusOK || len(rep.Promoted) != 0 {
		t.Fatalf("status %s, promoted %+v: one refusal made a promotion", rep.Status, rep.Promoted)
	}
	if v := check(rep, "creator_list", "creator").Verdict; v != canary.OK {
		t.Errorf("creator verdict %s, want ok on the next account", v)
	}
	if st := e.status(t, "creator_list", "creator"); st != registry.StatusActive {
		t.Errorf("creator is %s, want active", st)
	}
}

// An answer the parser cannot read is drift: nothing replaces it when the
// twin drifts too, and the operation is failing.
func TestDrift(t *testing.T) {
	e := start(t, 2, nil)
	f := func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/api/creator/item_list/" {
			return false
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"statusCode":0,"hasMore":false,"itemList":[{"id":"7300000000000000001"}]}`))
		return true
	}
	e.wrap.Store(&f)
	rep := e.run(t)
	if rep.Status != canary.StatusDegraded || !strings.Contains(strings.Join(rep.Failing, ","), "creator_list") {
		t.Fatalf("status %s, failing %v; want creator_list failing", rep.Status, rep.Failing)
	}
	if c := check(rep, "creator_list", "creator"); c.Verdict != canary.Drift || !strings.Contains(c.Reason, "createTime") {
		t.Errorf("creator check %+v, want drift naming the missing field", c)
	}
	if g := gauge("creator_list"); g != 0 {
		t.Errorf("creator_list healthy gauge %v, want 0", g)
	}
	// Comments had no list to read posts from.
	if !strings.Contains(strings.Join(rep.Unchecked, ","), "comments") {
		t.Errorf("unchecked %v, want comments", rep.Unchecked)
	}
}

// A dry run proposes and changes nothing.
func TestDryRun(t *testing.T) {
	e := start(t, 2, func(c *config.TTCanary) { c.Promote = false })
	f := fakett.Faults{RequireSignature: true,
		SignedPaths: []string{"/api/comment/list/", "/api/comment/list/reply/", "/api/creator/item_list/"}}
	e.fake.SetFaults(f)
	rep := e.run(t)
	if len(rep.Promoted) != 0 || len(rep.Proposed) != 1 || rep.Proposed[0].To != "creator-signed" {
		t.Fatalf("promoted %+v, proposed %+v; want creator-signed proposed only", rep.Promoted, rep.Proposed)
	}
	if st := e.status(t, "creator_list", "creator"); st != registry.StatusActive {
		t.Errorf("creator is %s after a dry run, want active", st)
	}
	if st := e.status(t, "creator_list", "creator-signed"); st != registry.StatusCandidate {
		t.Errorf("creator-signed is %s after a dry run, want candidate", st)
	}
}

// With the signer down nothing signed can be judged: the unsigned twins
// are refused, nothing is promoted, the signed operations are unchecked
// and the signer gauge says so.
func TestSignerDown(t *testing.T) {
	e := start(t, 2, nil)
	e.signer.fail.Store(true)
	rep := e.run(t)
	if len(rep.Promoted) != 0 {
		t.Fatalf("promoted %+v with the signer down", rep.Promoted)
	}
	if rep.Signer.Checks == 0 || rep.Signer.Errors != rep.Signer.Checks {
		t.Errorf("signer %+v, want every signed check failed", rep.Signer)
	}
	var m dto.Metric
	_ = metrics.CanarySigner.Write(&m)
	if v := m.GetGauge().GetValue(); v != 0 {
		t.Errorf("signer gauge %v, want 0", v)
	}
	if st := e.status(t, "comments", "web-signed"); st != registry.StatusActive {
		t.Errorf("comments web-signed is %s, want active", st)
	}
	for _, op := range []string{"comments", "replies"} {
		if !strings.Contains(strings.Join(rep.Unchecked, ","), op) {
			t.Errorf("unchecked %v, want %s", rep.Unchecked, op)
		}
	}
	// Signed again: healthy, and the refused unsigned twins retire after
	// retire_after runs.
	e.signer.fail.Store(false)
	for range 3 {
		rep = e.run(t)
	}
	if rep.Status != canary.StatusOK {
		t.Fatalf("status %s once signing works, failing %v unchecked %v", rep.Status, rep.Failing, rep.Unchecked)
	}
	if st := e.status(t, "comments", "web"); st != registry.StatusRetired {
		t.Errorf("comments web (refused unsigned) is %s after 3 runs, want retired", st)
	}
}

// Without accounts the run is an error, recorded.
func TestNoAccounts(t *testing.T) {
	e := start(t, 1, nil)
	e.c.Cfg.Accounts = nil
	rep, err := e.c.Run(context.Background())
	if err == nil || rep.Status != canary.StatusError {
		t.Fatalf("status %s, err %v; want an error", rep.Status, err)
	}
	var n int
	if err := e.c.DB.QueryRow(context.Background(), `SELECT count(*) FROM canary_runs WHERE status = 'error'`).Scan(&n); err != nil || n != 1 {
		t.Errorf("%d error runs recorded (%v), want 1", n, err)
	}
}

// One run at a time across replicas: while another process holds the
// canary's lease, a run is skipped and reads nothing (a lease, not a
// session advisory lock, which PgBouncer's transaction mode would share).
func TestSkippedWhileAnotherRuns(t *testing.T) {
	e := start(t, 1, nil)
	ctx := context.Background()
	if _, err := e.c.DB.Exec(ctx, `INSERT INTO leases (name, holder, until) VALUES ('tks:canary', 'another-pod', now() + interval '1 minute')`); err != nil {
		t.Fatal(err)
	}
	rep, err := e.c.Run(ctx)
	if err != nil || rep.Status != canary.StatusSkipped {
		t.Fatalf("status %s, err %v; want skipped", rep.Status, err)
	}
	if _, err := e.c.DB.Exec(ctx, `UPDATE leases SET until = now() - interval '1 second' WHERE name = 'tks:canary'`); err != nil {
		t.Fatal(err)
	}
	if rep, _ := e.c.Run(ctx); rep.Status == canary.StatusSkipped {
		t.Fatal("skipped after the other run's lease expired")
	}
}
