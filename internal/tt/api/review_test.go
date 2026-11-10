package api

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

// The review page's API: found accounts by every filter, removing one
// (discovery stops monitoring it), the cost and capacity figures, the
// imports list, keyword hits, and their scopes.
func TestReviewAPI(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	for name, scopes := range map[string][]string{
		"dr":  {ScopeDiscoveryRead},
		"dw":  {ScopeDiscoveryRead, ScopeDiscoveryWrite},
		"dwp": {ScopeDiscoveryRead, ScopeDiscoveryWrite, ScopePIIRead},
	} {
		key, id, err := CreateClient(ctx, e.db, name, scopes, 0)
		if err != nil {
			t.Fatal(err)
		}
		e.keys[name], e.ids[name] = key, id
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := e.db.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// Discovery found three accounts and added one (81, registered to its
	// client); a search for keyword 4 found 82.
	dc, err := discovery.EnsureClient(ctx, e.db, "discovery")
	if err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO discovery_candidates (account_ref, user_id, username, state, score, evidence, first_seen_at, last_seen_at, added_at, schedule_ref)
		VALUES ('sec:s81', 81, 'found_one', 'added', 1.5, '{"score":1.5,"pass":true,"evidence":[{"signal":"posts_in_iraq","strength":"strong","count":3,"of":15}]}',
			now() - interval '1 hour', now(), now(), 'sec:s81'),
		('sec:s82', 82, 'found_two', 'not_iraqi', 0.5, NULL, now() - interval '30 minutes', now(), NULL, NULL),
		('sec:s83', 83, NULL, 'dormant', NULL, NULL, now(), now(), NULL, NULL)`)
	exec(`INSERT INTO account_sources (account_ref, how, via, user_id, first_at, last_at) VALUES
		('sec:s81', 'mention', '7001', 81, now(), now()), ('sec:s82', 'search', '4', 82, now(), now()), ('sec:s83', 'commenter', '7001', 83, now(), now())`)
	exec(`INSERT INTO account_schedule (account_ref, user_id, interval_seconds, next_run_at, origin) VALUES ('sec:s81', 81, 9900, now(), 'discovery')`)
	exec(`INSERT INTO account_registrations (api_client_id, account_ref) VALUES ($1, 'sec:s81')`, dc)

	type found struct {
		ID       int64    `json:"id"`
		UserID   string   `json:"user_id"`
		Username *string  `json:"username"`
		State    string   `json:"state"`
		Sources  []string `json:"sources"`
	}
	var list struct {
		Accounts []found          `json:"accounts"`
		Counts   map[string]int64 `json:"counts"`
		Next     string           `json:"next"`
	}
	e.want(403, "reader", "GET", "/discovery/accounts", nil, nil)
	e.want(200, "dr", "GET", "/discovery/accounts", nil, &list)
	if len(list.Accounts) != 3 || list.Accounts[0].UserID != "83" || list.Accounts[0].Username != nil || list.Counts["added"] != 1 {
		t.Fatalf("all: %+v", list)
	}
	e.want(200, "dwp", "GET", "/discovery/accounts?source=search&via=4", nil, &list)
	if len(list.Accounts) != 1 || list.Accounts[0].Username == nil || *list.Accounts[0].Username != "found_two" ||
		len(list.Accounts[0].Sources) != 1 || list.Accounts[0].Sources[0] != "search" {
		t.Fatalf("keyword 4: %+v", list.Accounts)
	}
	e.want(200, "dr", "GET", "/discovery/accounts?min_score=1", nil, &list)
	if len(list.Accounts) != 1 || list.Accounts[0].UserID != "81" {
		t.Fatalf("score from 1: %+v", list.Accounts)
	}
	e.want(200, "dr", "GET", "/discovery/accounts?order=score&limit=1", nil, &list)
	if len(list.Accounts) != 1 || list.Accounts[0].UserID != "81" || !strings.Contains(list.Next, ":") {
		t.Fatalf("by score: %+v", list)
	}
	e.want(200, "dr", "GET", "/discovery/accounts?order=score&limit=1&after="+list.Next, nil, &list)
	if len(list.Accounts) != 1 || list.Accounts[0].UserID != "82" {
		t.Fatalf("by score, page 2: %+v", list)
	}
	for _, q := range []string{"source=nope", "via=4", "order=random", "min_score=high", "since=yesterday", "order=score&after=12", "state=gone"} {
		e.want(400, "dr", "GET", "/discovery/accounts?"+q, nil, nil)
	}

	// Removing 81: discovery's registration goes, its checks stop.
	var removed struct {
		State   string `json:"state"`
		Stopped bool   `json:"monitoring_stopped"`
		Still   bool   `json:"still_monitored"`
	}
	id81 := one[int64](t, e.db, `SELECT id FROM discovery_candidates WHERE user_id = 81`)
	e.want(403, "dr", "POST", "/discovery/accounts/"+itoa(id81)+"/remove", nil, nil)
	e.want(404, "dw", "POST", "/discovery/accounts/999999/remove", nil, nil)
	e.want(400, "dw", "POST", "/discovery/accounts/x/remove", nil, nil)
	e.want(200, "dw", "POST", "/discovery/accounts/"+itoa(id81)+"/remove", nil, &removed)
	if removed.State != "blocked" || !removed.Stopped || removed.Still {
		t.Fatalf("removed: %+v", removed)
	}
	if on := one[bool](t, e.db, `SELECT enabled FROM account_schedule WHERE account_ref = 'sec:s81'`); on {
		t.Fatal("81 is still checked")
	}
	if n := one[int64](t, e.db, `SELECT count(*) FROM account_blocks WHERE account_ref = 'sec:s81' AND api_client_id = $1`, e.ids["dw"]); n != 1 {
		t.Fatalf("%d blocks", n)
	}

	// Cost and capacity.
	var stats struct {
		Days []struct {
			Day   string           `json:"day"`
			Found map[string]int64 `json:"found"`
		} `json:"days"`
		Counts    map[string]int64 `json:"counts"`
		Admission map[string]any   `json:"admission"`
		Sources   map[string]bool  `json:"sources"`
		Pass      float64          `json:"score_pass"`
	}
	e.want(200, "dr", "GET", "/discovery/stats?days=3", nil, &stats)
	if len(stats.Days) != 3 || stats.Days[2].Found["search"] != 1 || stats.Counts["blocked"] != 1 || stats.Admission == nil ||
		len(stats.Sources) != len(config.DiscoverySources) || stats.Pass <= 0 {
		t.Fatalf("stats: %+v", stats)
	}
	e.want(400, "dr", "GET", "/discovery/stats?days=40", nil, nil)
	e.want(403, "reader", "GET", "/discovery/stats", nil, nil)

	// Keyword hits.
	var kws struct {
		Keywords []map[string]any `json:"keywords"`
	}
	exec(`INSERT INTO discovery_keywords (id, term, normalized, language) VALUES (4, 'Basra', 'basra', 'en'), (5, 'Mosul', 'mosul', 'en')`)
	e.want(200, "dr", "GET", "/discovery/keywords?hits=true", nil, &kws)
	if len(kws.Keywords) != 2 || kws.Keywords[0]["accounts_found"] != float64(1) || kws.Keywords[1]["accounts_found"] != float64(0) {
		t.Fatalf("hits: %v", kws.Keywords)
	}
	kws.Keywords = nil
	e.want(200, "dr", "GET", "/discovery/keywords", nil, &kws)
	if _, ok := kws.Keywords[0]["accounts_found"]; ok {
		t.Fatal("hits without hits=true")
	}

	// Imports: each client sees its own; admin every one.
	e.want(200, "a", "POST", "/accounts/import", map[string]any{"lines": []string{"@one"}, "dry_run": true}, nil)
	e.want(200, "b", "POST", "/accounts/import", map[string]any{"lines": []string{"@two"}, "labels": []string{"x"}}, nil)
	var imps struct {
		Imports []struct {
			Batch  string           `json:"batch"`
			DryRun bool             `json:"dry_run"`
			Labels []string         `json:"labels"`
			Counts map[string]int64 `json:"counts"`
		} `json:"imports"`
		NextBefore string `json:"next_before"`
	}
	e.want(200, "a", "GET", "/imports", nil, &imps)
	if len(imps.Imports) != 1 || !imps.Imports[0].DryRun || imps.Imports[0].Counts["lines"] != 1 {
		t.Fatalf("a's imports: %+v", imps)
	}
	e.want(200, "admin", "GET", "/imports?limit=1", nil, &imps)
	if len(imps.Imports) != 1 || imps.Imports[0].Labels[0] != "x" || imps.NextBefore == "" {
		t.Fatalf("admin, page 1: %+v", imps)
	}
	e.want(200, "admin", "GET", "/imports?limit=1&before="+imps.NextBefore, nil, &imps)
	if len(imps.Imports) != 1 || !imps.Imports[0].DryRun {
		t.Fatalf("admin, page 2: %+v", imps)
	}
	e.want(403, "dr", "GET", "/imports", nil, nil)
	e.want(400, "a", "GET", "/imports?before=soon", nil, nil)
}

// A change a browser marks as sent from another site is refused; reads,
// programs and the API's own page are not.
func TestCrossSiteChangesRefused(t *testing.T) {
	e := newEnv(t)
	send := func(method, path, site string) int {
		t.Helper()
		var body io.Reader
		if method != http.MethodGet {
			body = strings.NewReader(`{"account":"@someone"}`)
		}
		req, _ := http.NewRequest(method, e.srv.URL+"/v1"+path, body)
		req.Header.Set("Authorization", "Bearer "+e.keys["a"])
		if site != "" {
			req.Header.Set("Sec-Fetch-Site", site)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	for _, site := range []string{"cross-site", "same-site", "Cross-Site"} {
		if c := send("POST", "/accounts", site); c != 403 {
			t.Errorf("POST from %s: %d", site, c)
		}
	}
	if c := send("DELETE", "/accounts/someone", "cross-site"); c != 403 {
		t.Errorf("DELETE from another site: %d", c)
	}
	if c := send("GET", "/accounts", "cross-site"); c != 200 {
		t.Errorf("GET from another site: %d", c)
	}
	if c := send("POST", "/accounts", "same-origin"); c/100 != 2 {
		t.Errorf("POST from the page: %d", c)
	}
	if c := send("POST", "/accounts", ""); c/100 != 2 {
		t.Errorf("POST from a program: %d", c)
	}
}

// The page: served without a key (it holds no data), with headers that
// run only its own script and let nothing frame it; read-only.
func TestReviewPage(t *testing.T) {
	e := newEnv(t)
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Get(e.srv.URL + "/review")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMovedPermanently || resp.Header.Get("Location") != "/review/" {
		t.Fatalf("/review: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	for path, kind := range map[string]string{"/review/": "text/html", "/review/review.js": "javascript", "/review/review.css": "text/css"} {
		resp, err := http.Get(e.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		h := resp.Header
		if resp.StatusCode != 200 || !strings.Contains(h.Get("Content-Type"), kind) || len(b) < 100 {
			t.Fatalf("%s: %d %s %d bytes", path, resp.StatusCode, h.Get("Content-Type"), len(b))
		}
		if h.Get("Content-Security-Policy") != reviewCSP || h.Get("X-Frame-Options") != "DENY" || h.Get("X-Content-Type-Options") != "nosniff" ||
			h.Get("Referrer-Policy") != "no-referrer" {
			t.Fatalf("%s headers: %v", path, h)
		}
	}
	resp, err = http.Get(e.srv.URL + "/review/missing.js")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("a missing file: %d", resp.StatusCode)
	}
	resp, err = http.Post(e.srv.URL+"/review/", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST to the page: %d", resp.StatusCode)
	}
}

// The page writes what the API returns as text, never as markup, and runs
// no script but its own file: the CSP allows nothing inline.
func TestReviewPageWritesTextOnly(t *testing.T) {
	js, err := os.ReadFile("review/review.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function", "localStorage"} {
		if strings.Contains(string(js), bad) {
			t.Errorf("review.js uses %s", bad)
		}
	}
	html, err := os.ReadFile("review/index.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"<script>", " onclick=", " onload=", " onsubmit=", "style=", "http://", "https://"} {
		if strings.Contains(string(html), bad) {
			t.Errorf("index.html has %q", bad)
		}
	}
}
