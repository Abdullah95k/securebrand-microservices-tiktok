package api

import (
	"context"
	"testing"
)

// Bulk import through the API: a report per call, labels and sources on
// the accounts, the batch's later report, and the 1000-line limit.
func TestImportAPI(t *testing.T) {
	e := newEnv(t)
	var rep struct {
		Batch    string `json:"batch"`
		Added    int    `json:"added"`
		Joined   int    `json:"joined"`
		Already  int    `json:"already_monitored"`
		Dups     int    `json:"duplicates"`
		Rejected []struct {
			Line   int    `json:"line"`
			Reason string `json:"reason"`
		} `json:"not_accepted"`
		Queued int `json:"queued"`
		Queue  struct {
			Waiting   int  `json:"waiting"`
			Unlimited bool `json:"unlimited"`
		} `json:"queue"`
	}
	// tks has read acct_new's list (the seeded account): it needs no backfill.
	if _, err := e.db.Exec(context.Background(), `INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', 7001, 'complete')`); err != nil {
		t.Fatal(err)
	}
	e.want(403, "reader", "POST", "/accounts/import", map[string]any{"lines": []string{"x"}}, nil)
	e.want(400, "a", "POST", "/accounts/import", map[string]any{}, nil)
	e.want(400, "a", "POST", "/accounts/import", map[string]any{"lines": []string{"x"}, "labels": []string{"bad label"}}, nil)
	e.want(200, "a", "POST", "/accounts/import", map[string]any{"lines": []string{"@one", "One", "acct_new", "bad!", "https://vm.tiktok.com/x"},
		"labels": []string{"list:banks"}}, &rep)
	if rep.Added != 2 || rep.Dups != 1 || len(rep.Rejected) != 2 || rep.Queued != 1 || rep.Queue.Waiting != 1 || !rep.Queue.Unlimited {
		t.Fatalf("import: %+v", rep)
	}
	batch := rep.Batch
	// acct_new is admitted at once.
	var acc struct {
		Schedule map[string]any   `json:"schedule"`
		Sources  []map[string]any `json:"sources"`
		Labels   []string         `json:"labels"`
	}
	e.want(200, "a", "GET", "/accounts/acct_new", nil, &acc)
	if acc.Schedule["queued"] != false || acc.Schedule["origin"] != "owner" || len(acc.Labels) != 1 || acc.Labels[0] != "list:banks" ||
		len(acc.Sources) != 1 || acc.Sources[0]["how"] != "import" || acc.Sources[0]["via"] != batch {
		t.Fatalf("acct_new: %+v", acc)
	}
	e.want(200, "a", "GET", "/accounts/one", nil, &acc)
	if acc.Schedule["queued"] != true {
		t.Fatalf("a new account is not queued: %v", acc.Schedule)
	}
	// b imports a CSV naming one: joined, b's own label.
	e.want(200, "b", "POST", "/accounts/import", map[string]any{"text": "account,labels\n@one,mine\n"}, &rep)
	if rep.Joined != 1 {
		t.Fatalf("b's import: %+v", rep)
	}
	e.want(200, "b", "GET", "/accounts/one", nil, &acc)
	if len(acc.Labels) != 1 || acc.Labels[0] != "mine" || len(acc.Sources) != 2 {
		t.Fatalf("b's view: %+v", acc)
	}
	var list struct {
		Accounts []map[string]any `json:"accounts"`
	}
	e.want(200, "a", "GET", "/accounts?label=list:banks", nil, &list)
	if len(list.Accounts) != 2 {
		t.Fatalf("by label: %d", len(list.Accounts))
	}
	e.want(200, "b", "GET", "/accounts?label=list:banks", nil, &list)
	if len(list.Accounts) != 0 {
		t.Fatal("b sees a's labels")
	}
	e.want(200, "a", "GET", "/accounts?queued=true", nil, &list)
	if len(list.Accounts) != 1 || list.Accounts[0]["account_ref"] != "u:one" {
		t.Fatalf("queued: %v", list.Accounts)
	}
	e.want(200, "admin", "GET", "/accounts?source=import", nil, &list)
	if len(list.Accounts) != 2 {
		t.Fatalf("by source: %v", list.Accounts)
	}
	// The batch report: a's only.
	var br struct {
		Accounts []map[string]any `json:"accounts"`
	}
	e.want(200, "a", "GET", "/accounts/imports/"+batch, nil, &br)
	if len(br.Accounts) != 2 {
		t.Fatalf("batch report: %v", br)
	}
	e.want(404, "b", "GET", "/accounts/imports/"+batch, nil, nil)
	e.want(400, "a", "GET", "/accounts/imports/nope", nil, nil)
	// 1001 lines are too many for one call; disable_missing runs only dry.
	many := make([]string, 1001)
	for i := range many {
		many[i] = "acct" + string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676))
	}
	e.want(413, "a", "POST", "/accounts/import", map[string]any{"lines": many}, nil)
	e.want(400, "a", "POST", "/accounts/import", map[string]any{"lines": []string{"one"}, "disable_missing": true}, nil)
	e.want(200, "a", "POST", "/accounts/import", map[string]any{"lines": []string{"one"}, "disable_missing": true, "dry_run": true}, nil)
	var adm struct {
		Waiting int `json:"waiting"`
	}
	e.want(200, "reader", "GET", "/admission", nil, &adm)
	if adm.Waiting != 1 {
		t.Fatalf("admission: %+v", adm)
	}
}

// Removing an account blocks it for discovery; registering it again
// lifts the caller's block; labels go with the registration.
func TestRemovalBlocks(t *testing.T) {
	e := newEnv(t)
	e.want(201, "a", "POST", "/accounts", map[string]any{"account": "@leaver", "labels": []string{"x"}}, nil)
	e.want(204, "a", "DELETE", "/accounts/leaver", nil, nil)
	if n := one[int64](t, e.db, `SELECT count(*) FROM account_blocks WHERE account_ref = 'u:leaver'`); n != 1 {
		t.Fatalf("%d blocks", n)
	}
	if n := one[int64](t, e.db, `SELECT count(*) FROM account_labels WHERE account_ref = 'u:leaver'`); n != 0 {
		t.Fatalf("%d labels left", n)
	}
	// An account never registered can be removed too: it stays out.
	e.want(204, "a", "DELETE", "/accounts/never_seen", nil, nil)
	e.want(201, "a", "POST", "/accounts", map[string]any{"account": "@leaver"}, nil)
	if n := one[int64](t, e.db, `SELECT count(*) FROM account_blocks`); n != 1 {
		t.Fatalf("%d blocks after re-registering, want 1 (never_seen)", n)
	}
}

// Keywords: discovery:read lists, discovery:write adds and switches.
func TestKeywordsAPI(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	for name, scopes := range map[string][]string{"dr": {ScopeDiscoveryRead}, "dw": {ScopeDiscoveryRead, ScopeDiscoveryWrite}} {
		key, id, err := CreateClient(ctx, e.db, name, scopes, 0)
		if err != nil {
			t.Fatal(err)
		}
		e.keys[name], e.ids[name] = key, id
	}
	var k struct {
		Keyword struct {
			ID      int64 `json:"id"`
			Enabled bool  `json:"enabled"`
		} `json:"keyword"`
		Created bool `json:"created"`
	}
	e.want(403, "dr", "POST", "/discovery/keywords", map[string]any{"term": "Erbil", "language": "en"}, nil)
	e.want(403, "reader", "GET", "/discovery/keywords", nil, nil)
	e.want(201, "dw", "POST", "/discovery/keywords", map[string]any{"term": "Erbil", "language": "en", "group": "erbil"}, &k)
	e.want(200, "dw", "POST", "/discovery/keywords", map[string]any{"term": "ERBIL", "language": "en"}, &k)
	if k.Created {
		t.Fatal("a folded spelling was added again")
	}
	e.want(400, "dw", "POST", "/discovery/keywords", map[string]any{"term": "Paris", "language": "fr"}, nil)
	e.want(200, "dw", "PATCH", "/discovery/keywords/"+itoa(k.Keyword.ID), map[string]any{"enabled": false}, nil)
	e.want(404, "dw", "PATCH", "/discovery/keywords/999999", map[string]any{"enabled": false}, nil)
	var list struct {
		Keywords []map[string]any `json:"keywords"`
	}
	e.want(200, "dr", "GET", "/discovery/keywords?enabled=false", nil, &list)
	if len(list.Keywords) != 1 || list.Keywords[0]["term"] != "Erbil" {
		t.Fatalf("keywords: %v", list)
	}
	e.want(400, "dr", "GET", "/discovery/keywords?enabled=maybe", nil, nil)
}

func itoa(v int64) string {
	b := []byte{}
	for {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
		if v == 0 {
			return string(b)
		}
	}
}
