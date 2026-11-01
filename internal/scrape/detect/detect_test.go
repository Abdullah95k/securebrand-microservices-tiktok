package detect

import (
	"strings"
	"testing"
)

func TestClassify(t *testing.T) {
	r := DefaultRules()
	cases := []struct {
		name string
		in   Input
		want Class
	}{
		{"ok", Input{Status: 200, Body: []byte(`{"data":{}}`), Expect: "graphql"}, OK},
		{"429", Input{Status: 429, Expect: "graphql"}, RateLimited},
		{"login redirect", Input{Status: 302, Location: "https://www.facebook.com/login/?next=x", Expect: "html"}, LoginWall},
		{"checkpoint redirect", Input{Status: 302, Location: "/checkpoint/block/", Expect: "html"}, Checkpoint},
		{"rate limit code", Input{Status: 200, Errors: []GQLError{{Code: 1675004}}, Expect: "graphql"}, RateLimited},
		{"login code", Input{Status: 200, Errors: []GQLError{{Code: 1357005, Message: "Please log in"}}, Expect: "graphql"}, LoginWall},
		{"soft code", Input{Status: 200, Errors: []GQLError{{Code: 1357004}}, Expect: "graphql"}, SoftError},
		{"stale doc", Input{Status: 200, Errors: []GQLError{{Code: 1675012, Message: "PersistedQueryNotFound: doc_id"}}, Expect: "graphql"}, RegistryStale},
		{"html login page", Input{Status: 200, ContentType: "text/html", Body: []byte(`<form id="login_form">`), Expect: "graphql"}, LoginWall},
		{"html block page", Input{Status: 200, ContentType: "text/html", Body: []byte(`You're Temporarily Blocked`), Expect: "graphql"}, RateLimited},
		{"html error page", Input{Status: 200, ContentType: "application/json", Body: []byte("\n<!DOCTYPE html><title>Error</title>"), Expect: "graphql"}, SoftError},
		// Facebook labels its GraphQL JSON text/html.
		{"json labelled html", Input{Status: 200, ContentType: `text/html; charset="utf-8"`, Body: []byte(`{"data":{"node":{}}}`), HasData: true, Expect: "graphql"}, OK},
		{"guarded error labelled html", Input{Status: 200, ContentType: `text/html; charset="utf-8"`,
			Body: []byte(`for (;;);{"error":1675004}`), Errors: []GQLError{{Code: 1675004}}, Expect: "graphql"}, RateLimited},
		{"proxy auth", Input{Status: 407, Expect: "graphql"}, Transport},
		{"404 page", Input{Status: 404, Expect: "html"}, ContentUnavailable},
		{"unknown error", Input{Status: 200, Errors: []GQLError{{Code: 99}}, Expect: "graphql"}, SoftError},
		// A real timeline page streams field errors beside its posts.
		{"partial field error", Input{Status: 200, HasData: true, Expect: "graphql",
			Errors: []GQLError{{Message: "A server error field_exception occured.", Severity: "ERROR"}}}, OK}, //nolint:misspell // Facebook's own spelling
		{"critical error with data", Input{Status: 200, HasData: true, Expect: "graphql",
			Errors: []GQLError{{Code: 99, Severity: "CRITICAL"}}}, SoftError},
		{"rate limit code with data", Input{Status: 200, HasData: true, Errors: []GQLError{{Code: 1675004}}, Expect: "graphql"}, RateLimited},
	}
	for _, c := range cases {
		if got, reason := r.Classify(c.in); got != c.want {
			t.Errorf("%s: got %v (%s), want %v", c.name, got, reason, c.want)
		}
	}
}

// TestPlatformWallRules checks that wall paths and page markers are rule
// data: Facebook's defaults behave as before, and another platform's rules
// (here Instagram-like paths and a synthetic marker) classify its walls.
func TestPlatformWallRules(t *testing.T) {
	fb := DefaultRules()
	other := DefaultRules()
	other.CheckpointPaths = append(other.CheckpointPaths, "/challenge")
	other.LoginPaths = append(other.LoginPaths, "/accounts/login")
	other.LoginPageMarkers = [][]byte{[]byte(`data-test-login-wall`)}

	challenge := Input{Status: 302, Location: "https://www.example.com/challenge/?next=/p/", Expect: "html"}
	loginPage := Input{Status: 200, ContentType: "text/html", Body: []byte(`<div data-test-login-wall>`), Expect: "html"}
	checkpointPage := Input{Status: 200, ContentType: "text/html", Body: []byte(`<form action="/checkpoint/block/">`), Expect: "html"}

	cases := []struct {
		name  string
		rules Rules
		in    Input
		want  Class
	}{
		{"fb: unknown redirect stays soft", fb, challenge, SoftError},
		{"fb: no login page markers", fb, loginPage, OK},
		{"fb: checkpoint page", fb, checkpointPage, Checkpoint},
		{"other: challenge redirect", other, challenge, Checkpoint},
		{"other: login redirect", other, Input{Status: 302, Location: "/accounts/login/?next=%2Fx%2F", Expect: "html"}, LoginWall},
		{"other: login page", other, loginPage, LoginWall},
		{"other: checkpoint page", other, checkpointPage, Checkpoint},
	}
	for _, c := range cases {
		if got, why := c.rules.Classify(c.in); got != c.want {
			t.Errorf("%s: got %v (%s), want %v", c.name, got, why, c.want)
		}
	}
}

// TestStatusCodeLists checks the code lists a platform without GraphQL
// errors uses (TikTok's statusCode, mapped to GQLError codes by its
// decoder), and that Facebook's defaults leave them empty.
func TestStatusCodeLists(t *testing.T) {
	if d := DefaultRules(); len(d.CheckpointCodes) != 0 || len(d.UnavailableCodes) != 0 || d.EmptyIsStale {
		t.Fatalf("Facebook's defaults must not set the new lists: %+v", d)
	}
	r := Rules{CheckpointCodes: []int64{10000}, UnavailableCodes: []int64{10216, 10222}, RateLimitCodes: []int64{10101}}
	cases := []struct {
		name string
		code int64
		want Class
	}{
		{"captcha", 10000, Checkpoint},
		{"private post", 10216, ContentUnavailable},
		{"private account", 10222, ContentUnavailable},
		{"rate limit", 10101, RateLimited},
		{"unknown", 42, SoftError},
	}
	for _, c := range cases {
		if got, reason := r.Classify(Input{Status: 200, Errors: []GQLError{{Code: c.code}}, Expect: "graphql"}); got != c.want {
			t.Errorf("%s: got %v (%s), want %v", c.name, got, reason, c.want)
		}
	}
}

// TestHTTPStatuses checks the 4xx statuses that read as OK before M2 when
// their body held no error (fbs reference, chapter 1 §15), on both kinds
// of request, and that a platform's Statuses rules come first.
func TestHTTPStatuses(t *testing.T) {
	cases := []struct {
		status int
		expect string
		want   Class
	}{
		{401, "graphql", LoginWall},
		{401, "html", LoginWall},
		{403, "graphql", RateLimited},
		{403, "html", RateLimited},
		{400, "graphql", RegistryStale},
		{400, "html", SoftError},
		{404, "graphql", SoftError},
		{404, "html", ContentUnavailable},
		{410, "graphql", SoftError},
		{451, "html", SoftError},
	}
	for _, r := range []Rules{DefaultRules(), {}} {
		for _, c := range cases {
			// A JSON body without errors, as an API might send.
			in := Input{Status: c.status, Body: []byte(`{"data":{}}`), HasData: true, Expect: c.expect}
			if got, reason := r.Classify(in); got != c.want || reason != "http "+itoa(c.status) {
				t.Errorf("%d %s: got %v (%s), want %v", c.status, c.expect, got, reason, c.want)
			}
		}
	}
	r := Rules{Statuses: []StatusRule{{Status: 403, Expect: "html", Class: ContentUnavailable}, {Status: 400, Class: SoftError}}}
	for _, c := range []struct {
		status int
		expect string
		want   Class
	}{
		{403, "html", ContentUnavailable},
		{403, "graphql", RateLimited}, // the rule is for page loads only
		{400, "graphql", SoftError},
	} {
		if got, _ := r.Classify(Input{Status: c.status, Expect: c.expect}); got != c.want {
			t.Errorf("rule for %d %s: got %v, want %v", c.status, c.expect, got, c.want)
		}
	}
}

// TestOverrides checks that configuration replaces the lists it sets, keeps
// the ones it leaves unset, adds status rules, and reports what it cannot
// read.
func TestOverrides(t *testing.T) {
	base := DefaultRules()
	r, err := base.Apply(Overrides{
		RateLimitCodes: []int64{77},
		BlockMarkers:   []string{"slow down"},
		StaleMessages:  []string{`(?i)operation gone`},
		LoginPaths:     []string{},
		Statuses:       map[string]string{"403": "checkpoint", "html:403": "content_unavailable", "graphql:418": "ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		in   Input
		want Class
	}{
		{"new rate limit code", Input{Status: 200, Errors: []GQLError{{Code: 77}}, Expect: "graphql"}, RateLimited},
		{"replaced rate limit code", Input{Status: 200, Errors: []GQLError{{Code: 1675004}}, Expect: "graphql"}, SoftError},
		{"kept login code", Input{Status: 200, Errors: []GQLError{{Code: 1357005}}, Expect: "graphql"}, LoginWall},
		{"new block marker", Input{Status: 200, ContentType: "text/html", Body: []byte(`<p>slow down</p>`), Expect: "graphql"}, RateLimited},
		{"new stale message", Input{Status: 200, Errors: []GQLError{{Code: 5, Message: "Operation gone"}}, Expect: "graphql"}, RegistryStale},
		{"login paths emptied", Input{Status: 302, Location: "/login/?next=x", Expect: "html"}, SoftError},
		{"status for both kinds", Input{Status: 403, Expect: "graphql"}, Checkpoint},
		{"status for page loads wins", Input{Status: 403, Expect: "html"}, ContentUnavailable},
		{"status read as ok", Input{Status: 418, Body: []byte(`{"data":{}}`), HasData: true, Expect: "graphql"}, OK},
	}
	for _, c := range cases {
		if got, why := r.Classify(c.in); got != c.want {
			t.Errorf("%s: got %v (%s), want %v", c.name, got, why, c.want)
		}
	}
	if len(base.RateLimitCodes) != 4 || len(base.LoginPaths) != 1 {
		t.Fatal("Apply changed the rules it was called on")
	}
	if same, err := base.Apply(Overrides{}); err != nil || len(same.Statuses) != 0 || len(same.StaleMessages) != len(base.StaleMessages) {
		t.Fatalf("empty overrides changed the rules: %v", err)
	}
	_, err = base.Apply(Overrides{StaleMessages: []string{"("}, Statuses: map[string]string{"abc": "ok", "json:400": "ok", "400": "blocked"}})
	if err == nil {
		t.Fatal("bad overrides accepted")
	}
	for _, want := range []string{"stale_messages", `"abc"`, `"json:400"`, `unknown class "blocked"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
	for _, c := range []Class{OK, RateLimited, LoginWall, Checkpoint, SoftError, RegistryStale, SchemaDrift, ContentUnavailable, Transport} {
		if got, err := ParseClass(c.String()); err != nil || got != c {
			t.Errorf("ParseClass(%q) = %v, %v", c.String(), got, err)
		}
	}
}
