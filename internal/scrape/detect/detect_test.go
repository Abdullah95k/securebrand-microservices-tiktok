package detect

import "testing"

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
