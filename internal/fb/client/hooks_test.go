package client

import "testing"

// A page that names nothing (restricted to logged-in viewers, or removed,
// whatever else it holds) is unavailable; a public page, and the consent
// wall (answered by AfterLoad), are not.
func TestUnavailableHook(t *testing.T) {
	const tokens = `<script>{"define":[["LSD",[],{"token":"AVq"},323],["SiteData",[],{"hsi":"7431000000000000001"},317]]}</script>`
	unavailable := Hooks("en-US").Unavailable
	for name, c := range map[string]struct {
		body string
		want bool
	}{
		"restricted":   {`<html><title>Facebook</title>` + tokens + `</html>`, true},
		"login form":   {`<form id="login_form"></form>` + tokens, true},
		"escaped ids":  {tokens + `<script>require("x","{\"id\":\"100064000000777\"}")</script>`, false},
		"public page":  {tokens + `<script>{"user":{"id":"100064000000777"}}</script>`, false},
		"consent wall": {tokens + `<div data-cookiebanner="x"></div>`, false},
	} {
		if got := unavailable([]byte(c.body), nil); got != c.want {
			t.Errorf("%s: unavailable %v, want %v", name, got, c.want)
		}
	}
}
