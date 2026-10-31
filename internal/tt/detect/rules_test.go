package detect

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
)

func TestRules(t *testing.T) {
	r := Rules()
	cases := []struct {
		name string
		in   detect.Input
		want detect.Class
	}{
		{"captcha", detect.Input{Status: 200, Errors: []detect.GQLError{{Code: StatusCaptcha}}, Expect: "graphql"}, detect.Checkpoint},
		{"private account", detect.Input{Status: 200, Errors: []detect.GQLError{{Code: StatusPrivateAccount}}, Expect: "graphql"}, detect.ContentUnavailable},
		{"login redirect", detect.Input{Status: 302, Location: "https://www.tiktok.com/login?redirect_url=x", Expect: "html"}, detect.LoginWall},
		{"challenge page", detect.Input{Status: 200, Body: []byte(`<title>Please wait...</title><script>_wafchallengeid</script>`), Expect: "html"}, detect.Checkpoint},
		{"profile page", detect.Input{Status: 200, Body: []byte(`<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">{}</script>`), Expect: "html"}, detect.OK},
	}
	for _, c := range cases {
		if got, reason := r.Classify(c.in); got != c.want {
			t.Errorf("%s: got %v (%s), want %v", c.name, got, reason, c.want)
		}
	}
	if !r.EmptyIsStale {
		t.Fatal("an empty answer must fall back to the next variant")
	}
}
