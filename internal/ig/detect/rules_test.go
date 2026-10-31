package detect

import (
	"testing"

	scrapedetect "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
)

func TestInstagramWalls(t *testing.T) {
	r := Rules()
	for _, c := range []struct {
		in   scrapedetect.Input
		want scrapedetect.Class
	}{
		{scrapedetect.Input{Status: 302, Location: "https://www.instagram.com/accounts/login/?next=%2Fbrand%2F", Expect: "html"}, scrapedetect.LoginWall},
		{scrapedetect.Input{Status: 302, Location: "/challenge/?next=/brand/", Expect: "html"}, scrapedetect.Checkpoint},
		{scrapedetect.Input{Status: 200, ContentType: "text/html", Body: []byte(`<form action="/accounts/login/ajax/">`), Expect: "graphql"}, scrapedetect.LoginWall},
		{scrapedetect.Input{Status: 429, Expect: "graphql"}, scrapedetect.RateLimited},
		{scrapedetect.Input{Status: 200, ContentType: "text/html", Body: []byte(`<html>a profile</html>`), Expect: "html"}, scrapedetect.OK},
	} {
		if got, reason := r.Classify(c.in); got != c.want {
			t.Errorf("%+v: %s (%s), want %s", c.in, got, reason, c.want)
		}
	}
}
