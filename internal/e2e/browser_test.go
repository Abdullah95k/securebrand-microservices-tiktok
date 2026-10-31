package e2e

import (
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
)

// TestBrowserBootstrapCrawl: pages whose tokens only appear after their
// scripts run. The crawler's HTTP bootstraps find none, the browser
// fallback bootstraps the identities, and the page is collected in full.
func TestBrowserBootstrapCrawl(t *testing.T) {
	if _, err := browser.FindBinary(""); err != nil {
		t.Skip(err)
	}
	s := Start(t, Options{
		Spec:   fakefb.Spec{Seed: 21, Pages: 1, PostsPerPage: 4, PostSpacing: 20 * time.Hour, MaxComments: 15, ReplyProbability: 0.3, MaxReplies: 4},
		Faults: fakefb.Faults{JSOnlyTokens: true},
		Config: func(c *config.Config) {
			c.FB.Browser.Enabled = true
			c.FB.Browser.MaxConcurrent = 4
			c.FB.Browser.MaxShare = 1 // every page here needs the browser
			c.FB.Browser.Timeout = time.Minute
		},
	})
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	page := s.Fake.World.Pages()[0]
	before, _, _ := sample("igs_bootstrap_total", map[string]string{"method": "browser", "outcome": "ok"})
	j := s.runJob(t, key, "https://www.facebook.com/"+page.Vanity, "full")
	after, _, _ := sample("igs_bootstrap_total", map[string]string{"method": "browser", "outcome": "ok"})
	truth, _ := s.Fake.World.TruthFor(page.ID, time.Time{})
	want := 0
	for _, p := range truth.Posts {
		want += p.TotalComments
	}
	got := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1`, page.ID)
	t.Logf("job %s %s, %v browser bootstraps, comments %d/%d", j.ID, j.Status, after-before, got, want)
	if j.Status != "completed" || got != want || after-before < 1 {
		t.Fatalf("browser bootstrap crawl: %s, comments %d/%d, browser bootstraps %v", j.Status, got, want, after-before)
	}
}
