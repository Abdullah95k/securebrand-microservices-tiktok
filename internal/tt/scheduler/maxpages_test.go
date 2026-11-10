package scheduler

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/discovery"
)

// A reposts tab's first read goes deeper (its older reposts), later reads
// take max_pages; the other sources keep theirs.
func TestMaxPagesReposts(t *testing.T) {
	cfg := config.Discovery{Reposts: config.RepostPages{MaxPages: 3, FirstPages: 20}, Feed: config.FeedPages{Reads: 6}}
	for _, c := range []struct {
		t    discovery.DueTarget
		want int
	}{
		{discovery.DueTarget{Source: discovery.SourceRepost, FirstRead: true}, 20},
		{discovery.DueTarget{Source: discovery.SourceRepost}, 3},
		{discovery.DueTarget{Source: discovery.SourceFeed, FirstRead: true}, 6},
		{discovery.DueTarget{Source: discovery.SourceRelated}, 1},
	} {
		if got := maxPages(cfg, c.t); got != c.want {
			t.Errorf("%+v: %d pages, want %d", c.t, got, c.want)
		}
	}
}
