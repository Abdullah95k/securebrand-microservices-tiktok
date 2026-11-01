package e2e

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/fakett"
)

// TestChaosRestartsAndFaults backfills several accounts with their comments
// while fakett rate limits and errs now and then, and the crawler (killed:
// its in-flight tasks abandoned, like a pod's) and the persister are
// restarted at random. Every job must still complete, and the store must
// hold exactly fakett's world: every post, comment and reply, nothing extra.
// A round with nothing new then writes no metrics row.
func TestChaosRestartsAndFaults(t *testing.T) {
	s := Start(t, Options{
		Spec:         fakett.Spec{Seed: 31, Accounts: 3, PostsPerAccount: 16, MaxComments: 40, MaxReplies: 6, PostSpacing: 30 * time.Hour},
		Faults:       fakett.Faults{RateLimitEvery: 13, ListErrorEvery: 9},
		CrawlerDrain: 20 * time.Millisecond,
		// Few requests per second per account, so the jobs run through the
		// restarts on any machine.
		Config: func(c *config.Config) { c.TT.AccountRPS, c.TT.AccountRPSP0 = 10, 10 },
	})
	accs := s.World.Accounts()
	rng := rand.New(rand.NewPCG(31, 7))
	refs := make([]string, len(accs))
	ids := make([]string, len(accs))
	for i, a := range accs {
		refs[i], ids[i] = "u:"+a.Username, fmt.Sprintf("chaos-%d", i)
		s.Queue(t, ids[i], refs[i], full)
	}
	// A bounded number of restarts while the jobs run; then the pipeline
	// must settle on its own.
	restarts := map[string]int{}
	budget := map[string]int{"crawler": 5, "persister": 3}
	done := func() bool {
		for _, id := range ids {
			st, err := s.Tracker.Status(t.Context(), id)
			if err != nil || st.Spawned == 0 || st.Pending > 0 {
				return false
			}
		}
		return true
	}
	for deadline := time.Now().Add(2 * time.Minute); !done() && time.Now().Before(deadline); {
		time.Sleep(time.Duration(300+rng.IntN(700)) * time.Millisecond)
		switch c := []string{"crawler", "persister"}[rng.IntN(2)]; {
		case restarts[c] >= budget[c]:
		case c == "crawler":
			s.RestartCrawler(t)
			restarts[c]++
		default:
			s.RestartPersister(t)
			restarts[c]++
		}
	}
	t.Logf("restarts: %v", restarts)
	for id, st := range s.Wait(t, 3*time.Minute, ids...) {
		if st.Counters["tasks_failed"] != 0 {
			t.Errorf("job %s: %v", id, st.Counters)
		}
	}
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}
	if t.Failed() {
		t.FailNow()
	}
	s.World.Advance(3 * time.Hour)
	before := count(t, s, `SELECT count(*) FROM post_metrics`)
	round(t, s, "quiet", incremental, refs...)
	if got := count(t, s, `SELECT count(*) FROM post_metrics`) - before; got != 0 {
		t.Errorf("%d metrics rows written without any change", got)
	}
	for _, a := range accs {
		sameStore(t, s, a, truth{})
	}
}
