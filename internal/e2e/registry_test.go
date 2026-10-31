package e2e

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/registrysync"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

// TestRegistrySyncHealsRotation: Facebook rotates every doc_id at once.
// The registry sync discovers the new ids in the canary pages' bundles,
// checks and promotes them; after their registry reload the crawlers
// collect a page completely again.
func TestRegistrySyncHealsRotation(t *testing.T) {
	s := Start(t, Options{Spec: fakefb.Spec{Seed: 9, Pages: 2, PostsPerPage: 4, PostSpacing: 20 * time.Hour,
		MaxComments: 20, ReplyProbability: 0.4, MaxReplies: 5, Depth2Prob: 0.1}})
	ctx := context.Background()
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	pages := s.Fake.World.Pages()

	seedBytes, _ := configs.FS.ReadFile("queries.seed.yaml")
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string][]string{}
	for _, q := range seed.Queries {
		docs[q.FriendlyName] = []string{q.DocID}
	}
	for friendly, ids := range fakefb.RotatedDocs(docs) {
		s.Fake.RotateDoc(friendly, ids[0])
	}

	cfg := *s.Cfg
	cfg.RegistrySync.CanaryPages = []string{pages[1].Vanity}
	sync, reg, err := app.NewRegistrySync(ctx, &cfg, s.DB, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	rep, err := sync.Run(ctx)
	if err != nil || rep.Status != registrysync.StatusOK || len(rep.Promoted) != len(seed.Queries) {
		t.Fatalf("sync: %v %+v", err, rep)
	}
	if err := s.Crawler.Registry.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	// Operators see the run and each entry's last check.
	admin := s.Key(t, api.ScopeAdmin)
	runs := decodeBody[struct {
		Runs []struct {
			Status string              `json:"status"`
			Report registrysync.Report `json:"report"`
		} `json:"runs"`
	}](t, s.Do(t, admin, "GET", "/v1/admin/registry-sync/runs?limit=5", nil), http.StatusOK)
	if len(runs.Runs) != 1 || runs.Runs[0].Status != "ok" || len(runs.Runs[0].Report.Promoted) != len(seed.Queries) {
		t.Fatalf("runs: %+v", runs)
	}
	qs := decodeBody[struct {
		Queries []registry.Query `json:"queries"`
	}](t, s.Do(t, admin, "GET", "/v1/admin/queries", nil), http.StatusOK)
	for _, q := range qs.Queries {
		if q.Status == registry.StatusActive && (q.LastCheck != "ok" || q.LastOKAt == nil) {
			t.Fatalf("active entry without a passing check: %+v", q)
		}
	}

	j := s.runJob(t, key, "https://www.facebook.com/"+pages[0].Vanity, "full")
	truth, _ := s.Fake.World.TruthFor(pages[0].ID, time.Time{})
	want := 0
	for _, p := range truth.Posts {
		want += p.TotalComments
	}
	got := count(t, s, `SELECT count(*) FROM comments WHERE page_id = $1`, pages[0].ID)
	t.Logf("job %s %s, comments %d/%d, counters %v", j.ID, j.Status, got, want, j.Counters)
	if j.Status != "completed" || got != want {
		t.Fatalf("crawl after the promotion: %s, comments %d, want %d", j.Status, got, want)
	}
}

// TestConsumerLagGauges reads the lag of the stack's consumer groups.
func TestConsumerLagGauges(t *testing.T) {
	s := Start(t, Options{Spec: fakefb.Spec{Seed: 4, Pages: 1, PostsPerPage: 2, PostSpacing: 20 * time.Hour, MaxComments: 3}})
	key := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	if j := s.runJob(t, key, "https://www.facebook.com/"+s.Fake.World.Pages()[0].Vanity, "full"); j.Status != "completed" {
		t.Fatalf("job %+v", j)
	}
	lag := &scheduler.LagGauges{Admin: kadm.NewClient(s.Producer.Client()), Prefix: s.Cfg.Kafka.TopicPrefix}
	if err := lag.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	groups := map[string]bool{}
	ch := make(chan prometheus.Metric, 256)
	go func() { metrics.ConsumerLag.Collect(ch); close(ch) }()
	for m := range ch {
		var pb dto.Metric
		_ = m.Write(&pb)
		for _, l := range pb.GetLabel() {
			if l.GetName() == "group" {
				groups[l.GetValue()] = true
			}
			if l.GetName() == "topic" && strings.HasPrefix(l.GetValue(), s.Cfg.Kafka.TopicPrefix) {
				t.Fatalf("topic label keeps the prefix: %s", l.GetValue())
			}
		}
	}
	t.Logf("groups with lag series: %v", groups)
	if !groups["igs-persister"] || !groups["igs-crawler-page-p0"] {
		t.Fatalf("want the persister and on-demand page groups, got %v", groups)
	}
}
