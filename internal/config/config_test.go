package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadLayers(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.yaml")
	b := filepath.Join(dir, "b.yaml")
	_ = os.WriteFile(a, []byte("crawler:\n  yield_pages: 7\nfb:\n  base_url: http://a\n"), 0o644)
	_ = os.WriteFile(b, []byte("fb:\n  base_url: http://b\n  proxy:\n    tiers:\n      - {name: isp, kind: static_list, endpoints: [\"http://u:p@1.2.3.4:1\"]}\n"), 0o644)
	t.Setenv("IGS_KAFKA__BROKERS", "k1:9092,k2:9092")
	t.Setenv("IGS_MEDIA__TTL", "48h")
	c, err := Load(a + "," + b)
	if err != nil {
		t.Fatal(err)
	}
	if c.Crawler.YieldPages != 7 || c.FB.BaseURL != "http://b" || len(c.FB.Proxy.Tiers) != 1 {
		t.Fatalf("layering wrong: %+v %+v", c.Crawler, c.FB)
	}
	if len(c.Kafka.Brokers) != 2 || c.Media.TTL != 48*time.Hour {
		t.Fatalf("env override wrong: %v %v", c.Kafka.Brokers, c.Media.TTL)
	}
	// Defaults survive.
	if c.Limits.MaxAgeDays != 365 || c.Scheduler.FinalizerInterval != 2*time.Second {
		t.Fatalf("defaults lost: %+v", c.Limits)
	}
}

func TestValidate(t *testing.T) {
	c, _ := Load("")
	c.Service.Env = "prod"
	if err := c.Validate(); err == nil {
		t.Fatal("prod with the dev HMAC key must be rejected")
	}
}

func TestSchedulingDefaults(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	s := c.Scheduler
	if len(s.Tiers) != 4 || s.Tiers[0].Name != "T0" || s.Tiers[0].Interval != 3*time.Hour || s.Tiers[2].MinPostsPerDay < 0.14 {
		t.Fatalf("tiers: %+v", s.Tiers)
	}
	if s.DormantAfter != 90*24*time.Hour || s.RateWindow != 14*24*time.Hour {
		t.Fatalf("dormancy: %+v", s)
	}
	pr := s.PostRefresh
	if !pr.Enabled || len(pr.Windows) != 3 || pr.Windows[2].MaxAge != 30*24*time.Hour || len(pr.Windows[2].Tiers) != 2 ||
		len(pr.RecrawlAt) != 2 || pr.RecrawlMinComments != 20 {
		t.Fatalf("post refresh: %+v", pr)
	}
	if !s.Budget.Enabled || s.Budget.Stretch != 2 || len(s.Budget.StretchTiers) != 3 {
		t.Fatalf("budget: %+v", s.Budget)
	}
	c.Scheduler.Tiers[1].MinPostsPerDay = 50
	if err := c.Validate(); err == nil {
		t.Fatal("unordered tiers must fail validation")
	}
}
