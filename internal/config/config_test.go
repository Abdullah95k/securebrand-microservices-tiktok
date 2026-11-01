package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadLayers(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.yaml")
	b := filepath.Join(dir, "b.yaml")
	_ = os.WriteFile(a, []byte("crawler:\n  yield_pages: 7\nfb:\n  base_url: http://a\n"), 0o644)
	_ = os.WriteFile(b, []byte("fb:\n  base_url: http://b\n  proxy:\n    tiers:\n      - {name: isp, kind: static_list, endpoints: [\"http://u:p@1.2.3.4:1\"]}\n"), 0o644)
	t.Setenv("TKS_KAFKA__BROKERS", "k1:9092,k2:9092")
	t.Setenv("TKS_MEDIA__TTL", "48h")
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

// The author key must be a real secret in prod, and wherever authors are
// pseudonymised outside dev: not empty, not the dev key, not short.
func TestValidate(t *testing.T) {
	for name, tc := range map[string]struct {
		env, mode, key string
		ok             bool
	}{
		"prod, dev key":            {"prod", "full", "dev-only-change-me", false},
		"prod, empty key":          {"prod", "full", "", false},
		"prod, short key":          {"prod", "full", "0123456789", false},
		"prod, real key":           {"prod", "full", "0123456789abcdef0123456789abcdef", true},
		"pilot, pseudonymised":     {"pilot", "pseudonymized", "dev-only-change-me", false},
		"pilot, full names stored": {"pilot", "full", "dev-only-change-me", true},
		"dev":                      {"dev", "pseudonymized", "dev-only-change-me", true},
	} {
		c, _ := Load("")
		c.Service.Env, c.Privacy.PIIMode, c.Privacy.AuthorHMACKey = tc.env, tc.mode, tc.key
		if err := c.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
		}
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

// Detect rules come from the configuration too (fb.detect), and one the
// classifier cannot read fails validation.
func TestDetectOverrides(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.FB.Detect.RateLimitCodes != nil || c.FB.Detect.Statuses != nil {
		t.Fatalf("defaults must keep the built-in rules: %+v", c.FB.Detect)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(t.TempDir(), "d.yaml")
	_ = os.WriteFile(f, []byte("fb:\n  detect:\n    rate_limit_codes: [77, 78]\n    block_markers: [\"slow down\"]\n    statuses:\n      \"403\": checkpoint\n      \"html:400\": content_unavailable\n"), 0o644)
	if c, err = Load(f); err != nil {
		t.Fatal(err)
	}
	d := c.FB.Detect
	if len(d.RateLimitCodes) != 2 || d.RateLimitCodes[1] != 78 || len(d.BlockMarkers) != 1 || d.Statuses["html:400"] != "content_unavailable" {
		t.Fatalf("fb.detect not loaded: %+v", d)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.FB.Detect.Statuses["403"] = "blocked"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "fb.detect") {
		t.Fatalf("an unknown class must fail validation: %v", err)
	}
}

// A pod's identities can rotate fingerprint profiles (fb.fingerprint_profiles,
// passed to session.PoolConfig.Profiles); by default they all use
// fb.fingerprint_profile.
func TestFingerprintProfiles(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.FB.FingerprintProfiles) != 0 || c.FB.FingerprintProfile == "" {
		t.Fatalf("defaults: %q %v", c.FB.FingerprintProfile, c.FB.FingerprintProfiles)
	}
	t.Setenv("TKS_FB__FINGERPRINT_PROFILES", "chrome_152,chrome_146")
	if c, err = Load(""); err != nil {
		t.Fatal(err)
	}
	if len(c.FB.FingerprintProfiles) != 2 || c.FB.FingerprintProfiles[0] != "chrome_152" {
		t.Fatalf("fb.fingerprint_profiles: %v", c.FB.FingerprintProfiles)
	}
}
