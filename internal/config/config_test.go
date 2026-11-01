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

// The tt: section loads from configs/tt/default.yaml with the values ADR
// 0007 §4 and the build brief set, and env vars override it.
func TestTTDefaults(t *testing.T) {
	t.Setenv("TKS_TT__SIGNER__MODE", "none")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	tt := c.TT
	if tt.BaseURL != "https://www.tiktok.com" || tt.OperationSeed != "tt/operations.seed.yaml" || tt.FingerprintProfile != "chrome_152" {
		t.Fatalf("client defaults: %+v", tt)
	}
	if tt.Signer.Mode != SignerNone {
		t.Fatalf("env override lost: %q", tt.Signer.Mode)
	}
	f := tt.Freshness
	if f.ListInterval != 2*time.Hour+45*time.Minute || f.AccountRefresh != 24*time.Hour || f.BackfillDays != 365 {
		t.Fatalf("freshness: %+v", f)
	}
	if len(f.MetricsWalks) != 2 || f.MetricsWalks[1].Back != 365*24*time.Hour || len(f.CommentWindows) != 3 {
		t.Fatalf("tiers: %+v", f)
	}
	if tt.Comments.Order != CommentsRanked || !tt.Comments.IncludeReplies || tt.Comments.MaxPerPost != 0 {
		t.Fatalf("comments (every comment read, owner 2026-11-01): %+v", tt.Comments)
	}
	// tks keeps everything (owner, 2026-11-01): no retention of its own,
	// whatever the copied pipeline's privacy.retention says.
	if tt.Retention != (Retention{}) || c.Privacy.Retention.CommentsDays == 0 {
		t.Fatalf("tt.retention %+v (privacy.retention %+v)", tt.Retention, c.Privacy.Retention)
	}
	bad := *c
	bad.TT.Retention.CommentsDays = -1
	if bad.Validate() == nil {
		t.Fatal("a negative retention passed")
	}
	// The copied Facebook section is still there for the copied pipeline.
	if c.FB.BaseURL != "https://www.facebook.com" {
		t.Fatalf("fb section lost: %q", c.FB.BaseURL)
	}
}

func TestTTValidate(t *testing.T) {
	for name, tc := range map[string]struct {
		edit func(*TT)
		want string
	}{
		"defaults":           {func(*TT) {}, ""},
		"no base url":        {func(t *TT) { t.BaseURL = "" }, "tt.base_url"},
		"missing seed":       {func(t *TT) { t.OperationSeed = "tt/nope.yaml" }, "tt.operation_seed"},
		"bad seed":           {func(t *TT) { t.OperationSeed = "topics.yaml" }, "tt.operation_seed"},
		"bad detect pattern": {func(t *TT) { t.Detect.StaleMessages = []string{"("} }, "tt.detect"},
		"bad detect status":  {func(t *TT) { t.Detect.Statuses = map[string]string{"403": "nope"} }, "tt.detect"},
		"identity range":     {func(t *TT) { t.IdentityMaxRequestsMin = 900 }, "identity_max_requests_min"},
		"page size":          {func(t *TT) { t.CommentsPageSize = 0 }, "page_size"},
		"capture":            {func(t *TT) { t.CaptureRaw = "some" }, "tt.capture_raw"},
		"signer mode":        {func(t *TT) { t.Signer.Mode = "native" }, "tt.signer.mode"},
		"signer pages":       {func(t *TT) { t.Signer.Pages = 0 }, "tt.signer: browser mode"},
		"signer off":         {func(t *TT) { t.Signer.Mode, t.Signer.Pages = SignerNone, 0 }, ""},
		"signer share":       {func(t *TT) { t.Signer.MaxShare = 1.5 }, "max_share"},
		"list interval":      {func(t *TT) { t.Freshness.ListInterval = 0 }, "list_interval"},
		"backfill":           {func(t *TT) { t.Freshness.BackfillDays = 0 }, "backfill_days"},
		"walks order": {func(t *TT) {
			t.Freshness.MetricsWalks = []MetricsWalk{{Every: time.Hour, Back: 48 * time.Hour}, {Every: time.Hour, Back: 24 * time.Hour}}
		}, "metrics_walks"},
		"windows order": {func(t *TT) {
			t.Freshness.CommentWindows = []CommentWindow{{MaxAge: 48 * time.Hour, Interval: time.Hour}, {MaxAge: time.Hour, Interval: time.Hour}}
		}, "comment_windows"},
		"comment order":  {func(t *TT) { t.Comments.Order = "time" }, "tt.comments.order"},
		"comment cap":    {func(t *TT) { t.Comments.MaxPerPost = -1 }, "max_per_post"},
		"deletion slack": {func(t *TT) { t.Comments.DeletionSlack = 1.5 }, "deletion_slack"},
	} {
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		tc.edit(&c.TT)
		err = c.Validate()
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: %v, want an error naming %s", name, err, tc.want)
		}
	}
}

// A config file sets the tt: section like any other.
func TestTTFromFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	_ = os.WriteFile(p, []byte("tt:\n  base_url: http://fakett\n  freshness:\n    list_interval: 1h\n"), 0o644)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.TT.BaseURL != "http://fakett" || c.TT.Freshness.ListInterval != time.Hour || c.TT.Freshness.AccountRefresh != 24*time.Hour {
		t.Fatalf("%+v", c.TT)
	}
}
