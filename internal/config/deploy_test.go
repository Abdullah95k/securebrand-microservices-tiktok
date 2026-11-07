package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The chart renders its values' config key into /etc/tks/config.yaml and
// layers the proxies Secret over it; the Compose stack mounts local.yaml.
// Each must load and validate as the services load it, and keep the
// settings the owner decided for tks's deployment.

const chart = "../../deploy/helm/tks"

// chartConfig writes the config key of the chart's values, with the values
// files given layered over them as Helm does, to a file.
func chartConfig(t *testing.T, files ...string) string {
	t.Helper()
	merged := map[string]any{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var v map[string]any
		if err := yaml.Unmarshal(b, &v); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if c, ok := v["config"].(map[string]any); ok {
			merge(merged, c)
		}
	}
	out, err := yaml.Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// merge is Helm's coalescing of maps: later values win, maps merge.
func merge(dst, src map[string]any) {
	for k, v := range src {
		if sm, ok := v.(map[string]any); ok {
			if dm, ok := dst[k].(map[string]any); ok {
				merge(dm, sm)
				continue
			}
		}
		dst[k] = v
	}
}

func TestChartConfigLoads(t *testing.T) {
	// What the tks-env Secret holds (secretEnv).
	t.Setenv("TKS_PRIVACY__AUTHOR_HMAC_KEY", strings.Repeat("ab", 32))
	// What the chart sets from the release name (tks.env).
	t.Setenv("TKS_TT__SIGNER__REMOTE_URL", "http://tks-signer:8080")
	// What the tks-proxies Secret holds in production (tt.proxy.tiers).
	proxies := filepath.Join(t.TempDir(), "proxies.yaml")
	if err := os.WriteFile(proxies, []byte(`tt:
  proxy:
    tiers:
      - {name: isp, kind: static_list, endpoints: ["http://user:pass@192.0.2.1:8080"]}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	prod := chartConfig(t, chart+"/values.yaml")
	c, err := Load(prod + "," + proxies)
	if err != nil {
		t.Fatalf("the chart's production config: %v", err)
	}
	// The signer's pages in tks-signer's capped pod (M9d), with the guard:
	// one page running away to kill_page_mb next to the others at their
	// max_page_mb still fits the pod's 2 GiB.
	if c.TT.BaseURL != "https://www.tiktok.com" || c.TT.Signer.Mode != SignerRemote || !c.TT.Browser.NoSandbox {
		t.Errorf("tt: base_url %q, signer %q, no_sandbox %v", c.TT.BaseURL, c.TT.Signer.Mode, c.TT.Browser.NoSandbox)
	}
	if s := c.TT.Signer; s.Pages != 2 || s.KillPageMB <= 0 || s.KillPageMB+s.MaxPageMB*float64(s.Pages-1) > 2*1024 || s.MaxPageMB > s.KillPageMB {
		t.Errorf("tt.signer: %d pages, max_page_mb %v, kill_page_mb %v: one runaway and the other pages must fit 2 GiB", s.Pages, s.MaxPageMB, s.KillPageMB)
	}
	if !slices.Equal(c.TT.Proxy.Routing["default"], []string{"isp"}) {
		t.Errorf("routing %v, want the ISP tier (D12)", c.TT.Proxy.Routing)
	}
	// The owner, 2026-11-05: over the budget only backfills pause.
	if len(c.Scheduler.Budget.StretchTiers) != 0 {
		t.Errorf("scheduler.budget.stretch_tiers %v, want none", c.Scheduler.Budget.StretchTiers)
	}
	if c.TT.Media.Include {
		t.Error("media downloads on by default (D5)")
	}
	if !c.TT.Subtitles.Include {
		t.Error("subtitles off by default (M17: on)")
	}
	if !strings.HasPrefix(c.S3.MediaBucket, "securebrand-tks-") || !strings.HasPrefix(c.S3.RawBucket, "securebrand-tks-") {
		t.Errorf("buckets %q, %q: tks's own names (D6)", c.S3.MediaBucket, c.S3.RawBucket)
	}

	staging, err := Load(chartConfig(t, chart+"/values.yaml", chart+"/examples/fakett.yaml"))
	if err != nil {
		t.Fatalf("the chart's fakett example: %v", err)
	}
	if !strings.Contains(staging.TT.BaseURL, "fakett") || len(staging.TT.Canary.Accounts) == 0 {
		t.Errorf("fakett example: base_url %q, canary %v", staging.TT.BaseURL, staging.TT.Canary.Accounts)
	}

	local, err := Load("../../deploy/compose/local.yaml")
	if err != nil {
		t.Fatalf("the Compose stack's local.yaml: %v", err)
	}
	if len(local.Scheduler.Budget.StretchTiers) != 0 || len(local.TT.Canary.Accounts) == 0 {
		t.Errorf("local.yaml: stretch_tiers %v, canary %v", local.Scheduler.Budget.StretchTiers, local.TT.Canary.Accounts)
	}
}

// values-production.yaml's config loads with what the chart maps from the
// platform's Secret (tks.platformEnv) and the proxies Secret (M18).
func TestChartProductionConfigLoads(t *testing.T) {
	ca := testCA(t)
	for k, v := range map[string]string{
		"TKS_PRIVACY__AUTHOR_HMAC_KEY":    strings.Repeat("cd", 32),
		"TKS_TT__SIGNER__REMOTE_URL":      "http://tks-signer:8080",
		"TKS_KAFKA__BROKERS":              "kafka-1.example:25073,kafka-2.example:25073",
		"TKS_KAFKA__SASL__MECHANISM":      "SCRAM-SHA-256",
		"TKS_KAFKA__SASL__USERNAME":       "tks-kafka",
		"TKS_KAFKA__SASL__PASSWORD":       "pw,with,commas",
		"TKS_KAFKA__TLS__ENABLED":         "true",
		"TKS_KAFKA__TLS__CA":              ca,
		"TKS_REDIS__ADDR":                 "valkey.example:25061",
		"TKS_REDIS__PASSWORD":             "v",
		"TKS_REDIS__TLS__ENABLED":         "true",
		"TKS_REDIS__TLS__CA":              ca,
		"TKS_POSTGRES__HOST":              "pg.example",
		"TKS_POSTGRES__PORT":              "25060",
		"TKS_POSTGRES__DATABASE":          "tks",
		"TKS_POSTGRES__SSLMODE":           "require",
		"TKS_POSTGRES__CA":                ca,
		"TKS_POSTGRES__USER":              "tks_persister",
		"TKS_POSTGRES__PASSWORD":          "p",
		"TKS_S3__ENDPOINT":                "https://fra1.example.com",
		"TKS_S3__RAW_BUCKET":              "raw",
		"TKS_S3__MEDIA_BUCKET":            "media",
		"TKS_KAFKA__SCHEMA_REGISTRY__URL": "https://registry.example",
	} {
		t.Setenv(k, v)
	}
	proxies := filepath.Join(t.TempDir(), "proxies.yaml")
	if err := os.WriteFile(proxies, []byte(`tt:
  proxy:
    tiers:
      - {name: isp, kind: static_list, country: US, endpoints: ["http://user:pass@192.0.2.1:8080"]}
      - {name: res_iq, kind: gateway, country: IQ, gateway: "http://gate.example:7000", username: u, password: p,
         username_template: "user-{user}-country-iq-session-{session}-sessionduration-{ttl_min}", sticky_ttl: 60m}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(chartConfig(t, chart+"/values.yaml", chart+"/values-production.yaml") + "," + proxies)
	if err != nil {
		t.Fatalf("values-production.yaml: %v", err)
	}
	if c.Kafka.SASL.Mechanism != "scram-sha-256" || len(c.Kafka.Brokers) != 2 || !c.Kafka.TLS.Enabled || !c.Redis.TLS.Enabled {
		t.Errorf("kafka/valkey: %+v %v", c.Kafka.SASL, c.Kafka.Brokers)
	}
	if u, err := c.Postgres.URL(); err != nil || !strings.Contains(u, "pg.example:25060/tks") || !strings.Contains(u, "sslrootcert=") {
		t.Errorf("postgres url %q: %v", u, err)
	}
	if c.S3.CreateBuckets || c.S3.MediaPrefix != "tks/" || c.S3.RawKeepPrefix != "tks/keep/" || c.S3.RawRestPrefix != "tks/30d/" {
		t.Errorf("s3 %+v", c.S3)
	}
	if c.TT.CaptureRaw != "sample" || c.TT.CaptureRawSample != 0.01 || c.TT.Cost.BudgetPerAccountUSD != 0.0019 {
		t.Errorf("tt: capture %q %v, budget %v", c.TT.CaptureRaw, c.TT.CaptureRawSample, c.TT.Cost.BudgetPerAccountUSD)
	}
	// tt.subtitle.v1 is the platform's to make first (hand-over §6).
	if c.TT.Subtitles.Include {
		t.Error("tt.subtitles.include on in production before the platform has made tt.subtitle.v1")
	}
	if c.Service.Env != "prod" || c.TT.Proxy.Tiers[0].Country != "US" {
		t.Errorf("env %q, tiers %+v", c.Service.Env, c.TT.Proxy.Tiers)
	}
	// No real usernames in this public repository: the owner's canary
	// accounts are Argo CD parameters (config.tt.canary.accounts[0..2]).
	if n := len(c.TT.Canary.Accounts); n != 0 {
		t.Errorf("tt.canary.accounts: %d usernames in values-production.yaml, want none (they are the platform's parameters)", n)
	}
	// With the platform's three parameters (synthetic names here): enough for
	// tt.canary.confirm, which needs 2 of them to refuse a variant.
	params := filepath.Join(t.TempDir(), "params.yaml")
	if err := os.WriteFile(params, []byte("config:\n  tt:\n    canary:\n      accounts: [canary_one, canary_two, canary_three]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	withParams, err := Load(chartConfig(t, chart+"/values.yaml", chart+"/values-production.yaml", params) + "," + proxies)
	if err != nil {
		t.Fatalf("values-production.yaml with the canary parameters: %v", err)
	}
	if n := len(withParams.TT.Canary.Accounts); n < 3 || n < withParams.TT.Canary.Confirm+1 {
		t.Errorf("tt.canary.accounts with the parameters: %d accounts, want 3 or more", n)
	}
}
