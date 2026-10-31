// Package config defines the configuration shared by every fbs binary.
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/breaker"
	pconfig "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/config"
)

// EnvPrefix is the prefix of environment variable overrides.
const EnvPrefix = "IGS_"

type Config struct {
	Service   Service   `koanf:"service"`
	Log       Log       `koanf:"log"`
	HTTP      HTTP      `koanf:"http"`
	Postgres  Postgres  `koanf:"postgres"`
	Redis     Redis     `koanf:"redis"`
	Kafka     Kafka     `koanf:"kafka"`
	S3        S3        `koanf:"s3"`
	FB        FB        `koanf:"fb"`
	Crawler   Crawler   `koanf:"crawler"`
	Limits    Limits    `koanf:"limits"`
	Scheduler Scheduler `koanf:"scheduler"`
	Media     Media     `koanf:"media"`
	API       API       `koanf:"api"`
	Privacy   Privacy   `koanf:"privacy"`
	// RegistrySync follows Facebook's doc_id rotations (fbs-registry-sync).
	RegistrySync RegistrySync `koanf:"registry_sync"`
}

// RegistrySync configures the service that discovers rotated doc_ids on
// canary pages, validates them and promotes them in fb_queries.
type RegistrySync struct {
	Interval time.Duration `koanf:"interval"`
	// CanaryPages are public pages with regular posts, comments and reply
	// threads; validation reads one timeline page and a few comment pages
	// of each, in order, until every query has a verdict.
	CanaryPages []string `koanf:"canary_pages"`
	// Tier is the proxy tier page loads and probes use; "" = the first one.
	Tier string `koanf:"tier"`
	// MaxBundles caps the JS bundles fetched per canary page and run.
	MaxBundles int `koanf:"max_bundles"`
	// BundlesViaProxy fetches bundles through the tier instead of direct
	// egress (bundles are public and immutable, so direct is the default).
	BundlesViaProxy bool `koanf:"bundles_via_proxy"`
	// Promote switches promotion on; false records candidates and checks
	// only (dry run).
	Promote bool `koanf:"promote"`
	// KeepFallbacks is how many former active entries stay as fallbacks;
	// older ones are retired.
	KeepFallbacks int `koanf:"keep_fallbacks"`
	// RetireAfter retires a candidate after this many failed checks.
	RetireAfter int `koanf:"retire_after"`
	// RecheckRetired re-validates a retired candidate discovered again after
	// this long (a parser fix may have made it usable).
	RecheckRetired time.Duration `koanf:"recheck_retired"`
	// Timeout bounds one run.
	Timeout time.Duration `koanf:"timeout"`
}

type Service struct {
	Name string `koanf:"name"`
	Env  string `koanf:"env"`
}

type Log struct {
	Level  string `koanf:"level"`
	Format string `koanf:"format"`
}

type HTTP struct {
	Addr         string        `koanf:"addr"`
	OpsAddr      string        `koanf:"ops_addr"`
	ReadTimeout  time.Duration `koanf:"read_timeout"`
	WriteTimeout time.Duration `koanf:"write_timeout"`
}

type Postgres struct {
	DSN      string `koanf:"dsn"`
	ReadDSN  string `koanf:"read_dsn"`
	MaxConns int32  `koanf:"max_conns"`
}

type Redis struct {
	Addr     string `koanf:"addr"`
	Password string `koanf:"password"`
	DB       int    `koanf:"db"`
	TLS      TLS    `koanf:"tls"`
}

type Kafka struct {
	Brokers       []string      `koanf:"brokers"`
	ClientID      string        `koanf:"client_id"`
	TopicPrefix   string        `koanf:"topic_prefix"`
	ProduceLinger time.Duration `koanf:"produce_linger"`
	Compression   string        `koanf:"compression"`
	TLS           TLS           `koanf:"tls"`
	SASL          SASL          `koanf:"sasl"`
	// SchemaRegistry receives the fb.v1 schemas (`fbctl schemas apply`).
	SchemaRegistry SchemaRegistry `koanf:"schema_registry"`
}

// SASL authenticates to Kafka: Mechanism is plain, scram-sha-256 or
// scram-sha-512 ("" = none).
type SASL struct {
	Mechanism string `koanf:"mechanism"`
	Username  string `koanf:"username"`
	Password  string `koanf:"password"`
}

// SchemaRegistry is a Confluent-compatible registry (Redpanda's on :8081).
type SchemaRegistry struct {
	URL      string `koanf:"url"`
	Username string `koanf:"username"`
	Password string `koanf:"password"`
}

type S3 struct {
	Endpoint    string `koanf:"endpoint"`
	AccessKey   string `koanf:"access_key"`
	SecretKey   string `koanf:"secret_key"`
	UseSSL      bool   `koanf:"use_ssl"`
	Region      string `koanf:"region"`
	MediaBucket string `koanf:"media_bucket"`
	RawBucket   string `koanf:"raw_bucket"`
}

type FB struct {
	BaseURL                string        `koanf:"base_url"`
	RegistrySeed           string        `koanf:"registry_seed"`
	RegistryRefresh        time.Duration `koanf:"registry_refresh"`
	FingerprintProfile     string        `koanf:"fingerprint_profile"`
	Locale                 string        `koanf:"locale"`
	AcceptLanguage         string        `koanf:"accept_language"`
	RequestTimeout         time.Duration `koanf:"request_timeout"`
	IdentityRPS            float64       `koanf:"identity_rps"`
	IdentityBurst          int           `koanf:"identity_burst"`
	IdentityMaxRequestsMin int           `koanf:"identity_max_requests_min"`
	IdentityMaxRequestsMax int           `koanf:"identity_max_requests_max"`
	PageRPS                int           `koanf:"page_rps"`
	PageRPSP0              int           `koanf:"page_rps_p0"`
	GlobalRPS              int           `koanf:"global_rps"`
	BootstrapAttempts      int           `koanf:"bootstrap_attempts"`
	CaptureRaw             string        `koanf:"capture_raw"`
	Proxy                  Proxy         `koanf:"proxy"`
	// QueryBreaker stops sending a (query, doc_id) whose responses keep
	// being rejected or failing to parse.
	QueryBreaker breaker.Config `koanf:"query_breaker"`
	// AIMD moves the fleet-wide ceiling (starting at GlobalRPS) with the
	// block rate the whole fleet sees.
	AIMD AIMD `koanf:"aimd"`
	// Browser is the headless-Chromium fallback (internal/fb/browser).
	Browser Browser `koanf:"browser"`
	// ShadowRate is the share of responses a registered candidate parser
	// also parses (internal/crawl/shadow); nothing runs without one.
	ShadowRate float64 `koanf:"shadow_rate"`
}

// Browser configures the headless-Chromium fallback: bootstraps of pages
// whose HTML carries no tokens, and doc_id discovery from the requests a
// rendered page makes.
type Browser struct {
	Enabled bool   `koanf:"enabled"`
	Binary  string `koanf:"binary"` // "" = $IGS_BROWSER_BINARY, PATH, /opt/pw-browsers/chromium
	// MaxConcurrent caps the browsers a process runs at once; MaxShare the
	// share of HTML loads that may fall back to a browser.
	MaxConcurrent int           `koanf:"max_concurrent"`
	MaxShare      float64       `koanf:"max_share"`
	Timeout       time.Duration `koanf:"timeout"`
	// NoSandbox runs Chromium without its sandbox, for containers that do
	// not allow it (it is always off as root).
	NoSandbox bool `koanf:"no_sandbox"`
}

// AIMD tunes the fleet ceiling controller (internal/scrape/ratelimit).
type AIMD struct {
	Enabled        bool          `koanf:"enabled"`
	Interval       time.Duration `koanf:"interval"`
	MinRPS         int           `koanf:"min_rps"`
	MaxRPS         int           `koanf:"max_rps"`
	Increase       float64       `koanf:"increase"` // share added per step while blocks stay low
	Decrease       float64       `koanf:"decrease"` // factor applied when blocks are high
	LowBlockRate   float64       `koanf:"low_block_rate"`
	HighBlockRate  float64       `koanf:"high_block_rate"`
	MinSamples     int           `koanf:"min_samples"`
	MinUtilization float64       `koanf:"min_utilization"` // raise only when the fleet uses the ceiling
}

type Proxy struct {
	Tiers   []ProxyTier         `koanf:"tiers"`
	Routing map[string][]string `koanf:"routing"`
	// Breaker opens an exit bucket (a static /24, or a gateway and country)
	// that is blocked too often.
	Breaker breaker.Config `koanf:"breaker"`
}

type ProxyTier struct {
	Name             string        `koanf:"name"`
	Kind             string        `koanf:"kind"` // static_list | gateway | direct
	Endpoints        []string      `koanf:"endpoints"`
	Gateway          string        `koanf:"gateway"`
	Username         string        `koanf:"username"`
	Password         string        `koanf:"password"`
	UsernameTemplate string        `koanf:"username_template"`
	StickyTTL        time.Duration `koanf:"sticky_ttl"`
	Country          string        `koanf:"country"`
	CostPerGBUSD     float64       `koanf:"cost_per_gb_usd"`
}

type Crawler struct {
	Stages []string `koanf:"stages"`
	// Slots is the in-flight tasks per pod and stage, shared by the lanes in
	// priority order (p0 > p1 > p2). Lanes caps each lane and LaneMin is
	// the share a waiting lane always gets. Slots 0 gives every lane its
	// own fixed Lanes[lane] slots instead.
	Slots                   int            `koanf:"slots"`
	Lanes                   map[string]int `koanf:"lanes"`
	LaneMin                 int            `koanf:"lane_min"`
	MaxInflightPerPartition int            `koanf:"max_inflight_per_partition"`
	YieldPages              int            `koanf:"yield_pages"`
	YieldAfter              time.Duration  `koanf:"yield_after"`
	TimelinePageSize        int            `koanf:"timeline_page_size"`
	CommentsPageSize        int            `koanf:"comments_page_size"`
	ConsecutiveOldStop      int            `koanf:"consecutive_old_stop"`
	MaxAttempts             int            `koanf:"max_attempts"`
	MaxTotalAttempts        int            `koanf:"max_total_attempts"`
	LeaseTTL                time.Duration  `koanf:"lease_ttl"`
	FreshnessP0             time.Duration  `koanf:"freshness_p0"`
	FreshnessP1             time.Duration  `koanf:"freshness_p1"`
	SuppressionRefresh      time.Duration  `koanf:"suppression_refresh"`
}

type Limits struct {
	MaxAgeDays          int    `koanf:"max_age_days"`
	MaxAgeCapDays       int    `koanf:"max_age_cap_days"`
	MaxPosts            int64  `koanf:"max_posts"`
	MaxDepth            int32  `koanf:"max_depth"`
	IncludeComments     bool   `koanf:"include_comments"`
	IncludeReplies      bool   `koanf:"include_replies"`
	IncludeMedia        bool   `koanf:"include_media"`
	IncludeCommentMedia bool   `koanf:"include_comment_media"`
	VideoPolicy         string `koanf:"video_policy"`
	BackfillVideoPolicy string `koanf:"backfill_video_policy"`
}

type Scheduler struct {
	PlannerInterval      time.Duration `koanf:"planner_interval"`
	PlannerBatch         int           `koanf:"planner_batch"`
	DefaultInterval      time.Duration `koanf:"default_interval"`
	Jitter               float64       `koanf:"jitter"`
	FinalizerInterval    time.Duration `koanf:"finalizer_interval"`
	FinalizeTimeout      time.Duration `koanf:"finalize_timeout"`
	ReaperAfter          time.Duration `koanf:"reaper_after"`
	PartitionMonthsAhead int           `koanf:"partition_months_ahead"`
	IncrementalOverlap   time.Duration `koanf:"incremental_overlap"`
	// Adaptive tiers: pages registered without an explicit interval move
	// between Tiers (highest MinPostsPerDay first) by their posting rate
	// over RateWindow; pages without a post for DormantAfter use
	// DormantInterval.
	Tiers           []Tier        `koanf:"tiers"`
	RateWindow      time.Duration `koanf:"rate_window"`
	DormantAfter    time.Duration `koanf:"dormant_after"`
	DormantInterval time.Duration `koanf:"dormant_interval"`
	PostRefresh     PostRefresh   `koanf:"post_refresh"`
	Budget          Budget        `koanf:"budget"`
}

// Tier is one adaptive scheduling tier.
type Tier struct {
	Name           string        `koanf:"name"`
	MinPostsPerDay float64       `koanf:"min_posts_per_day"`
	Interval       time.Duration `koanf:"interval"`
}

// PostRefresh re-reads the comments of posts older than the timeline
// overlap, on the page's scheduled jobs.
type PostRefresh struct {
	Enabled bool `koanf:"enabled"`
	// Windows by post age (ascending MaxAge): how often to refresh. A window
	// with Tiers only applies to pages in those tiers. Posts older than the
	// last window are no longer refreshed.
	Windows []RefreshWindow `koanf:"windows"`
	// MaxPerJob caps the post refreshes added to one page job.
	MaxPerJob int `koanf:"max_per_job"`
	// MaxBackoff caps the doubling of the interval after refreshes that
	// found nothing new (2^MaxBackoff).
	MaxBackoff int `koanf:"max_backoff"`
	// Recrawl posts with at least RecrawlMinComments comments in full when
	// they reach each age in RecrawlAt, to pick up edits and deletions.
	RecrawlAt          []time.Duration `koanf:"recrawl_at"`
	RecrawlMinComments int64           `koanf:"recrawl_min_comments"`
	// DetectDeletions marks a known top-level comment (and its replies)
	// deleted when a full read of its post (a recrawl, or a full job on a
	// post crawled before) no longer returns it. Only a complete read
	// counts: the comments it returned, with the reply counts Facebook gave
	// for them, must add up to the post's comment count from the same read,
	// within DeletionCountSlack of it (a share; 0 = exactly). Any other read
	// may be a partial answer, and its post is skipped.
	DetectDeletions    bool    `koanf:"detect_deletions"`
	DeletionCountSlack float64 `koanf:"deletion_count_slack"`
}

// RefreshWindow is one post-age band.
type RefreshWindow struct {
	MaxAge   time.Duration `koanf:"max_age"`
	Interval time.Duration `koanf:"interval"`
	Tiers    []string      `koanf:"tiers"`
}

// Budget bounds daily Facebook traffic fleet-wide. When the day's
// projected use exceeds it, backfill (p2) pauses first; if scheduled and
// on-demand work alone would still exceed it, the slower tiers' intervals
// are stretched. On-demand (p0) work is never held back.
type Budget struct {
	Enabled        bool          `koanf:"enabled"`
	RequestsPerDay int64         `koanf:"requests_per_day"` // 0 = no request budget
	BytesPerDay    int64         `koanf:"bytes_per_day"`    // 0 = no traffic budget
	Interval       time.Duration `koanf:"interval"`
	Stretch        float64       `koanf:"stretch"`         // interval factor for StretchTiers
	StretchTiers   []string      `koanf:"stretch_tiers"`   // slowest first
	ProjectionSpan time.Duration `koanf:"projection_span"` // recent use the projection extrapolates
}

type Media struct {
	Concurrency          int           `koanf:"concurrency"`
	TTL                  time.Duration `koanf:"ttl"`
	RequiredConsumers    []string      `koanf:"required_consumers"`
	MaxRetryableFailures int           `koanf:"max_retryable_failures"`
	SweepInterval        time.Duration `koanf:"sweep_interval"`
	OrphanInterval       time.Duration `koanf:"orphan_interval"`
	Presign              bool          `koanf:"presign"`
	PresignTTL           time.Duration `koanf:"presign_ttl"`
	MaxBytes             int64         `koanf:"max_bytes"`
}

type API struct {
	MaxRunningP0JobsPerClient int `koanf:"max_running_p0_jobs_per_client"`
	// MaxRunningP0Jobs caps on-demand jobs running fleet-wide (0 = no cap).
	MaxRunningP0Jobs     int `koanf:"max_running_p0_jobs"`
	DefaultRatePerMinute int `koanf:"default_rate_per_minute"`
}

type Privacy struct {
	PIIMode               string    `koanf:"pii_mode"`
	AuthorHMACKey         string    `koanf:"author_hmac_key"`
	DownloadAuthorAvatars bool      `koanf:"download_author_avatars"`
	Retention             Retention `koanf:"retention"`
}

type Retention struct {
	CommentsDays        int `koanf:"comments_days"`
	AuthorsInactiveDays int `koanf:"authors_inactive_days"`
	MetricsDays         int `koanf:"metrics_days"`
	RawPayloadDays      int `koanf:"raw_payload_days"`
	MediaRowsDays       int `koanf:"media_rows_days"`
	JobsDays            int `koanf:"jobs_days"`
}

// Load reads the config from defaults, an optional file and env vars.
func Load(path string) (*Config, error) {
	var c Config
	if _, err := pconfig.Load(configs.Default(), path, EnvPrefix, &c); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// MustLoadFromFlags parses --config (or IGS_CONFIG) and loads the config.
func MustLoadFromFlags(fs *flag.FlagSet, args []string) *Config {
	path := fs.String("config", os.Getenv("IGS_CONFIG"), "YAML config file(s), comma separated; later files win")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	c, err := Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return c
}

// Validate rejects configs that would misbehave at runtime.
func (c *Config) Validate() error {
	var errs []error
	if c.Crawler.YieldPages <= 0 {
		errs = append(errs, errors.New("crawler.yield_pages must be > 0"))
	}
	if c.Limits.MaxAgeDays > c.Limits.MaxAgeCapDays {
		errs = append(errs, errors.New("limits.max_age_days exceeds limits.max_age_cap_days"))
	}
	switch c.Privacy.PIIMode {
	case "full", "pseudonymized":
	default:
		errs = append(errs, fmt.Errorf("privacy.pii_mode %q must be full or pseudonymized", c.Privacy.PIIMode))
	}
	if c.Service.Env == "prod" && c.Privacy.AuthorHMACKey == "dev-only-change-me" {
		errs = append(errs, errors.New("privacy.author_hmac_key must be set in prod"))
	}
	if c.Media.TTL <= 0 {
		errs = append(errs, errors.New("media.ttl must be > 0"))
	}
	if c.Scheduler.DefaultInterval <= 0 {
		errs = append(errs, errors.New("scheduler.default_interval must be > 0"))
	}
	if c.Scheduler.DormantAfter > 0 && c.Scheduler.DormantInterval <= 0 {
		errs = append(errs, errors.New("scheduler.dormant_interval must be > 0 when dormant_after is set"))
	}
	for i, t := range c.Scheduler.Tiers {
		if t.Name == "" || t.Interval <= 0 {
			errs = append(errs, fmt.Errorf("scheduler.tiers[%d] needs a name and an interval", i))
		}
		if i > 0 && t.MinPostsPerDay > c.Scheduler.Tiers[i-1].MinPostsPerDay {
			errs = append(errs, errors.New("scheduler.tiers must be ordered by min_posts_per_day, highest first"))
		}
	}
	for i, w := range c.Scheduler.PostRefresh.Windows {
		if w.Interval <= 0 || (i > 0 && w.MaxAge <= c.Scheduler.PostRefresh.Windows[i-1].MaxAge) {
			errs = append(errs, errors.New("scheduler.post_refresh.windows need intervals and ascending max_age"))
			break
		}
	}
	switch c.Kafka.SASL.Mechanism {
	case "", "plain", "scram-sha-256", "scram-sha-512":
	default:
		errs = append(errs, fmt.Errorf("kafka.sasl.mechanism %q: want plain, scram-sha-256 or scram-sha-512", c.Kafka.SASL.Mechanism))
	}
	if c.Kafka.SASL.Mechanism != "" && c.Kafka.SASL.Username == "" {
		errs = append(errs, errors.New("kafka.sasl needs a username"))
	}
	if s := c.Scheduler.PostRefresh.DeletionCountSlack; s < 0 || s >= 1 {
		errs = append(errs, errors.New("scheduler.post_refresh.deletion_count_slack must be in [0, 1)"))
	}
	if c.RegistrySync.Interval <= 0 || c.RegistrySync.Timeout <= 0 {
		errs = append(errs, errors.New("registry_sync.interval and registry_sync.timeout must be > 0"))
	}
	return errors.Join(errs...)
}
