// Package config defines the configuration shared by every fbs binary.
package config

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/breaker"
	pconfig "github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/registry"
)

// EnvPrefix is the prefix of environment variable overrides.
const EnvPrefix = "TKS_"

type Config struct {
	Service  Service  `koanf:"service"`
	Log      Log      `koanf:"log"`
	HTTP     HTTP     `koanf:"http"`
	Postgres Postgres `koanf:"postgres"`
	Redis    Redis    `koanf:"redis"`
	Kafka    Kafka    `koanf:"kafka"`
	S3       S3       `koanf:"s3"`
	FB       FB       `koanf:"fb"`
	// TT is tks's TikTok client and freshness (configs/tt/default.yaml).
	// FB is named for the Facebook pipeline tks was copied from (removed in
	// M9c2): tks-spike reads the owner's proxy tiers from fb.proxy
	// (phase0/proxies.yaml); the rest is unread.
	TT        TT        `koanf:"tt"`
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

// Postgres is the database: a DSN, or (when Host is set, as on the
// production platform) the server's parts, which win over the DSN. User is
// the component's own role (each component logs in with its own: M18);
// ReadUser, when set, opens the API's read pool as a read-only role on the
// same server. CA is a PEM certificate given as a value (CAFile as a
// file); with sslmode require it makes the connection verify the server's
// certificate, as libpq does. Roles are granted their rights by `tksctl
// migrate up` (ttstore.Grant).
type Postgres struct {
	DSN      string `koanf:"dsn"`
	ReadDSN  string `koanf:"read_dsn"`
	MaxConns int32  `koanf:"max_conns"`

	Host         string `koanf:"host"`
	Port         int    `koanf:"port"`
	Database     string `koanf:"database"`
	User         string `koanf:"user"`
	Password     string `koanf:"password"`
	ReadUser     string `koanf:"read_user"`
	ReadPassword string `koanf:"read_password"`
	SSLMode      string `koanf:"sslmode"`
	CA           string `koanf:"ca"`
	CAFile       string `koanf:"ca_file"`
	// Roles are the login roles of the components, granted their rights
	// after each migration; empty ones are skipped.
	Roles PostgresRoles `koanf:"roles"`
}

// PostgresRoles names each component's role (docs/runbooks/tks-production.md).
type PostgresRoles struct {
	// API writes registrations, jobs, keys, imports and reviews.
	API string `koanf:"api"`
	// Reader is read-only: the API's read pool, and people reading the data.
	Reader    string `koanf:"reader"`
	Persister string `koanf:"persister"`
	Scheduler string `koanf:"scheduler"`
	// Crawler is every component that reads TikTok: the crawlers, the
	// canary and the media downloader.
	Crawler string `koanf:"crawler"`
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
	// SchemaRegistry receives the tt.v1 schemas (`tksctl schemas apply`).
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
	// CreateBuckets makes missing buckets at start; off where the platform
	// owns them (production), which also skips the existence check.
	CreateBuckets bool `koanf:"create_buckets"`
	// MediaPrefix starts every media object's key ("tks/" in production).
	MediaPrefix string `koanf:"media_prefix"`
	// RawKeepPrefix holds the raw answers kept for good (failures and the
	// sample: "tks/keep/" in production); RawRestPrefix the others
	// ("tks/30d/", which the platform expires after 30 days).
	RawKeepPrefix string `koanf:"raw_keep_prefix"`
	RawRestPrefix string `koanf:"raw_rest_prefix"`
	// BackupBucket holds the database dumps of `tksctl backup run` (the
	// platform's S3_BUCKET_BACKUPS; M16), under BackupPrefix + "postgres/"
	// ("tks/" in production).
	BackupBucket string `koanf:"backup_bucket"`
	BackupPrefix string `koanf:"backup_prefix"`
}

type FB struct {
	BaseURL            string        `koanf:"base_url"`
	RegistrySeed       string        `koanf:"registry_seed"`
	RegistryRefresh    time.Duration `koanf:"registry_refresh"`
	FingerprintProfile string        `koanf:"fingerprint_profile"`
	// FingerprintProfiles, when set, are the profiles new identities pick
	// from at random, so a pod's identities do not all send one TLS and
	// header fingerprint; empty means FingerprintProfile alone.
	FingerprintProfiles    []string      `koanf:"fingerprint_profiles"`
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
	// Detect replaces parts of the built-in detect rules.
	Detect Detect `koanf:"detect"`
}

// TT configures tks's TikTok client, its browser signer and how fresh the
// data is kept (docs/tiktok ADR 0006 §6, ADR 0007 §4). Defaults are in
// configs/tt/default.yaml; values marked there as after Phase 0 are
// provisional.
// Content is tt.content: the shared contract's events (securebrand.content.v1,
// Abdullah95k/securebrand-contracts), published by the persister for each
// changed row alongside tt.*. Off until the platform has made the content.*
// topics: a missing topic would hold the persister back.
type Content struct {
	Enabled bool `koanf:"enabled"`
	// Production applies the contract's production checks to every event
	// before it leaves (reserved platforms, test-only values).
	Production bool `koanf:"production"`
	// Media is the contract's media hand-off (M15b): content.media.ready.v1
	// beside tt.media.ready.v1.
	Media ContentMedia `koanf:"media"`
}

// ContentMedia is tt.content.media: what every content.media.ready.v1
// event says about its file's life. tks deletes nothing (D5): another
// service deletes the file when every required consumer has acknowledged
// it, or at delete_after, whichever is first.
type ContentMedia struct {
	// DeleteAfter is the file's deadline after it is stored: 72 hours by
	// default, at least 24 (the contract's minimum).
	DeleteAfter time.Duration `koanf:"delete_after"`
	// RequiredConsumers must acknowledge a file before it may be deleted:
	// names from the contract's registry (registry/platforms.yaml).
	RequiredConsumers []string `koanf:"required_consumers"`
}

// MinMediaLifetime is the contract's shortest delete_after after a file
// is stored.
const MinMediaLifetime = 24 * time.Hour

type TT struct {
	BaseURL string `koanf:"base_url"`
	// OperationSeed is the registry seed's path in configs (configs.FS).
	OperationSeed       string        `koanf:"operation_seed"`
	RegistryRefresh     time.Duration `koanf:"registry_refresh"`
	FingerprintProfile  string        `koanf:"fingerprint_profile"`
	FingerprintProfiles []string      `koanf:"fingerprint_profiles"`
	AcceptLanguage      string        `koanf:"accept_language"`
	RequestTimeout      time.Duration `koanf:"request_timeout"`
	IdentityRPS         float64       `koanf:"identity_rps"`
	IdentityBurst       int           `koanf:"identity_burst"`
	// An identity serves a random number of requests in [min, max] before
	// it is replaced.
	IdentityMaxRequestsMin int `koanf:"identity_max_requests_min"`
	IdentityMaxRequestsMax int `koanf:"identity_max_requests_max"`
	// AccountRPS caps one account's requests fleet wide; AccountRPSP0 for
	// on-demand jobs. GlobalRPS is the fleet's ceiling and AIMD's start.
	AccountRPS       int    `koanf:"account_rps"`
	AccountRPSP0     int    `koanf:"account_rps_p0"`
	GlobalRPS        int    `koanf:"global_rps"`
	ListPageSize     int    `koanf:"list_page_size"`
	CommentsPageSize int    `koanf:"comments_page_size"`
	RepliesPageSize  int    `koanf:"replies_page_size"`
	CaptureRaw       string `koanf:"capture_raw"`
	// CaptureRawSample is the share of good answers kept for good with
	// capture_raw all or sample (0.01: the 1 % sample).
	CaptureRawSample float64 `koanf:"capture_raw_sample"`
	// Content publishes the shared contract's content.* events from the
	// persister (M15; docs/tiktok/contract-mapping.md).
	Content      Content        `koanf:"content"`
	Proxy        Proxy          `koanf:"proxy"`
	QueryBreaker breaker.Config `koanf:"query_breaker"`
	AIMD         AIMD           `koanf:"aimd"`
	// Browser is the bootstrap fallback for shell pages; the signer has
	// its own pages.
	Browser   Browser    `koanf:"browser"`
	Detect    Detect     `koanf:"detect"`
	Signer    Signer     `koanf:"signer"`
	Canary    TTCanary   `koanf:"canary"`
	Freshness Freshness  `koanf:"freshness"`
	Comments  TTComments `koanf:"comments"`
	Posts     TTPosts    `koanf:"posts"`
	Media     TTMedia    `koanf:"media"`
	// Subtitles fetches videos' subtitle texts (M17).
	Subtitles TTSubtitles `koanf:"subtitles"`
	// Retention is tks's own (privacy.retention was the removed Facebook
	// pipeline's): every value 0 by default, so tks keeps everything it
	// collects (owner, 2026-11-01; docs/tiktok ADR 0007 §14). A positive
	// value turns that retention step back on.
	Retention Retention `koanf:"retention"`
	// Admission paces the first reads of new accounts (docs/tiktok/
	// discovery-plan.md §5.3); Discovery switches account discovery.
	Admission Admission `koanf:"admission"`
	Discovery Discovery `koanf:"discovery"`
	// Cost prices tks's day (build brief §8 A5) and holds the owner's
	// budget per account (D9).
	Cost TTCost `koanf:"cost"`
}

// TTCost prices tks's day (build brief §8 A5): proxy IPs and data, browser
// time of the signer pages, compute per account and storage, over the
// accounts it checks; tks_cost_per_account_usd. The prices are the owner's
// (2026-11-05: an ISP IP a month; the rest assumptions to replace with real
// prices); 0 leaves a part unpriced. BudgetPerAccountUSD is the owner's
// budget per account a day: over it, backfills pause and checks go on
// (owner, 2026-11-05).
type TTCost struct {
	// Interval is how often the leader prices the last 24 hours.
	Interval   time.Duration `koanf:"interval"`
	IPMonthUSD float64       `koanf:"ip_month_usd"`
	// IPs paid for; 0 counts the endpoints of tt.proxy.tiers' static lists.
	IPs                  int     `koanf:"ips"`
	BrowserHourUSD       float64 `koanf:"browser_hour_usd"`
	ComputeUSDPerAccount float64 `koanf:"compute_usd_per_account_day"`
	StorageGBMonthUSD    float64 `koanf:"storage_gb_month_usd"`
	BudgetPerAccountUSD  float64 `koanf:"budget_per_account_usd"`
	// AlertShare is the share of the budget the alert fires at (0.9).
	AlertShare float64 `koanf:"alert_share"`
}

// Admission paces new accounts' first reads (their 365-day backfills) so
// that thousands registered at once do not spend the day's proxy data in
// hours (docs/tiktok/discovery-plan.md §5.3). Checks of accounts already
// read never wait. New accounts wait in a queue: the owner's own (API,
// import) first, then discovered ones by Iraq score.
type Admission struct {
	// PerHour admits at most this many new accounts an hour. 0 derives the
	// rate from the traffic budget (scheduler.budget.requests_per_day less
	// the day's projected traffic, over the hours left, at
	// RequestsPerAccount each), or admits every new account at once when
	// no request budget is set (as before M10a).
	PerHour int `koanf:"per_hour"`
	// RequestsPerAccount is a new account's first read in requests: its
	// year of posts and their comments (C3: about 380 posts a year at the
	// median account, a few signed comment pages each). It sizes the
	// derived rate and the capacity the queue needs.
	RequestsPerAccount int `koanf:"requests_per_account"`
	// Capacity per IP and per signer page a day, and the share of signed
	// calls, from Phase 0 (C4: per 1,000 accounts 678,256 calls a day
	// through 2 IPs and 2 signer pages, 95 % signed): what draining the
	// queue in a day would take.
	IPRequestsPerDay int     `koanf:"ip_requests_per_day"`
	PageSignsPerDay  int     `koanf:"page_signs_per_day"`
	SignedShare      float64 `koanf:"signed_share"`
}

// Discovery switches account discovery (docs/tiktok/discovery-plan.md
// §5.6). Bulk import (part A) is not discovery and needs none of it.
type Discovery struct {
	// Enabled turns every source off when false.
	Enabled bool `koanf:"enabled"`
	// Sources switches each source by name (mention, reply, commenter,
	// search, hashtag, sound, feed, related, following, follower); a source
	// not listed is off.
	Sources map[string]bool `koanf:"sources"`
	// MaxNewPerDay caps accounts discovery adds a day, for testing; 0 is no
	// cap (the owner's decision for production: the budget paces them).
	MaxNewPerDay int `koanf:"max_new_per_day"`
	// Client names the API client that holds discovered accounts'
	// registrations; tks creates it (with no usable key) when it is missing.
	Client string `koanf:"client"`
	// Entry is the entry check every found account goes through, and the
	// dormant list (docs/tiktok/discovery-plan.md §4).
	Entry EntryCheck `koanf:"entry"`
	// Score is the Iraq score's weights and thresholds (discovery-plan §3).
	Score IraqScore `koanf:"score"`
	// CountBatch is how many of part B's sightings (people a monitored
	// account's new posts, bio and comments bring in: discovery-plan §7)
	// the leader counts at a time; it counts batches for up to a few
	// seconds a tick.
	CountBatch int `koanf:"count_batch"`
	// Follows is part D, the following and followers lists of monitored
	// accounts (docs/tiktok/discovery-plan.md §9; sources following and
	// follower).
	Follows FollowLists `koanf:"follows"`
	// Part C, TikTok's discovery pages (docs/tiktok/discovery-plan.md §8;
	// sources search, hashtag, sound, related and feed): Pages paces every
	// page read, the others say what each source reads and how often.
	Pages    DiscoveryPages `koanf:"pages"`
	Search   SearchPages    `koanf:"search"`
	Hashtags UsedPages      `koanf:"hashtags"`
	Sounds   UsedPages      `koanf:"sounds"`
	Related  RelatedPages   `koanf:"related"`
	Feed     FeedPages      `koanf:"feed"`
}

// DiscoveryPages paces discovery part C's reads (docs/tiktok/
// discovery-plan.md §8): every read is one target (a keyword, a hashtag, a
// sound, a post's related posts, the feed) read up to its source's
// max_pages, in the backfill lane, which the budget pauses.
type DiscoveryPages struct {
	// Batch is how many targets the leader sends to be read per tick;
	// MaxRunning caps the reads running fleet-wide.
	Batch      int `koanf:"batch"`
	MaxRunning int `koanf:"max_running"`
	// DayPages caps the pages every part C read together fetches in a UTC
	// day (0: no cap; the budget still pauses the lane).
	DayPages int `koanf:"day_pages"`
	// RetryAfter: a claimed target whose read did not record is due again
	// after this.
	RetryAfter time.Duration `koanf:"retry_after"`
	// Refresh is how often the leader picks the hashtags, sounds and posts
	// monitored accounts use as targets (a query over their recent posts).
	Refresh time.Duration `koanf:"refresh"`
}

// SearchPages is keyword search over the Iraq list (discovery-plan §8.2).
// From US exits TikTok answers a logged-out search with 2483 ("Please
// login"); from Iraq it served 9 pages (T37). So search runs only through a
// tier TikTok places in or near Iraq: Tier empty is off, whatever the
// source switch says.
type SearchPages struct {
	Tier     string        `koanf:"tier"`
	Every    time.Duration `koanf:"every"`
	MaxPages int           `koanf:"max_pages"`
}

// UsedPages are hashtags' or sounds' posts (discovery-plan §8.2): those
// that at least MinAccounts monitored accounts used in posts of the last
// Window, the Top most used, plus, for hashtags with Keywords, the Iraq
// list's one-word terms as hashtags. Each is read every Every, up to
// MaxPages pages, newest first as TikTok serves them. Skip lists hashtags
// never read (fyp and its kind: worldwide, not Iraqi).
type UsedPages struct {
	Every       time.Duration `koanf:"every"`
	MaxPages    int           `koanf:"max_pages"`
	Top         int           `koanf:"top"`
	MinAccounts int           `koanf:"min_accounts"`
	Window      time.Duration `koanf:"window"`
	Keywords    bool          `koanf:"keywords"`
	Skip        []string      `koanf:"skip"`
}

// RelatedPages are monitored posts' related posts (discovery-plan §8.2):
// each post of a monitored account made within Window is read once, the
// most played first, up to PerTick new posts a tick.
type RelatedPages struct {
	Window  time.Duration `koanf:"window"`
	PerTick int           `koanf:"per_tick"`
}

// FeedPages is the For You feed: Reads reads every Every. From US exits it
// is American (C6 run 2), so it is worth running only through a tier
// TikTok places in or near Iraq (Tier; empty takes the list class's
// routing).
type FeedPages struct {
	Tier  string        `koanf:"tier"`
	Every time.Duration `koanf:"every"`
	Reads int           `koanf:"reads"`
}

// FollowLists paces discovery part D (docs/tiktok/discovery-plan.md §9):
// every monitored account's following list (unless its profile hides it)
// and followers list are read, each read taking the newest people down to
// those already stored, then going deeper from where the last read
// stopped, up to MaxPages. How deep a large account's list goes logged out
// is still open (T41): MaxPages and DayPages are the caps.
type FollowLists struct {
	// FollowingEvery and FollowersEvery: how often each list is read (a
	// week and a day, discovery-plan §9). A read that failed is tried again
	// after RetryAfter.
	FollowingEvery time.Duration `koanf:"following_every"`
	FollowersEvery time.Duration `koanf:"followers_every"`
	RetryAfter     time.Duration `koanf:"retry_after"`
	// MaxPages caps one read (one account's list, at most once a day for
	// followers); DayPages caps the pages every read together fetches in a
	// UTC day (0: no cap; the budget still pauses the lane).
	MaxPages int `koanf:"max_pages"`
	DayPages int `koanf:"day_pages"`
	// PageSize is the people a page asks for (the web app's 30).
	PageSize int `koanf:"page_size"`
	// KnownPages: a re-read's walk from the top stops after this many pages
	// in a row that hold nobody new.
	KnownPages int `koanf:"known_pages"`
	// Batch is how many lists the leader sends to be read per tick;
	// MaxRunning caps the reads running fleet-wide (lane p2).
	Batch      int `koanf:"batch"`
	MaxRunning int `koanf:"max_running"`
}

// EntryCheck is discovery's entry check (docs/tiktok/discovery-plan.md §4):
// one read of a found account's post list decides whether it is added
// (public, a post within ActiveFor, the Iraq score passes), kept as not
// Iraqi (yet), or put on the dormant list (private, quiet, gone) until
// RecheckAfter, after which it is read again only when a source finds it
// again.
type EntryCheck struct {
	// ActiveFor: an account qualifies only with a post this recent (90
	// days, the owner's decision 3).
	ActiveFor time.Duration `koanf:"active_for"`
	// RecheckAfter: a dormant account is not read again before this (180
	// days, the owner's decision 3).
	RecheckAfter time.Duration `koanf:"recheck_after"`
	// Batch is how many found accounts the leader sends to their entry
	// check per tick; MaxRunning caps the entry checks running fleet-wide
	// (they run in the backfill lane, which the budget pauses).
	Batch      int `koanf:"batch"`
	MaxRunning int `koanf:"max_running"`
	// MaxAttempts: an entry check that fails (no answer, not a status) is
	// tried this many times before the account goes on the dormant list as
	// unreadable.
	MaxAttempts int `koanf:"max_attempts"`
	// Posts is how many of the account's newest stored posts the score
	// reads (a list page: 15).
	Posts int `koanf:"posts"`
}

// IraqScore weighs the signals that an account is Iraqi, run from Iraq or
// followed by an Iraqi audience (docs/tiktok/discovery-plan.md §3). Each
// kind of signal counts once, with its strength's weight; an account
// qualifies when the sum reaches Pass. With the defaults one strong signal
// passes, as do two different medium ones; weak signals together add at
// most WeakMax, never enough alone or with one medium signal.
type IraqScore struct {
	Strong  float64 `koanf:"strong"`
	Medium  float64 `koanf:"medium"`
	Weak    float64 `koanf:"weak"`
	WeakMax float64 `koanf:"weak_max"`
	Pass    float64 `koanf:"pass"`
	// Regions are the post regions (locationCreated) that count as made in
	// Iraq.
	Regions []string `koanf:"regions"`
	// CaptionPostsStrong: captions or hashtags naming Iraqi places in this
	// many of the posts read make the signal strong (one post: medium).
	CaptionPostsStrong int `koanf:"caption_posts_strong"`
	// AudienceStrong: an audience signal (followed, mentioned or replied to
	// by monitored accounts, shown to viewers in Iraq) from this many
	// different accounts or searches is strong (fewer: medium).
	AudienceStrong int `koanf:"audience_strong"`
	// Languages are the post text languages (textLanguage) that count as
	// the weak "writes in Arabic or Kurdish" signal.
	Languages []string `koanf:"languages"`
}

// DiscoverySources are the ways discovery finds accounts (account_sources.how
// beyond api and import).
var DiscoverySources = []string{"mention", "reply", "commenter", "search", "hashtag", "sound", "feed", "related", "following", "follower"}

// SourceOn says whether discovery runs source.
func (d Discovery) SourceOn(source string) bool { return d.Enabled && d.Sources[source] }

// TTMedia is tks's media download switch (build brief D5). tks's tasks
// read it instead of the shared limits.include_media, which the copied
// Facebook pipeline's tests still run with on (docs/tiktok ADR 0007 §8).
// tks-media-downloader takes its concurrency, size cap and presigning from
// the shared media section; it sets no TTL and deletes nothing, so media.ttl,
// required_consumers and the sweep intervals are not read (deleting the
// files is another service's job: owner, 2026-11-02; ADR 0007 §15).
type TTMedia struct {
	// Include queues downloads of covers, photos and videos; media
	// metadata is published either way.
	Include bool `koanf:"include"`
	// CookieDomains are the hosts, with their subdomains, a video download
	// sends its tt_chain_token cookie to: a browser holds the cookie for
	// .tiktok.com, which reaches the video hosts (T18). Redirects to other
	// hosts go without it.
	CookieDomains []string `koanf:"cookie_domains"`
}

// TTSubtitles is tt.subtitles: videos' subtitle tracks (video.subtitleInfos:
// TikTok's speech-to-text and its machine translations), whose WebVTT files
// the account stage fetches before their links expire and publishes on
// tt.subtitle.v1 (M17). A track already stored is not fetched again.
type TTSubtitles struct {
	// Include fetches the files (on by default); the tracks' languages
	// are published on the post either way.
	Include bool `koanf:"include"`
	// MaxBytes caps one file; a larger one is recorded without its text.
	MaxBytes int64 `koanf:"max_bytes"`
	// MaxTracks caps the tracks fetched per post, speech-to-text first.
	MaxTracks int `koanf:"max_tracks"`
	// Timeout bounds one file's fetch.
	Timeout time.Duration `koanf:"timeout"`
}

// Signer modes.
const (
	SignerNone    = "none"
	SignerBrowser = "browser"
	// SignerRemote has tks-signer sign: the browser pages run in their own
	// capped service, not in the process that sends the calls (M9d).
	SignerRemote = "remote"
)

// Signer configures the browser signer (build brief D11): TikTok pages in
// headless Chromium whose own script signs the calls that need it. With
// mode browser the pages are the process's own (the probes, Phase 0);
// with remote they are tks-signer's, which reads the same section.
type Signer struct {
	Mode string `koanf:"mode"` // none | browser | remote
	// RemoteURL is tks-signer's address (mode remote), e.g.
	// http://tks-signer:8080.
	RemoteURL string `koanf:"remote_url"`
	// Pages is the pages one process keeps open (tks-signer's, or a
	// process's own in mode browser).
	Pages int `koanf:"pages"`
	// A page is reopened after PageMaxSigns signatures or PageMaxAge.
	PageMaxSigns int           `koanf:"page_max_signs"`
	PageMaxAge   time.Duration `koanf:"page_max_age"`
	SignTimeout  time.Duration `koanf:"sign_timeout"`
	PageTimeout  time.Duration `koanf:"page_timeout"`
	// MaxShare is the share of calls signed above which the signer alerts.
	MaxShare float64 `koanf:"max_share"`
	// IdleClose closes a page unused this long, and MaxPageMB a page whose
	// browser uses more memory (0: never). The pilot server froze when an
	// idle signer page's Chrome grew to 7 GB (2026-11-03).
	IdleClose time.Duration `koanf:"idle_close"`
	MaxPageMB float64       `koanf:"max_page_mb"`
	// KillPageMB closes a page whose browser uses more memory even while
	// it signs, within the sweep's 10 s (0: never): the guard that keeps a
	// runaway page from taking its pod's whole memory limit.
	KillPageMB float64 `koanf:"kill_page_mb"`
	// MaxRefusals replaces a page whose signed calls TikTok refused this
	// many times in a row (0: never).
	MaxRefusals int `koanf:"max_refusals"`
	// Freeze keeps each page frozen between its signatures; Lite starts
	// Chrome without autoplay and images; OpenOn is the page a new signer
	// page opens on: the call's own (call, a post page for comments) or the
	// account's profile page (profile).
	Freeze bool   `koanf:"freeze"`
	Lite   bool   `koanf:"lite"`
	OpenOn string `koanf:"open_on"`
}

// TTCanary configures tks-canary (build brief M8): it checks each
// operation variant the crawler uses on canary accounts and promotes a
// working one when TikTok stops answering the active one, or when an
// unsigned variant answers where the active one needs a signature.
type TTCanary struct {
	Interval time.Duration `koanf:"interval"`
	// Accounts are public TikTok usernames that post and get comments and
	// replies; they are checked in order until every operation has a
	// verdict. Set them in the deployment, never in git (build brief §7).
	Accounts []string `koanf:"accounts"`
	// Operations are the registry operations checked: the crawler's.
	Operations []string `koanf:"operations"`
	// Tier is the proxy tier the canary uses; "" = the first one.
	Tier string `koanf:"tier"`
	// Twins adds each variant's signed or unsigned twin as a candidate
	// (TikTok changes which calls it accepts unsigned: T4, T25).
	Twins bool `koanf:"twins"`
	// Confirm is how many canary accounts must refuse a variant in a run
	// before it counts as stale: one refusal may be the identity's.
	Confirm int `koanf:"confirm"`
	// Promote switches promotion on; false records checks only (dry run).
	Promote bool `koanf:"promote"`
	// KeepFallbacks is how many former variants stay as fallbacks.
	KeepFallbacks int `koanf:"keep_fallbacks"`
	// RetireAfter retires a candidate or fallback after this many failed
	// checks in a row; RecheckRetired makes a retired twin a candidate
	// again after this long.
	RetireAfter    int           `koanf:"retire_after"`
	RecheckRetired time.Duration `koanf:"recheck_retired"`
	// Timeout bounds one run.
	Timeout time.Duration `koanf:"timeout"`
}

// Where a signer page opens (tt.signer.open_on).
const (
	SignerOpenOnCall    = "call"
	SignerOpenOnProfile = "profile"
)

// Freshness says how often each kind of data is re-read (build brief D2).
type Freshness struct {
	// ListInterval is each account's post-list check (2 h 45 min).
	ListInterval time.Duration `koanf:"list_interval"`
	// AccountRefresh is the account's own refresh; PageRefresh the profile
	// page's, the bio link's only source.
	AccountRefresh time.Duration `koanf:"account_refresh"`
	PageRefresh    time.Duration `koanf:"page_refresh"`
	// BackfillDays is how far back a new account is read.
	BackfillDays int `koanf:"backfill_days"`
	// MetricsWalks walk the list deeper than the check, for metrics.
	MetricsWalks []MetricsWalk `koanf:"metrics_walks"`
	// CommentWindows bound comment re-reads by post age (ascending MaxAge).
	CommentWindows []CommentWindow `koanf:"comment_windows"`
	// Inactive spaces the checks of the owner's accounts that went private,
	// banned or missing (docs/tiktok/discovery-plan.md §5.4).
	Inactive InactiveChecks `koanf:"inactive"`
}

// InactiveChecks: a private account is checked every Private.First for
// Private.For after it was found private, then every Private.Then; a
// banned or missing one likewise by Gone. A check that finds it active
// puts it back on its interval.
type InactiveChecks struct {
	Private InactiveStep `koanf:"private"`
	Gone    InactiveStep `koanf:"gone"`
}

// InactiveStep is a check interval that lengthens after a while.
type InactiveStep struct {
	First time.Duration `koanf:"first"`
	For   time.Duration `koanf:"for"`
	Then  time.Duration `koanf:"then"`
}

// Interval is the time to the next check of an account inactive for d.
func (s InactiveStep) Interval(d time.Duration) time.Duration {
	if d < s.For {
		return s.First
	}
	return s.Then
}

// MetricsWalk walks an account's list every Every, back Back.
type MetricsWalk struct {
	Every time.Duration `koanf:"every"`
	Back  time.Duration `koanf:"back"`
}

// CommentWindow re-reads a post's comments at most every Interval while it
// is younger than MaxAge, and only when its comment count went up.
type CommentWindow struct {
	MaxAge   time.Duration `koanf:"max_age"`
	Interval time.Duration `koanf:"interval"`
}

// Comment orders (ADR 0007 §5).
const (
	CommentsRanked = "ranked"
	CommentsNewest = "newest"
)

// TTComments configures comment reads.
type TTComments struct {
	// Order is TikTok's comment order: ranked (re-read to the cap) or
	// newest (read until the first stored comment).
	Order          string `koanf:"order"`
	MaxPerPost     int64  `koanf:"max_per_post"`
	IncludeReplies bool   `koanf:"include_replies"`
	// DeletionSlack: how far, as a share of the post's comment count, the
	// comments a whole read returned (each with the replies TikTok counts
	// for it) may be from that count for the read to be checked for
	// deletions (hidden and filtered comments make up a gap; T15).
	DeletionSlack float64 `koanf:"deletion_slack"`
}

// TTPosts configures what the account stage does with posts.
type TTPosts struct {
	Unavailable PostsUnavailable `koanf:"unavailable"`
}

// PostsUnavailable finds stored posts that are no longer public
// (docs/tiktok/discovery-plan.md §17): a full list page covering a post's
// date that does not return it makes the post a candidate, and one read of
// its page by plain HTTP confirms it.
type PostsUnavailable struct {
	Enabled bool `koanf:"enabled"`
	// MinPagePosts: a list page holding fewer posts marks nothing (a short
	// or empty page may be TikTok's time window or a reading problem).
	MinPagePosts int `koanf:"min_page_posts"`
	// MaxPerPage: more candidates than this on one page is treated as a
	// reading problem, and none is checked.
	MaxPerPage int `koanf:"max_per_page"`
	// MaxChecks caps the post pages one account check reads to confirm
	// candidates; the rest wait for a later walk over their dates.
	MaxChecks int `koanf:"max_checks"`
	// Codes are the post page's statusCodes that mean the post is not
	// public; any other code marks nothing and is counted (unknown_code).
	Codes []int64 `koanf:"codes"`
	// AccountCodes mean the account, not the post, is unavailable: its
	// posts keep their status.
	AccountCodes []int64 `koanf:"account_codes"`
}

// Detect replaces parts of the platform's built-in detect rules
// (internal/scrape/detect), so a new error code, marker or HTTP status can
// be classed without a deploy. A list that is set replaces the built-in
// one; one left unset keeps it. Same fields as detect.Overrides, which it
// converts to.
type Detect struct {
	RateLimitCodes   []int64 `koanf:"rate_limit_codes"`
	SoftCodes        []int64 `koanf:"soft_codes"`
	LoginCodes       []int64 `koanf:"login_codes"`
	CheckpointCodes  []int64 `koanf:"checkpoint_codes"`
	UnavailableCodes []int64 `koanf:"unavailable_codes"`
	// StaleMessages are regular expressions matched against error messages.
	StaleMessages         []string `koanf:"stale_messages"`
	LoginMarkers          []string `koanf:"login_markers"`
	BlockMarkers          []string `koanf:"block_markers"`
	CheckpointPaths       []string `koanf:"checkpoint_paths"`
	LoginPaths            []string `koanf:"login_paths"`
	CheckpointPageMarkers []string `koanf:"checkpoint_page_markers"`
	LoginPageMarkers      []string `koanf:"login_page_markers"`
	// Statuses classes HTTP statuses before the built-in rules: "403" or
	// "html:400" (page loads only; "graphql:" for API calls) to a class
	// name: rate_limited, login_wall, checkpoint, soft_error,
	// registry_stale, content_unavailable, transport or ok.
	Statuses map[string]string `koanf:"statuses"`
}

// Browser configures the headless-Chromium fallback: bootstraps of pages
// whose HTML carries no tokens, and doc_id discovery from the requests a
// rendered page makes.
type Browser struct {
	Enabled bool   `koanf:"enabled"`
	Binary  string `koanf:"binary"` // "" = $TKS_BROWSER_BINARY, PATH, /opt/pw-browsers/chromium
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
	// PageLoads paces page loads on each static exit, apart from GraphQL.
	PageLoads PageLoads `koanf:"page_loads"`
}

// PageLoads paces page loads (HTML), the requests Facebook walls for
// logged-out sessions that view many pages, on each static exit. Zero
// values turn the pacing off.
type PageLoads struct {
	// Interval is the least time between two page loads on one exit.
	Interval time.Duration `koanf:"interval"`
	// Backoff pauses an exit's page loads after one is walled or fails,
	// doubling up to MaxBackoff; a page that loads resets it.
	Backoff    time.Duration `koanf:"backoff"`
	MaxBackoff time.Duration `koanf:"max_backoff"`
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
	MaxWaits                int            `koanf:"max_waits"`
	LeaseTTL                time.Duration  `koanf:"lease_ttl"`
	FreshnessP0             time.Duration  `koanf:"freshness_p0"`
	FreshnessP1             time.Duration  `koanf:"freshness_p1"`
	SuppressionRefresh      time.Duration  `koanf:"suppression_refresh"`
	// ProfileRefresh is how often a scheduled run reads the page's HTML for
	// its profile; in between it goes straight to GraphQL when its identity
	// already has tokens. 0 reads the HTML on every run.
	ProfileRefresh time.Duration `koanf:"profile_refresh"`
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
	ReaperMaxAge         time.Duration `koanf:"reaper_max_age"`
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
	if _, err := pconfig.LoadLayers([][]byte{configs.Default(), configs.TTDefault()}, path, EnvPrefix, &c); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// MustLoadFromFlags parses --config (or TKS_CONFIG) and loads the config.
func MustLoadFromFlags(fs *flag.FlagSet, args []string) *Config {
	path := fs.String("config", os.Getenv("TKS_CONFIG"), "YAML config file(s), comma separated; later files win")
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
	if _, err := detect.DefaultRules().Apply(detect.Overrides(c.FB.Detect)); err != nil {
		errs = append(errs, fmt.Errorf("fb.detect: %w", err))
	}
	errs = append(errs, c.TT.validate()...)
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
	// Discovery finds people by their user ids and registers them by
	// secUid, which pseudonymized mode does not keep (docs/tiktok/
	// discovery-plan.md §7).
	if c.Privacy.PIIMode == "pseudonymized" && c.TT.Discovery.Enabled {
		errs = append(errs, errors.New("tt.discovery.enabled cannot be on with privacy.pii_mode pseudonymized: discovery needs people's ids"))
	}
	// The author key pseudonymises commenters and matches erased authors on
	// later scrapes. A known or short key lets anyone confirm a guessed
	// Facebook id from an author_key. Generate one per deployment (e.g.
	// openssl rand -hex 32) and never change it: suppression entries are
	// stored under it.
	if c.Service.Env == "prod" || (c.Service.Env != "dev" && c.Privacy.PIIMode == "pseudonymized") {
		if k := c.Privacy.AuthorHMACKey; k == "" || k == "dev-only-change-me" || len(k) < 32 {
			errs = append(errs, errors.New("privacy.author_hmac_key must be a secret of at least 32 characters (e.g. openssl rand -hex 32)"))
		}
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
	c.Kafka.SASL.Mechanism = strings.ToLower(strings.TrimSpace(c.Kafka.SASL.Mechanism))
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
	if err := c.Postgres.validate(); err != nil {
		errs = append(errs, err)
	}
	if c.RegistrySync.Interval <= 0 || c.RegistrySync.Timeout <= 0 {
		errs = append(errs, errors.New("registry_sync.interval and registry_sync.timeout must be > 0"))
	}
	return errors.Join(errs...)
}

// validate checks the tt: section.
func (t TT) validate() []error {
	var errs []error
	if t.BaseURL == "" {
		errs = append(errs, errors.New("tt.base_url must be set"))
	}
	if b, err := configs.FS.ReadFile(t.OperationSeed); err != nil {
		errs = append(errs, fmt.Errorf("tt.operation_seed: %w", err))
	} else if seed, err := registry.ParseSeed(b); err != nil {
		errs = append(errs, fmt.Errorf("tt.operation_seed: %w", err))
	} else if len(seed.Queries) == 0 {
		errs = append(errs, fmt.Errorf("tt.operation_seed: %s has no operations", t.OperationSeed))
	}
	// What can be wrong is the overrides' syntax (patterns, status
	// classes), which does not depend on the rules they replace parts of,
	// so the neutral defaults check it (importing internal/tt/detect here
	// would be an import cycle).
	if _, err := detect.DefaultRules().Apply(detect.Overrides(t.Detect)); err != nil {
		errs = append(errs, fmt.Errorf("tt.detect: %w", err))
	}
	if t.RequestTimeout <= 0 || t.IdentityRPS <= 0 || t.GlobalRPS <= 0 {
		errs = append(errs, errors.New("tt.request_timeout, tt.identity_rps and tt.global_rps must be > 0"))
	}
	if t.IdentityMaxRequestsMin > t.IdentityMaxRequestsMax {
		errs = append(errs, errors.New("tt.identity_max_requests_min exceeds tt.identity_max_requests_max"))
	}
	if t.ListPageSize <= 0 || t.CommentsPageSize <= 0 || t.RepliesPageSize <= 0 {
		errs = append(errs, errors.New("tt.list_page_size, tt.comments_page_size and tt.replies_page_size must be > 0"))
	}
	switch t.CaptureRaw {
	case "all", "sample", "failures", "none":
	default:
		errs = append(errs, fmt.Errorf("tt.capture_raw %q must be all, sample, failures or none", t.CaptureRaw))
	}
	switch t.Signer.Mode {
	case SignerNone:
	case SignerBrowser, SignerRemote:
		// tks-signer reads the same section in mode remote: its pages
		// need the same limits.
		if t.Signer.Pages <= 0 || t.Signer.PageMaxSigns <= 0 || t.Signer.PageMaxAge <= 0 ||
			t.Signer.SignTimeout <= 0 || t.Signer.PageTimeout <= 0 {
			errs = append(errs, fmt.Errorf("tt.signer: %s mode needs pages, page_max_signs, page_max_age, sign_timeout and page_timeout > 0", t.Signer.Mode))
		}
		if t.Signer.Mode == SignerRemote {
			if u, err := url.Parse(t.Signer.RemoteURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				errs = append(errs, fmt.Errorf("tt.signer.remote_url %q must be tks-signer's http(s) address in remote mode", t.Signer.RemoteURL))
			}
		}
	default:
		errs = append(errs, fmt.Errorf("tt.signer.mode %q must be none, browser or remote", t.Signer.Mode))
	}
	if s := t.Signer.MaxShare; s <= 0 || s > 1 {
		errs = append(errs, errors.New("tt.signer.max_share must be in (0, 1]"))
	}
	if t.Signer.IdleClose < 0 || t.Signer.MaxPageMB < 0 || t.Signer.KillPageMB < 0 || t.Signer.MaxRefusals < 0 {
		errs = append(errs, errors.New("tt.signer.idle_close, max_page_mb, kill_page_mb and max_refusals must be >= 0"))
	}
	if k, m := t.Signer.KillPageMB, t.Signer.MaxPageMB; k > 0 && m > 0 && k < m {
		errs = append(errs, errors.New("tt.signer.kill_page_mb must not be below max_page_mb: a page past max_page_mb is reopened after its call, one past kill_page_mb closed mid-call"))
	}
	switch t.Signer.OpenOn {
	case "", SignerOpenOnCall, SignerOpenOnProfile:
	default:
		errs = append(errs, fmt.Errorf("tt.signer.open_on %q must be call or profile", t.Signer.OpenOn))
	}
	c := t.Canary
	if c.Interval <= 0 || c.Timeout <= 0 || c.Confirm < 1 || c.KeepFallbacks < 0 || c.RetireAfter < 0 || c.RecheckRetired < 0 {
		errs = append(errs, errors.New("tt.canary: interval, timeout and confirm must be > 0; keep_fallbacks, retire_after and recheck_retired >= 0"))
	}
	if len(c.Operations) == 0 {
		errs = append(errs, errors.New("tt.canary.operations must name the crawler's operations"))
	}
	f := t.Freshness
	if f.ListInterval <= 0 || f.AccountRefresh <= 0 || f.PageRefresh <= 0 {
		errs = append(errs, errors.New("tt.freshness.list_interval, account_refresh and page_refresh must be > 0"))
	}
	if f.BackfillDays <= 0 {
		errs = append(errs, errors.New("tt.freshness.backfill_days must be > 0"))
	}
	for i, w := range f.MetricsWalks {
		if w.Every <= 0 || w.Back <= 0 || (i > 0 && w.Back <= f.MetricsWalks[i-1].Back) {
			errs = append(errs, errors.New("tt.freshness.metrics_walks need every and back > 0, back ascending"))
			break
		}
	}
	for i, w := range f.CommentWindows {
		if w.Interval <= 0 || w.MaxAge <= 0 || (i > 0 && w.MaxAge <= f.CommentWindows[i-1].MaxAge) {
			errs = append(errs, errors.New("tt.freshness.comment_windows need intervals and ascending max_age"))
			break
		}
	}
	for name, st := range map[string]InactiveStep{"private": f.Inactive.Private, "gone": f.Inactive.Gone} {
		if st.First <= 0 || st.Then < st.First || st.For < 0 {
			errs = append(errs, fmt.Errorf("tt.freshness.inactive.%s needs first > 0, then >= first and for >= 0", name))
		}
	}
	if a := t.Admission; a.PerHour < 0 || a.RequestsPerAccount <= 0 || a.IPRequestsPerDay <= 0 || a.PageSignsPerDay <= 0 ||
		a.SignedShare < 0 || a.SignedShare > 1 {
		errs = append(errs, errors.New("tt.admission: per_hour >= 0; requests_per_account, ip_requests_per_day and page_signs_per_day > 0; signed_share in [0, 1]"))
	}
	if k := t.Cost; k.Interval < time.Minute || k.IPMonthUSD < 0 || k.IPs < 0 || k.BrowserHourUSD < 0 || k.ComputeUSDPerAccount < 0 ||
		k.StorageGBMonthUSD < 0 || k.BudgetPerAccountUSD < 0 || k.AlertShare <= 0 || k.AlertShare > 1 {
		errs = append(errs, errors.New("tt.cost: interval >= 1m; prices, ips and budget_per_account_usd >= 0; alert_share in (0, 1]"))
	}
	if t.Discovery.CountBatch <= 0 {
		errs = append(errs, errors.New("tt.discovery.count_batch must be positive"))
	}
	if t.Discovery.MaxNewPerDay < 0 || t.Discovery.Client == "" {
		errs = append(errs, errors.New("tt.discovery: max_new_per_day >= 0 (0: no cap) and client set"))
	}
	if e := t.Discovery.Entry; e.ActiveFor <= 0 || e.RecheckAfter <= 0 || e.Batch <= 0 || e.MaxRunning <= 0 || e.MaxAttempts <= 0 ||
		e.Posts <= 0 {
		errs = append(errs, errors.New("tt.discovery.entry: active_for, recheck_after, batch, max_running, max_attempts and posts > 0"))
	}
	// One strong signal passes; neither a medium one nor the weak ones
	// alone, nor the two together, do (discovery-plan §3).
	if s := t.Discovery.Score; s.Strong < s.Pass || s.Medium <= 0 || s.Weak < 0 || s.WeakMax < 0 || s.Pass <= 0 ||
		s.Medium+s.WeakMax >= s.Pass || 2*s.Medium < s.Pass || s.CaptionPostsStrong <= 0 || s.AudienceStrong <= 0 || len(s.Regions) == 0 {
		errs = append(errs, errors.New("tt.discovery.score: strong >= pass > 0; 2 × medium >= pass > medium + weak_max; "+
			"caption_posts_strong and audience_strong > 0; regions set"))
	}
	if f := t.Discovery.Follows; f.FollowingEvery <= 0 || f.FollowersEvery <= 0 || f.RetryAfter <= 0 || f.MaxPages <= 0 ||
		f.DayPages < 0 || f.PageSize <= 0 || f.KnownPages <= 0 || f.Batch <= 0 || f.MaxRunning <= 0 {
		errs = append(errs, errors.New("tt.discovery.follows: following_every, followers_every, retry_after, max_pages, page_size, "+
			"known_pages, batch and max_running > 0; day_pages >= 0 (0: no cap)"))
	}
	if p := t.Discovery.Pages; p.Batch <= 0 || p.MaxRunning <= 0 || p.DayPages < 0 || p.RetryAfter <= 0 || p.Refresh <= 0 {
		errs = append(errs, errors.New("tt.discovery.pages: batch, max_running, retry_after and refresh > 0; day_pages >= 0 (0: no cap)"))
	}
	if s := t.Discovery.Search; s.Every <= 0 || s.MaxPages <= 0 {
		errs = append(errs, errors.New("tt.discovery.search: every and max_pages > 0"))
	}
	for name, tier := range map[string]string{"search": t.Discovery.Search.Tier, "feed": t.Discovery.Feed.Tier} {
		if tier != "" && tier != "direct" && !slices.ContainsFunc(t.Proxy.Tiers, func(p ProxyTier) bool { return p.Name == tier }) {
			errs = append(errs, fmt.Errorf("tt.discovery.%s.tier %q is not one of tt.proxy.tiers", name, tier))
		}
	}
	for name, u := range map[string]UsedPages{"hashtags": t.Discovery.Hashtags, "sounds": t.Discovery.Sounds} {
		if u.Every <= 0 || u.MaxPages <= 0 || u.Top < 0 || u.MinAccounts <= 0 || u.Window <= 0 {
			errs = append(errs, fmt.Errorf("tt.discovery.%s: every, max_pages, min_accounts and window > 0; top >= 0", name))
		}
	}
	if r := t.Discovery.Related; r.Window <= 0 || r.PerTick < 0 {
		errs = append(errs, errors.New("tt.discovery.related: window > 0, per_tick >= 0"))
	}
	if f := t.Discovery.Feed; f.Every <= 0 || f.Reads <= 0 {
		errs = append(errs, errors.New("tt.discovery.feed: every and reads > 0"))
	}
	for name := range t.Discovery.Sources {
		if !slices.Contains(DiscoverySources, name) {
			errs = append(errs, fmt.Errorf("tt.discovery.sources: unknown source %q (%s)", name, strings.Join(DiscoverySources, ", ")))
		}
	}
	switch t.Comments.Order {
	case CommentsRanked, CommentsNewest:
	default:
		errs = append(errs, fmt.Errorf("tt.comments.order %q must be ranked or newest", t.Comments.Order))
	}
	if t.Comments.DeletionSlack < 0 || t.Comments.DeletionSlack > 1 {
		errs = append(errs, fmt.Errorf("tt.comments.deletion_slack %v must be between 0 and 1", t.Comments.DeletionSlack))
	}
	if t.Comments.MaxPerPost < 0 {
		errs = append(errs, errors.New("tt.comments.max_per_post must be >= 0 (0: no cap)"))
	}
	if u := t.Posts.Unavailable; u.Enabled && (u.MinPagePosts < 1 || u.MaxPerPage < 1 || u.MaxChecks < 1 || len(u.Codes) == 0) {
		errs = append(errs, errors.New("tt.posts.unavailable: min_page_posts, max_per_page and max_checks must be > 0, and codes set"))
	}
	for _, d := range t.Media.CookieDomains {
		if d == "" || strings.HasPrefix(d, ".") || strings.ContainsAny(d, "/:* ") || strings.ToLower(d) != d {
			errs = append(errs, fmt.Errorf("tt.media.cookie_domains: %q must be a lower-case host name without a leading dot", d))
		}
	}
	if t.Subtitles.Include && (t.Subtitles.MaxBytes < 1 || t.Subtitles.MaxTracks < 1 || t.Subtitles.Timeout <= 0) {
		errs = append(errs, errors.New("tt.subtitles: max_bytes, max_tracks and timeout must be > 0 when include is on"))
	}
	if t.Media.Include && len(t.Media.CookieDomains) == 0 {
		errs = append(errs, errors.New("tt.media.cookie_domains must name the video hosts' domain when tt.media.include is on"))
	}
	if t.Content.Media.DeleteAfter < MinMediaLifetime {
		errs = append(errs, fmt.Errorf("tt.content.media.delete_after %s must be at least 24h (the contract's minimum)", t.Content.Media.DeleteAfter))
	}
	if len(t.Content.Media.RequiredConsumers) == 0 || slices.Contains(t.Content.Media.RequiredConsumers, "") {
		errs = append(errs, errors.New("tt.content.media.required_consumers must name at least one consumer (sb-analysis)"))
	}
	r := t.Retention
	if r.CommentsDays < 0 || r.MetricsDays < 0 || r.AuthorsInactiveDays < 0 || r.JobsDays < 0 || r.MediaRowsDays < 0 || r.RawPayloadDays < 0 {
		errs = append(errs, errors.New("tt.retention values must be >= 0 (0: kept for good)"))
	}
	return errs
}
