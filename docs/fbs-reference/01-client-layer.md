# 1. Facebook HTTP client layer: transport, proxies, identities, GraphQL, detection, rate limits

[← Index](README.md)

Repository: this repository at commit `265ae3a`. `path:line` references are relative to the repository root and point at that commit.
Scope: `internal/fb/{transport,proxy,session,graphql,detect,htmlx,ratelimit,fbtest}`, `internal/platform/breaker`, the wiring in `internal/app/` (`clients.go`, plus `crawler.go` and `registrysync.go`, where the FB layer is actually built), `configs/fingerprints.yaml`, `configs/queries.seed.yaml`, and the `fb:` section of `configs/default.yaml`.

**How claims are marked**

- File references are `path:line` in the repo, for example `internal/fb/session/identity.go:111`.
- Unmarked statements describe what the **code** does. I read every in-scope file, tests included.
- **[LIB]** marks a fact about a third-party library that is not in the repo. I read these from the pinned module sources (tls-client v1.17.1, redis_rate v10.0.1) through the Go module proxy and raw GitHub, without writing any files.
- **[OBS]** marks an observation, inconsistency or gap that I found in the code. Each one is a candidate "don't copy this" item.
- **[SUGGESTION]** marks my recommendation. It is not in the code.
- **[IG-VERIFY]** marks a claim about Instagram's web surface that comes from general knowledge, not from this repo. Confirm each one with a live capture before relying on it.

---

## 0. TL;DR

- **Approach.** Every logged-out "identity" is one proxy exit, one Chrome fingerprint, one cookie jar and one set of tokens. Its tokens are scraped from a normal page HTML load: `lsd`, `__spin_*`, `hsi`, `server_revision`, `haste_session`, `_js_datr`, `__comet_req`, and `fb_dtsg` when present. The identity then calls `POST /api/graphql/` with a `doc_id` and a `variables` JSON. TLS, HTTP/2 and header order are those of Chrome 146, through `bogdanfinn/tls-client` (ADR `docs/adr/0001-logged-out-graphql-scraping.md`).
- **Response classes.** Every response falls into one of 9 classes: `ok`, `rate_limited`, `login_wall`, `checkpoint`, `soft_error`, `registry_stale`, `schema_drift`, `content_unavailable`, `transport` (`internal/fb/detect/detect.go:14-36`). The class drives three feedback loops:
  1. the identity's own health. It retires on a checkpoint, a login wall, 2 blocks in a row, health below 0.5, its request quota or sticky-session expiry.
  2. a circuit breaker per proxy *exit bucket* (one /24, or one gateway+country).
  3. the fleet-wide AIMD controller, which moves the global requests-per-second ceiling. Underneath it, a Redis GCRA limiter enforces per-page and global limits.
- **Query breaker.** A separate circuit breaker per `(query, doc_id)` stops sending a query whose responses keep being rejected or failing to parse. It falls back to an older `doc_id` from the registry when one is available.
- **Traffic budget.** A daily request/byte budget, projected from 5-minute slices in Redis, pauses backfill (lane p2) and stretches the slow scheduling tiers.
- **Instagram reuse.** About 60% of this code is platform-agnostic: breaker, transport, proxy, GCRA, AIMD, budget, and the pool/identity lifecycle. The Facebook-specific parts are the token regexes, the GraphQL form/header builder, the consent flow, the error codes and markers, and the seed queries.

---

## 1. Architecture and wiring

### 1.1 Component graph (code)

```
                    +------------------ fbs-scheduler (leader only) ------------------+
                    | ratelimit.Controller.Tick every fb.aimd.interval  (AIMD)         |
                    | ratelimit.BudgetController.Tick every scheduler.budget.interval  |
                    +-------------------------------^----------------------------------+
                                                    | Redis/Valkey: aimd:w:*, aimd:global_rps,
                                                    | budget:d:*, budget:s:*, budget:state
 fbs-crawler pod                                    v
 +-------------------------------------------------------------------------------------+
 | stage (page/post/thread)  --identity(class,attempt)-->  proxy.Pool.TiersFor/Available |
 |      |                                                   |                           |
 |      v                                                   v                           |
 | session.Pool.Acquire(tier) --create--> proxy.Pool.Next(tier) -> Endpoint (bucket)    |
 |      |                         \-> transport.NewTLS(profile, proxyURL)  (Doer)       |
 |      v                                                                               |
 | session.Loader.Load/Bootstrap(id, pageURL)  --GET HTML-->  detect.Classify(html)     |
 |      |   htmlx.ExtractTokens -> id.SetTokens          (consent POST, datr cookie)    |
 |      v                                                                               |
 | graphql.Client.Do(id, Request{Query,Vars,PageID,Fast,Lane})                          |
 |      | query breaker (breaker.Set) / registry.Get / Fallback                         |
 |      | pace: id.Wait (x/time/rate + jitter) -> GCRA "page:<id>" -> GCRA "global"     |
 |      |       (global = ratelimit.Ceiling.RPS() if AIMD on, else fb.global_rps)       |
 |      | POST /api/graphql/  (form + Chrome headers)                                   |
 |      | SplitParts -> Errors/HasData -> detect.Classify(graphql)                      |
 |      | session.Outcome -> Identity.Record (health/retire) + proxy bucket breaker     |
 |      | Outcomes.Observe -> ratelimit.Reporter (AIMD + budget counters, flush 2 s)    |
 |      v                                                                               |
 | session.Pool.Release(id)  (retire if RetireReason != "")                             |
 +-------------------------------------------------------------------------------------+
```

### 1.2 `internal/app/clients.go`

This file does **not** build the FB layer. It only provides:

- `KafkaClient(cfg *config.Config, id string) (kafka.ClientConfig, error)` (`internal/app/clients.go:18-35`): sets the client id suffix, TLS and SASL (plain or SCRAM-256/512).
- `OpenRedis(ctx context.Context, cfg *config.Config) (*redis.Client, error)` (`internal/app/clients.go:38-44`): connects to Valkey/Redis with optional TLS through `redisx.Open`. This `*redis.Client` is what the ratelimit package uses for GCRA, AIMD and the budget.

`clients_test.go` only tests the Kafka/Redis TLS/SASL plumbing.

### 1.3 `internal/app/crawler.go` — the real FB wiring (`NewCrawler`, `internal/app/crawler.go:71-240`)

1. Loads the registry seed (`cfg.FB.RegistrySeed`, default `queries.seed.yaml`) into a Postgres-backed `registry.DB`, hot-reloaded every `fb.registry_refresh` (30s) (`internal/app/crawler.go:72-83,143`).
2. `session.LoadFingerprints(configs.FS "fingerprints.yaml")` (`internal/app/crawler.go:84-88`).
3. `proxy.New(cfg.FB.Proxy)` (`internal/app/crawler.go:89`).
4. Builds `session.NewPool` (`internal/app/crawler.go:99-104`) with:
   - `BaseURL = fb.base_url`
   - `Profile = fb.fingerprint_profile`
   - `Timeout = fb.request_timeout`
   - `IdentityRPS` and `IdentityBurst` from config
   - `MaxRequestsMin` and `MaxRequestsMax` from config
   - `MaxPerTier = max(16, slots*len(stages)*13/10)`. With the defaults (slots 240, 3 stages) that is **936 identities per tier per pod**.
   - `DoerFactory`: `nil` means tls-client; tests inject `NewStd`.
5. `rules := detect.DefaultRules()` (`internal/app/crawler.go:108`). **[OBS]** The rules are hard-coded. Nothing reads them from config, even though the `detect.Rules` comment says "they are config so they can change without a deploy".
6. Builds `graphql.Client` (`internal/app/crawler.go:109-118`) with:
   - `Limiter: ratelimit.NewRedis(rdb)`
   - `PageRPS = fb.page_rps`, `PageRPSFast = fb.page_rps_p0`, `GlobalRPS = fb.global_rps`
   - `AcceptLanguage` and `Locale`
   - `Breakers: breaker.New(cfg.FB.QueryBreaker)`, whose `OnChange` sets the `fbs_breaker_state{name="query:<name|doc_id>"}` gauge and logs a warning.
7. `session.Loader{Rules, AcceptLanguage}`, plus `Browser` (headless-Chromium fallback) when `fb.browser.enabled` (`internal/app/crawler.go:120-127`).
8. If `fb.aimd.enabled || scheduler.budget.enabled`, sets `gql.Outcomes = ratelimit.NewReporter(rdb, cfg.Kafka.TopicPrefix)` and runs `rep.Run`, which flushes every 2s (`internal/app/crawler.go:147-151`).
9. If `fb.aimd.enabled`, sets `gql.Ceiling = ratelimit.NewCeiling(rdb, prefix, fb.global_rps)` and runs `ceil.Run`, which refreshes every 5s (`internal/app/crawler.go:152-156`).
10. Raw capture: `gql.Raw = rawsink`, and `CaptureAll = (fb.capture_raw == "all")` (`internal/app/crawler.go:157-161`).
11. Budget gate: if `scheduler.budget.enabled`, a `BudgetGate` refreshes every 15s and closes or reopens the p2 (backfill) lane slots (`internal/app/crawler.go:224-238`).

The Redis **namespace** for AIMD and budget keys is `cfg.Kafka.TopicPrefix` (default `""`). **[OBS]** The GCRA limiter keys are *not* namespaced (see §9.1).

### 1.4 `internal/app/registrysync.go`

The registry-sync service (`internal/app/registrysync.go:25-76`) builds its **own small pool**: `MaxPerTier = len(canary_pages)+2`. It runs on `registry_sync.tier` or, when that is empty, on the first tier routed for class `"registry_sync"`. It uses the same fingerprint, loader and rules. It builds throw-away `graphql.Client`s per check (`internal/registrysync/sync.go:684`).

### 1.5 `cmd/fbs-scheduler/main.go:87-105` (leader-only loops)

- The `BudgetController` runs **only if** `scheduler.budget.enabled && (requests_per_day > 0 || bytes_per_day > 0)`. **[OBS]** Both budgets default to 0, so the budget controller is off by default, even though `budget.enabled: true` and the crawlers still run the Reporter and Gate.
- The AIMD `Controller` runs when `fb.aimd.enabled`. It ticks every `fb.aimd.interval` (10s), starting at `fb.global_rps`.

---

## 2. One request, end to end (code)

1. **Pick a tier.** A stage calls `d.identity(ctx, class, attempt)` (`internal/crawl/stage/deps.go:98-109`). The classes used are `"timeline"`, `"comments"`, `"replies"` and `"registry_sync"`. It works as follows:
   - `tiers := Proxies.TiersFor(class)` returns the class's route, or `routing.default`.
   - It starts at index `min(attempt, len-1)`. **Each retry escalates to the next, more expensive tier.**
   - It skips tiers where `Available()` is false (every bucket's breaker open).
2. **Get an identity.** `Pool.Acquire(ctx, tier)` (`internal/fb/session/pool.go:72-116`):
   - It reuses the most recently released idle identity (LIFO scan from the end of the slice, `internal/fb/session/pool.go:80`).
   - It drops (retires) idle identities whose exit bucket's breaker is open, with reason `breaker` (`internal/fb/session/pool.go:82-90`).
   - It skips identities that are cooling down after a rate limit (`internal/fb/session/pool.go:91-93`).
   - Otherwise it creates a new identity while `live < MaxPerTier` (`internal/fb/session/pool.go:97-108`).
   - At the cap it waits on a `sync.Cond`, woken by a `Release`, by context cancel, or by a 1-second timer (`internal/fb/session/pool.go:110-114`).
3. **Create the identity (`create`).** (`internal/fb/session/pool.go:145-159`)
   - `ep := proxies.Next(tier)` gives the next endpoint whose bucket breaker allows it; in a half-open bucket, this identity is the probe.
   - `fp := fps[cfg.Profile]`. **[OBS]** Every identity of the pod uses the **same** fingerprint profile.
   - `Doer := NewTLS({Profile, ProxyURL: ep.URL, Timeout})`.
   - `maxReq := randRange(min, max)`.
   - The `observe` hook forwards each outcome to the proxy bucket breaker.
4. **Bootstrap.** If the identity has no tokens (`!id.Bootstrapped()`), or this is the first segment of a page task, the stage calls `Loader.Load`/`Bootstrap` on the page URL. The URL is `/<page_id>` in `ensureBootstrapped` (`internal/crawl/stage/deps.go:112-120`), or the page's own URL in the page stage. The loader:
   - GETs the URL with Chrome navigation headers;
   - follows same-site redirects (up to 4 hops);
   - classifies the response;
   - extracts the tokens;
   - plants the `datr` cookie from `_js_datr`;
   - accepts the EU cookie-consent wall if one is present;
   - installs the tokens on the identity (§5.4).
5. **GraphQL.** `graphql.Client.Do(ctx, id, Request{Query, Vars, PageID, Fast, Lane, Referer})` (`internal/fb/graphql/client.go:110-137`). It checks the query breaker and resolves the registry entry, then in `do()` (`internal/fb/graphql/client.go:139-209`):
   - renders the variables;
   - paces the request;
   - builds the form and the headers;
   - POSTs to `/api/graphql/`;
   - decodes the parts;
   - classifies the response;
   - reports the outcome to the identity, the proxy bucket breaker, the AIMD/budget Reporter and the metrics;
   - optionally captures the raw body.

   If the class is `registry_stale`, it retries **once** with a fallback `doc_id`.
6. **Parse (out of scope).** The stage parses the response and calls `gql.Parsed(resp, ok)`, which feeds the query breaker (`internal/fb/graphql/client.go:90-102`). A non-OK class becomes a task retry in 30s or 5m (`internal/crawl/stage/deps.go:138-150`).
7. **Release.** `Pool.Release(id)` runs as a `defer` (`internal/crawl/stage/page.go:46`). If `RetireReason != ""`, the identity is closed and counted in `fbs_identity_retired_total{tier,reason}`; otherwise it goes back to the idle list (`internal/fb/session/pool.go:119-133`).

Identities are **leased exclusively** to one task at a time. Tokens and cookies survive across tasks for the identity's whole lifetime.

---

## 3. `internal/fb/transport` — browser-consistent HTTP

### 3.1 Purpose

Facebook fingerprints the TLS ClientHello (JA3/JA4), the HTTP/2 SETTINGS, WINDOW_UPDATE and pseudo-header order, and the header order. Go's `net/http` is trivially identifiable. This package hides a fingerprinted client behind a small interface so it can be faked in tests (`internal/fb/transport/transport.go:1-3`).

### 3.2 Types and functions

```go
type Request struct { Method, URL string; Header [][2]string; Body []byte }   // header ORDER preserved (transport.go:21-26)
type Response struct {
    Status int; Header http.Header; Body []byte; Location string
    BytesIn, BytesOut int64   // wire bytes incl. TLS overhead, compressed
    Elapsed time.Duration
}                                                                              // transport.go:29-38
type Doer interface {
    Do(ctx context.Context, req *Request) (*Response, error)
    Cookie(u *url.URL, name string) string
    SetCookie(u *url.URL, name, value string)
    Cookies(u *url.URL) map[string]string
    Close()
}                                                                              // transport.go:41-47
type Options struct { Profile, ProxyURL string; Timeout time.Duration; MaxBody int64 } // transport.go:50-55
var ErrBodyTooLarge = errors.New("transport: response body too large")       // transport.go:58
func NewTLS(o Options) (Doer, error)                                          // transport.go:66
func NewStd(timeout time.Duration) Doer                                       // std.go:21 (tests / fingerprint-agnostic traffic)
```

### 3.3 `NewTLS` (`internal/fb/transport/transport.go:66-94`)

- **Library**: `github.com/bogdanfinn/tls-client v1.17.1`, with `github.com/bogdanfinn/fhttp v0.7.2` (from `go.mod`).
- **Profile**: `profiles.MappedTLSClients[o.Profile]`. An unknown key is a hard error (`internal/fb/transport/transport.go:67-70`). The default is `chrome_146`.
- **Defaults**: `Timeout` 30s and `MaxBody` 32 MiB (`32 << 20`) (`internal/fb/transport/transport.go:71-76`).
- **Client options** (`internal/fb/transport/transport.go:77-88`):
  - `WithClientProfile(prof)`
  - `WithTimeoutMilliseconds`
  - `WithNotFollowRedirects()`. The loader handles redirects itself.
  - `WithCookieJar(NewCookieJar())`. Each identity gets its own jar.
  - `WithRandomTLSExtensionOrder()`. **[LIB]** Chrome has shuffled TLS extension order since v110; this mimics it.
  - `WithBandwidthTracker()`, used for wire-byte accounting.
  - `WithDisableHttp3()`
  - `WithProxyUrl(o.ProxyURL)` when a proxy is set. Credentials travel in the URL userinfo.
- **[LIB] What the `chrome_146` profile sends** (tls-client v1.17.1, `profiles/internal_browser_profiles.go`, `var Chrome_146`):
  - Cipher suites: GREASE, TLS_AES_128_GCM, TLS_AES_256_GCM, CHACHA20_POLY1305, then the ECDHE-ECDSA/RSA AES-GCM and CHACHA20 suites, the ECDHE-RSA AES-CBC suites, and the RSA AES-GCM and AES-CBC suites.
  - Key shares: GREASE, **X25519MLKEM768** and X25519.
  - Supported groups: GREASE, X25519MLKEM768, X25519, P-256, P-384.
  - ALPS (new codepoint) for `h2`.
  - Brotli certificate compression.
  - GREASE ECH.
  - ALPN `h2, http/1.1`.
  - PSK modes DHE.
  - The trust-anchors extension `0xca34`.
  - Supported versions: GREASE, TLS 1.3, TLS 1.2.
  - **HTTP/2 SETTINGS, in this order**: `HEADER_TABLE_SIZE=65536`, `ENABLE_PUSH=0`, `INITIAL_WINDOW_SIZE=6291456`, `MAX_HEADER_LIST_SIZE=262144`.
  - Connection-level `WINDOW_UPDATE` = **15663105**.
  - **Pseudo-header order**: `:method, :authority, :scheme, :path`.

### 3.4 `tlsDoer.Do` (`internal/fb/transport/transport.go:96-148`)

- Builds an `fhttp.Request`. Each header pair is `Add`ed, and `req.Header[fhttp.HeaderOrderKey]` is set to the caller's key order (`internal/fb/transport/transport.go:105-110`). That makes the regular-header order on the wire match Chrome.
- **[OBS]** Empty header values are still added. For example, `x-fb-lsd: ""` goes out when the identity has no tokens yet.
- **[OBS]** The code never sets the `cookie` header explicitly; the jar adds it. Its position in the header order is up to fhttp. **[SUGGESTION]** Check it against a Chrome capture on a TLS/H2 echo service (e.g. tls.peet.ws) and put `cookie` into the order list explicitly if needed.
- Bandwidth: it takes the tracker's read/write deltas around the call. Because one identity is used by one task at a time, the per-client tracker is not polluted by concurrent requests. For plain `http://` (fakefb in dev) the tracker reads zero, so it falls back to `len(body)` (`internal/fb/transport/transport.go:133-138`).
- Decompression: `fhttp.DecompressBody(resp)` handles gzip, deflate, br and zstd, all advertised in `accept-encoding`. The read is limited to `MaxBody+1`; anything over gives `ErrBodyTooLarge` (`internal/fb/transport/transport.go:120-128`).
- `Location` is copied out for the redirect logic.

### 3.5 Cookies

- `Cookie`, `SetCookie` and `Cookies` wrap the jar (`internal/fb/transport/transport.go:150-169`).
- `SetCookie` writes `Path=/` and `Domain=cookieDomain(u)` (`internal/fb/transport/transport.go:159-161`).
- `cookieDomain` strips a leading `www.`, so `www.facebook.com` becomes `.facebook.com`; any other host is used as-is (`internal/fb/transport/transport.go:175-181`).
- Only one cookie is ever planted by hand: **`datr`**, from the HTML `_js_datr` (§5.4). All other cookies come from the server's `Set-Cookie` headers.

### 3.6 `stdDoer` (`internal/fb/transport/std.go`)

- It uses `net/http` with Go's TLS fingerprint, so it is meant "only for tests and for traffic where fingerprinting does not matter" (`internal/fb/transport/std.go:13-14`).
- It does not follow redirects, uses a `cookiejar`, has a 32 MiB cap, and **drops the caller's `accept-encoding`** so Go negotiates gzip and decodes it transparently (`internal/fb/transport/std.go:39-41`).
- Header order is not preserved. Byte counts are payload sizes.

---

## 4. `internal/fb/proxy` — tiers, buckets, sticky sessions

### 4.1 Purpose

Hands out one proxy exit per identity. The package comment says that only Facebook HTML and GraphQL traffic goes through proxies; media and static files use direct datacenter egress (`internal/fb/proxy/proxy.go:1-3`). It groups exits into **buckets** that Facebook probably treats alike, and keeps a **circuit breaker per bucket** (`internal/fb/proxy/proxy.go:62-68`).

### 4.2 Types and functions

```go
type Endpoint struct {
    Tier, URL, SessionID, Country string
    ExpiresAt time.Time  // zero = no sticky expiry
    Bucket    string     // breaker key; never contains credentials
}                                                                   // proxy.go:26-35
func (e Endpoint) Redacted() string                                 // URL without userinfo, "direct", or "invalid" (proxy.go:38-48)
type Tier interface { Name() string; Next(allow func(bucket string) bool) (Endpoint, error); Buckets() []string } // proxy.go:52-57
var ErrTierOpen = errors.New("proxy: every exit of the tier is cooling down")                                   // proxy.go:60
type Pool struct { tiers map[string]Tier; routing map[string][]string; breakers *breaker.Set }                   // proxy.go:64-68
func New(cfg config.Proxy) (*Pool, error)                           // proxy.go:71-113
func (p *Pool) TiersFor(class string) []string                      // proxy.go:116-121
func (p *Pool) Next(tier string) (Endpoint, error)                  // proxy.go:125-131
func (p *Pool) Available(tier string) bool                          // proxy.go:134-148
func (p *Pool) IsOpen(ep Endpoint) bool                             // proxy.go:151
func (p *Pool) Observe(ep Endpoint, c detect.Class)                 // proxy.go:155-160
```

### 4.3 Tier kinds (`config.ProxyTier`, `internal/config/config.go:204-215`)

| Kind | Fields | Behaviour |
|---|---|---|
| `direct` (always present; also `kind: ""`) | — | `Endpoint{Tier:"direct"}`. No URL, no bucket, **no breaker** (`internal/fb/proxy/proxy.go:178-182`). |
| `static_list` (ISP / static proxies) | `endpoints: [url...]`, `country` | Round-robin over the endpoints through an atomic counter, skipping endpoints whose bucket's breaker does not `Allow`. If all are skipped it returns `ErrTierOpen` (`internal/fb/proxy/proxy.go:195-203`). An empty list is a config error. |
| `gateway` (rotating residential) | `gateway`, `username`, `password`, `username_template`, `sticky_ttl`, `country` | A new random session id per identity: 8 random bytes as **16 hex chars** (`internal/fb/proxy/proxy.go:254-258`). The id is embedded in the proxy **username**, which pins the exit IP for `sticky_ttl` (`internal/fb/proxy/proxy.go:223-252`). |

- `cost_per_gb_usd` exists on tiers, but only the Phase-0 spike tool reads it.
- Unknown kinds and routes that reference unknown tiers fail `New` (`internal/fb/proxy/proxy.go:98-111`).
- With no `routing`, the default is `{"default": ["direct"]}` (`internal/fb/proxy/proxy.go:102-104`).

### 4.4 Gateway username template (`internal/fb/proxy/proxy.go:228-243`)

- `sticky_ttl` defaults to **10m**.
- `username_template` defaults to `{user}-session-{session}`.
- Placeholders: `{user}`, `{country}`, `{session}`, `{ttl_min}` (the TTL in whole minutes).
- The example in `configs/default.yaml` is `"{user}-country-{country}-session-{session}-sesstime-{ttl_min}"`.
- The password goes into `url.UserPassword` and the endpoint carries `ExpiresAt = now + ttl`.

### 4.5 Buckets (`internal/fb/proxy/proxy.go:163-176,213-219`; tests `internal/fb/proxy/proxy_test.go:37-55`)

| Exit | Bucket key |
|---|---|
| static IPv4 endpoint | `<tier>\|a.b.c.0/24` (the /24 of the endpoint) |
| static IPv6 endpoint | `<tier>\|<ip>/48` |
| static hostname | `<tier>\|<host>` |
| gateway tier | `<tier>\|<gateway host:port>\|<country>`. One bucket for the whole gateway tier. |

Buckets never contain credentials (`internal/fb/proxy/proxy_test.go:52-54`).

### 4.6 Routing and escalation

- `routing` maps a query class to an **ordered tier list, cheapest first** (`internal/fb/proxy/proxy.go:115-121`).
- Unknown classes use `routing.default`.
- The stage starts at index `attempt` and moves to the next `Available` tier (§2 step 1). In practice an ISP-first route escalates to residential on retries.

### 4.7 Breaker feedback

- `Observe(ep, c)` counts a **failure** when `c.Blocking()` (rate_limited, login_wall, checkpoint) **or** `c == Transport`. Every other class counts as success: soft errors, registry and schema problems, unavailable content and OK are "not the exit's fault" (`internal/fb/proxy/proxy.go:155-160`; test `internal/fb/proxy/proxy_test.go:113-123`).
- Breaker transitions set `fbs_breaker_state{name="proxy:<bucket>"}` and are logged (`internal/fb/proxy/proxy.go:73-76`).
- The config is `fb.proxy.breaker`: window 1m, min_events 20, open_rate 0.15, cooldown 2m, max_cooldown 30m, probe_successes 3 (`configs/default.yaml:139-145`).

### 4.8 Credential handling

- Credentials live in the endpoint URL and the config. They can be supplied through env vars (koanf `FBS_FB__PROXY__...`).
- They are stripped from logs with `Redacted()` and never put into bucket names.
- `Snapshot` (§5.3) does serialise `ProxyURL` *with* credentials. **[OBS]**

---

## 5. `internal/fb/session` — identities, pool, bootstrap

### 5.1 Fingerprint (`internal/fb/session/fingerprint.go`)

```go
type Fingerprint struct {
    Profile         string `yaml:"-"`          // tls-client key, filled from the YAML map key
    UserAgent       string `yaml:"user_agent"`
    SecChUa         string `yaml:"sec_ch_ua"`
    SecChUaMobile   string `yaml:"sec_ch_ua_mobile"`
    SecChUaPlatform string `yaml:"sec_ch_ua_platform"`
}                                                           // fingerprint.go:11-17
func LoadFingerprints(b []byte) (map[string]Fingerprint, error)  // fingerprint.go:20-32
```

`configs/fingerprints.yaml` pins each tls-client profile to a matching UA and client hints. Its header comment warns that mixing a newer UA with an older TLS fingerprint "is a strong bot signal", and asks for the GREASE brand strings to be verified against a real Chrome (`configs/fingerprints.yaml:1-5`).

| Profile | UA | `sec-ch-ua` | platform |
|---|---|---|---|
| `chrome_146` (default) | Win10 x64, Chrome/146.0.0.0 | `"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"` | `"Windows"` |
| `chrome_144` | Win10 x64, Chrome/144.0.0.0 | `"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"` | `"Windows"` |
| `chrome_133` | macOS 10_15_7, Chrome/133.0.0.0 | `"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"` | `"macOS"` |

`sec-ch-ua-mobile` is `?0` for all three.

**[OBS]** Only `fb.fingerprint_profile` is ever used (`internal/fb/session/pool.go:150`). The other profiles are dormant: there is no per-identity rotation.

### 5.2 Identity (`internal/fb/session/identity.go`)

```go
type Identity struct {
    ID string; Endpoint proxy.Endpoint; FP Fingerprint; Doer transport.Doer; Base *url.URL
    // private: tokens htmlx.Tokens, bootstrapped, health float64, consecBlocks, requests,
    // maxRequests, born, cooldown, retired string, req atomic counter, limiter *rate.Limiter,
    // sessionID string (__s), observe func(detect.Class)
}                                                              // identity.go:26-50
func (id *Identity) Tokens() htmlx.Tokens                      // :62
func (id *Identity) SetTokens(t htmlx.Tokens)                  // :69
func (id *Identity) Bootstrapped() bool  // tokens.Valid() == (LSD != "")   :77
func (id *Identity) NextReq() string     // __req: base-36 counter "1","2",…  :84
func (id *Identity) SessionParam() string // __s                             :87
func (id *Identity) Wait(ctx context.Context) error            // :90-107
func (id *Identity) Record(c detect.Class) (retire bool, reason string) // :111-147
func (id *Identity) RetireReason() string                      // :150
func (id *Identity) Snapshot() ([]byte, error)                 // :175-181
```

- **ID**: `id-<10 alnum>` (`internal/fb/session/identity.go:209`).
- **`__s`**: `xxxxxx:xxxxxx:xxxxxx`, three groups of 6 random `[a-z0-9]` characters, fixed for the identity's life (`internal/fb/session/identity.go:57`).
- **Pacing (`Wait`)**: a `golang.org/x/time/rate` token bucket with `rate = identity_rps` (0.33) and `burst = identity_burst` (2). After each token it adds a random think-time in `[0, 0.4 × 1/rps)` (`internal/fb/session/identity.go:90-107`).
  - At 0.33 rps that is up to about 1.2s on top of a 3.03s interval, so roughly one request every 3.6s on average.
  - **[OBS]** The comment says "±40%", but the jitter is only ever *added* (0 to +40%).
  - **Every** HTTP request goes through `Wait`: page GETs, each redirect hop, the consent POST, and GraphQL calls.
- **Health and retirement (`Record`)** (`internal/fb/session/identity.go:111-147`):
  - The score is 0 for blocking classes (and `consecBlocks++`), 0.5 for `transport`, and 1 otherwise (which resets `consecBlocks`).
  - `health = 0.9·health + 0.1·score`, starting at 1.
  - The first matching rule applies:

    | Condition | Effect |
    |---|---|
    | `checkpoint` | retire `"checkpoint"` |
    | `login_wall` | retire `"login_wall"` |
    | `consecBlocks >= 2` | retire `"blocked"` |
    | `rate_limited` | **cooldown** `30s << min(consecBlocks,5)`, capped at 15 min |
    | `health < 0.5` | retire `"unhealthy"` |
    | `maxRequests > 0 && requests >= maxRequests` | retire `"max_requests"` |
    | sticky endpoint with `ExpiresAt - now < 1 min` | retire `"sticky_expiry"` |

  - **[OBS]** A second consecutive block always retires the identity first. So the rate-limit cooldown is effectively always 30s<<1 = **60s**, and the 15-minute cap is unreachable.
  - **[OBS]** Transport errors do not reset `consecBlocks`. Five transport failures alone keep health ≥ 0.5 (0.5 + 0.5·0.9ⁿ never drops below 0.5), so transport failures never retire an identity on their own; they only feed the bucket breaker.
  - Retirement is sticky: once set, the reason stays. It takes effect at `Release`.
- **`maxRequests`**: `randRange(min, max)` with an exclusive upper bound. The defaults 150..400 give 150–399 (`internal/fb/session/identity.go:201-207`, `internal/fb/session/pool.go:155`).
- **Lifetime** (worked example from the defaults): at about 3.6s per request, 150–400 requests take roughly 9–24 minutes. On a gateway tier with `sticky_ttl` 10m, `sticky_expiry` (at 9 minutes) usually comes before `max_requests`.
- **[OBS]** Sticky expiry is only checked inside `Record`. An *idle* identity whose sticky session has expired can be acquired again and send one request from a possibly rotated IP before it retires.
- **`Snapshot`** serialises `{id, tier, proxy_url, session_id, expires, profile, cookies, tokens}` "so another pod can rebuild it on the same sticky exit" (`internal/fb/session/identity.go:162-181`). **[OBS]** Nothing calls it today; it is groundwork for session-bound cursors.

### 5.3 Pool (`internal/fb/session/pool.go`)

```go
type PoolConfig struct {
    BaseURL, Profile string; Timeout time.Duration
    IdentityRPS float64; IdentityBurst int
    MaxRequestsMin, MaxRequestsMax int
    MaxPerTier int  // default 512 if <= 0
}                                                                   // pool.go:18-28
type DoerFactory func(o transport.Options) (transport.Doer, error) // pool.go:31
func NewPool(cfg PoolConfig, proxies *proxy.Pool, fps map[string]Fingerprint, f DoerFactory) (*Pool, error) // :47-65
func (p *Pool) Base() *url.URL
func (p *Pool) Acquire(ctx context.Context, tier string) (*Identity, error) // :72-116
func (p *Pool) Release(id *Identity)                                        // :119-133
func (p *Pool) Retire(id *Identity, reason string)                          // :136-143
func Outcome(id *Identity, c detect.Class)  // bucket breaker + Identity.Record   :163-170
```

- The pool is **per pod and per tier**. `live[tier]` counts the identities that exist, idle or leased; `idle[tier]` is a LIFO stack.
- **Stickiness.** LIFO reuse keeps a small set of "warm" identities that already hold tokens and cookies. Fresh ones are only created under concurrency or when the warm ones are cooling down.
- **Retirement points**:
  - at `Release`, when `RetireReason() != ""`, with the reason label;
  - in `Acquire`, when the identity's bucket breaker is open, with reason `breaker`. **[OBS]** That path does not update the `identity_active` gauge.
  - `Retire(id, reason)` forces a reason (it first-writer-wins on `retired`) and then releases.
- **`Outcome`** is the single funnel. It calls `id.observe(c)`, which feeds the proxy bucket breaker, and then `id.Record(c)`. The GraphQL client and the loader call it for every classified response.

### 5.4 Loader / bootstrap (`internal/fb/session/bootstrap.go`)

```go
type PageLoad struct { URL string; Status int; HTML []byte; Class detect.Class; Reason string; Bytes int64 } // :17-24
var ErrBootstrap = errors.New("session: bootstrap failed")                  // :27
type BrowserFallback interface { Observe(); Bootstrap(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error) } // :36-39
type Loader struct { Rules detect.Rules; AcceptLanguage string; Browser BrowserFallback } // :43-49
func NavHeaders(fp Fingerprint, acceptLanguage, secFetchSite string) [][2]string // :52-68
func (l *Loader) Load(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error)      // :73-145
func (l *Loader) Bootstrap(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error) // :150-159
func IsWall(path string) bool  // "/login*" or "/checkpoint*"                // :193
```

**Navigation headers, in wire order** (`internal/fb/session/bootstrap.go:52-68`):

```
sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform, upgrade-insecure-requests: 1, user-agent,
accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7,
sec-fetch-site: <none|same-origin>, sec-fetch-mode: navigate, sec-fetch-user: ?1, sec-fetch-dest: document,
accept-encoding: gzip, deflate, br, zstd, accept-language: en-US,en;q=0.9, priority: u=0, i
```

`sec-fetch-site` is `none` on the first hop and `same-origin` after a redirect or the consent POST. No `referer` is sent on navigation.

**`Load` algorithm** (`internal/fb/session/bootstrap.go:73-145`), for at most 4 hops:

1. `id.Wait`, then `GET target`. A transport error gives `Outcome(Transport)`, increments `fbs_fb_requests_total{query="page_html",class="transport"}` and returns an error.
2. It adds `BytesIn+BytesOut` to the total and to `fbs_proxy_bytes_total{tier,dir}`.
3. On a **3xx with `Location`**: it resolves the location against the base URL. It **follows** the redirect if it is same-site (empty host, the same host, or any subdomain of the base host without `www.`) **and** the path does not start with `/login` or `/checkpoint`. Locale and consent redirects are followed this way (`internal/fb/session/bootstrap.go:92-98,188-197`).
4. Otherwise it classifies the response with `Expect:"html"` (§8). It counts `fbs_fb_requests_total{query="page_html",class,tier}` and calls `Browser.Observe()`, which counts HTML loads for the browser fallback's share cap.
5. A non-OK class gives `Outcome(cls)` and returns the `PageLoad` **without an error**. The caller decides what to do.
6. It runs `htmlx.ExtractTokens(body)`.
7. **datr planting**: if `_js_datr` was found and the jar has no `datr` cookie, it sets `datr=<_js_datr>` (`internal/fb/session/bootstrap.go:111-113`). This is the logged-out device cookie that Facebook's JavaScript would otherwise set.
8. **Consent wall**: if the page shows one, an LSD is present and `hop < 3`, it calls `acceptConsent` and then re-GETs the same target with `sec-fetch-site: same-origin` (`internal/fb/session/bootstrap.go:114-119`).
9. **No tokens** (`!tok.Valid()`, meaning no LSD): it increments `bootstrap_total{http,no_tokens}`. If a `Browser` is configured, it tries `Browser.Bootstrap`; a non-nil `PageLoad` from it is returned as-is. Otherwise it records `Outcome(SoftError)` and returns `Class = soft_error` with the reason "no tokens in html[; browser: …]".
10. **Success**: `id.SetTokens(tok)`, `Outcome(OK)`, `bootstrap_total{http,ok}`.

After 4 hops it returns `ErrBootstrap: too many redirects`.

**`acceptConsent`** (`internal/fb/session/bootstrap.go:161-186`):

- Request: `POST /cookie/consent/`, `application/x-www-form-urlencoded`, body `accept_only_essential=true&lsd=<lsd>&jazoest=<jazoest>`.
- Headers, in order: the three sec-ch-ua headers, `user-agent`, `content-type`, `accept: */*`, `origin`, `sec-fetch-site: same-origin`, `sec-fetch-mode: cors`, `sec-fetch-dest: empty`, `referer: <page>`, `accept-encoding`, `accept-language`.
- An HTTP status ≥ 400 is an error. On success it increments `bootstrap_total{http,consent}`.

**`Bootstrap`** calls `Load` and returns `ErrBootstrap` (wrapping the class and reason) if the identity still has no tokens (`internal/fb/session/bootstrap.go:150-159`).

**Every page load doubles as a bootstrap.** The page stage reloads the page HTML on segment 0, which refreshes the identity's tokens (`internal/crawl/stage/page.go:62-89`).

**[OBS]** `fb.bootstrap_attempts: 3` (`configs/default.yaml:115`, `internal/config/config.go:150`) is **not read anywhere**. Bootstrap failures fall through to the task-retry mechanism instead.

---

## 6. `internal/fb/htmlx` — tokens from server-rendered HTML

```go
type Tokens struct {
    LSD          string `json:"lsd"`        DTSG string `json:"fb_dtsg"`  JSDatr string `json:"js_datr"`
    SpinR        string `json:"__spin_r"`   SpinB string `json:"__spin_b"` SpinT string `json:"__spin_t"`
    HSI          string `json:"__hsi"`      Rev  string `json:"__rev"`
    HasteSession string `json:"__hs"`       CometReq string `json:"__comet_req"`
    ConsentWall  bool   `json:"consent_wall"`
}                                                   // tokens.go:15-27
func (t Tokens) Valid() bool      // LSD != ""      // :30
func (t Tokens) Jazoest() string                    // :35-45
func ExtractTokens(html []byte) Tokens              // :78-92
func PageID(html []byte) int64                      // :106-116
func JSONBlobs(html []byte) []gjson.Result          // :122-130
```

### 6.1 Every token, with its source pattern (`internal/fb/htmlx/tokens.go:54-73`)

Each pattern runs as a regex over the whole HTML, first match wins, and the patterns are tried in order. A later pattern for the same field overwrites an earlier one.

| Token | Form key it feeds | Regex(es), in order | Where it lives in Facebook HTML |
|---|---|---|---|
| LSD | `lsd` form field + `x-fb-lsd` header | `\["LSD",\[\],\{"token":"([^"]+)"`; `"LSD"\s*:\s*\{\s*"token"\s*:\s*"([^"]+)"`; `name="lsd" value="([^"]+)"` | `ScheduledServerJS` → `__bbox.define` entry `["LSD",[],{"token":…},323]`; alternative JSON form; hidden `<input name="lsd">` |
| fb_dtsg | `fb_dtsg` (sent only when present) | `\["DTSGInitialData",\[\],\{"token":"([^"]+)"` | `DTSGInitialData` define (normally absent when logged out) |
| _js_datr | `datr` **cookie** | `"_js_datr"\s*,\s*"([^"]+)"`; `"_js_datr"\s*:\s*\{\s*"value"\s*:\s*"([^"]+)"` | `["CookieCoreConfig","set",null,[["_js_datr","…",1]]]` |
| __spin_r | `__spin_r` | `"__spin_r"\s*:\s*(\d+)` | `SiteData` |
| __spin_b | `__spin_b` | `"__spin_b"\s*:\s*"([^"]+)"` | `SiteData` (e.g. `"trunk"`) |
| __spin_t | `__spin_t` | `"__spin_t"\s*:\s*(\d+)` | `SiteData` (unix seconds) |
| hsi | `__hsi` | `"hsi"\s*:\s*"(\d+)"` | `SiteData` |
| server_revision / client_revision | `__rev` | `"server_revision"\s*:\s*(\d+)`, falling back to `"client_revision"\s*:\s*(\d+)` | `SiteData` |
| haste_session | `__hs` | `"haste_session"\s*:\s*"([^"]+)"` | `SiteData` (e.g. `"20390.HYP:comet"`) |
| __comet_req | `__comet_req` (overrides the client param) | `"__comet_req"\s*:\s*(\d+)` | inline config |
| consent wall flag | — | byte markers `"cookie_consent_dialog"`, `data-cookiebanner=`, `/cookie/consent/` | EU banner |

### 6.2 Derived values

- **`jazoest`** = `"2"` followed by the decimal sum of the Unicode code points of `fb_dtsg` (or of `lsd` when there is no dtsg). The comment says "Verify against real traffic in Phase 0" (`internal/fb/htmlx/tokens.go:32-45`); the test pins the algorithm (`internal/fb/htmlx/tokens_test.go:18-25`).
- **`PageID`** tries these patterns in order, returning the first positive int64 (`internal/fb/htmlx/tokens.go:94-116`): `"pageID":"…"`, `"page_id":"…"`, `"delegate_page":{"id":"…"`, `fb://page/?id=…`, `fb://page/…`, `fb://profile/…`, `"userID":"\d{5,}"`.
- **`JSONBlobs`** returns every `<script type="application/json" …>…</script>` payload that is valid JSON. Facebook preloads GraphQL results this way; `parse.Page` consumes them.

### 6.3 Not extracted or sent

**[OBS]** The code does not extract or send `__dyn`, `__csr`, `__hsdp`, `__hblp`, `__sjsp`, `__crn` or `__jssesw`, nor any `x-fb-*` header beyond `x-fb-lsd` and `x-fb-friendly-name`. A real Comet browser sends several of these. **[SUGGESTION]** Treat the absence as a known fingerprint delta and measure whether it matters.

---

## 7. `internal/fb/graphql` — request building and response decoding

### 7.1 Types and functions

```go
type RawSink interface { Capture(ctx context.Context, query string, cls detect.Class, body []byte, vars map[string]any) } // client.go:26-28
type Request struct {
    Query   string         // registry name (timeline, comments, replies, …)
    Vars    map[string]any // template values
    PageID  int64          // per-page GCRA key; 0 = none
    Fast    bool           // p0 → PageRPSFast
    Referer string
    Lane    string         // p0|p1|p2 for the budget
}                                                                 // client.go:31-38
type Response struct {
    Query registry.Query; Parts []gjson.Result; Body []byte; Status int; ContentType string
    Class detect.Class; Reason string; Errors []detect.GQLError; Elapsed time.Duration
    Vars map[string]any; BytesIn, BytesOut int64
}                                                                 // client.go:41-57
type Client struct {
    Registry registry.Registry; Rules detect.Rules; Limiter ratelimit.Limiter
    PageRPS, PageRPSFast, GlobalRPS int; AcceptLanguage, Locale string
    Raw RawSink; CaptureAll bool
    Ceiling  interface{ RPS() int }
    Outcomes interface{ Observe(c detect.Class, lane string, wireBytes int64) }
    Breakers *breaker.Set
}                                                                 // client.go:60-82
var ErrTransport = errors.New("graphql: transport")               // client.go:105
func (c *Client) Do(ctx context.Context, id *session.Identity, req Request) (*Response, error) // :110-137
func (c *Client) Parsed(resp *Response, ok bool)                  // :90-102
func SplitParts(body []byte) []gjson.Result                       // parts.go:20-46
func HasData(parts []gjson.Result) bool                           // parts.go:51-63
func Errors(parts []gjson.Result) []detect.GQLError               // parts.go:66-91
```

It depends on `registry.Registry` (out of scope; `internal/fb/registry/registry.go:52-61`): `Get(name)`, `Fallback(name, failedDocID)`, `ClientParams()`, `All()`, and `registry.Render(tmpl, vars)`. `Render` replaces string values of the form `"{{name}}"` with the caller's value, or with JSON `null` when the value is absent (`internal/fb/registry/registry.go:383-416`). **[OBS]** `Render` round-trips the template through a Go `map`, so the keys of the `variables` JSON come out **alphabetically sorted**, not in the browser's order. That is a minor fingerprint difference.

### 7.2 Endpoint and headers (`internal/fb/graphql/client.go:150-173`)

- `POST <base>/api/graphql/`, with `content-type: application/x-www-form-urlencoded`.
- Header order, exactly as on the wire:

```
sec-ch-ua-platform: "Windows"
x-fb-lsd: <lsd>
sec-ch-ua: <brands>
sec-ch-ua-mobile: ?0
x-fb-friendly-name: <registry friendly_name, e.g. ProfileCometTimelineFeedRefetchQuery>
x-asbd-id: <client_params.x_asbd_id = "359341">
user-agent: <UA>
content-type: application/x-www-form-urlencoded
accept: */*
origin: https://www.facebook.com
sec-fetch-site: same-origin
sec-fetch-mode: cors
sec-fetch-dest: empty
referer: <req.Referer or base URL>   (the page stage passes the base URL, the post stage the post permalink)
accept-encoding: gzip, deflate, br, zstd
accept-language: en-US,en;q=0.9
priority: u=1, i
```

### 7.3 Form body, in browser order (`internal/fb/graphql/client.go:240-294`)

Values come from tokens (T), the identity (I), the registry `client_params` (P, from `configs/queries.seed.yaml:11-18`) or the query (Q).

| # | Key | Value |
|---|---|---|
| 1 | `av` | `0` (logged out) |
| 2 | `__aaid` | `0` |
| 3 | `__user` | `0` |
| 4 | `__a` | P `1` |
| 5 | `__req` | I: base-36 counter per identity |
| 6 | `__hs` | T: haste_session |
| 7 | `dpr` | P `1` |
| 8 | `__ccg` | P `EXCELLENT` |
| 9 | `__rev` | T: server_revision |
| 10 | `__s` | I: `xxxxxx:xxxxxx:xxxxxx` |
| 11 | `__hsi` | T: hsi |
| 12 | `__comet_req` | T if present, else P `15` |
| 13 | `locale` | `fb.locale` (`en_US`), only if set |
| 14 | `fb_dtsg` | T, only if present |
| 15 | `lsd` | T |
| 16 | `jazoest` | derived (§6.2) |
| 17 | `__spin_r` | T |
| 18 | `__spin_b` | T |
| 19 | `__spin_t` | T |
| 20 | `fb_api_caller_class` | P `RelayModern` |
| 21 | `fb_api_req_friendly_name` | Q `friendly_name` |
| 22 | `variables` | rendered JSON (§7.1) |
| 23 | `server_timestamps` | P `true` |
| 24 | `doc_id` | Q `doc_id` |

`encodeForm` drops any pair whose value is empty, **except `variables`**, and uses `url.QueryEscape` (`internal/fb/graphql/client.go:280-294`). `x_asbd_id` is a client param, but it is used as a header, not a form field.

### 7.4 Pacing (`internal/fb/graphql/client.go:212-235`)

1. `id.Wait(ctx)`: per-identity token bucket plus jitter.
2. If `PageID != 0`: `Limiter.Wait("page:<id>", rps)`, where `rps = PageRPS` (3), or `PageRPSFast` (6) when `Fast` (p0).
3. `Limiter.Wait("global", g)`, where `g = Ceiling.RPS()` if a ceiling is set and the value is > 0, else `GlobalRPS` (50).

Without a `Limiter` (tests), only identity pacing applies.

### 7.5 Response reading (`internal/fb/graphql/parts.go`)

- **`SplitParts`** (`internal/fb/graphql/parts.go:20-46`):
  - trims whitespace, then strips one leading `for (;;);` anti-JSON-hijacking guard;
  - decodes consecutive JSON values with a streaming `json.Decoder`. That covers NDJSON `@defer` and `@stream` payloads, and also back-to-back documents with no newline.
  - On a decode error it skips to the next `\n` and carries on, so one bad part does not lose the rest of the stream.
- **`HasData`**: true if *any* part has a `data` object with at least one non-null field. `{"data":{"node":null}}` beside an error counts as a failed request, not a partial one (`internal/fb/graphql/parts.go:47-63`; test `internal/fb/graphql/parts_test.go:5-20`).
- **`Errors`** collects:
  - every entry of every part's `errors[]`: `code`, falling back to `extensions.code`, plus `message`, `severity` and `summary`;
  - the legacy top-level `{"error": <number>, "errorSummary": …, "errorDescription": …}` (`internal/fb/graphql/parts.go:66-91`).
- **Content type**: Facebook labels its GraphQL JSON as `text/html; charset="utf-8"`. The classifier therefore only treats a body as HTML when it starts with `<`, or when it is not JSON-looking (`{`, `[`, `for (;;);`) *and* the content type says `text/html` (`internal/fb/detect/detect.go:183-190`).

### 7.6 `do()` result handling (`internal/fb/graphql/client.go:174-209`)

- A transport error gives:
  - `Outcome(Transport)`;
  - `Outcomes.Observe(Transport, lane, len(body))`, because the request did go out;
  - `fbs_fb_requests_total{class="transport"}`;
  - the return value `fmt.Errorf("%w: %w", ErrTransport, err)`.
- Otherwise the client:
  - updates the `proxy_bytes_total` and `fb_response_bytes` metrics;
  - computes `parts`, `errs` and `cls := Rules.Classify(...Expect:"graphql")`, and turns `OK` with zero parts into `soft_error "empty body"`;
  - calls `Outcome(id, cls)`, `Outcomes.Observe(cls, lane, BytesIn+BytesOut)` and the metric;
  - calls `Raw.Capture` when `CaptureAll` or `cls != OK`.

### 7.7 Query breaker and doc_id fallback (`internal/fb/graphql/client.go:84-137`)

- The breaker key is `"<name>|<doc_id>"`.
- **Before sending**: if the breaker does not `Allow`, the client tries `Registry.Fallback(name, doc_id)`, the best entry with a different doc_id. If there is none, or that one's breaker is also closed to traffic, it **does not send** and instead returns `Response{Class: SchemaDrift, Reason: "breaker open for … waiting for a registry or parser update"}` with a nil error, and increments `fbs_fb_query_short_circuit_total{query}`.
- **After sending**: on `registry_stale` (doc_id rejected) it calls `Breakers.Observe(key, failed=true)` and, when a fallback exists, **retries once** with it. The fallback's response is returned as-is.
- **`Parsed(resp, ok)`**: the stages call it after parsing an `OK` response. It feeds the breaker with `!ok`. If parsing failed and the class was OK, it re-captures the raw body as `schema_drift` so replays find it under the failures prefix.
- Other classes (rate limits and so on) do **not** feed the query breaker.

---

## 8. `internal/fb/detect` — response classification

### 8.1 Classes and actions

`Blocking()` is true for `RateLimited`, `LoginWall` and `Checkpoint` (`internal/fb/detect/detect.go:46-48`).

The actions below combine the effects inside this layer with how `internal/crawl/stage/deps.go:138-150` (out of scope) maps classes to task retries.

| Class (`String()`) | Meaning (code comment) | Identity (`Record`) | Exit bucket breaker | AIMD | Task (stage) |
|---|---|---|---|---|---|
| `ok` | — | score 1, resets the block streak | success | total++ | continue |
| `rate_limited` | "back off this identity and retry elsewhere" | score 0; 60s cooldown; a 2nd consecutive block retires (`blocked`) | **failure** | total++, blocked++ | retry in 30s from the cursor; the next attempt escalates the tier |
| `login_wall` | "logged-out access stopped here; repeated at the same cursor on fresh identities = coverage truncated" | retire `login_wall` | **failure** | blocked++ | the stage counts walls per cursor: 2 walls at the same cursor stop the stream as `truncated=login_wall` (`internal/crawl/stage/page.go:335-341`, `fbs_timeline_truncated_total`); otherwise it saves a checkpoint and retries in 30s |
| `checkpoint` | "identity/IP flagged. Retire it." | retire `checkpoint` | **failure** | blocked++ | retry in 30s |
| `soft_error` | "transient server error. Re-bootstrap tokens and retry once." | score 1 (not blocking) | success | total++ | retry in 30s. **[OBS]** Nothing re-bootstraps automatically. The identity keeps its tokens, and LIFO reuse may hand the same identity back. Only the page stage reloads HTML (on segment 0). |
| `registry_stale` | doc_id/variables rejected | score 1 | success | total++ | the client retries once on a fallback; the query breaker records a failure; the task retries in **5m** |
| `schema_drift` | parser could not find required fields (set by stages or by the query breaker) | — | — | — | query breaker failure; retry in **5m** |
| `content_unavailable` | deleted / restricted / geo-blocked | score 1 | success | total++ | page: emit "unavailable" and finish |
| `transport` | network/proxy failure | score 0.5 (does not reset the streak) | **failure** | excluded (counted for the budget only) | `ErrTransport`, retry in 30s |

### 8.2 Rules (`DefaultRules`, `internal/fb/detect/detect.go:72-86`)

The comment says these defaults "come from public observations and must be confirmed in Phase 0".

- `RateLimitCodes`: **1675004, 1675007, 368, 1390008**
- `SoftCodes`: **1357004, 1357001, 1357031, 2**
- `LoginCodes`: **1357005, 1348007**
- `StaleMessages` (case-insensitive regexes, matched on `message` or `summary`): `persisted ?query`, `\bdoc_?id\b`, `unknown (query|operation)`, `variable .* (is|was) (not|never) (defined|provided)`
- `LoginMarkers` (bytes): `id="login_form"`, `"loginForm"`, `/login/?next=`, `login_required`
- `BlockMarkers` (bytes): `temporarily blocked`, `Temporarily Blocked`, `rate limit`

### 8.3 `Classify` evaluation order (`internal/fb/detect/detect.go:105-178`)

The first match wins.

1. **Location header**: contains `/checkpoint` → Checkpoint; contains `/login` → LoginWall.
2. **Status**:
   - `429` → RateLimited.
   - `407`, `502`, `503`, `504` → **Transport**. These are proxy or gateway errors, not Facebook's answer.
   - Any other `≥ 500` → SoftError.
   - `404` with `Expect=="html"` → ContentUnavailable.
   - Any other `3xx` → SoftError ("unknown redirect … re-bootstrap once").
3. **GraphQL errors**, checked per error in order: a rate-limit code → RateLimited; a login code → LoginWall; a stale-message regex → RegistryStale; a soft code → SoftError. Rate and login codes win even when `data` is present (`internal/fb/detect/detect_test.go:35`).
4. **`Expect=="graphql"` and the body is HTML** (only the first 256 KiB is scanned): a block marker → RateLimited "block page"; a login marker → LoginWall "login page"; otherwise SoftError "html instead of graphql".
5. **`Expect=="html"`** (only the first 512 KiB is scanned): a body containing both `checkpoint` and `action="/checkpoint` → Checkpoint; a block marker → RateLimited.
6. **Leftover errors**, when there is no data **or** any error has severity `CRITICAL` → SoftError "fb error <code> <msg>". Non-critical errors *beside* data stay **OK**. Facebook streams `field_exception` errors with severity `ERROR` beside timeline posts (`internal/fb/detect/detect_test.go:31-34`).
7. Otherwise → **OK**.

### 8.4 Gaps

- **[OBS]** **401, 403 and 400 are not handled.** A 4xx other than 404/429/407 with no parsed errors classifies as **OK**; the GraphQL path then usually downgrades it to `soft_error "empty body"`. For an HTML page it stays OK.
- **[OBS]** A *non-redirect* HTML login wall (a `200` login page) is not detected as LoginWall for `Expect=="html"`. Login markers are only checked on the GraphQL path. Such a page still contains an `lsd`, so it would "bootstrap" and then fail as drift in `parse.Page`.
- **[OBS]** The block marker `rate limit` is scanned in the first 512 KiB of normal page HTML. That can produce false positives if Facebook ships that string in inline JavaScript.

---

## 9. `internal/fb/ratelimit` — GCRA, AIMD and the traffic budget

### 9.1 Fleet GCRA limiter (`internal/fb/ratelimit/ratelimit.go`)

```go
type Limiter interface { Wait(ctx context.Context, key string, perSecond int) error } // :15-17
type Redis struct{ l *redis_rate.Limiter }; func NewRedis(c *redis.Client) *Redis     // :22-26
type Noop struct{}                                                                     // :59-61
```

- **Library**: `github.com/go-redis/redis_rate/v10 v10.0.1`. `Wait` loops on `Allow(ctx, "rl:"+key, redis_rate.PerSecond(n))`. When the request is denied it sleeps `RetryAfter` (minimum 10ms) and tries again (`internal/fb/ratelimit/ratelimit.go:28-56`). `perSecond <= 0` disables the limit.
- **Fail-open**: on a Redis error it logs `ratelimit: redis unavailable, failing open` and returns nil, relying on per-identity pacing (`internal/fb/ratelimit/ratelimit.go:34-40`).
- **[LIB] Key names**: redis_rate prefixes `"rate:"`, so the real keys are **`rate:rl:global`** and **`rate:rl:page:<page_id>`**. `PerSecond(n)` means `Rate=n`, `Period=1s`, **`Burst=n`**. **[OBS]** These keys are not namespaced by `kafka.topic_prefix`, unlike the AIMD and budget keys.
- **[LIB] Lua (GCRA, `allowN`, based on rwz/redis-gcra)**. This is the only Lua on the client path; the repo has no custom Lua here.
  - `emission_interval = period/rate`, `increment = emission_interval*cost`, `burst_offset = emission_interval*burst`.
  - `now` comes from `redis.call("TIME")`, rebased to 2017-01-01.
  - `tat = max(GET key or now, now)`, `new_tat = tat + increment`, `allow_at = new_tat - burst_offset`.
  - If `now < allow_at` it returns `{0, 0, retry_after = allow_at - now, reset_after = tat - now}`.
  - Otherwise it runs `SET key new_tat EX ceil(new_tat-now)` and returns `{cost, remaining, -1, reset_after}`.
  - The script calls `redis.replicate_commands()`.

### 9.2 AIMD fleet ceiling (`internal/fb/ratelimit/aimd.go`)

Design comment (`internal/fb/ratelimit/aimd.go:19-24`): the crawlers count outcomes into 10s windows in Redis, the leader reads each finished window and moves the ceiling, and the crawlers read the ceiling back for the global GCRA. "Adding pods therefore never adds load."

```go
const Window = 10 * time.Second; const settle = 3 * time.Second              // :27, :31
type Reporter struct{…}; func NewReporter(rdb *redis.Client, ns string) *Reporter       // :42-51
func (r *Reporter) Observe(c detect.Class, lane string, wireBytes int64)                // :72-83
func (r *Reporter) Flush(ctx context.Context) error; func (r *Reporter) Run(ctx context.Context) error // :86-146 (flush every 2 s)
type Ceiling struct{…}; func NewCeiling(rdb *redis.Client, ns string, fallback int) *Ceiling // :149-158
func (c *Ceiling) RPS() int; func (c *Ceiling) Refresh(ctx context.Context) error; func (c *Ceiling) Run(ctx context.Context) error // refresh every 5 s
type Decision struct{ From, To int; Total int64; BlockRate, Utilization float64; Reason string } // :200-206
func Decide(cfg config.AIMD, cur int, total, blocked int64, window time.Duration) Decision        // :219-247
type Controller struct{…}; func NewController(rdb *redis.Client, ns string, cfg config.AIMD, start int) *Controller // :250-264
func (c *Controller) Tick(ctx context.Context) error                                               // :267-301
```

**Reporter.**

- `Observe` is a few atomic adds.
- Per lane (p0/p1/p2; unknown or empty lanes count as p1) it adds `reqs` and `bytes`.
- `Transport` is counted for the budget **only**; other classes count towards `total`, and blocking ones also towards `blocked`.
- HTML page loads are reported too, from the stages (`internal/crawl/stage/deps.go:124-134`).
- `Flush` swaps the counters to zero and writes them in a single `MULTI/EXEC` pipeline:
  - `HINCRBY <ns>aimd:w:<windowStartUnix> total|blocked`, then `EXPIRE` 10 minutes;
  - `HINCRBY` into `<ns>budget:d:<YYYYMMDD UTC>` (`EXPIRE` 72h) and `<ns>budget:s:<5-min slice start unix>` (`EXPIRE` 3h), on the fields `req:p0|p1|p2` and `bytes:p0|p1|p2`.
- If the pipeline fails, the counts are added back for the next attempt (`internal/fb/ratelimit/aimd.go:117-125`; test `internal/fb/ratelimit/aimd_test.go:137-150`). On shutdown there is one last flush with a 2s timeout.

**Controller.Tick** (leader, every `fb.aimd.interval`):

- It processes the latest window that ended at least `settle` (3s) ago: `w = (now-3s).Truncate(10s) - 10s`.
- It processes each window **once** (`c.last`). **[OBS]** Windows it misses are skipped, not caught up.
- It reads `total` and `blocked`, and `cur = GET <ns>aimd:global_rps`. When that is missing or ≤ 0 it uses `start = fb.global_rps`, and the result is published even if unchanged.
- It sets the gauges `fleet_rps = total/10`, `fleet_block_rate` and `aimd_ceiling_rps`. When the value changes it `SET`s the ceiling key (no TTL) and increments `aimd_adjustments_total{direction=<reason>}`.
- `NewController` sets `MaxRPS = max(MaxRPS, start)`, so a configured start above `max_rps` is kept (`internal/fb/ratelimit/aimd.go:261-264`).

**Decide** (`internal/fb/ratelimit/aimd.go:219-247`):

```
blockRate   = blocked/total
utilization = total / (cur × window_seconds)
if total < min_samples                         → hold:few_samples
elif blockRate > high_block_rate               → To = floor(min(cur, total/window_s) × decrease)   "decrease"
elif blockRate < low_block_rate && util ≥ min_util → To = cur + max(1, int(cur × increase))        "increase"
elif blockRate < low_block_rate                → hold:underused
else                                           → hold:block_rate
To = clamp(To, min_rps, max_rps)  (reason "clamp" if it moved a hold)
```

- The decrease is computed from the rate **actually sent**, not from the ceiling. That way it still lowers real load when the fleet runs below the ceiling (test case "decrease from what was sent": 300 → 70 when only 100 rps were sent).
- **Defaults** (`configs/default.yaml:104-114`): min_rps 5, max_rps 500, increase 0.02 (+2%, at least +1), decrease 0.7 (×0.7), low_block_rate 0.02, high_block_rate 0.06, min_samples 100, min_utilization 0.8, start = global_rps 50.
- **[OBS]** `min_samples 100` per 10s window means AIMD only acts when the fleet does at least 10 responses per second.

**Ceiling**: crawlers poll `GET <ns>aimd:global_rps` every 5s. A missing key means the fallback (`global_rps`); a Redis error keeps the last value.

### 9.3 Daily traffic budget (`internal/fb/ratelimit/budget.go`)

```go
const BudgetSlice = 5 * time.Minute                                  // :24
type Usage struct{ Requests, Bytes [3]int64 }                         // :37-40
type BudgetState struct {
    PauseP2 bool; Stretch float64; StretchTiers []string
    P2ReqRate, P2ByteRate float64; Used Usage; Projected [2]int64
    UpdatedAt time.Time; Explanation string
}                                                                     // :64-77
func (s BudgetState) StretchFor(tier string) float64                  // :80-90
func LoadBudgetState(ctx context.Context, rdb *redis.Client, ns string) (BudgetState, error) // :94-107 (missing → neutral {Stretch:1})
type BudgetController struct{…}; func NewBudgetController(rdb *redis.Client, ns string, cfg config.Budget) *BudgetController // :110-126
func (b *BudgetController) Tick(ctx context.Context) error            // :133-234
type BudgetGate struct{…; OnChange func(BudgetState)}; func NewBudgetGate(rdb *redis.Client, ns string) *BudgetGate // :237-246
func (g *BudgetGate) State() BudgetState; func (g *BudgetGate) Refresh(ctx context.Context) error; func (g *BudgetGate) Run(ctx context.Context) error // refresh every 15 s
```

**Tick algorithm**, run per dimension (requests, bytes) whose budget is > 0:

- `used` = today's day hash (UTC). `recent` = the sum of the last `ProjectionSpan/5min` *complete* slices (default 12).
- `rate = recent/span`. `project(lanes) = used_all_lanes + rate(lanes) × seconds_left_until_UTC_midnight`.
- `withP2 = project(p0,p1,p2)` and `without = project(p0,p1)`.
- **Pause p2** if `withP2 > limit`. While paused, it stays paused while `without + lastP2Rate×left > 0.95×limit`. `lastP2Rate` is remembered from the last time p2 was running (`internal/fb/ratelimit/budget.go:166-171,196-199`).
- **Stretch** (`Stretch` = `cfg.Stretch`, default 2) if `without > limit`. It stays stretched while `without > 0.95×limit` (`internal/fb/ratelimit/budget.go:201-203`). p0 is never held back.
- The state JSON is published at `<ns>budget:state` with TTL `max(10×interval, 10min)`. If the leader dies, the state expires and crawlers fall back to neutral rather than pausing forever (`internal/fb/ratelimit/budget.go:229-233`; test `internal/fb/ratelimit/budget_test.go:127-154`).
- Gauges: `budget_used{kind,lane}`, `budget_projected{kind}`, `budget_limit{kind}`, `budget_backfill_paused`, `budget_stretch`.

The `BudgetGate` on each crawler calls `OnChange` when `PauseP2` flips; the crawler then sets the p2 lane cap to 0 or back to its configured max (`internal/app/crawler.go:226-236`).

### 9.4 Redis/Valkey keys used by this layer

`ns` = `kafka.topic_prefix`, default empty.

| Key | Type | Writer | Reader | TTL |
|---|---|---|---|---|
| `rate:rl:global` | string (GCRA TAT) | GCRA Lua | GCRA Lua | `ceil(reset_after)` |
| `rate:rl:page:<page_id>` | string (GCRA TAT) | GCRA Lua | GCRA Lua | same |
| `<ns>aimd:w:<unix window start>` | hash `total`, `blocked` | Reporter | Controller | 10m |
| `<ns>aimd:global_rps` | int | Controller | Ceiling | none |
| `<ns>budget:d:<YYYYMMDD>` | hash `req:pN`, `bytes:pN` | Reporter | BudgetController | 72h |
| `<ns>budget:s:<unix 5-min slice>` | same | Reporter | BudgetController | 3h |
| `<ns>budget:state` | JSON `BudgetState` | BudgetController | Gate, planner | `max(10×interval, 10m)` |

---

## 10. `internal/platform/breaker` — keyed failure-rate breakers

```go
type State int // Closed=0, HalfOpen=1, Open=2 (the value exported to fbs_breaker_state)
type Config struct {
    Window time.Duration `koanf:"window"`              // default 1m
    MinEvents int `koanf:"min_events"`                 // default 20
    OpenRate float64 `koanf:"open_rate"`              // default 0.15
    Cooldown time.Duration `koanf:"cooldown"`          // default 2m
    MaxCooldown time.Duration `koanf:"max_cooldown"`   // default max(Cooldown, 30m) when < Cooldown
    ProbeSuccesses int `koanf:"probe_successes"`       // default 3
}                                                     // breaker.go:34-63
type Set struct { OnChange func(key string, from, to State); … }  // :82-90
func New(cfg Config) *Set
func (s *Set) Observe(key string, failed bool)  // :129-174
func (s *Set) Allow(key string) bool            // :184-206
func (s *Set) IsOpen(key string) bool           // :210 (does not admit a probe)
func (s *Set) State(key string) State           // :215
```

- **Window**: a ring of 10 slots, each `Window/10` wide. Events older than `Window` drop out (`internal/platform/breaker/breaker.go:65,148-168`).
- **Closed → Open** when, within the window, `n ≥ MinEvents` and `failed/n ≥ OpenRate`. Opening clears the ring and sets `openUntil = now + cooldown`.
- **Open**: `Allow` returns false. Observations that arrive while open are ignored; they belong to requests already in flight.
- **Open → HalfOpen** happens lazily on any `Allow`, `Observe` or `State` call after `openUntil` (`advance`).
- **HalfOpen**:
  - `Allow` admits **one probe at a time** (`probeAt`). A probe that never reports back is replaced after one `cooldown` (`internal/platform/breaker/breaker.go:196-200`).
  - A successful `Observe` counts towards `ProbeSuccesses` and admits the next probe. After `ProbeSuccesses` successes it goes to **Closed**, resets the cooldown to the base value and clears the ring.
  - A failed `Observe` **doubles the cooldown**, up to `MaxCooldown`, and reopens (`internal/platform/breaker/breaker.go:136-147`).
  - **[OBS]** *Any* observation counts in half-open, not only the probe's: for example, outcomes from identities created before the bucket opened.
- `OnChange` runs outside the lock, on every transition.
- **Two instances**:

  | Breaker | Key | Failure signal | Config |
  |---|---|---|---|
  | Proxy | exit bucket | blocking or transport | `fb.proxy.breaker`: 1m / 20 / 0.15 / 2m / 30m / 3 |
  | Query | `name\|doc_id` | parse failure or `registry_stale` | `fb.query_breaker`: **5m** / 20 / **0.5** / 2m / 30m / 3 (`configs/default.yaml:150-156`) |

---

## 11. Configuration knobs and magic numbers

### 11.1 The `fb:` section (`configs/default.yaml:66-156`; structs in `internal/config/config.go:135-215`)

Overridable via `FBS_FB__<KEY>` env vars.

| Key | Default | Meaning / where used |
|---|---|---|
| `base_url` | `https://www.facebook.com` | origin for every request (fakefb in tests) |
| `registry_seed` | `queries.seed.yaml` | registry seed (embedded FS) |
| `registry_refresh` | 30s | registry hot-reload period |
| `fingerprint_profile` | `chrome_146` | tls-client profile plus UA/CH, used for **every** identity |
| `locale` | `en_US` | `locale` form field |
| `accept_language` | `en-US,en;q=0.9` | header on every request |
| `request_timeout` | 30s | tls-client timeout per request |
| `identity_rps` | 0.33 | per-identity token bucket (plus jitter of 0 to +40%) |
| `identity_burst` | 2 | per-identity burst |
| `identity_max_requests_min` / `_max` | 150 / 400 | random request quota per identity |
| `page_rps` | 3 | fleet GCRA per page (key `page:<id>`) |
| `page_rps_p0` | 6 | per-page rate for on-demand (p0) tasks |
| `global_rps` | 50 | global GCRA when AIMD is off; AIMD start and fallback |
| `browser.enabled` | false | headless-Chromium bootstrap fallback |
| `browser.max_concurrent` | 2 | browsers per process |
| `browser.max_share` | 0.02 | share of HTML loads allowed to fall back to the browser |
| `browser.timeout` | 45s | |
| `browser.binary` / `browser.no_sandbox` | "" / false | |
| `aimd.enabled` | true | |
| `aimd.interval` | 10s | leader tick (the window is a fixed 10s constant) |
| `aimd.min_rps` / `max_rps` | 5 / 500 | |
| `aimd.increase` / `decrease` | 0.02 / 0.7 | |
| `aimd.low_block_rate` / `high_block_rate` | 0.02 / 0.06 | |
| `aimd.min_samples` | 100 | per window |
| `aimd.min_utilization` | 0.8 | |
| `bootstrap_attempts` | 3 | **unused** [OBS] |
| `capture_raw` | `failures` | `all` \| `failures` \| `none` (raw S3 sink) |
| `shadow_rate` | 0.05 | candidate parser sampling (out of scope) |
| `proxy.tiers` | `[]` | see §4 (`static_list` / `gateway` / `direct`) |
| `proxy.routing` | `{default: [direct]}` | class → tier list |
| `proxy.breaker.*` | 1m / 20 / 0.15 / 2m / 30m / 3 | exit-bucket breaker |
| `query_breaker.*` | 5m / 20 / 0.5 / 2m / 30m / 3 | `(query, doc_id)` breaker |

Related knobs outside `fb:`:

- `scheduler.budget` (`configs/default.yaml:247-254`): `enabled: true`, `requests_per_day: 0`, `bytes_per_day: 0` (both 0, so the controller does not run), `interval: 1m`, `stretch: 2`, `stretch_tiers: [T4, T3, T2]`, `projection_span: 1h`.
- `crawler.slots` 240 and `crawler.stages` (pool sizing).
- `registry_sync.tier`.
- `kafka.topic_prefix` (the Redis namespace).

### 11.2 Hard-coded constants

| Constant | Value | Where |
|---|---|---|
| default request timeout | 30s | `internal/fb/transport/transport.go:71-73` |
| max body | 32 MiB | `internal/fb/transport/transport.go:74-76`, `internal/fb/transport/std.go:29` |
| HTTP/3 | disabled | `internal/fb/transport/transport.go:84` |
| gateway sticky TTL default | 10m | `internal/fb/proxy/proxy.go:228-231` |
| gateway username default | `{user}-session-{session}` | `internal/fb/proxy/proxy.go:232-235` |
| gateway session id | 16 hex chars | `internal/fb/proxy/proxy.go:254-258` |
| static bucket granularity | IPv4 /24, IPv6 /48 | `internal/fb/proxy/proxy.go:163-176` |
| `MaxPerTier` default | 512 (the crawler overrides it with `max(16, slots·stages·1.3)`) | `internal/fb/session/pool.go:58-60`, `internal/app/crawler.go:103` |
| Acquire re-check timer | 1s | `internal/fb/session/pool.go:111` |
| think-time jitter | `[0, 0.4/rps)` | `internal/fb/session/identity.go:95` |
| health EWMA | 0.9 / 0.1; transport score 0.5 | `internal/fb/session/identity.go:115-125` |
| retire thresholds | 2 consecutive blocks; health < 0.5 | `internal/fb/session/identity.go:131,139` |
| rate-limit cooldown | `30s << min(consecBlocks,5)`, cap 15m (effectively 60s) | `internal/fb/session/identity.go:133-138` |
| sticky-expiry margin | 1 min | `internal/fb/session/identity.go:143` |
| `__s` format | 6:6:6 alnum | `internal/fb/session/identity.go:57` |
| redirect hops / consent hops | 4 / consent only when hop < 3 | `internal/fb/session/bootstrap.go:77,114` |
| classify scan limits | 256 KiB (graphql→html), 512 KiB (html) | `internal/fb/detect/detect.go:145,162` |
| GCRA retry floor | 10ms; key prefix `rl:` (+ library `rate:`) | `internal/fb/ratelimit/ratelimit.go:33,44-47` |
| AIMD window / settle | 10s / 3s | `internal/fb/ratelimit/aimd.go:27,31` |
| Reporter flush / Ceiling refresh | 2s / 5s | `internal/fb/ratelimit/aimd.go:131,185` |
| AIMD window key TTL | 10m | `internal/fb/ratelimit/aimd.go:103` |
| budget slice / day TTL / slice TTL | 5m / 72h / 3h | `internal/fb/ratelimit/budget.go:24`, `internal/fb/ratelimit/aimd.go:113-114` |
| budget resume margin | 0.95 | `internal/fb/ratelimit/budget.go:130` |
| budget defaults | projection 1h, stretch 2 | `internal/fb/ratelimit/budget.go:118-126` |
| budget state TTL | `max(10·interval, 10m)` | `internal/fb/ratelimit/budget.go:233` |
| gate refresh | 15s | `internal/fb/ratelimit/budget.go:270` |
| breaker ring | 10 slots | `internal/platform/breaker/breaker.go:65` |
| task retry delays | 30s (blocks / soft / transport), 5m (stale / drift) | `internal/crawl/stage/deps.go:138-150` |
| login-wall truncation | 2 walls at the same cursor | `internal/crawl/stage/page.go:335-341` |

---

## 12. Metrics, alerts and what they are for

All metrics are prefixed `fbs_` (`internal/platform/metrics/metrics.go`).

| Metric | Type | Labels | Emitted by | Purpose |
|---|---|---|---|---|
| `fbs_fb_requests_total` | counter | `query` (registry name or `page_html`), `class`, `tier` | graphql `do`, Loader | block ratio, stale ratio, per-tier health |
| `fbs_fb_request_seconds` | histogram (0.05s ×2, 10 buckets) | `query`, `tier` | graphql | latency per tier |
| `fbs_fb_response_bytes` | histogram (1 KiB ×2, 12 buckets) | `query` | graphql | decoded size |
| `fbs_proxy_bytes_total` | counter | `tier`, `dir` (in/out) | graphql, Loader | proxy cost (wire bytes) |
| `fbs_identity_active` | gauge | `tier` | Pool | live identities |
| `fbs_identity_retired_total` | counter | `tier`, `reason` (`checkpoint`, `login_wall`, `blocked`, `unhealthy`, `max_requests`, `sticky_expiry`, `breaker`, or a custom `Retire` reason) | Pool | why identities die |
| `fbs_bootstrap_total` | counter | `method` (`http`, `browser`), `outcome` (http: `ok`, `no_tokens`, `consent`; browser: `capped`, `busy`, `error`, `no_tokens`, `ok`, `<class>`) | Loader, browser | bootstrap health |
| `fbs_breaker_state` | gauge 0/1/2 | `name` = `proxy:<bucket>` \| `query:<name\|doc_id>` | `OnChange` hooks | breaker visibility |
| `fbs_fb_query_short_circuit_total` | counter | `query` | graphql | requests saved by the query breaker |
| `fbs_aimd_ceiling_rps`, `fbs_fleet_rps`, `fbs_fleet_block_rate` | gauges | — | Controller (leader) | the AIMD loop |
| `fbs_aimd_adjustments_total` | counter | `direction` (= Decision.Reason: `increase`, `decrease`, `clamp`) | Controller | |
| `fbs_budget_used` | gauge | `kind`, `lane` | BudgetController | |
| `fbs_budget_projected`, `fbs_budget_limit` | gauges | `kind` | BudgetController | |
| `fbs_budget_backfill_paused`, `fbs_budget_stretch` | gauges | — | BudgetController | |

Recording rules and alerts (`deploy/helm/fbs/files/alerts.yaml`):

- `fbs:block_ratio:rate5m` = rate of {rate_limited, login_wall, checkpoint} / rate of non-transport responses.
- `FbsBlockRateHigh` > 10% for 10m; `FbsBlockRateCritical` > 25% for 10m.
- `FbsQueryBreakerOpen`: `query:*` open for 15m.
- `FbsProxyBucketOpen`: `proxy:*` open for 30m.
- `FbsRegistryStale`: stale ratio > 20% for 45m.
- `FbsBudgetOverrun`: projected > 1.1 × limit for 30m.
- `FbsBackfillPausedLong`: paused for 12h.
- `FbsBrowserFallbackSaturated` / `FbsBrowserFallbackFailing`.
- Leader-only gauges are read joined on `fbs_scheduler_leader == 1`.

---

## 13. Errors and retry semantics (summary)

**Errors returned by the client layer** (everything else is a *class* on a non-error response):

| Error | Raised by | Meaning |
|---|---|---|
| `transport.ErrBodyTooLarge` | Doer | response over 32 MiB |
| `proxy.ErrTierOpen` | `Pool.Next` / `Pool.Acquire` | every bucket of the tier is cooling down |
| `session.ErrBootstrap` | Loader | too many redirects, or still no tokens after a load |
| `graphql.ErrTransport` (wrapped) | `Client.Do` | network or proxy failure. `Outcome(Transport)` was already recorded |
| `registry.ErrNotFound` | `Client.Do` | unknown query name |
| `ctx.Err()` | `Wait`, `Acquire`, limiter | cancellation |

**Retries inside the layer.** There is exactly one: on `registry_stale`, `Client.Do` retries once with a fallback doc_id. It never retries on rate limits or soft errors.

**Retries outside the layer.** The stage converts both errors and non-OK classes into a task retry through Kafka retry topics: 30s for blocks, soft errors and transport; 5m for stale and drift (`internal/crawl/stage/deps.go:138-150`). Retries go to a new or another identity, starting from the cursor saved in the checkpoint. Each attempt escalates the proxy tier. Attempt caps are `crawler.max_attempts` (5, without progress) and `max_total_attempts` (50), after which the task goes to the DLQ (out of scope).

**Fail-open choices.**

- A GCRA Redis error lets the request proceed.
- Without a published ceiling, the client uses `global_rps`; when Redis cannot be read, it keeps the last ceiling read.
- A missing or expired budget state means neutral (nothing paused).
- Reporter flush errors keep the counts for the next attempt.

---

## 14. Tests: what they prove

### 14.1 `internal/fb/fbtest`, the integration harness

`fbtest.New(t, Options)` (`internal/fb/fbtest/fbtest.go:44-99`) wires the real client layer against an in-process **fakefb** HTTP server:

- The seed `doc_id`s are the accepted ones (or rotated ones with `RotateDocIDs`), with `InlineReply: true`.
- The proxy pool comes from `proxy.New(config.Proxy{})`, i.e. direct only.
- The Doer is `NewStd` by default, or the real **tls-client** with `UseTLS`.
- Pool settings: `IdentityRPS` 1000, burst 10, `MaxRequests` 100000, `MaxPerTier` 64.
- `DefaultRules`, a Loader, and a GraphQL client with no limiter.

fakefb enforces the protocol the client must get right (`internal/fakefb/server.go:162-345`):

- The `lsd` must be one it issued in page HTML **and** must equal the `x-fb-lsd` header. Otherwise it returns legacy `for (;;);{"error":1357001,…}`.
- `x-fb-friendly-name` must equal `fb_api_req_friendly_name`, else error 1357004.
- An unknown `doc_id` gets `1675012 "PersistedQueryNotFound: doc_id …"`.
- The identity is keyed by the **`datr` cookie**. That makes the `_js_datr` → `datr` cookie planting essential to per-identity fault counters.
- GraphQL bodies are labelled `text/html; charset="utf-8"`.
- Faults: `RateLimitEvery` (legacy `for (;;);{"error":1675004}`), `LoginWallAfter` (1357005 CRITICAL), `ConsentWall`, `Drift`, `DriftRequired`, `JSOnlyTokens`, `FieldErrors`, `Latency`.

Tests (`internal/fb/fbtest/client_test.go`):

| Test | Proves |
|---|---|
| `TestClientLayerCollectsEverything` (:127), run with **both** the std and the tls-client transport | Bootstrapping on a page vanity URL, then paging the timeline (`count` 3), all comments and **recursively** every reply thread, collects exactly the world's truth: every post and every comment and reply. The fingerprinted transport, form and headers, token flow and multipart parsing all work end to end. |
| `TestLoginWallAndRateLimitAreClassified` (:167) | After `LoginWallAfter=2`, the 3rd call is `login_wall` and the identity is marked for retirement. With `RateLimitEvery=1`, a legacy guarded error labelled text/html is `rate_limited`. |
| `TestPartialFieldErrorsAreOK` (:203) | A `field_exception` (severity ERROR) part beside data stays OK, with 1 error surfaced, and the timeline still parses. |
| `TestDocRotationFallsBackToFallbackEntry` (:221) | A rotated doc_id with **no fallback** in a static registry gives `registry_stale`. The name suggests the fallback path, but it asserts the no-fallback case. [OBS] |
| `TestSchemaDriftIsReported` (:239) | Drift in an optional field keeps posts and loses only the text. |
| `TestQueryBreakerShortCircuits` (:263) | After 3 `Parsed(false)` (MinEvents 3, rate 0.5) the next `Do` returns `schema_drift "breaker open"` **without sending** (fakefb counter unchanged). Other queries are unaffected. |
| `TestGlobalLimitFollowsTheCeiling` (:325) | The global limiter key gets `GlobalRPS` (50) without a ceiling and the ceiling value (17) with one. `Outcomes` sees every response. |

`vars_test.go` only declares `timeZero`.

### 14.2 Unit tests

- **`internal/fb/proxy/proxy_test.go`**:
  - bucket derivation (/24, host, IPv6 /48) and credential-free buckets (:37);
  - a static list skips open /24s, `Available` stays true while one bucket is closed, and `ErrTierOpen` appears when all are open (:57);
  - the gateway breaker opens at 4 failures out of 8 at 50%, and `direct` never opens (:83);
  - parser and registry classes do not count against exits (:113).
- **`internal/fb/session/pool_test.go:16`**:
  - outcomes on an identity reach its bucket's breaker;
  - the idle identity is reused while its bucket is closed;
  - once the bucket opens, the next `Acquire` retires it and creates a fresh identity on the other /24 (`live` stays 1).
- **`internal/fb/detect/detect_test.go:5`**: 19 classification cases, covering every rule in §8.3, including "JSON labelled html → OK" and "rate-limit code with data → RateLimited".
- **`internal/fb/htmlx/tokens_test.go:8`**:
  - a realistic `ScheduledServerJS`/`__bbox.define` sample gives LSD, SiteData tokens and `_js_datr`;
  - `jazoest = "2"+Σ`;
  - the page id comes from `fb://page/`;
  - there is exactly 1 JSON blob.
- **`internal/fb/graphql/parts_test.go:5`**: `HasData` semantics over multi-part, `for (;;);` and `$defer` error parts.
- **`internal/fb/ratelimit/aimd_test.go`**:
  - a `Decide` table (:18) covering few samples, decrease, floor clamp, increase, step size, cap, underused, in-between, start below min and decrease-from-sent;
  - the Reporter → Controller → Ceiling loop on miniredis (:48), checking the settle delay, that each window is used once, increase, hold, a start above max being kept, and per-namespace fallback;
  - counts are kept on a Redis failure (:137).
- **`internal/fb/ratelimit/budget_test.go`**:
  - p2 pauses, hysteresis holds the pause, the stretch kicks in, and a new day lifts everything (:33);
  - the byte budget (:97);
  - no state means neutral (:118);
  - the state expires when the controller stops (:127).
- **`internal/platform/breaker/breaker_test.go`**:
  - opening at the exact threshold (:29);
  - the sliding window (:45);
  - a single half-open probe that closes after N successes, with the transition sequence (:56);
  - a failed probe doubling the cooldown up to the max (:86);
  - a lost probe being replaced (:102);
  - concurrent use (:115).

---

## 15. Things not to copy blindly ([OBS] collected)

1. `fb.bootstrap_attempts` is declared but never used.
2. `detect.Rules` are hard-coded (`DefaultRules()` at every construction site) although documented as config.
3. The rate-limit cooldown is effectively a constant 60s, because the second consecutive block retires the identity before the exponential backoff can grow.
4. The jitter is one-sided (0 to +40%) despite the "±40%" comment.
5. There is one fingerprint profile per pod; the other profiles in `fingerprints.yaml` are never used.
6. Empty header values are sent (for example `x-fb-lsd: ""` before bootstrap). The `cookie` header's position is not controlled.
7. `variables` JSON keys are alphabetised by `registry.Render`.
8. `SoftError` does not trigger the re-bootstrap the comment promises.
9. 400/401/403 are not classified, and an HTML login page without a redirect is not detected on page loads.
10. GCRA keys are not namespaced, unlike the AIMD and budget keys.
11. Breaker retirement in `Acquire` does not update `identity_active`.
12. Sticky expiry is only checked after a request, so an idle identity whose sticky session has expired can be used once more.
13. The budget controller never runs with the default config (both budgets 0).
14. The half-open breaker counts any observation, not only the probe's.
15. `Snapshot` exists but is unused, and it would serialise proxy credentials.
16. `__dyn`, `__csr`, `__crn` and similar Comet parameters are not sent.

---

## 16. Instagram adaptation notes

Legend:

- **(a)** generic, reusable as-is;
- **(b)** reusable with Instagram-specific parameters or small changes;
- **(c)** Facebook-specific, must be rewritten.

Everything in this section that describes Instagram is **[SUGGESTION]** and/or **[IG-VERIFY]**. Run a Phase-0 capture of a real logged-out Chrome session on `www.instagram.com` (DevTools HAR plus an HTTP/2 fingerprint echo) before coding against these specifics. Instagram's web surface changes as often as Facebook's.

### 16.1 Classification at a glance

| Component | Class | What changes for Instagram |
|---|---|---|
| `internal/platform/breaker` | **(a)** | Nothing. Reuse it for exit buckets and `(query, doc_id)`. |
| `transport` (`Doer`, `NewTLS`, `NewStd`) | **(a)** | Nothing structural. `cookieDomain` already maps `www.instagram.com` to `.instagram.com`. [SUGGESTION] Also expose `Set-Cookie` and specific response headers through `Response.Header` (already present) so the client can follow `x-ig-set-www-claim` (§16.4). Pin the `cookie` header position. |
| `configs/fingerprints.yaml` + `session.Fingerprint` | **(a)** | The same Chrome profiles work: Instagram is served by the same Meta edge. [SUGGESTION] Rotate profiles per identity instead of one per pod. |
| `proxy` | **(a)**, config **(b)** | Code as-is. New routing classes (for example `profile_html`, `profile_posts`, `post`, `comments`). [SUGGESTION] Expect logged-out Instagram to need residential or mobile exits much earlier. ISP /24s get walled fast. Consider a shorter `sticky_ttl`, and possibly a per-country gateway bucket. |
| `session.Pool` / `Identity` lifecycle | **(b)** | Keep leasing, LIFO reuse, the health EWMA, the retire reasons and sticky expiry. Change the token type (`htmlx.Tokens` → an IG token struct with `csrftoken`, `lsd`, rollout/revision, `app_id`, `www_claim`), the `__s` / `x-web-session-id` handling, and the numbers: a much lower `maxRequests` (e.g. 10–40) and `identity_rps`. [SUGGESTION] Fix the cooldown logic so backoff actually grows. |
| `session.Loader` (bootstrap) | **(b)/(c)** | `NavHeaders`, the redirect loop and `sameSite` are reusable. Rewrite three things: (1) wall paths (`/accounts/login`, `/challenge`, `/accounts/suspended` instead of `/login`, `/checkpoint`); (2) the cookie bootstrap (no `_js_datr` injection; IG sets `csrftoken`, `mid`, `ig_did` and `datr` through `Set-Cookie` and inline JS [IG-VERIFY]); (3) the consent flow (IG's EU cookie dialog is not `/cookie/consent/` [IG-VERIFY]). |
| `htmlx` | **(c)** (structure **(a)**) | Keep the "ordered regex candidates per token" design and `JSONBlobs`. IG also ships `<script type="application/json" data-sjs>` with `ScheduledServerJS`/`__bbox.define` [IG-VERIFY]. Rewrite the patterns and `PageID` (→ IG user id / username). |
| `graphql.Client` | **(b)** | Keep the pacing chain, breakers, fallback, outcome reporting, raw capture and `SplitParts`/`HasData`/`Errors` (IG web GraphQL is Relay too). Change the endpoint, headers and form fields (§16.4). **Add a second request kind for `api/v1` REST** (GET, JSON, different headers and errors). |
| `detect` | **(b)** | Keep the taxonomy, the `Blocking()` semantics and the evaluation order. Rewrite the rules (codes, markers, regexes) and **add 401/403/400 handling** (§16.5). |
| `ratelimit` GCRA | **(a)** | Rename the `page:<id>` key to `user:<id>` / `profile:<username>`. [SUGGESTION] Add the namespace prefix to the GCRA keys if Facebook and Instagram fleets share a Valkey. |
| `ratelimit` AIMD | **(a)** | Same algorithm. Retune: a lower start (`global_rps`), probably a lower `min_samples` (Instagram fleets may run below 10 rps), and maybe a tighter `high_block_rate`. |
| `ratelimit` budget | **(a)** | As-is. Bytes matter more for IG because responses carry large media URL sets. |
| `fbtest` | **(b)** | The pattern (real client stack against an in-process fake with fault injection) is the most valuable thing to copy. Build a `fakeig` that enforces `x-csrftoken == csrftoken cookie`, `x-ig-app-id`, `lsd`, doc_ids, and IG's error shapes. |
| `internal/app` wiring | **(b)** | Same shape, with an `ig:` config section. |
| `configs/queries.seed.yaml` | **(c)** | New client params and IG queries (§16.4). |
| `configs/default.yaml fb:` | **(b)** | Same knobs, IG values (§16.6). |

### 16.2 Instagram's logged-out web surface (what the new client must speak) [IG-VERIFY]

1. **HTML bootstrap.** `GET https://www.instagram.com/<username>/` with the same Chrome navigation headers. Logged out, this usually returns 200 with server-rendered JSON blobs, but it often redirects to `/accounts/login/?next=…` after a few views from one IP. Expect these Set-Cookies: `csrftoken`, `mid`, `ig_did`, `datr`, maybe `ig_nrcb`. Tokens appear in `ScheduledServerJS` defines much like Facebook's: `["LSD",[],{"token":…}]`, `SiteData` (`server_revision`, `haste_session`, `hsi`, `__spin_r/b/t`), and `RelayAPIConfigDefaults` with custom headers such as `X-IG-App-ID`. The web app id has long been **`936619743392459`**. A `csrf_token` also appears in the inline config, and a "rollout hash" or version feeds `x-instagram-ajax`.
2. **Web GraphQL (Relay persisted queries).** `POST https://www.instagram.com/graphql/query` (some captures show `/api/graphql`), form-encoded, carrying `doc_id`, `variables`, `fb_api_req_friendly_name` and the same `av`/`__user`/`__a`/`__req`/`__hs`/`__rev`/`__s`/`__hsi`/`__comet_req`/`lsd`/`jazoest`/`__spin_*` family. Instagram typically adds `__d=www`, and its `__comet_req` differs from Facebook's (often `7`). Example friendly names: `PolarisProfilePostsQuery` and `PolarisProfilePostsTabContentQuery_connection` (profile grid), `PolarisPostActionLoadPostQueryQuery` (single post by shortcode), and comment pagination queries. Recent responses root at `data.xdt_api__v1__feed__user_timeline_graphql_connection{edges,page_info}`. All of these rotate, so the registry-sync approach (doc_ids as data, candidates validated on canaries) carries over directly.
3. **REST `api/v1` web endpoints.** For example `GET https://www.instagram.com/api/v1/users/web_profile_info/?username=<u>`, which returns the profile plus first posts, with `x-ig-app-id` required. Logged-out access is heavily throttled; typical refusals are HTTP 401/429 with `{"message":"Please wait a few minutes before you try again.","require_login":true,"status":"fail"}`. The legacy `?__a=1&__d=dis` and `query_hash` GraphQL paths are deprecated and unreliable.

### 16.3 Token and cookie mapping, Facebook → Instagram [IG-VERIFY]

| FB code concept | IG equivalent | Notes |
|---|---|---|
| `lsd` (+ `x-fb-lsd`) | `lsd` form field + `x-fb-lsd` header on `/graphql/query` | same `LSD` define |
| `fb_dtsg` | absent when logged out | same as FB |
| `_js_datr` → `datr` cookie | `datr`, `ig_did`, `mid` arrive via `Set-Cookie` / inline JS | drop the manual planting unless a capture shows it is needed |
| — (none on FB) | **`csrftoken` cookie → `x-csrftoken` header** | required on IG POSTs; read it from the jar (`Doer.Cookie(base,"csrftoken")`) before every POST |
| — | **`x-ig-app-id: 936619743392459`** | on every XHR (GraphQL and api/v1) |
| — | `x-ig-www-claim` | starts as `0`; the server returns `x-ig-set-www-claim`, which later requests echo; store it on the identity |
| — | `x-instagram-ajax` | rollout hash or revision from the HTML |
| — | `x-requested-with: XMLHttpRequest` | XHRs |
| `__s` | `__s` and/or `x-web-session-id` | same `xxxxxx:xxxxxx:xxxxxx` shape; reuse `Identity.SessionParam()` |
| `x-asbd-id` | `x-asbd-id` | the value differs by surface and over time (historically 129477 on IG; FB's seed has 359341); keep it in the registry `client_params` |
| `jazoest` | `jazoest` | same algorithm ("2" + Σ code points) over the CSRF-ish token; verify which token IG uses |
| `__spin_*`, `__hsi`, `__rev`, `__hs` | same names | same `SiteData` patterns likely work unchanged |

### 16.4 Request builders for Instagram [SUGGESTION]

**GraphQL POST** (`/graphql/query`). Keep `graphql.Client.do`, but give it a per-platform `form()` and header list. Header order to start from (verify against a HAR):

```
sec-ch-ua-platform, x-ig-app-id, x-fb-lsd, sec-ch-ua, sec-ch-ua-mobile, x-fb-friendly-name,
x-csrftoken, x-asbd-id, x-ig-www-claim?, x-requested-with?, user-agent, content-type: application/x-www-form-urlencoded,
accept: */*, origin: https://www.instagram.com, sec-fetch-site: same-origin, sec-fetch-mode: cors,
sec-fetch-dest: empty, referer: https://www.instagram.com/<username>/, accept-encoding, accept-language, priority: u=1, i
```

The form is the same list as §7.3, plus `__d=www` and IG's `__comet_req`, with IG `client_params`. Make the `variables` key order follow the template (an ordered encoder) rather than Go's map order.

**REST GET** (`/api/v1/...`). This is a new `Client.DoREST(ctx, id, RESTRequest{Path, Query, Lane, ProfileKey})` that reuses `pace`, `Outcome`, `Outcomes.Observe`, the metrics and raw capture. Headers:

```
sec-ch-ua…, x-ig-app-id, x-csrftoken, x-asbd-id, x-ig-www-claim, x-requested-with: XMLHttpRequest,
user-agent, accept: */*, sec-fetch-site/mode/dest: same-origin/cors/empty, referer, accept-encoding, accept-language
```

Responses are plain JSON with `status: "ok"|"fail"`. `SplitParts` still works on a single JSON document.

**Registry seed** (`queries.seed.yaml` → `ig_queries.seed.yaml`):

- `client_params`: `__a: "1"`, `__d: "www"`, `__comet_req: <IG>`, `dpr`, `__ccg`, `fb_api_caller_class: RelayModern`, `server_timestamps: "true"`, `x_asbd_id: <IG>`, `x_ig_app_id: "936619743392459"`.
- Queries: `profile_posts` (PolarisProfilePostsQuery-style, with variables such as `{"data":{"count":12,…},"username":"{{username}}","after":"{{cursor}}","first":12,…}`), `post` (shortcode), `comments`, `comment_replies`.
- Keep `parser`, `parser_version` and `priority` exactly as they are.

### 16.5 `detect` rules for Instagram [SUGGESTION, IG-VERIFY]

Keep the class set. Map the signals as follows:

| Class | IG signals |
|---|---|
| LoginWall | `Location` contains `/accounts/login`; HTTP 401 with `"require_login":true`; JSON `"message":"login_required"`; an HTML login form (`loginForm`, `/accounts/login/`) on **both** page and API paths (fixes [OBS] §8.4) |
| Checkpoint | `Location` contains `/challenge`, `/accounts/suspended` or `/checkpoint`; JSON `"message":"checkpoint_required"` / `"challenge"` |
| RateLimited | HTTP 429; 401/403 with `"Please wait a few minutes"`; JSON `"spam":true` or `"feedback_required"`; FB-family GraphQL codes 1675004 and similar (the same Relay infra probably returns the same codes) |
| SoftError | 5xx (except 502/503/504/407 → Transport), `"status":"fail"` without a known message, an HTML body where JSON was expected, critical GraphQL errors |
| RegistryStale | `PersistedQueryNotFound`, `doc_id` / `unknown operation` messages (same regexes) |
| ContentUnavailable | 404 on the profile or post page, `"user":null` on `web_profile_info`, "Sorry, this page isn't available." |
| Transport | unchanged |

Add explicit status branches for **400, 401 and 403**: their bodies decide between LoginWall and RateLimited, and the fallback is SoftError, never OK.

Make the rules loadable from config, as the Facebook comment intended but never implemented.

### 16.6 Starting numbers for an `ig:` config [SUGGESTION]

Logged-out Instagram tolerates far less traffic per IP than Facebook. Start conservatively and let AIMD and the breakers find the real limits:

- `identity_rps` 0.1–0.2, `identity_burst` 1;
- `identity_max_requests` 10–40;
- `profile_rps` (the per-page analogue) 1;
- `global_rps` (AIMD start) 5–10, `aimd.min_samples` 20–30;
- `proxy.breaker.open_rate` 0.15–0.25 with `min_events` 10;
- gateway `sticky_ttl` 5–10m.

Treat a login wall at the *first* page of a profile as a strong exit-quality signal, not as coverage truncation. Keep the "2 walls at the same cursor means truncated" rule only for deeper cursors.

### 16.7 What to keep exactly

These are the design decisions worth copying unchanged:

- the one-funnel `Outcome()` feeding three loops: identity health, the exit-bucket breaker, and AIMD/budget;
- exclusive identity leasing with LIFO reuse;
- tier escalation on retry;
- `(query, doc_id)` breakers with registry fallback;
- the fail-open GCRA;
- AIMD computing its decrease from the rate actually sent;
- leader-published state with TTLs, so a dead leader means neutral rather than stuck;
- an in-process fake server that enforces the protocol, with the whole crawl test run on both transports.
