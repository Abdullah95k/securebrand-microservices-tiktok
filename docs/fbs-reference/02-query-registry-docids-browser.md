# 2. Query registry, doc_id discovery and rotation, headless-browser fallback

[← Index](README.md)

Repository: this repository at commit `265ae3a`. `path:line` references are relative to the repository root and point at that commit.
Audience: the team building an Instagram scraper on the same design.
Method: I read every file in scope, plus the code they depend on (GraphQL client, session loader, detect rules, breaker, config, metrics, alerts, Helm chart, Dockerfile, fakefb). All `file:line` references are to that commit.

> **About Instagram claims.** Nothing in this repository mentions Instagram (a grep for `instagram`, `cdninstagram` and `x-ig-app-id` finds nothing). Everything in section 9 about Instagram is a **suggestion to verify** against real traffic, not a fact taken from this code.

---

## 0. File map

| File | Lines | Role |
|---|---|---|
| `internal/fb/registry/registry.go` | 418 | Registry model, seed parsing, in-memory snapshot, Postgres-backed reloadable registry, upsert, variable templating |
| `internal/fb/registry/registry_test.go` | 94 | Seed parse/render, fallback ordering, seed round-trip and placeholders |
| `internal/fb/docids/docids.go` | 142 | Finds `friendly_name -> doc_id` in HTML and JS bundles (regexes, bundle URL filter, caching scanner, HTTP fetcher) |
| `internal/fb/docids/docids_test.go` | 86 | Scan shapes, bundle host filter, fetch-once cache, fetcher errors |
| `internal/registrysync/sync.go` | 981 | The rotation follower: canary load, discovery, render discovery, candidates, probe chain, verdicts, promotion, retirement, reports, metrics |
| `internal/registrysync/sync_test.go` | 401 | Rotation, dry run, drift, retirement, spare kept, all-blocked, lock, render discovery |
| `cmd/fbs-registry-sync/main.go` | 83 | Binary: loop every interval, or `--once` with exit codes |
| `internal/app/registrysync.go` | 76 | Wiring: identity pool, tier, loader, fetcher, optional browser |
| `internal/fb/browser/render.go` | 517 | One headless-Chromium render over CDP: options, client hints, capture of GraphQL exchanges, media blocking, proxy auth, settle and scroll |
| `internal/fb/browser/cdp.go` | 279 | Minimal CDP websocket client and the Chromium launcher (flags, 45 s start budget, process group) |
| `internal/fb/browser/bootstrap.go` | 192 | `Bootstrapper`: share cap and concurrency cap, bootstrap of an identity, `Capture` for discovery |
| `internal/fb/browser/browser_test.go`, `launch_test.go` | 297 + 72 | Render, proxy auth, bootstrap fallback, share cap, client hints, SOCKS refusal, wall refusal, media blocking, process cleanup, launch failures |
| `internal/e2e/registry_test.go` | 127 | End to end: rotation healed, admin endpoints, full crawl afterwards |
| `internal/e2e/browser_test.go` | 45 | End to end: JS-only-token pages crawled completely through the browser fallback |
| `configs/queries.seed.yaml` | 121 | Seed registry: client params and 5 queries (placeholder doc_ids) |
| `migrations/00003_registry_sync.sql` | 25 | Check-state columns and the `registry_sync_runs` table |
| `configs/default.yaml` | 299 | `fb.registry_*`, `fb.browser`, `fb.query_breaker`, `registry_sync` defaults |
| `docs/runbooks/operations.md` | 265 | "Registry stale" (l.63-101) and "Browser fallback" (l.103-120) sections |

Code read for context only (other people cover it): `internal/fb/graphql/client.go` (how the registry is used per request), `internal/fb/session/bootstrap.go` (the browser trigger), `internal/fb/detect/detect.go` (stale and drift classes), `internal/platform/breaker/breaker.go`, `internal/app/crawler.go`, `internal/crawl/stage/{page,post,deps}.go`, `internal/api/admin.go`, `cmd/fbctl/commands.go`, `internal/spike/{registry,capture}.go`, `cmd/fbs-spike/main.go`, `internal/fakefb/{server,render}.go`, `deploy/docker/Dockerfile`, `deploy/helm/fbs/{values.yaml,templates/deployments.yaml,files/alerts.yaml}`, `internal/platform/metrics/metrics.go`, `migrations/00001_init.sql`.

---

## 1. The mental model in one page

Facebook's web client (Comet/Relay) calls one endpoint, `POST /api/graphql/`. Each operation is identified by a **persisted-query id (`doc_id`)** and a **friendly name** (the Relay operation name, e.g. `ProfileCometTimelineFeedRefetchQuery`). Variables travel as a JSON string. Facebook **rotates doc_ids with its deploys**, so fbs treats the mapping as data, not code:

```
                    configs/queries.seed.yaml  --(fbctl migrate: ApplySeed)-->  Postgres fb_queries / fb_client_params
                                                                                     ^              |
      PUT /v1/admin/queries/{name}, fbctl registry set  --(Upsert)------------------|              |  every fb.registry_refresh (30s)
                                                                                     |              v
  fbs-registry-sync (every 30m):                                                     |     crawler: registry.DB snapshot (in memory)
    load canary pages -> scan HTML + JS bundles (docids) -> [render page in Chromium |              |
    for names still missing] -> insert unknown ids as 'candidate' -> probe active +  |              v
    candidates with real requests (timeline -> comments -> replies) -> promote a  ---+     graphql.Client.Do(query name):
    passing candidate; old active -> 'fallback'; trim/retire                                Get(name) -> active doc_id
                                                                                            breaker(name|doc_id) open? -> Fallback()
                                                                                            response registry_stale? -> retry once with Fallback()
```

The headless-Chromium package (`internal/fb/browser`) does two jobs:
1. **Bootstrap fallback.** If a page's plain-HTTP HTML carries no tokens (it needs JavaScript first), the page is rendered in Chromium through the identity's own proxy and fingerprint. The cookies and tokens are then copied back into the HTTP identity.
2. **Discovery fallback.** When the bundle scan does not find a registry query's friendly name, registry-sync renders a canary page. It then reads `doc_id` and `fb_api_req_friendly_name` from the GraphQL requests the page itself sends.

---

## 2. The query registry model

### 2.1 What a "query" is

`registry.Query` (`internal/fb/registry/registry.go:31-46`):

| Field | JSON / column | Meaning |
|---|---|---|
| `Name` | `name` | **Logical** name used by the code: `timeline`, `comments`, `comments_newest`, `comments_relevant`, `replies`. Several logical names can share one friendly name and doc_id. They differ only in variables, for example the comment ordering. |
| `DocID` | `doc_id` | Facebook's persisted-query id, sent as form field `doc_id` (`graphql/client.go:275`). |
| `FriendlyName` | `friendly_name` | Relay operation name. It is sent twice: as form field `fb_api_req_friendly_name` (`client.go:272`) and as header `x-fb-friendly-name` (`client.go:160`). Discovery joins bundle declarations to registry entries on this field. |
| `Variables` | `variables_template` (jsonb) | JSON template. A string value of exactly `"{{name}}"` is replaced by the caller's value (section 2.5). |
| `Parser` | `parser` | `timeline`, `comments` or `replies`. It decides which **probe kind** registry-sync uses (`sync.go:539`) and which parser `fbctl replay` uses (`cmd/fbctl/replay.go:70`). |
| `ParserVersion` | `parser_version` | Stored and shown only. The version stamped on emitted records comes from compiled constants `parse.TimelineVersion` / `parse.CommentsVersion` (`internal/fb/parse/timeline.go:14`, `comments.go:13`; used in `stage/page.go:150`, `post.go:71`). Nothing dispatches on it. |
| `Status` | `status` | `candidate`, `active`, `fallback` or `retired` (`registry.go:23-29`). |
| `Priority` | `priority` | Tie-breaker among entries of one name (higher first). Every seed entry uses 10. Candidates copy their template's priority (`sync.go:496`). |
| `Source` | `source` | `seed`, `bundle`, `har` or `manual` (schema comment, `migrations/00001_init.sql:305`). Code writes `seed` (ApplySeed), `manual` (Upsert default and the admin API) and `bundle` (registry-sync, also for ids found by **rendering**). Nothing writes `har`: the spike's HAR import produces a seed YAML instead. |
| `LastCheck`, `LastCheckedAt`, `LastOKAt` | `last_check`, `last_checked_at`, `last_ok_at` | Check state written by registry-sync. They are read-only in the model and filled by `List` (`registry.go:42-45`, `305-318`). |

There is **no explicit version entity**. A query's history is the set of `(name, doc_id)` rows with their statuses and timestamps (`discovered_at`, `promoted_at`, `last_ok_at`, `updated_at`).

**Status semantics:**
- `active`: the entry `Get` returns. Normally exactly one per name. The schema does **not** enforce that (see 2.10).
- `fallback`: a former active entry (or a working spare). `Fallback()` uses it when the active doc_id is rejected or its breaker is open.
- `candidate`: discovered, not yet proven. **Crawlers never see candidates** (they are filtered out of the snapshot, `registry.go:166`).
- `retired`: dead. Crawlers never see it. Registry-sync can revive it (2.9).

### 2.2 Storage

**Postgres** (`migrations/00001_init.sql:296-317`):

```sql
CREATE TABLE fb_queries (
    name text NOT NULL, doc_id text NOT NULL, friendly_name text NOT NULL,
    variables_template jsonb NOT NULL, parser text NOT NULL, parser_version int NOT NULL,
    status text NOT NULL,            -- candidate | active | fallback | retired
    priority int NOT NULL DEFAULT 0,
    source text NOT NULL,            -- seed | bundle | har | manual
    discovered_at timestamptz NOT NULL DEFAULT now(), promoted_at timestamptz,
    last_ok_at timestamptz, updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, doc_id));
CREATE TABLE fb_client_params (key text PRIMARY KEY, value text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
```

`migrations/00003_registry_sync.sql` adds:
- `fb_queries.last_checked_at timestamptz`;
- `last_check text` (`ok | stale | drift | inconclusive: <reason>`);
- `check_failures int NOT NULL DEFAULT 0` (consecutive failed checks; lines 5-8);
- `registry_sync_runs (run_id identity PK, started_at, finished_at, status text /* ok|degraded|error */, report jsonb)`, with index `registry_sync_runs_started_idx (started_at DESC)` (lines 11-18).

**Cache:** each process keeps an immutable in-memory `snapshot` behind an `RWMutex` (`registry.go:156-161`, `214-219`). There is **no Redis or other shared cache**. Every crawler pod reads Postgres every `fb.registry_refresh` (default 30s; `configs/default.yaml:71`; `app/crawler.go:143`; `registry.DB.Run` at `registry.go:289-302`). A failed reload is logged (`registry: reload failed`) and the previous snapshot stays in use. `NewDB` does the first load synchronously and fails startup if it cannot (`registry.go:222-228`).

**Seed file location:** `fb.registry_seed` (`queries.seed.yaml`) is read from the **embedded** `configs.FS` (`app/crawler.go:72`, `app/registrysync.go:26`), not from disk. Changing the seed therefore means rebuilding the binary, or writing to the DB through the API.

### 2.3 Seed handling

- `ParseSeed` (`registry.go:85-107`) unmarshals the YAML. Each `variables` map is re-marshalled to JSON. The status defaults to `active` and the source is always `seed`.
- **Placeholders.** `placeholderDocID = ^9000000000000\d{3}$` (`registry.go:111`). The shipped seed only works against `fakefb`. `IsPlaceholderDocID` and `PlaceholderQueries` let the spike refuse to send placeholders to real Facebook (`internal/spike/registry.go:86-90`, `preflight.go:129`). The test asserts that **every** embedded seed doc_id is a placeholder (`registry_test.go:65-67`).
- `MarshalSeed` (`registry.go:129-154`) writes the seed back to YAML; `active` is omitted as the default. `fbs-spike docids|render|har --write-registry` uses it to produce a reviewed seed (`internal/spike/registry.go:51-59`). The round trip is lossless (`registry_test.go:56-94`).
- `ApplySeed` (`registry.go:322-344`) runs on `fbctl migrate` (`cmd/fbctl/commands.go:50-60`). It upserts each seed row as `source='seed'`, `promoted_at=now()`. **ON CONFLICT it updates only rows whose source is still `seed`** (`WHERE fb_queries.source = 'seed'`), and even then never changes the `status`. A seed therefore never overrides a doc_id that was discovered or set by hand.

### 2.4 Selection: `Get` and `Fallback`

`newSnapshot` (`registry.go:163-184`):
- drops `retired` and `candidate` rows;
- groups the rest by `Name`;
- sorts each group with a stable sort: **active first, then priority descending**.

The input order is `List`'s `ORDER BY name, priority DESC` (`registry.go:306-308`).

- `Get(name)` returns the first entry of the group, or `ErrNotFound` (`registry.go:186-192`).
- `Fallback(name, failedDocID)` returns the **first entry in the same order whose doc_id differs from the failed one** (`registry.go:194-201`).

Two implementations:
- `Static`: built from a `Seed`, never reloads. Used by tests and the spike, and by registry-sync's per-probe client (2.8).
- `DB`: Postgres, reloaded (`registry.go:203-302`).

`TestFallbackOrdering` (`registry_test.go:40-54`) pins the behaviour: an active entry with priority 1 beats a fallback with priority 50, and a candidate with priority 99 is invisible.

### 2.5 Variables templating: `Render`

`Render(tmpl, vars)` (`registry.go:379-418`):
- decodes the template with `UseNumber`;
- walks maps and arrays recursively;
- replaces every **string value that is exactly** `{{identifier}}` (regex `^\{\{(\w+)\}\}$`) with `vars[identifier]`, of whatever JSON type (number, string, null, object);
- writes **`null` for placeholders the caller left out**.

There is no interpolation inside longer strings, and keys are never templated. Example (`registry_test.go:23-37`): `comments` with `{feedback_id:"ZmVlZA==", cursor:"C1"}` renders `"id":"ZmVlZA=="`, `"commentsAfterCursor":"C1"`, `"commentsAfterCount":-1`. Without `cursor`, `commentsAfterCursor` is `null` (the first page).

Side effect: `json.Marshal` of a Go `map[string]any` sorts keys alphabetically. The `variables` JSON fbs sends therefore has sorted keys, not the browser's order. Nothing in the code suggests Facebook cares, but it is a small fingerprint difference.

### 2.6 Client params

`client_params` in the seed (`queries.seed.yaml:11-18`):

| Key | Value |
|---|---|
| `__a` | `"1"` |
| `__comet_req` | `"15"` |
| `dpr` | `"1"` |
| `__ccg` | `"EXCELLENT"` |
| `fb_api_caller_class` | `"RelayModern"` |
| `server_timestamps` | `"true"` |
| `x_asbd_id` | `"359341"` |

`DB.Reload` merges the seed params with the `fb_client_params` table; **the table wins key by key** (`registry.go:254-286`). The GraphQL client uses them as form fields (`client.go:240-278`), uses `x_asbd_id` as the `x-asbd-id` header (`client.go:161`), and lets a `__comet_req` token found in the HTML override the param (`client.go:241-244`). Registry-sync passes the live params to its probes (`app/registrysync.go:62`, `sync.go:680-684`).

### 2.7 Write paths

| Path | Code | Effect |
|---|---|---|
| `fbctl migrate` | `cmd/fbctl/commands.go:50-60` -> `ApplySeed` | Inserts seed rows; refreshes seed-sourced rows; never touches others' status |
| `PUT /v1/admin/queries/{name}` (scope admin) | `internal/api/admin.go:62-82`, route `server.go:73` | Requires `doc_id`, `friendly_name`, `parser`, `variables`; `parser_version` defaults to 1; source is forced to `manual`; calls `Upsert` |
| `fbctl registry set --file q.json` | `cmd/fbctl/commands.go:163-185` | `Upsert` with the source from the file, or `manual` |
| `GET /v1/admin/queries` | `admin.go:18-25` | `List` (all rows with check state) |
| `GET /v1/admin/registry-sync/runs?limit=N` (default 20, max 200) | `admin.go:28-59` | Recent run reports, newest first |
| `fbctl registry list` | `commands.go:149-161` | `List` as JSON |
| registry-sync | `sync.go` (2.9) | Inserts candidates, writes check state, promotes, demotes, retires |

`Upsert` (`registry.go:348-377`), in one transaction:
- When the new status is `active`, it first demotes every **other** `active` row of the name to `fallback`.
- It then inserts or updates the row. `promoted_at` is set only when the status is active (`COALESCE` keeps the old value otherwise).
- The `variables` must be valid JSON.

### 2.8 How a crawler request picks its doc_id (and the per-(query, doc_id) breaker)

Crawler wiring (`internal/app/crawler.go:109-118`): one `graphql.Client` per process with `Registry: registry.DB` and `Breakers: breaker.New(cfg.FB.QueryBreaker)`. On every state change the breaker sets `fbs_breaker_state{name="query:<name>|<doc_id>"}` (0 closed, 1 half-open, 2 open) and logs `graphql: query breaker`.

`Client.Do(ctx, id, Request{Query: name, Vars})` (`client.go:110-137`):
1. `q := Registry.Get(name)` (the active entry).
2. **Breaker gate**, keyed `name|doc_id` (`client.go:84`). If `Allow(name|activeDoc)` is false:
   - take `fb := Registry.Fallback(name, activeDoc)`;
   - if there is none, or the fallback's own breaker also denies, **short-circuit**: increment `fbs_fb_query_short_circuit_total{query}` and return a synthetic `Response{Class: SchemaDrift, Reason: "breaker open for … waiting for a registry or parser update"}` **without sending anything**;
   - otherwise use the fallback.
3. Send. `Render(q.Variables, req.Vars)` builds the variables; the form carries `fb_api_req_friendly_name`, `variables` and `doc_id` (`client.go:139-209`, `240-278`).
4. If the response class is `RegistryStale`, the client calls `Breakers.Observe(name|doc_id, failed=true)`. If a `Fallback` exists, it **retries once** with it and returns that result. This retry does not check the fallback's breaker.
5. After parsing, the stages call `GQL.Parsed(resp, rep.OK())` (`stage/page.go:184`, `post.go:112`, `post.go:396`). That records success or failure (drift) on `resp.Query`'s key, which is the fallback's key when the fallback was used (`client.go:90-102`). A drifted response is also captured raw under the failures prefix.
6. The stage maps `RegistryStale` and `SchemaDrift` (including the synthetic short-circuit) to **retry in 5 minutes** via the `5m` retry topic (`stage/deps.go:144-146`).

**What counts as "stale".** `detect.DefaultRules().StaleMessages` (`internal/fb/detect/detect.go:77-82`) matches any GraphQL error whose message or summary matches one of:
- `(?i)persisted ?query`
- `(?i)\bdoc_?id\b`
- `(?i)unknown (query|operation)`
- `(?i)variable .* (is|was) (not|never) (defined|provided)`

The check runs after rate-limit and login codes and before soft codes (`detect.go:127-142`). `fakefb` answers an unknown doc_id with code 1675012 `PersistedQueryNotFound: doc_id … is not a persisted query` (`internal/fakefb/server.go:305-307`).

**Breaker parameters** (`configs/default.yaml:146-156`; mechanics in `internal/platform/breaker/breaker.go`):

| Setting | Default |
|---|---|
| `window` | 5m (10 ring slots) |
| `min_events` | 20 |
| `open_rate` | 0.5 |
| `cooldown` | 2m, doubling on each failed probe |
| `max_cooldown` | 30m |
| `probe_successes` | 3 |

When half-open, one probe is admitted at a time (`breaker.go:184-200`).

The breakers are **in memory, per crawler process**, not fleet-wide.

**How the breaker and the registry fit together:**
- A stale doc_id opens `query:timeline|<old>` on each pod.
- Requests move to the fallback doc_id if there is one; otherwise tasks are parked for 5 minutes.
- When registry-sync promotes a new doc_id, crawlers pick it up within 30s. The **new key `timeline|<new>` starts closed**, so traffic resumes at once. The old key's breaker no longer matters (operations.md:65-80).
- Registry-sync's own probes use a client with **no breakers** (`sync.go:684`), so an open crawler breaker never stops validation.

### 2.9 Lifecycle state machine (who changes what, when)

| Transition | Who | Code | Condition |
|---|---|---|---|
| none -> `active` (seed) | `fbctl migrate` | `registry.go:322-344` | new `(name, doc_id)` in the seed |
| none -> `candidate` | registry-sync `recordCandidates` | `sync.go:474-517` | the discovered `found[friendly]` doc_id is unknown for that **name**; the row copies `variables_template`, `parser`, `parser_version` and `priority` from the name's template entry (first non-retired, i.e. the active one if any; `sync.go:458-468`); `source='bundle'` |
| `retired` -> `candidate` (revival) | registry-sync `recordCandidates` | `sync.go:498-500` | the same doc_id is discovered again, the row is `source='bundle'` and `retired`, and `last_checked_at` is null or older than `recheck_retired` (24h); resets `check_failures=0` |
| `candidate` -> `active` | registry-sync `decide` + `promote` | `sync.go:884-913`, `949-959` | candidate checked `ok` **and** `found[friendly_name] == its doc_id` (declared by the current bundles or seen in the render), even if the active entry is fine. Or, when the active entry is stale/drift/missing, any `ok` candidate |
| `fallback` -> `active` | registry-sync | `sync.go:890-899` | the active entry failed and no current candidate passed; an `ok` fallback is promoted |
| `active` -> `fallback` | registry-sync `promote`, `Upsert` | `sync.go:951-953`, `registry.go:359-364` | another entry of the name was made active |
| `candidate` -> `fallback` ("spare") | registry-sync | `sync.go:929-937` | a candidate passed but was not picked (not the currently declared id) |
| `candidate`/`fallback` -> `retired` | registry-sync `recordChecks` | `sync.go:834-843` | `stale` or `drift` on `retire_after` (3) **consecutive** conclusive checks. The **active entry is never retired**; only a promotion demotes it |
| `fallback` -> `retired` | registry-sync `trimFallbacks` | `sync.go:964-974` | more than `keep_fallbacks` (2) fallbacks; keeps those most recently known to work: `ORDER BY coalesce(last_ok_at, promoted_at) DESC NULLS LAST, updated_at DESC` |
| any -> any | `Upsert` (admin API, fbctl) | `registry.go:348-377` | manual |

Check-state bookkeeping (`recordChecks`, `sync.go:818-853`, one `pgx.Batch`):
- `ok`: sets `last_checked_at=now()`, `last_check='ok'`, `check_failures=0`, `last_ok_at=now()`.
- `stale` / `drift`: increments `check_failures` and may retire the entry.
- `inconclusive`: updates `last_checked_at` and `last_check` only. **Failures are not reset or incremented.**
- The note is `"<verdict>: <reason truncated to 200 chars>"`.

### 2.10 Caveats seen in the code (worth fixing in a rewrite)

1. **Fallback order in the crawler is not by recency.** Registry-sync orders fallbacks by `promoted_at DESC` (`sync.go:412-440`) and trims by `last_ok_at`. The crawler snapshot sorts only active-first, then priority (`registry.go:171-179`), over `ORDER BY name, priority DESC`. With equal priorities (always 10), *which* fallback `Fallback()` returns depends on Postgres row order. With `keep_fallbacks: 2` the crawler can try the older one first. Suggestion: order the snapshot by `coalesce(last_ok_at, promoted_at) DESC` too.
2. **More than one `active` row is possible.** `ApplySeed` inserts a new seed `(name, doc_id)` as `active` without demoting others (`registry.go:325-337`). If the seed's doc_id for a name changes after a promotion, two active rows exist. The snapshot then picks between them arbitrarily, and `decide` only considers `candidate`/`fallback` rows as replacements (`sync.go:892`). Suggestion: add a partial unique index `ON fb_queries(name) WHERE status='active'`, or demote inside `ApplySeed`.
3. The **friendly-name map is last-writer-wins.** If two bundles declare different ids for one friendly name, the later bundle in page order wins (`docids.go:113-130`), and `relayOpLoad` matches override `relayOp` matches within one blob (`docids.go:26-30`).
4. A run **skipped** because another instance holds the lock is not stored and not counted (`sync.go:201-205` returns before `finish`).
5. `comments` and its variants share one friendly name. Discovery therefore creates **one candidate per logical name**: `comments`, `comments_newest` and `comments_relevant` each get their own row and probe.

---

## 3. The seed queries (`configs/queries.seed.yaml`)

All five use placeholder doc_ids (`90000000000000NN`) that only `fakefb` accepts. Status defaults to `active`, priority 10, parser_version 1.

### 3.1 `timeline`: page posts

- **Friendly name** `ProfileCometTimelineFeedRefetchQuery`, placeholder doc `9000000000000001`, parser `timeline` (`queries.seed.yaml:21-45`).
- **Used by:**
  - the **page** crawl stage, `PageStage.Run` (`internal/crawl/stage/page.go:149-167`);
  - the registry-sync timeline probe (`sync.go:709-744`);
  - the Phase 0 spike.
- **Caller variables:**
  - `page_id`: the numeric page id as a string, put into `id`;
  - `count`: page size, `crawler.timeline_page_size` = 10 in the crawl and 5 in the probe;
  - `cursor`: `end_cursor` of the previous page; absent, so `null`, on the first page.
- **Fixed variables, as captured from the web client:**
  - `afterTime: null`, `beforeTime: null` (no time window);
  - `feedLocation: TIMELINE`, `feedbackSource: 0`, `focusCommentID: null`;
  - `memorializedSplitTimeFilter: null`, `omitPinnedPost: false` (pinned post included);
  - `postedBy: null`, `privacy: null`, `privacySelectorRenderLocation: COMET_STREAM`;
  - `renderLocation: timeline`, `scale: 1` (device pixel ratio), `stream_count: 1`;
  - `taggedInOnly: null`, `useDefaultActor: false`.
- **Request fields:** the `Referer` is the identity base URL (`page.go:167`), and the request carries the per-page rate key `PageID`.

### 3.2 `comments`: "All comments" ordering (full crawls)

- **Friendly name** `CommentsListComponentsPaginationQuery`, doc `9000000000000002`, parser `comments` (`queries.seed.yaml:47-64`).
- The ordering is set by `commentsIntentToken: RANKED_UNFILTERED_CHRONOLOGICAL_REPLIES_INTENT_V1`. The seed comment calls it "the most complete logged-out view".
- **Caller variables:**
  - `feedback_id` -> `id`. This is the post's Feedback node id: base64 of `feedback:<post_id>` (`internal/fb/parse/fields.go:129-131`), or the `FeedbackId` parsed from the timeline;
  - `cursor` -> `commentsAfterCursor`.
- **Fixed variables:**
  - `commentsAfterCount: -1` (fixed as captured; the config key `crawler.comments_page_size` = 50 exists in `config.go:230` but **is not wired** into any template);
  - `commentsBeforeCount: null`, `commentsBeforeCursor: null`;
  - `feedLocation: DEDICATED_COMMENTING_SURFACE`, `focusCommentID: null`, `scale: 1`, `useDefaultActor: false`.
- **Used by:** the **post** stage (`PostStage.Run`) for full crawls, and incremental crawls of a post without a comments watermark (`stage/post.go:64-71`). The `Referer` is the post permalink (`post.go:95-96`).

### 3.3 `comments_newest`: newest-first (incremental re-scrapes)

Same friendly name and doc_id as `comments`, but `commentsIntentToken: RECENT_ACTIVITY_INTENT_V1` (`queries.seed.yaml:66-83`). The post stage uses it when the task mode is `CRAWL_MODE_INCREMENTAL` and the post has a `CommentsWatermark`. It reads newest-first and stops at the watermark minus 1 minute (`post.go:66-69`). This is also the "post refresh" read, where an unchanged post costs one request (operations.md:205).

### 3.4 `comments_relevant`: "Most relevant" ordering (spike only)

Same friendly name and doc_id, `commentsIntentToken: RANKED_FILTERED_INTENT_V1` (`queries.seed.yaml:85-102`). Only the Phase 0 spike uses it, to compare orderings (`cmd/fbs-spike/main.go:109`, `internal/spike/spike.go:38`). Because it is in the registry, **registry-sync still probes it** like the other comment queries.

### 3.5 `replies`: reply threads

- **Friendly name** `Depth1CommentsListPaginationQuery`, doc `9000000000000003`, parser `replies` (`queries.seed.yaml:104-121`).
- **Caller variables:**
  - `feedback_id` -> `id`, the *comment's* feedback id (base64 `feedback:<post>_<comment>` per `parse/fields.go:133-135`);
  - `expansion_token` -> `expansionToken`, taken from the parent comment's `expansion_info.expansion_token` or `replies_connection.expansion_token` (`parse/comments.go:128-139`);
  - `cursor` -> `repliesAfterCursor`.
- **Fixed variables:** `clientKey: null`, `feedLocation: DEDICATED_COMMENTING_SURFACE`, `focusCommentID: null`, `repliesAfterCount: null`, `repliesBeforeCount: null`, `repliesBeforeCursor: null`, `scale: 1`, `useDefaultActor: false`.
- **Used by:** the **thread** stage, `ThreadStage.Run` (`stage/post.go:334-380`), and the registry-sync replies probe.

### 3.6 Stage ↔ query summary

| Crawl stage (`crawler.stages`) | Registry name(s) | Probe kind (`parser`) |
|---|---|---|
| `page` (`PageStage`) | `timeline` | `timeline` |
| `post` (`PostStage`) | `comments` (full), `comments_newest` (incremental with watermark) | `comments` |
| `thread` (`ThreadStage`) | `replies` | `replies` |
| spike only | `comments_relevant` | `comments` |

Each emitted record carries `QueryName`, `DocId` and `ParserVersion` for lineage (`internal/crawl/emit/emit.go:63`). The stage reads `DocId` once from `Registry.Get` at the start of the task (`page.go:149-150`, `post.go:70-71`, `post.go:360-361`), so a request that went to a fallback is still stamped with the active doc_id.

---

## 4. doc_id discovery and rotation

### 4.1 `internal/fb/docids`: finding ids in HTML and bundles

**Three regexes** (`docids.go:17-22`):

1. **Bundle URLs:**
   ```
   scriptSrc = (?:https?:)?(?:\\/\\/|//)[A-Za-z0-9.\-:]+(?:\\/|/)rsrc\.php(?:\\/|/)[^"'\s<>]+?\.js[^"'\s<>]*
   ```
   - Matches **absolute or protocol-relative** URLs whose first path segment is `rsrc.php` and that contain `.js`, anywhere in the HTML: in `<script src>`, and JSON-escaped (`https:\/\/…`) in inline data such as resource maps.
   - The trailing `[^"'\s<>]*` keeps query strings (`?_nc_x=1`).
   - Root-relative URLs (`/rsrc.php/...` with no host) are **not** matched.

2. **Relay operation modules:**
   ```
   relayOp = __d\("([A-Za-z0-9_]+)_facebookRelayOperation"[^{]*\{[^}]*?exports\s*=\s*"(\d+)"
   ```
   This is Facebook's module system (Haste `__d(name, deps, factory, flags)`). Each persisted operation is a tiny module named `<FriendlyName>_facebookRelayOperation` whose factory sets `exports` to the doc_id:
   ```js
   __d("ProfileCometTimelineFeedRefetchQuery_facebookRelayOperation",[],(function(a,b,c,d,e,f){e.exports="7316000000000001"}),null);
   ```
   - Group 1 is the friendly name and group 2 the doc_id.
   - `[^}]*?` stops at the first `}` of the factory body.
   - Any `…exports="digits"` matches, so both `e.exports` and `module.exports` work.
   - There is no minimum length on the id.

3. **Name/id pairs:**
   ```
   relayOpLoad = "([A-Za-z0-9_]+Query)"\s*,\s*\{?\s*"?(?:queryID|id)"?\s*:\s*"(\d{8,})"
   ```
   Matches `"<Name>Query",{"queryID":"<8+ digits>"}`, `"<Name>Query",{id:"…"}` and `"<Name>Query", "id":"…"`. **Only names ending in `Query`** match, so mutations and subscriptions do not.
   - **Not matched:** a Relay ConcreteRequest `params:{id:"…",metadata:{},name:"…Query",operationKind:"query",text:null}`, where `id` comes *before* `name`. The task brief mentioned that shape. The code does not handle it, and neither regex would match it.

`Scan(b, out)` (`docids.go:25-31`) runs `relayOp`, then `relayOpLoad`, over the blob and writes `out[friendly] = docID`. Later matches overwrite earlier ones.

`Bundles(html, pageURL)` (`docids.go:36-55`) works as follows:
- replaces every `\/` with `/` and resolves `//` against the page scheme;
- deduplicates;
- keeps only hosts that are **exactly `fbcdn.net`, end in `.fbcdn.net`, or equal the page's own host**. `evilfbcdn.net` and `evil.example` are rejected (`docids_test.go:23-37`);
- returns the URLs in page order.

`HTTPFetcher(hc)` (`docids.go:65-88`):
- sends a plain GET with **no headers**, so Go's default User-Agent;
- treats non-200 as an error;
- treats a body over `MaxBundleBytes` = 20 MiB (`docids.go:61`) as an error rather than truncating it.

`Scanner` (`docids.go:93-142`):
- `Page(ctx, html, pageURL, out)` scans the HTML itself, then each referenced bundle in page order.
- A cache `map[url]map[friendly]docID` remembers each bundle's ids. Bundle URLs are content-addressed and immutable, so each URL is fetched once per Scanner. Cached bundles cost nothing and do not count against `MaxBundles`.
- At most `MaxBundles` **new** bundles are fetched per `Page` call. Failed fetches still count against that limit, but are **not cached**, so they are retried on the next call (`docids_test.go:39-68`).
- `MaxCached` defaults to 4096. When it is reached, the whole cache is cleared (`clear(s.cache)`).
- Returns the number of bundles fetched.

How the callers use it:
- **Registry-sync** keeps **one Scanner for the life of the process** (`sync.go:254-256`) and sets `MaxBundles = registry_sync.max_bundles` (60) **per canary page** (`sync.go:258-261`). In a long-running process, later runs only fetch bundles that are new. With `--once` the process is fresh, so the cache is cold.
- **`fbs-spike docids`** (`internal/spike/capture.go:148-170`) uses a fresh Scanner with a **total** budget (`MaxBundles = maxBundles - n`, default 150 from `--max-bundles`, `cmd/fbs-spike/main.go:113`).

**Bundle egress:** `Sync.fetcher` (`sync.go:379-394`):
- **Default: direct egress** with `docids.HTTPFetcher(media.NewHTTPClient())` (`app/registrysync.go:65`). That client ignores `HTTPS_PROXY` and has a 5 min timeout (`internal/media/downloader.go:47-56`). The reasoning: bundles are public and immutable, and direct egress saves proxy bandwidth.
- With `registry_sync.bundles_via_proxy: true`: the bundle is fetched through the canary identity's TLS-fingerprinted `Doer` and exit, with headers `user-agent`, `accept: */*` and `accept-encoding: gzip, deflate, br, zstd`. This path does **not** apply the 20 MiB cap.

**What fakefb emits** (the shapes the tests pin): one bundle at a content-addressed path `/rsrc.php/v3/<fnv-hash>/relay.js`. The path changes whenever the accepted doc_ids change (`internal/fakefb/server.go:139-152`). The bundle contains one `__d("<Friendly>_facebookRelayOperation",[],(function(a,b,c,d,e,f){e.exports="<id>"}),null);` line per accepted friendly name (`server.go:686-707`). `OmitFromBundle` removes a name to simulate lazily loaded operations (`server.go:109-114`). `RotatedDocs` turns `9000000000000001` into `7316000000000001`, and so on (`server.go:709-726`).

### 4.2 `fbs-registry-sync`: one run, step by step

Entry point: `Sync.Run` (`sync.go:187-213`).

**0. Serialise.**
- Acquire a dedicated pool connection and take `pg_try_advisory_lock(hashtextextended('fbs:registry-sync', 0))`.
- If the lock is not free, return `Status: skipped` (not stored, not counted).
- Unlock in a `defer` using `context.WithoutCancel`, because the lock belongs to the connection.
- Helm runs one replica anyway (`values.yaml:227-233`).

**1. Load canaries** (`loadCanaries` / `loadCanary`, `sync.go:341-376`). For each `registry_sync.canary_pages` entry (vanity name or URL):
- normalise it (`task.NormalizeRef`);
- acquire an identity from the sync's **own** pool on tier `registry_sync.tier`. If that is empty, the tier is the first of the `registry_sync` routing class, falling back to the `default` routing (`app/registrysync.go:47-50`, `proxy.go:116-121`);
- bootstrap it on the page through `session.Loader.Bootstrap`. This is the same HTTP loader as the crawler and **includes the browser fallback** when `fb.browser.enabled` (`app/registrysync.go:67-74`);
- parse the page id from the HTML (`parse.Page`). "No page id in the page html" counts as a failure;
- record each page's outcome in `rep.Pages`.

The pool is sized `MaxPerTier = len(canary_pages)+2` (`app/registrysync.go:51-56`). Identities are released at the end of the run. If **no** canary loads, the run fails with `StatusError` ("no canary page could be loaded"). An empty `canary_pages` is also an error with a descriptive message (`sync.go:240-242`).

**2. Discover** (`sync.go:253-264`):
- Scan each loaded canary's HTML and bundles into a single `found` map (`friendly -> doc_id`).
- `rep.Operations = len(found)` counts every operation declared, not just registry ones.
- Gauge `fbs_registry_discovered_operations` = that count.

**3. Render the missing names** (`renderMissing`, `sync.go:307-338`). Only when the browser is enabled (`s.Render = Bootstrapper.Capture`).
- `missing` = friendly names of non-retired entries not in `found`.
- For each canary, while names are still missing: `Capture` renders the page (2 scrolls) and returns the GraphQL exchanges.
- Any exchange with a non-empty `DocID` whose `FriendlyName` is missing goes into `found` and `rep.Rendered`.
- Render errors are logged and skipped.

`rep.Discovered` then lists only the registry's friendly names that were found (`sync.go:271-276`).

**4. Record candidates** (`recordCandidates`, `sync.go:474-517`):
- For every logical name (sorted), take its template entry and look up `found[template.FriendlyName]`.
- Run `INSERT … SELECT … FROM fb_queries WHERE name=$1 AND doc_id=<template doc>` with status `candidate` and source `bundle`, copying the template's variables, parser and priority. On conflict, only revive a retired bundle row older than `recheck_retired`.
- `RETURNING true` means "added/revived"; no row means it is already known.
- `rep.Candidates` lists `name/doc_id`.
- `rep.NotFound` lists the friendly names that neither the bundles nor the render produced. Per the runbook this usually means **Facebook renamed the operation**.
- The entries are then re-read.

**5. Check** (`check`, `sync.go:531-667`): the probe chain. Entries are grouped by their template's `parser` into the kinds `timeline`, `comments` and `replies`, in that fixed order (`kinds`, `sync.go:520`), because each kind's output feeds the next kind's input:
- timeline -> posts with comments (feedback ids);
- comments -> reply threads (comment feedback id + expansion token);
- replies.

`want(name, e, needed)` (`sync.go:556-581`) decides whether to probe an entry now:
- never once it has a `stale`/`drift` verdict, and never if `retired`;
- `active` and `candidate` entries are probed until they have a conclusive verdict;
- `fallback` entries are probed **only** when the active entry of the name is `stale`/`drift` this run **and** no candidate passed;
- an entry that already passed is probed again on a later page only when its output is the input a later kind still `needed`. For example, if timeline passed on page 1 but comments were inconclusive there, the timeline is re-run on page 2 to get fresh posts.

Pages are visited in config order until `done()` (no kind pending) or the context ends. For each page:
- **timeline probe** (`probeTimeline`, `sync.go:709-744`): variables `{page_id, count: 5}`, first page.
  - `parse.Timeline` misses required fields -> `drift` ("missing required fields: …").
  - no posts -> `inconclusive`.
  - otherwise `ok`, plus posts with `comment_count > 0`, sorted busiest first ("likeliest to have reply threads").
- **comments probe** (`probeComments`, `sync.go:772-797`). If no timeline probe passed on this page, it reads up to 2 **stored** posts from the `posts` table (`page_id`, `comment_count_total > 0`, created in the last 30 days, busiest first; `storedPosts`, `sync.go:748-770`). It tries up to 2 posts until a verdict is conclusive.
  - `parse.Comments` drift -> `drift`.
  - no comments -> `inconclusive`.
  - otherwise `ok`, plus reply threads: first those with more replies than shown inline, then the rest.
- **replies probe** (`probeReplies`, `sync.go:799-813`): uses `threads[0]`, variables `{feedback_id, expansion_token}`.
  - drift -> `drift`.
  - no replies -> `inconclusive`.
  - otherwise `ok`.
- Without inputs: `inconclusive` with "no post with comments to read" or "no reply thread to read".

`set()` never overwrites a conclusive verdict. It increments `fbs_registry_checks_total{query, status, verdict}` each time it records one (`sync.go:546-553`).

**Probe client isolation** (`client`, `sync.go:676-686`). Each probe uses a fresh `graphql.Client` whose registry is a `Static` holding **only the entry under test**:
- `Name` is set to `"registry_sync:" + name` (`ProbeQueryPrefix`, `sync.go:672`);
- `Status` is forced to `active`;
- the client params are live;
- there are no breakers, no fleet `Limiter` (only the identity's own pacing, `fb.identity_rps` 0.33/s, burst 2) and no fallback.

So a failure is the entry's own and is not hidden by a fallback. The probes also show up in `fbs_fb_requests_total{query="registry_sync:<name>"}`, apart from crawl traffic. The `Referer` is the canary URL.

**Verdict mapping** (`do`, `sync.go:688-703`):

| Response | Verdict |
|---|---|
| transport error | `inconclusive` (the error text) |
| `detect.OK` | `ok` (then the parse check above) |
| `detect.RegistryStale` | `stale` |
| `detect.SchemaDrift` | `drift` |
| anything else (rate limit, login wall, checkpoint, soft error) | `inconclusive: <class>: <reason>` |

**6. Record checks:** see 2.9.

**7. Decide** (`decide`, `sync.go:859-947`). For each logical name:
- `active` = the first active entry.
- `activeBad` = no active entry, or its verdict is `stale`/`drift`.
- Pick 1: a `candidate` with `ok` **and** `found[friendly] == doc_id`, with reason *"declared by Facebook's current bundles and parsed on a canary page"*. **This promotes even when the active entry still works**, so the registry tracks Facebook's current id proactively.
- Pick 2, only if `activeBad`: the first `candidate` or `fallback` with `ok`, with reason *"the active doc_id failed; this one parsed on a canary page"*, or *"no active doc_id; …"*.

Outcomes:
- **promote** (if `registry_sync.promote`), in one transaction: demote other actives to `fallback`, set the pick `active` with `promoted_at=now()`. Increment `fbs_registry_promotions_total{query}`, set `fbs_registry_query_healthy{query}=1`, and log at WARN `registry-sync: promoted`.
- else the active entry is `ok`: healthy = 1.
- else (no active, or the active is conclusively bad): append to `rep.Failing`, healthy = 0.
- else (the active is inconclusive): append to `rep.Unchecked`. **The gauge keeps its last value.**
- Dry run (`promote: false`): the pick goes to `rep.Proposed` and statuses are not changed (`sync.go:923-928`).
- Housekeeping (not in a dry run): every other passing candidate becomes a `fallback` spare (`sync.go:929-937`), then `trimFallbacks` keeps `keep_fallbacks` (`sync.go:938-944`, `964-974`).

**8. Status and report** (`sync.go:297-301`, `215-237`):
- `ok` if nothing is failing or unchecked, otherwise `degraded`; `error` on DB failure or no canaries.
- `finish()`:
  - sets `RegistrySyncRuns{status}++`;
  - updates `fbs_registry_sync_last_success_timestamp_seconds` **only if `Report.Checked()`**, i.e. status ok or degraded **and** no unchecked queries (`sync.go:123-125`). A run where every probe was blocked "validates nothing" (`TestSyncAllBlockedIsNotASuccess`);
  - inserts the JSON report into `registry_sync_runs`;
  - logs at INFO if ok, WARN otherwise, with counts and lists.
- `InitMetrics` (`sync.go:170-180`) seeds the last-success gauge at startup from `max(finished_at)` of stored runs that were ok/degraded with an empty `unchecked`, so a restart does not look like a sync that stopped working.

**Report JSON** (`Report`, `sync.go:90-118`; stored and served by the admin API):

| Field | Content |
|---|---|
| `started_at`, `finished_at`, `status`, `error` | run summary |
| `pages[]` | `{page, ok, reason}` per canary |
| `discovered` | registry friendly names found -> doc_id |
| `operations` | number of operations found in bundles |
| `not_found[]` | registry friendly names nobody declared |
| `rendered` | friendly name -> doc_id found by rendering |
| `candidates[]` | `name/doc_id` recorded this run |
| `checks[]` | `{query, doc_id, status, verdict, reason, page}` |
| `promoted[]` | `{query, from, to, why}` |
| `proposed[]` | dry-run promotions |
| `retired[]` | `name/doc_id` retired |
| `failing[]` | queries whose active entry failed with nothing to promote |
| `unchecked[]` | queries no canary gave a verdict on |

### 4.3 Scheduling, binary, deployment

- `cmd/fbs-registry-sync/main.go`:
  - `fbs-registry-sync [--config f]` serves `/metrics`, `/healthz` and `/readyz` (Postgres ping check) on `http.ops_addr`, and runs `scheduler.Every("registry-sync", registry_sync.interval, sync)`. That **runs once at startup, then every interval** (`internal/scheduler/loops.go:31-44`).
  - Each sync gets `context.WithTimeout(registry_sync.timeout)` (10m), runs `reg.Reload` (fresh client params), then `Run`.
  - `--once` prints the indented JSON report and exits **1 on error, 3 when degraded, 0 otherwise** (`main.go:62-74`).
- Config validation requires `registry_sync.interval > 0` and `registry_sync.timeout > 0` (`internal/config/config.go:444-446`).
- Helm: deployment `registry-sync` with `browser: true`, 1 replica, requests `cpu 100m / mem 256Mi`, limit `1Gi` ("room for a headless Chromium") (`deploy/helm/fbs/values.yaml:224-233`). It needs the crawlers' egress: proxies, plus Facebook's CDN for bundles.
- Crawlers do not talk to registry-sync. They pick up promotions through their 30s reload (`app/crawler.go:143`). Parked tasks come back from the 5m retry topic. Tasks that exhausted retries go to the DLQ (`fbctl dlq replay --reason max_attempts`; operations.md:94-99).

### 4.4 Dry run

`registry_sync.promote: false`: candidates are still recorded and checks still written (check_failures and retirement still apply), but no promotion, spare demotion or trimming happens. The report lists `proposed`. Because the active entry stays stale, the run is `degraded` with those queries under `failing` (`TestSyncDryRunProposes`, `sync_test.go:225-238`).

### 4.5 Failure modes and what the system does

| Failure | Detection | Behaviour / operator action |
|---|---|---|
| doc_id rotated, new one in bundles | `stale` verdict on active; candidate `ok` | auto-promoted within one run (≤ 30m); crawlers follow within 30s |
| Operation lazily loaded (not in scanned bundles) | name in `not_found` | with `fb.browser.enabled`, `rendered` fills it **if the page load itself sends that query**. Usually only the timeline: a page load does not open comments (`phase0-spike.md:133`). Otherwise: HAR capture, then `PUT /v1/admin/queries/{name}` |
| Operation renamed | `not_found` + active `stale` | `failing`, `fbs_registry_query_healthy=0` -> alert `FbsRegistryQueryFailing`; manual HAR + PUT |
| Response shape changed (accepted, but required fields missing) | `drift` | `failing`; parser fix needed (operations.md "Schema drift") |
| Variables changed (Facebook says variable not provided) | classified **stale** by `StaleMessages` | candidates with copied (old) variables also fail -> `failing`; fix the variables manually (HAR import lists missing variables) |
| Canary blocked or rate-limited | all checks `inconclusive` | `degraded` with `unchecked`; last-success gauge does not advance -> `FbsRegistrySyncNotRunning` after 3h |
| Canary page dead or quiet | page load error / "no posts" / "no post with comments" | replace canaries; stored posts help the comment probes when the timeline fails |
| Bundle download fails | skipped, retried next run | none |
| Bundle > 20 MiB (direct fetch) | error, skipped | none (no metric) |
| Candidate never works | `stale`/`drift` x `retire_after` (3) | retired; revived for one re-check if rediscovered after 24h |
| Two instances | advisory lock | the second returns `skipped` |
| Chromium unavailable with `fb.browser.enabled` | `NewBootstrapper` -> `FindBinary` error | the binary refuses to start (`fb.browser is enabled: …`, `app/registrysync.go:69-71`) |

### 4.6 Request cost of a run (from the code, rough)

Per run, at steady state (no rotation), on the first canary that yields data:
- 1 HTML page load per canary, for all canaries;
- bundles: up to 60 new ones per canary, direct egress by default;
- 1 timeline request;
- 1–2 requests per comment variant (3 variants);
- 1 replies request.

That is roughly **5–8 GraphQL requests** if the first canary has data. After a rotation, roughly twice that (old and new id for each name). Probes are paced only by identity pacing (0.33 rps per identity, burst 2), not by the fleet limiter. With the browser enabled and names missing, add one Chromium render (2 scrolls) per canary until nothing is missing.

### 4.7 Manual and offline paths (Phase 0 spike)

- `fbs-spike docids --page p1,p2 [--max-bundles 150] --write-registry seed.yaml`: the same `docids.Scanner` scan, total budget 150, then `MergeDocIDs` applies ids **by friendly name to every query sharing it** (`internal/spike/registry.go:24-47`).
- `fbs-spike render --tier T --page P --write-registry seed.yaml`: one `Bootstrapper.Capture`. It prints `friendly_name`, `doc_id`, status and variable keys of every GraphQL request the page made, and exits "needs action" if registry names are missing (`cmd/fbs-spike/main.go:516-567`).
- `fbs-spike har --in session.har --write-registry …`: imports a manually recorded HAR. It writes small constant variables into templates (for example `__relay_internal__pv__*` provider flags Facebook requires), and lists added and removed variables (`phase0-spike.md:131-170`).
- `PUT /v1/admin/queries/{name}` or `fbctl registry set --file`.

---

## 5. The headless-Chromium fallback (`internal/fb/browser`)

### 5.1 When it triggers

**Bootstrap path.** `session.Loader.Load` (`internal/fb/session/bootstrap.go:73-145`) follows same-site redirects, classifies the HTML load with `detect` rules, and calls `Browser.Observe()` for **every classified HTML load** (`bootstrap.go:102-104`). It then extracts tokens with `htmlx.ExtractTokens`:
- a `_js_datr` value is copied into a `datr` cookie;
- an EU consent wall is accepted over HTTP.

If `!tok.Valid()` (valid means **an `LSD` token is present**, `internal/fb/htmlx/tokens.go:30`):
- `fbs_bootstrap_total{method="http",outcome="no_tokens"}` is incremented;
- **`Browser.Bootstrap(ctx, id, target)` is called** (`bootstrap.go:120-131`);
- if a render happened (non-nil `PageLoad`), its result is returned with the HTTP bytes added;
- otherwise (capped, busy, failed to run) the load is a `SoftError` with reason `"no tokens in html; browser: <err>"`.

The browser is never used for walls or blocks on the HTTP load: those return before token extraction.

The interface is `session.BrowserFallback { Observe(); Bootstrap(ctx, id, rawURL) (*PageLoad, error) }` (`bootstrap.go:36-39`). It is wired in the crawler (`app/crawler.go:121-127`) and in registry-sync (`app/registrysync.go:67-74`) when `fb.browser.enabled`.

**Discovery path.** `Bootstrapper.Capture` is used by registry-sync's `renderMissing` and by `fbs-spike render`.

### 5.2 `Bootstrapper`: limits and the bootstrap

Construction (`NewBootstrapper`, `bootstrap.go:48-63`):
- `FindBinary(cfg.Binary)`. A missing binary is an error.
- `sem` = buffered channel of `max(max_concurrent, 1)`.
- `Timeout` = `fb.browser.timeout`, or **45s** if ≤ 0.
- `credit = burst = max_concurrent` (a fresh pod can render its first pages right away).
- `Rules = detect.DefaultRules()`.

**Share cap (credit bucket).**
- `Observe()` adds `MaxShare` per HTML load, capped at `burst` (`bootstrap.go:66-70`).
- `take()` needs ≥ 1 credit, with a 1e-9 float tolerance (`bootstrap.go:72-80`).
- With defaults (`max_share 0.02`, `max_concurrent 2`): at most **1 browser bootstrap per 50 HTML loads**, burst 2.
- `TestBootstrapperShareCap` (`browser_test.go:168-202`) proves that 9 loads at 10% do not earn a render, 10 do, and that credit never exceeds the burst.

**Concurrency cap.**
- In `Bootstrap`, a non-blocking semaphore send; if it is full, the call returns `ErrBusy` at once (`bootstrap.go:90-95`). Note that **the credit was already taken** at that point and is not refunded.
- `Capture` **blocks** on the semaphore until there is a slot or the context ends (`bootstrap.go:173-178`), and **ignores the share cap**.
- The design rule: a capped bootstrap fails immediately rather than hold a crawl slot. The task retries (operations.md:112).

**`Bootstrap(ctx, id, rawURL)`** (`bootstrap.go:85-150`):
1. `take()`, else `fbs_bootstrap_total{method="browser",outcome="capped"}` and `ErrCapped`. Then the semaphore, else `outcome="busy"` and `ErrBusy`.
2. `id.Wait(ctx)`: the render counts as **one page load for the identity's pacing**; the subrequests are the page's own. This wait happens *before* the render timeout starts.
3. `context.WithTimeout(b.Timeout)`.
4. `options(id, rawURL)` (`bootstrap.go:154-166`) builds the render options from the identity:
   - URL;
   - **User-Agent** `id.FP.UserAgent`;
   - Accept-Language;
   - **all current cookies** of the identity (e.g. `datr` from the HTTP load);
   - `NoSandbox`, `BlockMedia: true`;
   - **client hints** parsed from the fingerprint's `sec-ch-ua`, `sec-ch-ua-platform` and `sec-ch-ua-mobile` (`HintsFrom`, `render.go:89-98`);
   - **Proxy** = `id.Endpoint.URL` (the identity's sticky exit, including credentials). Direct if none.
5. For a bootstrap: `Capture = func(string) bool { return false }` (no response bodies kept), `Idle = 300ms`, no scrolls.
6. `render(...)`. An error gives `outcome="error"`. The wire bytes are added to `fbs_proxy_bytes_total{tier, "in"}`.
7. **Classify like an HTTP load** with `Expect: "html"`. Additionally, if the final URL's path is `/login…` it is a `LoginWall`; if `/checkpoint…` it is a `Checkpoint` (`session.IsWall`). On a non-OK class:
   - `outcome=<class name>`;
   - `session.Outcome(id, cls)`, so the identity's health and the proxy-bucket breaker see it;
   - return the `PageLoad` with an error.
   A login wall that carries an LSD token is therefore **not** taken for a bootstrap (`TestBootstrapRefusesWalls`).
8. On OK: copy **every browser cookie** into the identity's HTTP cookie jar (set on `id.Base`, whatever the cookie's domain). Set `datr` from `_js_datr` if missing. Extract tokens from the **rendered DOM**:
   - no LSD: `outcome="no_tokens"`, identity `SoftError`, error;
   - LSD present: `id.SetTokens(tok)`, `Outcome OK`, `outcome="ok"`.

   The plain HTTP client then continues as usual with those cookies and tokens.

### 5.3 `Capture` (doc_id discovery)

`Capture(ctx, id, rawURL)` (`bootstrap.go:172-192`):
- uses the same identity options, but `Scrolls = 2` and the default `Capture = IsGraphQL` (URL path starts with `/api/graphql`; `render.go:149-152`), `Idle 500ms` and `MaxSettle 5s`;
- does not call `id.Wait` and ignores the share cap;
- shares the semaphore;
- returns `[]Exchange`.

Each `Exchange` (`render.go:122-133`) contains:
- `URL`, `Method`;
- the parsed POST `Form`;
- `FriendlyName` = `form["fb_api_req_friendly_name"]`;
- `DocID` = `form["doc_id"]`;
- `Variables` = `form["variables"]`, if it is valid JSON;
- response `Status`, `Headers` and `Body` (base64-decoded if needed).

### 5.4 `Render`: how Chromium is driven (direct CDP, no library)

There is **no Chromedp, Rod or Playwright library**. The package has its own minimal Chrome DevTools Protocol client over `golang.org/x/net/websocket` (`cdp.go:20-147`), and runs **one fresh Chromium process per render**: a fresh profile and its own proxy, which keeps identities apart (`cdp.go:20-23`).

**Finding the binary** (`FindBinary`, `render.go:25-40`), in order:
1. `fb.browser.binary`;
2. `$FBS_BROWSER_BINARY`;
3. on PATH: `chromium`, `chromium-browser`, `google-chrome`, `google-chrome-stable`, `chrome`;
4. `/opt/pw-browsers/chromium` (Playwright's build).

**Before launch** (`render.go:155-191`):
- Defaults: `Capture = IsGraphQL`, `Idle = 500ms`, `MaxSettle = 5s`.
- Arguments: `--user-agent=<UA>` and `--lang=<first Accept-Language token>`.
- Proxy:
  - **SOCKS with credentials is refused before launch**: "Chromium does not support SOCKS proxies with credentials; use an HTTP exit for this tier" (`TestSocksProxyWithCredentialsIsRefused`);
  - otherwise `--proxy-server=<scheme>://<host:port>` (credentials stripped and answered on the challenge later) and `--proxy-bypass-list=<-loopback>`;
  - with no proxy, `--no-proxy-server` ("direct means direct", never a proxy from the environment).

**Launch** (`launch`, `cdp.go:193-273`):
- Profile `os.MkdirTemp("", "fbs-browser-")`.
- Flags:
  ```
  --headless=new --remote-debugging-port=0 --user-data-dir=<profile>
  --remote-allow-origins=http://127.0.0.1
  --no-first-run --no-default-browser-check --disable-gpu --disable-extensions
  --disable-background-networking --disable-sync --disable-dev-shm-usage
  --mute-audio --hide-scrollbars --window-size=1366,900
  --disable-component-update --disable-domain-reliability --disable-client-side-phishing-detection
  --disable-default-apps --disable-breakpad --no-pings --metrics-recording-only
  --disable-field-trial-config --disable-features=OptimizationHints,MediaRouter,Translate,AutofillServerCommunication
  --safebrowsing-disable-auto-update
  [--no-sandbox]  (if fb.browser.no_sandbox, running as root, or $FBS_BROWSER_NO_SANDBOX set)
  <render args> about:blank
  ```
  The "no own traffic" flags exist because anything Chromium sends goes through the identity's **paid exit** (`cdp.go:205-210`).
- Process hygiene:
  - `Setpgid: true`, so the process gets its own group;
  - `cmd.Cancel` sends SIGKILL to the whole group (`-pid`);
  - `cmd.WaitDelay = 2s`, so a helper that left the group while holding stderr cannot block `Wait` (`TestLaunchDoesNotWaitForEscapedHelpers`);
  - stderr goes to a 4 KiB ring buffer (`tail`) for error messages.
- **Start budget.** The launcher polls `<profile>/DevToolsActivePort` every 50ms until it has two lines (port and browser target path). That gives `ws://127.0.0.1:<port><path>`. The wait is bounded by **`launchTimeout = 45 * time.Second`** (`cdp.go:184-189`; a comment says 20s was not always enough on a cold CI runner building font caches), **and** by the caller's context (`fb.browser.timeout`, also 45s by default, which bounds the whole render, launch included).
- Error reporting:
  - early exit -> "chromium exited before opening its DevTools port (exit status N): <stderr>";
  - clean exit -> "(exit status 0)";
  - context expiry -> "launch: context deadline exceeded: <stderr>";
  - deadline -> "did not open its DevTools port within 45s: <stderr>".
  These are pinned by `launch_test.go`.
- `stop()`: SIGKILL the group, wait, `RemoveAll(profile)`.

**DevTools connection** (`dial`, `cdp.go:57-70`):
- websocket with Origin `http://127.0.0.1`, which must match `--remote-allow-origins`;
- `MaxPayloadBytes = 64 MiB`;
- a reader goroutine routes responses by `id`;
- events go to a 4096-slot channel and are **dropped when it is full**, so command responses never block.

**CDP command sequence** (`render.go:203-309`):
1. `Target.createTarget {url: about:blank}`, then `Target.attachToTarget {targetId, flatten: true}`, which gives `sessionId`.
2. If there are proxy credentials **or** `BlockMedia`: `Fetch.enable {handleAuthRequests: <has creds>, patterns: [{urlPattern: "*"}]}`. Every request then pauses.
3. If there is a UA and client hints: `Network.setUserAgentOverride {userAgent, acceptLanguage, platform: navigatorPlatform(...), userAgentMetadata}`.
   - `navigatorPlatform`: Windows -> `Win32`, macOS -> `MacIntel`, Android -> `Linux armv8l`, else `Linux x86_64`.
   - `userAgentMetadata`: `{brands, platform, platformVersion: "", architecture: "x86", model: "", mobile}`.
4. `Network.enable`, `Page.enable`, `Runtime.enable`.
5. `Network.setExtraHTTPHeaders {Accept-Language}`.
6. `Network.setCookies` with each identity cookie as `{name, value, url: opts.URL}`.
7. `navigate`: `Page.navigate {url}` in a goroutine. Waits until `Page.loadEventFired` **and** the navigate call returned. A non-empty `errorText` (DNS, proxy, TLS) fails at once.
8. `settle`: waits until **no captured request is in flight and none started for `Idle`**, or `MaxSettle` passed. Pages that keep the network busy with beacons still settle (`TestRenderBlocksMediaAndSettles`). Only *captured* requests are tracked, so for a bootstrap (nothing captured) settle is just a 300ms pause after load.
9. `Scrolls` times: `Runtime.evaluate("window.scrollTo(0, document.body.scrollHeight)")`, then settle.
10. Results:
    - main-frame document `url` and `status` from the last `Network.responseReceived` of type `Document` for the frame;
    - DOM via `Runtime.evaluate("document.documentElement.outerHTML", returnByValue)`;
    - cookies via `Network.getCookies {urls: [opts.URL, finalURL]}`;
    - for each captured request with a response, the body via `Network.getResponseBody`.
11. `Browser.close`; deferred websocket close and `proc.stop()`.

**Event handling** (`handle`, `render.go:408-517`):
- `Fetch.requestPaused`:
  - with `BlockMedia`, `resourceType` `Image`, `Media` or `Font` -> `Fetch.failRequest {errorReason: BlockedByClient}`;
  - everything else -> `Fetch.continueRequest`.
  - Both run asynchronously.
- `Fetch.authRequired`:
  - `source == "Proxy"` -> `ProvideCredentials` with the stripped user and password, **at most 2 times per requestId** (parallel connections each get their first challenge). A third challenge sets `authFailed` and the render fails with "proxy rejected the credentials". Wrong credentials therefore fail fast instead of looping (`TestRenderThroughAnAuthenticatedProxy`).
  - A server (non-proxy) auth challenge -> `CancelAuth`.
- `Network.requestWillBeSent` for captured URLs:
  - record method and URL;
  - read `postData`, or fetch it with `Network.getRequestPostData` when `hasPostData` is set without inline data;
  - parse it as `application/x-www-form-urlencoded` and pick `fb_api_req_friendly_name`, `doc_id` and `variables`;
  - track it as in-flight.
  - Only **POST bodies** are parsed; doc_ids sent in a GET query string would not be picked up.
- `Network.responseReceived`: record the status and headers of captured requests. `Document` responses are stored per frame.
- `Network.loadingFinished` / `loadingFailed`: add `encodedDataLength` to `wire` for **all** requests (proxy-billed bytes) and clear in-flight.

`Result` (`render.go:136-146`): `URL` (after redirects), `Status`, `HTML` (DOM after scripts), `Cookies`, `Captured`, `Bytes` (wire, compressed), `Elapsed`.

What the tests prove (`browser_test.go`):
- `TestRenderCapturesThePagesOwnGraphQL`: the fakefb page with `JSOnlyTokens` has no tokens over plain HTTP. After rendering, the DOM carries a valid LSD, and exactly one GraphQL exchange (`ProfileCometTimelineFeedRefetchQuery`, the right doc_id, status 200, a body containing `timeline`, variables containing `count`) is captured.
- `TestBootstrapFallback`: without the browser the bootstrap fails (`ErrBootstrap`). With the browser (`MaxConcurrent 1`, `MaxShare 0.02`, `Timeout 1m`) the identity is bootstrapped and a `timeline` GraphQL request with the browser tokens succeeds.
- `TestRenderBlocksMediaAndSettles`: 0 image fetches, ≥ 5000 wire bytes counted, done in < 30s despite a 50ms beacon loop, and **no Chromium processes left** (scanned via `/proc`).

### 5.5 Image, process reaping, deployment

- **Image**: `docker build --target browser` (`deploy/docker/Dockerfile:13-26`):
  - `debian:12-slim` with `chromium`, `ca-certificates`, `fonts-liberation` and `tini`;
  - the same Go binaries;
  - `ENV FBS_BROWSER_BINARY=/usr/bin/chromium HOME=/tmp`;
  - user 65532;
  - `ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/fbs-crawler"]`; tini reaps Chromium orphans under `docker run`.
  
  The default image is distroless static (`Dockerfile:28-33`) and has no browser.
- **Helm** (`deploy/helm/fbs/values.yaml:5-8, 37-41`; `templates/deployments.yaml:31-39`):
  - `image.browserTag` selects the browser image for components marked `browser: true` (crawler-page, crawler-post, crawler-thread, registry-sync);
  - those pods get `shareProcessNamespace: true`, so the pause process (PID 1) reaps orphans. The pod `command` bypasses tini;
  - the chart sets `config.fb.browser.no_sandbox: true` because pods drop every capability, which leaves Chromium's sandbox unusable (operations.md:114);
  - `fb.browser.enabled` is false by default in both `default.yaml` and the chart.
- **Sizing note** in the runbook: a fakefb render took about a second in development; size real pages under load first, and give crawler pods the CPU and memory for `max_concurrent` browsers (operations.md:119).

---

## 6. Configuration knobs and constants

Override any key with `--config file` or environment variables `FBS_<SECTION>__<KEY>`, for example `FBS_REGISTRY_SYNC__PROMOTE=false` or `FBS_FB__BROWSER__ENABLED=true` (`configs/default.yaml:1-3`).

### 6.1 `fb.*` (registry and browser related)

| Key | Default | Where used | Meaning |
|---|---|---|---|
| `fb.registry_seed` | `queries.seed.yaml` | `app/crawler.go:72`, `app/registrysync.go:26`, `fbctl migrate` | seed file inside the embedded `configs` FS |
| `fb.registry_refresh` | `30s` | `app/crawler.go:143` | crawler registry reload interval |
| `fb.locale` | `en_US` | GraphQL form `locale` (probes too) | |
| `fb.accept_language` | `en-US,en;q=0.9` | loader, GraphQL, browser `--lang` + header | |
| `fb.identity_rps` / `identity_burst` | `0.33` / `2` | identity pacing (also probes and browser bootstraps) | |
| `fb.request_timeout` | `30s` | identity transport | |
| `fb.query_breaker.window` | `5m` | per `(query, doc_id)` breaker | |
| `fb.query_breaker.min_events` | `20` | | |
| `fb.query_breaker.open_rate` | `0.5` | | |
| `fb.query_breaker.cooldown` | `2m` | doubles per failed probe | |
| `fb.query_breaker.max_cooldown` | `30m` | | |
| `fb.query_breaker.probe_successes` | `3` | | |
| `fb.browser.enabled` | `false` | crawler + registry-sync wiring | |
| `fb.browser.binary` | `""` | `FindBinary` | `""` -> `$FBS_BROWSER_BINARY`, PATH, `/opt/pw-browsers/chromium` |
| `fb.browser.max_concurrent` | `2` | semaphore and credit burst, **per process** | |
| `fb.browser.max_share` | `0.02` | credit per observed HTML load | ≤ 2% of HTML loads |
| `fb.browser.timeout` | `45s` (also the code default if ≤ 0) | whole render budget, launch included | |
| `fb.browser.no_sandbox` | `false` (chart: `true`) | `--no-sandbox` | always on as root or with `$FBS_BROWSER_NO_SANDBOX` |
| `fb.proxy.routing.registry_sync` | (unset) | `TiersFor("registry_sync")` | falls back to `routing.default` |

### 6.2 `registry_sync.*` (`configs/default.yaml:284-299`; struct `internal/config/config.go:40-66`)

| Key | Default | Meaning |
|---|---|---|
| `interval` | `30m` | run period (first run at startup); must be > 0 |
| `canary_pages` | `[]` | vanity names or URLs; **must be set** (empty is an error); "set at least three in production", pages that post daily with comments and reply threads (`values.yaml:45-48`) |
| `tier` | `""` | proxy tier; `""` = first tier routed for `registry_sync` (else default) |
| `max_bundles` | `60` | new bundles fetched per canary page per run |
| `bundles_via_proxy` | `false` | fetch bundles through the canary identity's exit instead of direct egress |
| `promote` | `true` | `false` = dry run (`proposed`) |
| `keep_fallbacks` | `2` | fallbacks kept per name |
| `retire_after` | `3` | consecutive stale/drift checks that retire a candidate or fallback (0 disables) |
| `recheck_retired` | `24h` | a retired bundle candidate discovered again is re-checked after this long |
| `timeout` | `10m` | one run's budget; must be > 0 |

### 6.3 Constants

| Constant | Value | File |
|---|---|---|
| Placeholder doc_id regex | `^9000000000000\d{3}$` | `registry.go:111` |
| Template placeholder regex | `^\{\{(\w+)\}\}$` | `registry.go:379` |
| `docids.MaxBundleBytes` | 20 MiB | `docids.go:61` |
| Scanner `MaxCached` default | 4096 bundles (then cleared) | `docids.go:132-135` |
| `relayOpLoad` id min length | 8 digits | `docids.go:21` |
| Bundle hosts allowed | `fbcdn.net`, `*.fbcdn.net`, page host | `docids.go:48` |
| `registrysync.lockKey` | `fbs:registry-sync` (advisory lock via `hashtextextended`) | `sync.go:183` |
| `registrysync.ProbeQueryPrefix` | `registry_sync:` | `sync.go:672` |
| Probe timeline `count` | 5 | `sync.go:710` |
| Comment posts tried per entry per page | 2 | `sync.go:641-649` |
| Stored-post fallback | `comment_count_total > 0`, last 30 days, top 2 | `sync.go:749-751` |
| Check note truncation | 200 chars | `sync.go:827` |
| Probe kinds order | `timeline`, `comments`, `replies` | `sync.go:520` |
| Registry-sync identity pool | `len(canary_pages) + 2` per tier | `app/registrysync.go:55` |
| `launchTimeout` | 45 s | `cdp.go:189` |
| DevToolsActivePort poll | 50 ms | `cdp.go:271` |
| `cmd.WaitDelay` | 2 s | `cdp.go:226` |
| stderr tail | 4 KiB | `cdp.go:166` |
| CDP max payload | 64 MiB | `cdp.go:66` |
| CDP event buffer | 4096 (drop when full) | `cdp.go:67` |
| DevTools origin | `http://127.0.0.1` | `cdp.go:25` |
| Window size | 1366x900 | `cdp.go:204` |
| Render `Idle` / `MaxSettle` defaults | 500 ms / 5 s | `render.go:160-165` |
| Bootstrap `Idle` | 300 ms, no capture, no scroll | `bootstrap.go:108-109` |
| Capture scrolls | 2 | `bootstrap.go:185` |
| Proxy auth retries per request | 2 | `render.go:438` |
| Event loop tick | 20 ms | `render.go:369` |
| UA metadata | `architecture: "x86"`, `platformVersion: ""`, `model: ""` | `render.go:227-228` |
| Bootstrap default timeout | 45 s | `bootstrap.go:55-57` |
| Stale/drift retry delay in stages | 5 m | `stage/deps.go:144-146` |

---

## 7. Metrics, alerts, dashboards

All metrics are prefixed `fbs_` (`internal/platform/metrics/metrics.go`).

| Metric | Type, labels | Set by |
|---|---|---|
| `fbs_registry_sync_runs_total` | counter `{status}` (ok, degraded, error; skipped not counted) | `sync.go:220` |
| `fbs_registry_sync_last_success_timestamp_seconds` | gauge; only runs with every query checked | `sync.go:221-223`, `177` |
| `fbs_registry_checks_total` | counter `{query, status, verdict}` | `sync.go:552` |
| `fbs_registry_promotions_total` | counter `{query}` | `sync.go:911` |
| `fbs_registry_query_healthy` | gauge `{query}`: 1 ok or promoted, 0 failing, unchanged when inconclusive | `sync.go:912-918` |
| `fbs_registry_discovered_operations` | gauge, operations in bundles last run | `sync.go:264` |
| `fbs_fb_requests_total` | counter `{query, class, tier}`; probes under `query="registry_sync:<name>"`, HTML loads under `query="page_html"` | `client.go:203`, `session/bootstrap.go:101` |
| `fbs_breaker_state` | gauge `{name="query:<name>\|<doc_id>"}` 0/1/2 | `app/crawler.go:115-117` |
| `fbs_fb_query_short_circuit_total` | counter `{query}` | `client.go:118` |
| `fbs_bootstrap_total` | counter `{method="http"\|"browser", outcome}`; browser outcomes: `capped`, `busy`, `error`, `ok`, `no_tokens`, or a class name (`login_wall`, `checkpoint`, `rate_limited`, `soft_error`, ...); http: `ok`, `no_tokens`, `consent` | `bootstrap.go`, `session/bootstrap.go` |
| `fbs_proxy_bytes_total` | `{tier, direction}`; browser render wire bytes all counted as `in` | `bootstrap.go:115`, `190` |

Alerts (`deploy/helm/fbs/files/alerts.yaml`):

| Alert | Expression | `for` | Severity |
|---|---|---|---|
| Recording rule `fbs:registry_stale_ratio:rate10m` (l.30-33) | per query, `registry_stale` share of requests, **excluding `registry_sync:*`** | | |
| `FbsRegistryStale` (l.137-146) | that ratio > 0.2 ("registry-sync has not promoted a replacement within a run or two") | 45m | warning |
| `FbsRegistryQueryFailing` (l.147-157) | `min by (query) (fbs_registry_query_healthy) == 0` | 5m | critical |
| `FbsRegistrySyncNotRunning` (l.158-173) | `time() - max(last_success) > 3h and count(fbs_registry_sync_runs_total) > 0` (every binary exports the gauge at 0; only the sync has the counter) | 10m | warning |
| `FbsQueryBreakerOpen` (l.91-101) | `max by (name) (fbs_breaker_state{name=~"query:.*"}) == 2` | 15m | warning |
| `FbsBrowserFallbackSaturated` (l.113-123) | `sum(rate(fbs_bootstrap_total{method="browser",outcome=~"capped\|busy"}[15m])) > 0.05` | 30m | warning |
| `FbsBrowserFallbackFailing` (l.124-133) | `(error\|no_tokens) / (ok\|error\|no_tokens)` over 30m > 0.5 | 30m | warning |

All of these link to operations.md "Registry stale" or "Browser fallback".

Dashboard **fbs / Overview** (`deploy/helm/fbs/files/dashboards/fbs-overview.json`) has panels "Registry checks", "Registry query health", "Failing registry queries", "Bootstraps", "Breakers (2 = open)" and "Open breakers". The runbook's key-signals table lists the registry signals and "healthy" targets: all 1, last success < 1h ago (operations.md:18).

---

## 8. Tests as executable specification

These tests are a useful acceptance template for the Instagram port.

- **Registry** (`registry_test.go`): the seed has every query; render fills and nulls placeholders; active beats priority; candidates are hidden; YAML round trip; all embedded doc_ids are placeholders.
- **docids** (`docids_test.go`): both module shapes are scanned; bundle host allow-list including JSON-escaped URLs and protocol-relative URLs; fetch once, retry failures, respect `MaxBundles`; 404 is an error.
- **registrysync** (`sync_test.go`). They use real Postgres (`testenv.Postgres`), fakefb with 2 pages, and `app.NewRegistrySync`.
  - `TestSyncFollowsRotation`:
    - steady state: all `ok`, nothing promoted;
    - after rotation, **one run** discovers, checks and promotes all 5 names (old ids `stale`, new ids `ok`; rows new=active, old=fallback);
    - after a reload, `Get` returns the new id and `Fallback` the old one;
    - probe traffic is counted under `registry_sync:timeline` with class `registry_stale`;
    - `Discovered` has 3 friendly names;
    - a third run is steady;
    - 3 `ok` rows exist in `registry_sync_runs`.
  - `TestSyncDryRunProposes`: degraded, 5 proposed, 5 failing, nothing promoted.
  - `TestSyncReportsDrift`: `DriftRequired` fault. The timeline is `drift`, while comments and replies are still `ok` via stored posts; status degraded, `failing=[timeline]`.
  - `TestSyncRetiresFailingCandidates`: `retire_after=2`; the first stale check keeps the candidate, the second retires it; status ok.
  - `TestSyncKeepsWorkingSpare`: `keep_fallbacks=1`; a working non-declared candidate becomes a fallback and the 10-day-old fallback is retired.
  - `TestSyncAllBlockedIsNotASuccess`: `RateLimitEvery=1`. Degraded, all unchecked, the last-success gauge does not advance, and `InitMetrics` does not count it.
  - `TestSyncSkipsWhileAnotherRunHoldsTheLock`.
  - `TestSyncRendersOperationsTheBundlesOmit` (needs Chromium): the timeline is omitted from the bundle; render discovery finds it; it is promoted; `not_found` is empty; status ok.
- **e2e**:
  - `TestRegistrySyncHealsRotation`: rotation, sync with one canary, crawler reload, admin endpoints show the run and `last_check=ok` / `last_ok_at` for active rows; a **full crawl then collects exactly the true comment count**.
  - `TestBrowserBootstrapCrawl`: JS-only tokens with `max_share=1`, `max_concurrent=4`; the job completes with all comments and at least 1 `fbs_bootstrap_total{method="browser",outcome="ok"}`.
- **browser**: section 5.4, plus `launch_test.go` (early exit with stderr, clean exit vs cancel, escaped helper does not hang).

---

## 9. Instagram adaptation notes

> Everything here is a **suggestion**. The repository has no Instagram code or data. The Instagram details below (Relay/GraphQL with doc_ids in bundles on `static.cdninstagram.com`, `Polaris…Query`-style friendly names, the `x-ig-app-id` header) come from the task brief and must be **verified with a Phase-0-style HAR capture of the logged-out Instagram web app** before anything is built on them.

### 9.1 Component classification

**(a) Generic and reusable as is** (rename `fb_` prefixes and metric names):

| Component | Why it ports |
|---|---|
| Registry model and storage (`Query` fields, statuses, `(name, doc_id)` PK, `*_queries` / `*_client_params` tables, `registry_sync_runs`, `last_check*`/`check_failures` columns) | Pure data model; nothing Facebook-specific except table names |
| `registry.DB` snapshot, reload loop, `Get`/`Fallback`, `Static` | Generic |
| `Render` variables templating (`{{name}}` -> typed value, missing -> null) | Generic; works for any Relay operation |
| `ApplySeed` / `Upsert` / `MarshalSeed` semantics (seed never overrides discovered/manual) | Generic (but see caveats 2.10 #1-#2) |
| `platform/breaker` and the per-`(query, doc_id)` gating plus single fallback retry in the client | Generic logic (the client itself is someone else's scope) |
| `docids.Scanner` mechanics (content-addressed cache, `MaxBundles`, retry failed fetches, 20 MiB cap, `HTTPFetcher`) | Generic; only the regexes and host filter are Facebook-specific |
| Registry-sync framework: advisory lock, candidate recording by friendly name, verdict model (`ok/stale/drift/inconclusive`), "conclusive verdicts are never overwritten", retire after N, revive after `recheck_retired`, `decide` (promote current-declared passing candidate; else any passing when active is bad), spares, trim by last-known-good, dry run, `Checked()` semantics for the staleness gauge, `InitMetrics`, report JSON, `--once` exit codes | Generic. This is the most valuable part to copy |
| CDP client and launcher (`cdp.go`), the `Render` engine (flags, process group, proxy auth over `Fetch.authRequired`, media blocking, settle and scroll, cookie in/out, UA plus client-hints override, wire-byte accounting) | Generic browser automation |
| `Bootstrapper` share-credit bucket and semaphore, fail-fast design, "one page load for pacing" | Generic |
| Docker `--target browser`, Helm `browserTag` / `shareProcessNamespace` / `no_sandbox`, alerts and dashboard patterns | Generic |

**(b) Reusable with Instagram parameters:**

| Component | Facebook-specific parameter today | What to parameterise (suggestion) |
|---|---|---|
| `docids.scriptSrc` + `Bundles` host filter | path `rsrc.php`, hosts `fbcdn.net`/`*.fbcdn.net`/page host | Allow the Instagram static host(s) (brief: `static.cdninstagram.com`; verify whether paths are also `/rsrc.php/...` and whether other hosts such as `*.cdninstagram.com` or `www.instagram.com/static/...` serve bundles). Make hosts and path pattern config. Also accept root-relative URLs |
| `docids.relayOp` | module suffix `_facebookRelayOperation` | Verify how Instagram's Haste modules name persisted operations (it may well be the same `__d("<Name>_…RelayOperation",…)` convention with a different suffix, or the same suffix). Make the suffix a list |
| `docids.relayOpLoad` | name must end in `Query`; key `queryID`/`id` | Probably fine for `Polaris…Query` names; extend to `Mutation`/`Subscription` if needed |
| (missing shape) | not supported | Add a regex for Relay ConcreteRequest `params:{id:"<digits>",metadata:{…},name:"<Name>",operationKind:"query"…}` (id before name), plus the reverse order. Build a test corpus from real Instagram bundles |
| Seed `client_params` | `__a, __comet_req, dpr, __ccg, fb_api_caller_class, server_timestamps, x_asbd_id` | Instagram's web form and header set (brief mentions `x-ig-app-id`; likely also a CSRF token from a cookie, `lsd`, `x-asbd-id`; **verify**). The registry already supports arbitrary key/value params; the GraphQL client (chapter 1) decides which become headers |
| `browser.IsGraphQL` (path prefix `/api/graphql`) and `Exchange` form parsing (`fb_api_req_friendly_name`, `doc_id`, `variables`) | Facebook endpoint and field names | Make the capture predicate and field names config. Instagram's web client may call more than one GraphQL path (for example a `/graphql/query` style endpoint as well as `/api/graphql`; verify). **Also parse GET query strings**: today only POST bodies are read |
| `Bootstrapper` wall detection (`session.IsWall`: `/login`, `/checkpoint`) and `detect.Rules` markers/codes/stale messages | Facebook paths and error codes | Instagram's login and challenge redirects (likely something like `/accounts/login` and `/challenge/`; verify) and its persisted-query error text |
| Token validity (`htmlx.Tokens.Valid()` = LSD present) | Facebook tokens | Define "bootstrapped" for Instagram (for example a CSRF cookie plus whatever the logged-out GraphQL call requires; verify). This module is covered in chapter 1, but the browser fallback's success test depends on it |
| Canary concept, probe chain order | pages; timeline -> comments -> replies | Canary **profiles**; chain profile -> posts -> post comments -> child comments (section 9.2) |
| Config defaults (`max_share 0.02`, `max_concurrent 2`, `interval 30m`, `keep_fallbacks 2`, `retire_after 3`) | tuned for Facebook | Re-baseline in an Instagram Phase 0; Instagram's logged-out web may need JS more often, so `max_share` may need to rise |
| Placeholder doc_id regex | `9000000000000ddd` | Pick a fake-server placeholder range |

**(c) Facebook-specific; must be rewritten:**

| Component | Why |
|---|---|
| `configs/queries.seed.yaml` queries and variables | Every operation name, intent token, `feedLocation`, `renderLocation`, etc. is Facebook Comet |
| Registry-sync probes `probeTimeline`, `probeComments`, `probeReplies` | Built on `parse.Timeline`, `parse.Comments`, `ThreadRef` (expansion token, inline replies), `parse.FeedbackID` (base64 `feedback:<id>`) |
| `loadCanary` page-id extraction (`parse.Page(pl.HTML)` -> `PageId`) | Instagram needs a profile id/username resolution from its HTML or an API call |
| `storedPosts` SQL | Reads the Facebook `posts` table (`page_id`, `feedback_id`, `comment_count_total`) |
| `kinds` (`timeline`, `comments`, `replies`) and the `needed` chaining | Must reflect Instagram's own dependency graph |
| fakefb bundle/page/script shapes | The Instagram fake server must emit Instagram-shaped bundles, pages and the page's own GraphQL call for render discovery |

### 9.2 What the logged-out Instagram web app would need (suggestions to verify)

1. **A Phase 0 HAR capture first.** Record a logged-out session: open a public profile, scroll the grid, open a post, open its comments, expand replies. List the GraphQL calls:
   - endpoint path and method (POST form vs GET);
   - form/query fields (is it also `fb_api_req_friendly_name` + `doc_id` + `variables`?);
   - required headers (`x-ig-app-id`, `x-csrftoken`, `x-fb-lsd`/`lsd`, `x-asbd-id`, `x-fb-friendly-name`?);
   - which tokens come from HTML versus cookies;
   - the error body for a bad doc_id (to write the `StaleMessages` regexes).
   
   The existing `fbs-spike har` tool (`internal/spike/har.go`) is a good model: it extracts friendly names, doc_ids and variables, and diffs templates.
2. **Registry seed for Instagram** (illustrative names in the brief's style; real names to be discovered):
   - `profile_posts`: e.g. a `PolarisProfilePostsQuery`-style operation, variables like a user id/username, `first`/`count`, `after` cursor;
   - `post_comments`: e.g. a `PolarisPostCommentsQuery`-style operation, variables like the media id/shortcode, `first`, `after`, perhaps an ordering flag;
   - `comment_replies`: the child-comments operation (parent comment id, cursor);
   - possibly `profile_info` (username -> user id) and `post_by_shortcode`.
   
   Keep the logical-name/friendly-name split. Facebook needed it for orderings (`comments` / `comments_newest`), and Instagram may have similar variants. Expect Relay provider flags (`__relay_internal__pv__…`) as fixed variables, as the Facebook runbook warns.
3. **Discovery.** Scan the profile HTML for bundle URLs on the Instagram CDN (allow-list host). Keep the content-addressed cache and the per-canary `max_bundles` budget. Add regexes for whichever declaration shapes Instagram's bundles use (9.1b). Keep the render fallback: a profile page load probably triggers the "profile posts" query, so render discovery covers it. Comment and reply queries will likely need **scripted interaction in the render**, not just scrolling (for example click a post, click "view replies"). That is a natural extension of `Render`: add an `Actions` list of `Runtime.evaluate` snippets, each followed by `settle`.
4. **Canaries.** Several public, active profiles with posts that have comments **and** reply threads. The Facebook runbook's warnings apply directly: quiet or dead canaries, or all-blocked probes, mean "unchecked", not "healthy".
5. **Probe chain.** profile -> posts (pick those with comments, busiest first) -> comments (pick a comment with child comments) -> replies. Keep the "stored posts" fallback so comment probes work when the profile query has drifted.
6. **Browser fallback.** If Instagram's logged-out HTML lacks the needed tokens or cookies without JavaScript, the bootstrap fallback ports as is: render through the identity's exit, copy cookies and DOM tokens. Wall detection needs Instagram paths. Keep fail-fast caps and size `max_share` from Phase 0 data.
7. **Request headers on bundle fetch.** The direct `HTTPFetcher` sends Go's default User-Agent. If Instagram's CDN filters on that (unknown), set a browser UA. Apply the 20 MiB cap on the via-proxy path too.

### 9.3 Improvements to carry into the rewrite (from this code's caveats)

1. Order crawler fallbacks by `coalesce(last_ok_at, promoted_at) DESC` (2.10 #1).
2. Enforce a single active row per name (partial unique index), and make `ApplySeed` demote properly (2.10 #2).
3. Record render-discovered candidates with their own `source` (for example `render`) so reports distinguish them. Today both are `bundle`.
4. Refund the share credit when the bootstrap is rejected as `busy` (5.2).
5. Use the capture variables: today registry-sync copies the *old* template's variables into new candidates. When a render captures the real variables, diff them against the template (as `fbs-spike har` does) and flag differences in the report. That would catch "variables changed" rotations, which today show up only as `failing`.
6. Parse GET query strings in `Render` captures, and add the ConcreteRequest `params:{id,…,name}` regex shape.
7. Consider recording `skipped` runs (or at least counting them), and making the `RegistryDiscovered` gauge include rendered ids.
8. Consider making the query breakers shared fleet-wide (for example in Redis) if Instagram's per-pod traffic is too low for `min_events` to trip quickly. Today each pod learns on its own.

### 9.4 Phase-0 verification checklist for Instagram (suggested)

- [ ] GraphQL endpoint path(s), method, body encoding, and the field names carrying operation name, doc_id and variables.
- [ ] Headers required logged out (`x-ig-app-id`, CSRF, `lsd`, `x-asbd-id`, `x-fb-friendly-name`?) and where each value comes from (HTML, cookie, constant).
- [ ] Bundle host(s) and URL shape; how operations are declared in bundles (module name suffix, `params:{id,name}`, other); whether all needed operations are in bundles referenced from a profile page or lazily loaded.
- [ ] Error body for an unknown or rotated doc_id, and for missing variables (for the stale rules).
- [ ] Login-wall and challenge redirects and markers for logged-out users; how far pagination goes logged out.
- [ ] Whether a plain HTTP GET of a profile yields usable tokens, or JavaScript is needed (sizes `max_share`).
- [ ] Rotation frequency of Instagram doc_ids (sizes `registry_sync.interval`).
