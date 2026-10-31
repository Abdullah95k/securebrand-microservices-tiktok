# Phase 0: measure logged-out Facebook before scaling up

Phase 0 answers four questions with real data before any money goes into
scale:

1. Does a plain HTTP client get a working logged-out session (bootstrap)?
2. How many comments does logged-out Facebook actually serve, per ordering?
3. Which proxy tier stays under a 5% block rate at production pacing, and
   what would it cost per month?
4. How far back do logged-out timelines go?

It also replaces the placeholder doc_ids in `configs/queries.seed.yaml` and
captures real response fixtures so the parsers can be checked against
Facebook's current shapes.

Everything runs from one command, `fbs-spike phase0`, on one machine. No
Kafka, Postgres or Redis is needed. The result is a per-tier verdict and a
bundle to send back.

**Get legal sign-off before you start.** The plan's legal items (Facebook
ToS review, legitimate-interest assessment, DPIA) also cover this
measurement, because it collects public comments by real people.

## What you need

| | |
|---|---|
| A machine | Linux or macOS, with Go 1.27+, or Docker, or a static binary (`make spike-bin`) |
| Proxies | Credentials for every tier you want to compare, for example static ISP IPs and a rotating residential gateway |
| Pages | About 150 public pages, ideally the ones you will monitor |
| Time | With the defaults: about 200 requests per page, 4 pages in parallel at 0.3 requests/second each, so roughly **7 hours per tier for 150 pages**. The smoke step prints an estimate for your pages before the long run starts. |
| Traffic | Roughly 1 to 2 GB per tier for 150 pages (about 35 KB per request). At $4/GB of residential traffic, that is single-digit dollars. The smoke step prints this too. |

## 0. Rehearse against fakefb (about a minute, no proxies)

```bash
make phase0-dryrun
```

This starts a local fakefb whose doc_ids have "rotated" away from the
placeholders, then runs every step against it: preflight, doc_id discovery,
capture and the fixture check, smoke, the measurement run, the report and
the bundle. It must end with `OVERALL: GO`. If it doesn't, the kit is
broken on your machine; fix that before spending proxy traffic. The output
goes to `phase0/out-dryrun/`.

## 1. Fill in the two files

```bash
cp phase0/proxies.example.yaml phase0/proxies.yaml
```

```bash
chmod 600 phase0/proxies.yaml
```

```bash
cp phase0/pages.example.csv phase0/pages.csv
```

**`phase0/proxies.yaml`:** one entry per tier.
- `static_list` is for ISP or datacenter IPs. Each endpoint is one exit.
- `gateway` is for rotating residential. The session id in the user name pins the exit for `sticky_ttl`. The example file lists common user name formats; check your provider's.
- Set `cost_per_gb_usd` on each tier so the report can project monthly cost. Per-IP plans can leave it at 0.
- The file is git-ignored and never copied into the bundle.

**`phase0/pages.csv`:** one page per row (URL, vanity or `profile.php?id=`), plus label columns.
- The header `page,size,category,region,language` is a suggestion; any columns work. The report breaks every result down by each label.
- Aim for a stratified sample: small to mega pages; brands, news, creators and government; US, EU (cookie consent wall) and APAC.
- **Put a few busy pages first.** The first rows also feed doc_id discovery (3 pages), fixture capture (2) and the smoke run (3).

## 2. Run

```bash
make phase0 PHASE0_TIERS=isp,residential
```

`PHASE0_TIERS` names the tiers in `proxies.yaml` to measure, side by side.
Extra flags go in `PHASE0_ARGS`. Without Go:

```bash
docker run --rm -it --user "$(id -u)" -v "$PWD/phase0:/phase0" --entrypoint fbs-spike fbs:dev \
  phase0 --config /phase0/proxies.yaml --pages /phase0/pages.csv --tiers isp,residential --out-dir /phase0/out
```

(build the image with `make docker`), or with a static binary built by
`make spike-bin`:

```bash
bin/fbs-spike-linux-amd64 phase0 --config phase0/proxies.yaml --pages phase0/pages.csv --tiers isp,residential
```

The steps, in order. Each prints what it found, and the output goes to `phase0/out/`:

| Step | What it does | Writes | Stops when |
|---|---|---|---|
| `preflight` | Checks each tier's exits through an IP echo service: the IP stays the same within a session, and different sessions get different IPs. Then loads the first page through each tier. Also checks the config file permissions. | `preflight.json` | any FAIL line (wrong credentials, non-sticky sessions, a login wall) |
| `docids` | Loads the first 3 pages, scans their JS bundles for the registry's queries and writes a registry with the current doc_ids | `docids.json`, `registry.yaml` | a query is not found. Use the [HAR fallback](#3-har-fallback-when-doc_ids-are-missing). |
| `capture` | Records the page HTML, a timeline page, a comments page and a replies page of the first 2 pages; scrubs people's names, ids, links and text, and signed URLs; runs the parsers over them | `fixtures/`, `fixtures.json`, `failures/` | a parser does not understand a response: Facebook's shapes changed. Or no timeline came back: the console line gives the class, HTTP status and content type, and `failures/` keeps the response. Send the bundle. |
| `smoke` | Measures the first 3 pages per tier at reduced limits, then prints an estimate of the full run's time, traffic and cost | `smoke-<tier>.jsonl` | no bootstrap, blocks above 50%, or the timeline parser finds no posts |
| `run` | The measurement, per tier. It can be resumed: pages already finished are skipped. | `run-<tier>.jsonl` | none |
| `report` | The go/no-go report | `report.txt`, `report.json` | none |
| `bundle` | Packs everything to send back, then checks that it contains no proxy credentials | `phase0-bundle.tar.gz` | a credential is found |

When a step stops because something needs your action, the bundle is still
written.

**Exit codes:** 0 GO, 3 NO-GO, 4 action needed (missing doc_ids, parser drift, smoke findings), 1 error, 130 interrupted.

**Interrupting:** Ctrl-C is safe. Pages that were in progress are measured again. To resume the long run without repeating the earlier steps:

```bash
make phase0 PHASE0_TIERS=isp,residential PHASE0_ARGS="--from run"
```

**Tuning:** the defaults suit about 150 pages. `fbs-spike phase0 -h` lists every flag.

| Flag | Default | Notes |
|---|---|---|
| `--rps` | 0.3 | Requests per second per identity. Keep it at or below 0.33, the pipeline's production pacing; faster understates blocks. |
| `--concurrency` | 4 | Pages measured in parallel, each on its own exit |
| `--posts-per-page` | 3 | Posts per page measured for comment completeness |
| `--max-post-comments` | 300 | Prefers posts small enough to count completely; bigger ones are only used to fill up |
| `--threads-per-post`, `--comment-pages` | 100, 40 | Limits per measurement. Measurements cut by these limits are marked and left out of the median (see below). |
| `--orderings`, `--compare-posts` | all three, 1 | The first ordering ("All comments") is measured on every sampled post; the others on one post per page, for comparison |
| `--timeline-pages` | 60 | About 600 posts. Raise it for pages that post many times a day. |
| `--min-days` | 365 | The backfill window you need. Use a lower value if you accept a shorter default. |
| `--req-per-day` | 16M | Production volume for the cost projection (the README sizing) |
| `--ip-echo-url` | api.ipify.org | Any service that returns the caller's IP. Pass an empty value to skip the exit checks. |

## 3. HAR fallback, when doc_ids are missing

First try `fbs-spike render --config phase0/proxies.yaml --tier <tier> --page <page> --write-registry phase0/out/registry.yaml`. It needs Chromium (`fb.browser.binary`, `$IGS_BROWSER_BINARY` or one on PATH). It loads the page in headless Chromium through the tier and takes the doc_ids from the GraphQL requests the page makes itself. A page load only requests the first timeline page, so the comment and reply queries usually still need the HAR below.

The bundle scan can miss queries that Facebook loads lazily. A browser
recording of a logged-out session has them all:

1. Open a **private window, logged out**, then open DevTools → Network. Tick "Preserve log" and filter on `graphql`.
2. Open a busy public page and scroll down a few screens, **slowly**. Facebook rate-limits logged-out timeline scrolling early. The first real recording got one page of posts. After that, the browser retried about 6 times a second for minutes, and every retry came back as "Rate limit exceeded" (code 1675004, sent with HTTP 200). If posts stop loading, stop scrolling and move on to the post.
3. Open a post with many comments. Switch the comment filter to **"All comments"**, then to "Newest". Click "View more comments" and "View N replies" a few times.
4. Right-click the request list → **Save all as HAR** (recent Chrome versions export a sanitized HAR, without cookies, by default). The tool never reads headers or cookies either way, and never writes them anywhere.

Then:

```bash
go run ./cmd/fbs-spike har --in session.har --write-registry phase0/out/registry.yaml \
  --fixtures-dir phase0/out/fixtures --name brand_a
```

```bash
make phase0 PHASE0_TIERS=isp,residential PHASE0_ARGS="--registry phase0/out/registry.yaml --from capture"
```

For each query, `har` also prints two lists: the variables Facebook now sends that the registry template lacks, and the template variables Facebook no longer sends.

**Variables written to the registry.** `har` writes a variable into the template when every request sent it with the same small value: a flag, a number below a million, a short enum without an id in it, or null. This covers both new variables and fixed values that changed. Facebook rejects requests that leave out required variables, such as the `__relay_internal__pv__*` provider flags. These are listed under "variables set in the registry".

**What stays as it is.** Caller values (`{{cursor}}`, `{{page_id}}`, ...) and the comment ordering are never changed. A query whose ordering the recording lacks keeps its fixed values too.

**Variables not set.** Some variables are left out of the template:
- a value that changed between requests;
- a variable only some requests sent;
- a value that repeats a caller value, such as the page id;
- long strings and objects.

Look at "variables not set" and "variables removed". If either is non-empty, send the bundle so the templates can be updated by hand.

**The fixture.** For each query, the fixture is the largest successful response that carries data, never an error.

**More than one recording.** Pass `--registry phase0/out/registry.yaml` to merge a second HAR into the registry the first one wrote.

## 4. Reading the report

`report.txt` starts with a comparison of the tiers, then the details of
each tier:

```
== Phase 0 report ==
tier           pages bootstrap  blocks  comments   days      $/month  verdict
isp              150       97%    7.9%      0.93    365         4200    NO-GO
residential      150       98%    1.2%      0.93    365        19800       GO

-- residential --
...
comments [comments]: 450 posts, median 0.93, p10 0.71, walls 2
comments [comments_newest]: 150 posts, median 0.90, p10 0.64, walls 1
comments [comments_relevant]: 150 posts, median 0.41, p10 0.12, walls 0
bytes [comments]: n=3120 on the wire mean=31.0 KB p95=58.2 KB, decoded mean=212.4 KB
...
by region:
  value          pages bootstrap  blocks   days  comments
  eu                50       93%    1.9%    365      0.91
...
PASS median comment completeness 0.93 (comments) >= 0.70
...
OVERALL: GO (recommended tier: residential)
```

(These numbers are illustrative.)

| Line | Meaning |
|---|---|
| `bootstrap success`, `tokens` | Share of page loads that yielded a usable session, and which tokens were found. `consent walls` counts EU cookie walls that were handled. A load that gets no response (a dead or stalled proxy endpoint) retires that identity and is tried once more on a fresh one; the failed attempt still counts as a failed bootstrap. |
| `block rate`, `classes` | Share of GraphQL requests that were rate limited, hit a login wall or a checkpoint. `classes` shows every outcome. |
| `timeline` | Posts and days reached per page, and why timelines ended: `end` (no older posts), `wall` (login wall), `max_pages` (the `--timeline-pages` limit), `blocked:*`, `drift`, `no_page_id` (the page's HTML named no id it could use; see below) |
| `comments [<ordering>]` | Collected ÷ reported comments per post. The median and p10 leave out measurements cut short by the spike's own limits; when there are any, the line also gives their count and the median including them, which is a lower bound. `walls` counts measurements that hit a login wall. |
| `bytes [<query>]` | Response sizes: on the wire (compressed, what proxies bill) and decoded |
| `cursor portability` | Whether a timeline cursor still works on a fresh identity. Low values mean cursors are tied to the session, so tasks need session affinity (`SessionSnapshot` in the task envelope). |
| `media success`, `median media expiry` | CDN downloads over direct egress, and how long signed media URLs stay valid. Expiry sets how fast the media downloader must work. |
| `traffic`, `cost` | Traffic of this run, and a projection to `--req-per-day` at the tier's `cost_per_gb_usd` |
| `by <label>` | The same numbers for each value of each pages-file column |

A page that ends `no_page_id` loaded fine, but its HTML named none of the
ids the extractor reads. A page asked for by its numeric id uses that id. For
one asked for by its vanity, capture it (a single page load) and
send the result:

```bash
go run ./cmd/fbs-spike capture --config phase0/proxies.yaml --registry phase0/out/registry.yaml --tier isp \
  --page <vanity> --name <vanity> --dir phase0/out/diag --fail-dir phase0/out/diag-failures
```

`phase0/out/diag-failures/page_html/<date>_<vanity>/meta.json` gives the path
the page ended on and the id counts; the HTML next to it shows what to read
instead. Like every fixture, it holds Facebook content: keep it off git.

The verdict: a tier is GO when all four checks pass. Phase 0 is GO when at
least one tier is. The recommended tier is the cheapest passing one.

| Check | Threshold | If it fails |
|---|---|---|
| Median comment completeness, best ordering | ≥ 0.70 | Compare the orderings and their walls. If even "All comments" is low, logged-out access hides comments: plan the browser fallback (Phase 2) or other surfaces. |
| HTTP bootstrap success | ≥ 0.90 | Look at the region table (EU consent wall) and the tokens found; try another `fb.fingerprint_profile` or another tier |
| Block rate | < 5% | Lower `--rps`, add exits (more static endpoints or gateway sessions), or use another tier. Mixing tiers in production (`fb.proxy.routing`) is an option. |
| Timeline depth | median ≥ `--min-days`, or no login walls | Timelines stop early behind login walls: lower `limits.max_age_days`, or accept partial backfills |

## 5. Sending the results back

`phase0/out/phase0-bundle.tar.gz` holds:
- the reports;
- the preflight, docids and fixture checks;
- the discovered registry;
- the run records: per request the query, outcome class, sizes and timing, with no content;
- the scrubbed fixtures;
- `failures/`: the responses capture could not use, scrubbed like the fixtures, each with its HTTP status, content type, outcome class, and which page tokens and cookies the identity held (names only, never values). A page whose id capture could not find is kept too, with the path it ended on and how many ids its HTML holds under each key the extractor knows (counts only);
- `meta.json` (versions and flags).

It never holds `proxies.yaml`. Before writing it, the tool checks that no
configured proxy user name or password, and no URL with credentials,
appears in it.

**Share it privately. Never commit it to this repository, which is public.**
**What the fixtures still hold.** The scrubber walks each response as JSON, including JSON nested inside strings. It removes:
- people's names and ids, including people mentioned in comments;
- people's profile and `/people/` links;
- signed media URLs;
- the text people wrote: comment and post bodies (any text inside a comment), text that mentions someone, a profile's status, and media captions. Letters and digits become `x`, and the length is kept so mention offsets stay valid.

What stays is the page's own name, links and text (such as its follower line), and real structure: post and comment ids, permalinks, timestamps and counts.

**Re-scrubbing.** Each fixture records the scrubber version. When the bundle is built, fixtures scrubbed by an older version are scrubbed again.

To get the parsers and doc_ids updated, start a Claude Code session on this
repository and attach the bundle, or share it with whoever maintains the
parsers. With it, they can:
- adjust the parsers to the real shapes;
- update `configs/queries.seed.yaml` with the discovered doc_ids and variables;
- set the pacing and proxy defaults (`fb.identity_rps`, `fb.proxy.routing`, `limits.max_age_days`) to the measured values;
- regenerate the golden files (`make golden`).

## 6. Update the registry in a running deployment

```bash
curl -X PUT https://fbs/v1/admin/queries/timeline -H "Authorization: Bearer $ADMIN_KEY" \
  -d '{"doc_id":"<new>","friendly_name":"ProfileCometTimelineFeedRefetchQuery","parser":"timeline","variables":{...}}'
```

Crawlers reload the registry within `fb.registry_refresh` (30s). The previous
entry becomes the fallback.

## 7. Cost experiments

Three measurements decide what production costs. Run them after the Phase 0 run, not beside it: they use the same proxy IPs, and would skew its block rates.

### Smaller responses: `make phase0-trim`

```
make phase0-trim                    # the first 3 pages of pages.csv, on the isp tier
make phase0-trim TRIM_ARGS="--each" # also every provider flag on its own (about 10 minutes more)
```

For each page it sends the timeline request the crawler makes, then variants:
- `flags_off`: every `__relay_internal__pv__*` provider flag that is on, switched off. These flags switch parts of the response on, mostly display-only.
- `off:<flag>` (with `--each`): one flag at a time, to see what each costs.
- `count=N`: other timeline page sizes (`--timeline-counts`, default 3,25; the baseline is the crawler's 10).

Then the same for the comments of the page's busiest post, for both comment queries the crawler sends (`comments` for full reads, `comments_newest` for the refreshes), with `after_count=N` for comment page sizes (`--comment-counts`, default 10,25,50; the template's is -1, Facebook's default). The baseline is the crawler's own request: `crawler.timeline_page_size`, and the Referer it sends.

It keeps no response content: each request leaves one line in `phase0/out/trim.jsonl` with its size, class and the field names the parser missed. The summary (again with `fbs-spike trim-report --in phase0/out/trim.jsonl`) shows each variant's **wire bytes per post or comment** against the production request, and whether it is usable: every request ok, no required field missing and, for flag changes, as many items as the baseline. "loses optional ..." names fields the variant drops that the baseline had: decide whether you need them.

**Adopting a result:**
- A provider flag that is usable off: set it to `false` in the `variables` of each query it was measured on, in the registry (`phase0/out/registry.yaml`, or the `fb_queries` table in a deployment, section 6). `comments` and `comments_newest` are separate entries.
- A bigger timeline page that is usable: `crawler.timeline_page_size`.
- A comment page size Facebook honours (more items per request): tell the maintainers; `crawler.comments_page_size` is not wired into the comments template yet.

### Faster per IP: `make phase0-rate`

```
make phase0-rate                                # the first 30 pages at 0.5, then 1.0 requests/s per IP, isp tier
make phase0-rate RATES="0.5 1.0 2.0" RATE_PAGES=50
```

Each rate runs the same pages with the production comment ordering only, and writes `phase0/out/rate-<r>.jsonl` and its report `rate-<r>.txt`. The highest rate whose block rate stays near 0 sets `fb.identity_rps` (now 0.33): twice the rate halves the IPs production needs. Leave headroom: blocks tend to start a while after a rate turns unsafe.

### Real steady-state load: the pilot

Phase 0 measures first crawls. What production pays for each day is the steady state: every page every 3 hours for new posts, plus comment refreshes. The 48-hour pilot (`docs/runbooks/pilot.md`) runs the real pipeline on the Phase 0 pages and measures requests per page per day.

## Reference: the steps as separate commands

`phase0` runs these for you; each also works on its own. Add
`--config phase0/proxies.yaml` to each.

```bash
fbs-spike preflight --tiers isp,residential --page https://www.facebook.com/<page>
```

```bash
fbs-spike docids --tier residential --page <page>,<page> --write-registry my-seed.yaml
```

```bash
fbs-spike har --in session.har --write-registry my-seed.yaml --fixtures-dir testdata/fb --name brand_a
```

```bash
fbs-spike capture --tier residential --registry my-seed.yaml --page <page> --name brand_a --dir testdata/fb [--fail-dir failures]
```

```bash
fbs-spike check-fixtures --dir testdata/fb
```

```bash
fbs-spike run --tier residential --registry my-seed.yaml --pages phase0/pages.csv --out run-residential.jsonl
```

```bash
fbs-spike report --in run-isp.jsonl,run-residential.jsonl
```

`run` and `capture` refuse placeholder doc_ids when `fb.base_url` is
Facebook, so no traffic is spent on requests that cannot succeed.
