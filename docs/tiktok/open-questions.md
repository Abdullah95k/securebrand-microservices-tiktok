# Open questions about TikTok (T-numbers)

Everything tks assumes about TikTok's behaviour is a **hypothesis** until the owner's recordings or a probe run confirms it. Each has a number, T1, T2, …, used everywhere in the docs and code comments. A session that finds a new one adds the next number here.

The hypotheses come from public sources ([methods.md](methods.md) cites each). No request to TikTok was made from the kickoff session.

**How each gets answered:**
- **HAR:** the owner's Chrome recordings ([recording-guide.md](recording-guide.md), checkpoint C1). Good for shapes, fields, cookies and what a browser sees; one person, one network, one day.
- **Probe test:** `make tks-probe`, 3 to 5 accounts, ISP tier (C2). Good for "does a crawler get this at all".
- **Probe full:** `make tks-probe-all`, the whole list, 365 days (C3). Good for depth, rates and averages.
- **Phase 0:** the measurement kit (M7, C4). Good for per-IP tolerance and cost at volume.
- **Canary:** `tks-canary` over months (M8). Good for how often things break.

**Status** is `open` until answered; then `confirmed`, `refuted` or `partly`, with the date and the evidence (HAR file name, probe report field).

---

## Access and bootstrap

| # | Hypothesis | Why it matters | Answered by | Status |
|---|---|---|---|---|
| T1 | A profile page and a post page, loaded logged out with plain HTTP, embed one JSON document (`<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">`) holding the account (id, `secUid`, username, nickname, bio, bio link, avatars, verified, private, counts) or the post (full item with counts) | The bootstrap, the daily account refresh, and a post's details when the list lacks them all depend on it | HAR (R1, R3), probe test (steps 1, 2, 9) | open |
| T2 | The page and the cookies it sets (`ttwid`, `tt_chain_token`, `msToken`, `tt_csrf_token`, …) give every value the web client sends with its calls (device id, region, language, `odinId`) | Whether one page load bootstraps an identity for its tens of calls, as in igs | HAR (with sensitive data), probe test (steps 1, 8) | open |
| T3 | A plain HTTP load (no JavaScript) through the ISP proxies gets the real page, not a captcha or a JavaScript challenge | If not, every bootstrap needs a browser, which multiplies cost | Probe test (step 1) | open |
| T4 | Some calls work **unsigned**; others need `X-Bogus`/`X-Gnarly` (and `msToken`). An unsigned or badly signed call answers **HTTP 200 with an empty body** | **The biggest question.** It decides between the simple method (plain HTTP) and the signer (browser pages), and the cost per call | HAR (which calls carry signatures), probe test (steps 3–5, 8, 10, 11 unsigned) | open |
| T5 | TikTok checks the TLS and HTTP/2 fingerprint, and a `tls-client` Chrome profile (`chrome_152`) passes | The transport choice; a mismatch is a strong bot signal | Probe test (step 1 with two profiles) | open |
| T6 | Logged-out traffic from ISP IPs meets few captchas ("Verify to continue") at a person's pace; captchas are recognisable by path or marker | Block handling, identity retirement, and whether ISP IPs are good enough | Probe test and full (captchas per step and per thousand requests) | open |
| T7 | A call made with `fetch` inside a TikTok page in headless Chromium is signed by TikTok's own script and succeeds where the unsigned call failed | The fallback for T4 without re-implementing signatures | Probe test (step 13, `--signer browser`) | open |
| T27 | A call signed inside one browser page is also accepted when the same signed URL is sent by plain HTTP from another identity with the same user agent | If yes, a few signer pages serve the whole fleet (sign, then send cheaply); if no, every identity that makes signed calls needs its own page, which multiplies browser memory | Probe test (step 13: the signed URL the page sent, replayed by plain HTTP) | open |
| T24 | The user detail call (`/api/user/detail/`) works unsigned, and is much smaller than the profile page | The cheapest daily account refresh | Probe test (step 3) | open |

## Data

| # | Hypothesis | Why it matters | Answered by | Status |
|---|---|---|---|---|
| T8 | A post id's top 32 bits are its Unix time (`id >> 32`), and comment ids follow the same rule | A backfill stops at 365 days from ids alone; ids fit int64 until 2038 (ADR 0007 §1) | HAR, probe test (`id_time_matches`) | open |
| T9 | Each item in the post list is complete: counts (`playCount`, `diggCount`, `commentCount`, `shareCount`, `collectCount`), music, hashtags and mentions (`textExtra`, `challenges`), duration, covers, labels | If so, a new post costs nothing beyond the list check, and metrics refresh with every check. If not, each new post needs its page (as igs) | HAR (R1), probe test (step 4) | open |
| T10 | Pinned posts come first in the list regardless of date, flagged `isPinnedItem` | The watermark must skip them, or a pinned old post hides new ones (EnsembleData's docs warn of exactly this) | HAR (R1), probe test (`pinned_seen`) | open |
| T11 | The list cursor is a time in milliseconds, `hasMore` ends it, pages hold up to about 30–35 posts (`count`), and the list pages back 365 days or to the first post without a wall | Backfill feasibility and cost; incremental checks with a small `count` | HAR (R1, R5), probe test (step 4 counts), probe full (depth) | open |
| T12 | Top-level comments are served logged out through `/api/comment/list/`, about 20 per page, with an offset cursor and `has_more`; the post's `comment_count` is available | Objective §3 at all | HAR (R2), probe test (step 10) | open |
| T13 | Top-level comments come in TikTok's ranking order, **not newest first**, and the web client has no sort parameter | Decides the comment refresh strategy and its cost (ADR 0007 §5) | HAR (R2), probe test (order verdict) | open |
| T14 | Replies are served logged out through `/api/comment/list/reply/` | Replies in scope or not (D1) | HAR (R2: "View replies"), probe test (step 11) | open |
| T15 | For a post read to the end, top-level comments plus their `reply_comment_total` come close to `comment_count` (the gap is hidden, filtered or deleted comments) | The completeness check and target A4 | Probe test and full (ratio per post) | open |
| T16 | Ads and paid partnerships are flagged on the item (`isAd`, and branded-content labels) | Objective §2 "sponsored or ad flag where shown" | HAR (R6), probe (`ad_seen`) | open |
| T17 | Every user object (account, comment author, mention) carries the numeric `id`; `secUid` stays the same when the username changes; an account can be read by `secUid` after a rename | The HMAC key (D8), username history, following renamed accounts | HAR, probe test, then the canary over time (renames are rare) | open |
| T18 | Media links (covers, video renditions, photos, avatars) download without cookies, are signed and expire after hours | Media downloads (D5): when to fetch and what to retry | Probe test (step 12) | open |
| T19 | Photo posts appear in the list with `imagePost` (each photo's links) and have a `/photo/<id>` page | Objective §2 covers photo posts | HAR (R4), probe (`photo_seen`) | open |
| T26 | oEmbed (`/oembed?url=`) answers without signature or captcha, with caption, author and cover (no counts) | A cheap fallback to check that a post exists and refresh its caption | HAR (R7), probe test (step 14) | open |

## Scale and cost

| # | Hypothesis | Why it matters | Answered by | Status |
|---|---|---|---|---|
| T20 | One ISP IP sustains a useful daily volume of calls (order of a few thousand) at a person's pace before captchas or blocks rise | **The main cost driver:** IPs needed = daily calls ÷ calls per IP | Probe full (first look), Phase 0 (measured) | open |
| T21 | Wire bytes: profile page ~100–300 KB, list page ~20–60 KB compressed at 30 posts and less at a small `count`, comments page ~5–15 KB | Proxy cost if billed per GB, and the byte budget | Probe test and full (`mean_wire_bytes` per call) | open |
| T22 | A list or comment cursor works from another identity | Task affinity: whether a continuation can run on any identity | Probe test (step 7) | open |
| T23 | What is served does not depend on the proxy's country for public Iraqi accounts (a US ISP IP sees the same posts and counts) | Proxy country choice; geo-blocked posts | Probe full (compare with the owner's HAR from Iraq or the region), Phase 0 | open |
| T25 | TikTok changes paths, parameters or signing every few weeks, sometimes silently (fewer items, HTTP 200) | Upkeep, canary design, alerting on silent drops | Canary over months; providers' changelogs as a weak signal | open |
