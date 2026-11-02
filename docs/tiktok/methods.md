# Collecting public TikTok account data: the methods compared

**The question:** what is the simplest, least fragile way to monitor a list of public TikTok accounts (a few hundred Iraqi media, brand and public-figure accounts first, hundreds of thousands later) for the objective in the [build brief §2](build-brief.md#2-decisions): account details daily, every post back 365 days, new posts within 3 hours, metrics, comments (and replies if served logged out), and media metadata?

**How this was researched (2026-10-31):** public sources only. **No request was sent to any TikTok host** from this session, including TikTok's developer documentation: TikTok's own pages are cited from search-engine snippets, marked *(snippet)*, and should be read directly by whoever relies on them. No paid service was called and no credit was spent. Prices come from providers' public pages and third-party comparisons, and must be confirmed on the live pricing page before any budget.

**Facts and hypotheses:** a statement about a document, a price, a licence or a court decision is a fact with its source. **Every statement about how TikTok behaves is a hypothesis** with its number ([open-questions.md](open-questions.md)) until the owner's recordings or a probe run confirm it, however many sources repeat it.

Sources are numbered [W1], [O1], [P1], [L1] by section and listed in §8.

---

## 1. Summary

**Recommendation (D11, approved by the owner on 2026-10-31): the logged-out website, through our own tool.**
- Plain HTTP with a current Chrome TLS fingerprint for pages and for the JSON calls TikTok's own web app makes, from bootstrapped identities on the owner's ISP proxies, paced like a person. This is igs's method, with TikTok's request format.
- The public evidence favours it more than expected: two maintained open-source tools (yt-dlp and gallery-dl) read an account's whole post list today through an **unsigned** call, `/api/creator/item_list/`, with no browser [W1][W3] (T4). Post pages and the account page carry their data as JSON [W1][W3] (T1).
- Calls that TikTok refuses unsigned (reported today: the other list call `/api/post/item_list/`, possibly the account call) go through a small, capped pool of headless Chromium pages, where TikTok's own script signs them [W4][W9] (T7). tks never re-implements TikTok's signature algorithms.
- Why: it is the only route that can cover the whole objective for accounts we do not control, with no eligibility gate, at a cost that stays low at hundreds of thousands of accounts, and it reuses igs's code and operations.

**Fallback: the headless browser for everything.** If plain HTTP is refused even for bootstrap (T3, T28), or signed calls only work from inside their page (T27), every call runs inside browser pages. Same data and the same code above the client; several times the memory and CPU per call (§3.1).

**Contingency, the owner's decision (D4): a per-call data provider.** ScrapeCreators-class APIs advertise the whole objective, replies included, for about **$0.001 per call** (§4.5): tens to hundreds of dollars a month for a few hundred accounts, six figures a month at 300,000. They put a third party between us and the data, under its terms and outages. It is the right answer only if the probe shows our own tool cannot work, or as a bridge while it is built.

**Comments, the biggest risk in the public sources, are served.** Public reports conflicted on whether TikTok still serves comments to logged-out visitors (§2.5, T12). The owner's recordings of 2026-10-31 settled it for a browser: TikTok served top-level comments and replies to a logged-out session in Iraq ([har-findings-2026-10-31.md](har-findings-2026-10-31.md)). Whether a crawler gets them unsigned is part of T4, for the probe.

**Rejected:**
- **Official APIs** (§4): the Research API is for non-profit academic researchers and forbids commercial use; the Display and Business APIs return only accounts that log in to our app; the Commercial Content API covers ads in Europe.
- **The mobile app API** (§3.2): device registration and request signing computed in the app's native code, frequent breakage, and the weakest legal position.
- **Embeds and oEmbed** (§3.3): no counts and no comments; a supplementary source at most (T26).

**What decides between the recommendation's variants:** T4 (which calls need a signature), T3 and T28 (plain HTTP bootstrap, the JavaScript challenge), T7 and T27 (browser signing, and whether one page can sign for many identities), T12 (comments logged out), and T20 (calls per ISP IP per day, the main cost driver). The recordings (C1) and the probe's test pass (C2) answer most of them; the full pass (C3) and Phase 0 (C4) answer T20. The method is confirmed at decision G1.

---

## 2. TikTok's website, logged out

Sources here are mostly open-source projects that read TikTok logged out. Their merged code shows what worked for their maintainers on a date; their issues show what broke. Vendor pages and one-person measurements are weaker. All of it is hypothesis for tks until recorded or probed.

### 2.1 Data embedded in pages
- Pages embed their data as JSON in `<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">`, under `__DEFAULT_SCOPE__`: `webapp.user-detail` on profile pages (`userInfo.user`, `userInfo.stats`, `userInfo.statsV2`, `statusCode`), `webapp.video-detail` on post pages (`itemInfo.itemStruct`, `statusCode`), and `webapp.app-context` (region and other client values) [W1][W3]. **T1, T2.**
- The format has changed twice: `__NEXT_DATA__`, then `SIGI_STATE` (yt-dlp support added 2021-12-30), then the current tag (yt-dlp broke on 2023-11-19 and was fixed on 2024-03-01) [W1][W2]. TikTok-Api still reads the old tag first, and its users hit errors in November 2025 [W5].
- **Profile pages come in two forms**, reported in August 2026: a server-rendered page of about 620 KB with the account embedded, and a client-rendered shell of about 149 KB with nothing embedded, whose script then calls `/api/user/detail/` [W6]. **No profile page embeds the post list** any more; the grid arrives through a JSON call [W6], and yt-dlp has paged through the API since 2024-05-26 [W2]. **T1.**
- **The account object** carries `id`, `uniqueId` (username), `secUid`, `nickname`, `signature` (bio), avatars in three sizes (`avatarLarger`, `avatarMedium`, `avatarThumb`), `verified`, `privateAccount`, settings flags, and `stats` with `followerCount`, `followingCount`, `heart`/`heartCount` (total likes), `videoCount`, `diggCount` (likes given), `friendCount`; `statsV2` holds the same as strings [W3][W7]. **No open-source code reads a bio link** (`bioLink`); only a provider's listing mentions one [W3]. **T1, T17.**
- **The post object** (`itemStruct`) carries `id`, `desc`, `createTime`, `scheduleTime`, `video` (duration in seconds, size, `cover`, `originCover`, `dynamicCover`, `playAddr`, `downloadAddr`, `bitrateInfo`), `music`, `author`, `challenges` and `textExtra` (hashtags and mentions), `stats` (`playCount`, `diggCount`, `commentCount`, `shareCount`, `collectCount`), `statsV2` (strings, plus `repostCount`), and flags including `isAd`, `itemCommentStatus`, `privateItem`, `locationCreated`, `poi`; photo posts carry `imagePost` (`images[]` with links and sizes, `cover`, `title`) instead of a video [W1][W3][W7]. AI-generated labels (`AIGCDescription`) and shop anchors appear in some items [W3]. **No public source shows a pinned flag** (`isPinnedItem`) **or a paid-partnership flag** in the web data. **T9, T10, T16, T19.**
- **Walls seen by yt-dlp:** a redirect to `/login`; age-restricted posts served without their video; `statusCode` 10216 (private post) and 10222 (private account) [W1]. **T6.**

### 2.2 JSON calls the web app makes
All are **GET** requests. The open-source clients send the web client's standard parameters (`aid=1988`, `app_name=tiktok_web`, `channel=tiktok_web`, `device_platform=web_pc`, `device_id`, language, region, browser and screen fields) [W1][W3][W4]; a real browser adds `WebIdLastTime`, `odinId` and `user_is_login=false`, then `msToken` and its signatures [W7][W8]. **T2.**

- **`device_id` is required.** Without it every call is reported to answer HTTP 200 with an empty body and a `tt_orcas_res: 1` header; any random 19-digit value is accepted [W7]. yt-dlp and gallery-dl draw a random one [W1][W3]. TikTok can reject a device id, after which the list keeps returning the same page [W2] (issue 14031, 2025-08-15). **T2, T29.**
- **`msToken`:** sources conflict on whether a call needs a real one [W7][W10]. **T2.**

| Purpose | Path | Key parameters | Paging | What sources report about logged-out use |
|---|---|---|---|---|
| **Post list** | `/api/creator/item_list/` | `secUid`, `count=15`, `cursor` (time in ms, starting at now), `type=1` | next cursor = last item's `createTime` × 1000; ends when `hasMorePrevious` is false | **No signature, no `msToken`, no cookies.** yt-dlp since 2024-05-26 and still on 2026-08-19 [W1][W2]; gallery-dl by default since 2026-01-23, as "less flaky" [W3]. Occasional empty answers [W2] (issue 17500, 2026-08-21). **T4, T11** |
| Post list, the app's own | `/api/post/item_list/` | `secUid`, `count` (≤ 35), `cursor`, `post_item_list_request_type` (newest, popular, oldest), `needPinnedItemIds` | `hasMore`, `cursor` | **Needs `X-Gnarly`** [W5] (issue 1311, 2026-07-18) and is the only call reported to check its environment values [W7]; empty answers even with cookies [W3] (issue 8812, 2026-01-04). **T4** |
| Account | `/api/user/detail/` | `uniqueId` or `secUid` | — | Same shape as the embedded account; reported in March 2026 to "no longer return details", possibly signature-gated [W5] (issue 1290). **T4, T24** |
| Post | `/api/item/detail/` | `itemId` | — | Reported to return the full post once `device_id` is sent [W7]. **T4** |
| Comments | `/api/comment/list/` | `aweme_id`, `count=20`, `cursor` (offset), `current_region` | `has_more`, `cursor`, `total` | **Conflicting** (§2.5). **T12** |
| Replies | `/api/comment/list/reply/` | `item_id`, `comment_id`, `count` (the web app asks for 3), `cursor` | as comments | Unknown. **T14** |

The usual failure is not an error but **HTTP 200 with an empty body** [W1][W3][W7][W10]. Every empty answer must therefore count as a failure, never as "no posts". **T4.**

### 2.3 Signatures, and how open-source projects obtain them
- TikTok's page script (`webmssdk`, a bytecode virtual machine that changes with each release [W11]) computes request signatures over the final query string: `_signature` (2019), `X-Bogus` (2022), `X-Gnarly` (first publicly analysed in May 2025), and from July 2026 `X-Dynosaur`; it also refreshes `msToken` [W8][W9]. A vendor's version history lists about ten releases of the script between May 2025 and September 2026 [W8], and one project reports that `X-Bogus` became a constant on ordinary calls in September 2026 while another still produces real values [W7][W9]. **T4, T25.**
- Projects take one of two routes:
  - **let TikTok's own script sign**, in a real browser page: TikTok-Api (MIT) calls the page's signing function and makes the request with an in-page `fetch` [W4]; carcabot's signer service (MIT) runs a pool of browser identities [W9]; pytok reports that since September 2026 the page's script signs `fetch` calls by itself [W6];
  - **re-implement the algorithm** without a browser: Douyin_TikTok_Download_API (Apache-2.0) [W7], TikTokDownloader (GPL-3.0) [W12], go-tiktok (BSD-3-Clause) [W10] and others, each rewritten when TikTok releases a new script.
- tks takes the first route only (ADR 0006 §4): TikTok updates its script and the page runs the new one; tks copies no third-party code; it is the easier position to explain to the legal team (§5).

### 2.4 Blocking
- **A JavaScript challenge.** Since about October 2025, some requests get a "Please wait…" page of 0.5–1.5 KB holding a proof-of-work puzzle that a browser solves and answers with a short-lived cookie. In December 2025–January 2026 it was region-dependent (failing from Asia, Australia, Canada and New Zealand; working from Belgium, Mexico, the Netherlands and the US) [W2] (issues 15418, 17393). yt-dlp and gallery-dl now compute it themselves [W1][W3]. tks lets a real browser page pass it (the bootstrap fallback) and does not compute it itself; whether that policy holds is for the owner and the legal team. **T28.**
- **TLS and header fingerprints.** yt-dlp needed browser TLS impersonation from 2025-10-01; in August 2026 its maintainer reported that "TikTok and/or Akamai" block common TLS and header fingerprints, and that a missing `Referer` triggered blocks; in September 2026 one Chrome 150 impersonation profile was "blocked outright" [W2] (issues 12574, 17604; PR 17437). No source publishes the fingerprints that pass. **T5.**
- **Captchas:** "Verify to continue" with slide, rotate and 3D-shape puzzles; in the API, `statusCode` 10000 with a `verify_event` [W6][W7]. tks records them and retires the identity; it never solves them. **T6.**
- **IP reputation:** residential IPs recommended, datacenter IPs "often blocked"; pages are reported reliable while the API is gated from datacenter IPs [W9][W13]. A "burnt" identity gets empty lists from any IP, a fresh one works [W9]. A provider's changelog (2026-10-19) reports datacenter IPs receiving empty lists [P9]. ISP IPs sit between the two, untested. **T6, T20.**
- **Rate limits:** no source publishes numbers [W3][W7][W9]. **T20.**
- **Region:** the calls carry `region` and `priority_region`. Since 2026-01-22, TikTok's US service is run by the TikTok USDS Joint Venture, whose terms are now separate [L1]; no source covers whether a US exit sees the same public posts and counts as an exit elsewhere. **T23.**

### 2.5 What it covers

| Objective | Covered | Source in the website (hypothesis) |
|---|---|---|
| Account details and counts, daily | yes | server-rendered profile page; or `/api/user/detail/` if unsigned (T1, T24); or the creator embed (T26) |
| Bio link | **yes** (owner's recordings) | `bioLink.link` on the profile page (T1) |
| Username history | yes, by observation | the numeric id and `secUid` stay, the username changes (T17) |
| Posts: caption, hashtags, mentions, time, duration, cover, music | yes | list items (T9) |
| Pinned flag | **yes** (owner's recordings) | `isPinnedItem` on the app's own list call (T10) |
| Sponsored or ad flag | partly | `isAd` seen; paid-partnership labels not seen (T16) |
| Counts: plays, likes, comments, shares, saves | yes | list items and post pages (T9) |
| Photo posts | yes | `imagePost` (T19) |
| 365-day backfill | likely | the unsigned list pages back by time; yt-dlp's tests expect ≥ 593 posts for one account [W1] (T11) |
| New posts within 3 hours | yes | first list page every 2 h 45 min |
| **Top-level comments** | **yes, for a browser** (owner's recordings, 2026-10-31) | public reports conflicted (TikTok-Api issues [W5], pytok [W6] against tiktok-cli [W13]); the recordings show them served logged out, in TikTok's ranking order (T12, T13) |
| Replies | **yes, for a browser** (owner's recordings) | `/api/comment/list/reply/`, 3 per call (T14) |
| Media metadata and files | yes | links in each item; media fetches reportedly need the page's cookies and `Referer` [W1] (T18) |

### 2.6 Requests and bytes per account per day
A model, to be replaced by Phase 0's measurements. "Calls" are JSON calls from an already bootstrapped identity; one page load bootstraps an identity for tens of calls (as in igs). Pages are large (a server-rendered profile page about 620 KB, a post page about 390–400 KB [W6][W2]), so tks reads them only to bootstrap and, if the account call is refused, once a day per account.

| Part | Light account (≤ 1 post/day, few comments) | Typical | Busy (TV or news: 5+ posts/day, thousands of comments) |
|---|---|---|---|
| List checks (every 2 h 45 min) | 9 | 9–10 | 10–15 |
| Account refresh | 1 | 1 | 1 |
| Metrics walks (daily 30 days, weekly 365 days) | 1–2 | 3–5 | 10–15 |
| Comments (initial reads and refreshes by age tier, ADR 0007 §4) | 2–10 | 20–80 | 100–500 |
| **Total calls** | **15–25** | **35–95** | **120–530** |
| Wire bytes (T21) | ~0.3–1 MB | ~1–3 MB | ~4–10 MB |

Comments dominate, and the comment order (T13) decides whether a refresh costs one or two calls (newest first, stop at the first stored comment) or a re-read to the cap. The caps and age tiers are the owner's levers after Phase 0.

**Proxy cost.** ISP proxies are billed per IP, so the cost is IPs × price per IP, where IPs = daily calls ÷ calls one IP sustains (T20). At 300,000 typical accounts (~18 million calls a day), 3,000 calls per IP per day would need about 6,000 IPs; 10,000 would need 1,800. Each doubling of per-IP tolerance halves the proxy bill, which is why Phase 0 measures it before the budget is set. A few hundred accounts need a handful of IPs.

**Browser cost, if signing is needed.** An identity paces at about 0.3 calls per second, so 18 million calls a day keep about 700 identities busy. If each needs its own signing page (T27 false), that is about 700 headless Chromium pages (some 200 GB of memory); if one page can sign for many identities (T27 true), a handful serves the fleet. If the unsigned list call keeps working (T4), only the account call and any refused call need the signer, a small share.

### 2.7 Breakage and upkeep
- The record of the last three years: the page JSON moved twice [W1][W2]; signatures were added and replaced (`X-Bogus`, `X-Gnarly`, `X-Dynosaur`) [W8]; the JavaScript challenge arrived in late 2025 [W2]; TLS and header checks tightened in October 2025 and August 2026 [W2]; the account call and the app's own list call stopped answering open-source clients in 2026 [W5]. The unsigned `creator` list call has kept working for yt-dlp since May 2024 [W1][W2]. A provider's comparison claims changes "every 4–8 weeks", often silent (fewer items, HTTP 200) [P12]: a weak source, but consistent with the above. **T25.**
- What keeps upkeep low in tks: data-driven operation variants with automatic fallback and breakers (igs's registry), every empty answer counted as a failure, parsers that tolerate missing fields and report them, a canary that checks every variant daily (M8), alerts on empty answers and falling item counts, and the browser signer running TikTok's current script rather than a copy of an old one.

### 2.8 Dependencies
Our own code (igs's scraping layer); `bogdanfinn/tls-client` (already pinned in igs) for Chrome fingerprints, with the fingerprint profile kept current (T5); Chromium (Playwright's build or the distribution package) only for the signer and the bootstrap fallback; the owner's ISP proxies.

### 2.9 Licences of the projects read
Read for behaviour only; tks copies no code from any of them (build brief §7). Their licences, as each repository declares them [W1]–[W13]: yt-dlp Unlicense; TikTok-Api MIT; carcabot/tiktok-signature MIT; Douyin_TikTok_Download_API Apache-2.0; go-tiktok BSD-3-Clause; tiktok-cli Apache-2.0; gallery-dl GPL-2.0-only; TikTokDownloader GPL-3.0; **pytok PolyForm-Noncommercial-1.0.0** (not usable commercially in any form). A GPL or non-commercial licence rules out copying even small pieces into tks.

---

## 3. Other technical routes

### 3.1 A headless browser
Three ways to use one:
- **Bootstrap only:** render a page once per identity to obtain cookies when plain HTTP is refused (T3), then send calls by plain HTTP. igs's browser fallback does this already (capped share of page loads).
- **Signer:** keep a page open and run calls from inside it (T7), or take the signed URL it produced and send it by plain HTTP (T27). This is the recommendation's second half.
- **Everything (the fallback):** every call from inside pages. Same coverage as §2.5; TikTok-Api works this way [W4], and carcabot's signer service keeps a pool of such browser identities, retiring "burnt" ones [W9]. pytok drives a hardened Firefox and reports that anonymous sessions "barely work any more" for it, so it now uses logged-in accounts, which tks never does [W6].

| | Bootstrap only | Signer | Everything |
|---|---|---|---|
| Coverage | as §2.5 | as §2.5 | as §2.5 |
| Extra cost | one render per identity (seconds of CPU, ~1–3 MB with media blocked) | browser time per signed call; pages per identity unless T27 | ~200–400 MB of memory per live page; every call pays browser overhead |
| Block risk | lower than plain HTTP on page loads (a real browser) | same as plain HTTP for the calls | automation flags (CDP, headless) are themselves detectable |
| Breakage | low | low (TikTok's current script) | low for signing; page-flow changes break navigation |
| Dependencies | Chromium | Chromium | Chromium at fleet scale |

### 3.2 The mobile app API
- The Android and iOS apps call hosts such as `api16-normal-c-useast1a.tiktokv.com` and `api22-normal-c-alisg.tiktokv.com` (`/aweme/v1/...` paths). yt-dlp keeps an app-API route beside its web route, configured with a host and "app info" that includes an install id (`iid`) obtained from a registered app install [W1][W14]. In June 2024, yt-dlp's maintainers moved single-video extraction to `/aweme/v1/multi/aweme/detail/` after the older feed endpoint stopped working, found by watching the Android app's traffic [W15].
- The app API needs a registered device and install, and requests signed by code inside the app's native libraries, which TikTok changes with app releases. Its fields are richer (exact counts, more flags), but every part of it is undocumented and changes without notice.
- **Assessment:** covers the objective in principle; history depth as the web; block risk tied to device registration; **high upkeep**; depends on reverse engineering the app, which is the weakest position for the legal review (§5). **Rejected.**

### 3.3 Embeds and oEmbed
- **oEmbed** (`GET https://www.tiktok.com/oembed?url=<post link>`) is documented by TikTok and returns `title` (the caption), `author_name`, `author_url`, `thumbnail_url` and size, and the embed `html`; **no counts, no time, no comments**, and only for a link already known [O17] *(snippet)*. No rate limit is documented.
- **The Embed Player** (`/player/v1/<id>`) is a display widget with player parameters and `postMessage` controls; it returns no data [O18] *(snippet)*. The **creator embed** (`/embed/@<username>`) carries a JSON state (`__FRONTITY_CONNECT_STATE__`) with the account and a list of its recent posts; yt-dlp uses it as a fallback to find an account's `secUid` [W1]. Whether it carries counts is unverified (T26, recording R7).
- **Assessment:** does not meet the objective (no counts in oEmbed, no full post list, no comments). Useful as a cheap check that a post still exists, and the creator embed as a fallback way to resolve an account. **Supplementary only.**

---

## 4. Official APIs and third-party providers

### 4.1 Research API (with the VCE and the EU's DSA Article 40 route)
**Usable by us? No.**
- **Eligibility:** researchers at non-profit academic institutions in the US, EEA, UK or Switzerland, independent of commercial interests [O1] *(snippet)*; TikTok "hopes to expand" [O2] *(snippet)*. An ethics approval is required [O3] *(snippet)*. TikTok says it typically answers within 4 weeks [O1]; a Slovak non-profit institute needed 21 months to get working access [O4].
- **The VCE** (for non-academic not-for-profit bodies) returns aggregated results only, 5,000 records a day in its test stage [O1] *(snippet)*.
- **DSA Article 40:** for researchers independent of commercial interests (public data) or vetted researchers at research organisations studying EU systemic risks [O5]. In October 2025 the European Commission preliminarily found TikTok in breach of its duty to give researchers access, citing "burdensome procedures" and "partial or unreliable data" [O6].
- **What it returns:** user info (display name, bio, avatar, verified, follower, following, likes and video counts), video queries (id, time, username, region, description, music, like, comment, share and view counts, hashtags, voice-to-text, and more), and up to the top 1,000 comments per video with reply counts [O7][O8][O9] *(snippets)*.
- **Limits:** 1,000 requests and up to 100,000 records a day; at most 30 days per video query; new videos can take up to **48 hours** to appear and counts up to **10 days** to refresh [O2][O10] *(snippets)*. That alone fails the 3-hour target.
- **Terms:** non-commercial use only; refresh data at least every 15 days and delete what is no longer available; give TikTok a copy before publication [O1][O11].
- **Data quality:** independent studies found missing videos (about 1 in 8 of 260,000 donated links returned nothing), undercounted views and shares, and server errors on historical queries [O12][O13][O14][O15].

### 4.2 Display API and Business Accounts API
**Usable by us? Only for accounts that log in to our app and authorise it** (for example a client brand's own account).
- The Display API returns the authorising user's profile (with `user.info.stats`: follower, following, likes and video counts) and their public videos with like, comment, share and view counts, 20 per page; no comments; 600 requests a minute per endpoint [O16][O19] *(snippets)*.
- The Business Accounts API adds comments and replies on the authorising business account's own videos [O19].
- Neither reaches third-party accounts. Not a route for monitoring media and public figures.

### 4.3 Commercial Content API
**Usable by us? No.** It covers ads and labelled branded content shown in Europe only, for approved researchers and professionals [O20][O21] *(snippets)*. Iraq is not covered.

### 4.4 Others
TikTok One / Creator Marketplace API (partners only, enrolled creators' audience data) [O22]; Data Portability API (a user exports their own data, EEA and UK) [O23]: neither applies.

### 4.5 Third-party data providers
All are paid; none was called. They differ most in **what they bill**: per call, per daily unit, or per result row. Our workload re-reads the same posts and comment threads many times, so per-call pricing is 10 to 30 times cheaper than per-result pricing.

| Provider | Pricing (as published or reported, 2026) | Covers profile / posts / counts / comments / replies / backfill | Notes |
|---|---|---|---|
| **ScrapeCreators** | credit packs that do not expire: $47 for 25,000 credits ($1.88 per 1,000), $497 for 500,000 ($0.99 per 1,000); 1 credit per call [P1][P2] | yes / yes / yes / yes / yes [P3] / yes (`max_cursor`) [P4] | says it collects logged-out public data only [P5]; public status page with incidents [P6] |
| **TikHub** | $0.001 per call, up to 50 % volume discount; failed calls free [P7] | yes / yes / yes / yes / unconfirmed / yes | opaque operator; mixed reviews [P8] |
| **TikAPI** | $29 / $79 / $189 a month for 300 / 2,000 / 10,000 calls a day [P10] | yes / yes / yes / yes / reported / yes | daily caps limit scale and backfills |
| **EnsembleData** | daily units: $100 / $200 / $400 / $1,400 a month for 1,500 / 5,000 / 11,000 / 50,000 units a day; posts 10 per unit, comments 30 per unit [P11] | yes / yes (`oldest_createtime`) / yes / yes / yes / yes | warns that pinned posts are often not the newest [P11] (our T10) |
| **Apify** (clockworks actors) | per result: from $1.70 per 1,000 posts; comments $0.50–$1.25 per 1,000 by plan, replies supported [P13][P14] | yes / yes / yes / yes / yes / yes | community actors, no SLA; changelogs show "0 videos" fixes [P15] |
| **Bright Data** | per record: $1.50 per 1,000, or $499 a month for 384,000 then $1.30 [P16] | yes / yes / yes / yes / reply counts / yes | strongest compliance posture; expensive for re-polling |
| Others | SociaVault, LamaTok, Lobstr.io, Data365, RapidAPI resellers [P12][P17][P18] | similar | more expensive per call, or sales-gated, or batch-only |

**Cost for our workload** (calls per account per day from §2.6; per-call providers at ~$0.001 per call):

| Accounts | Our tool (proxies + compute; T20 decides) | Per-call provider (typical account, 35–95 calls/day) | Per-result provider |
|---|---|---|---|
| 300 | a handful of ISP IPs and one small machine | about $0.04–$0.10 per account per day: **$300–$850 a month** | 10–30 × more |
| 10,000 | tens to a few hundred IPs | **$10k–$29k a month** | — |
| 300,000 | 1,800–6,000 IPs (§2.6) | **$300k–$850k a month** (a lean 15-call workload: about $135k) | — |

With a leaner workload (15 calls per account per day, comments read once), ScrapeCreators' published prices give about $0.015 per account per day, and a one-off 365-day backfill (about 1,135 calls) about $1.13 per account [P1][P2]. Whatever the workload, providers win on build time at small scale and lose by an order of magnitude or more at large scale, and they add a dependency: their terms, their outages, silent drops, and our account list sent to a third party (a processor under data-protection law).

---

## 5. Legal and terms-of-service notes for the legal team

A factual briefing to help counsel, **not legal advice**. The research could verify only a few legal sources in this session (search snippets; law-firm and court sites were not reachable). Items marked **[verified]** were seen in a source; items marked **[to verify]** are well-known citations given so counsel can pull the primary text, and were **not** checked in this session.

**What tks does, as designed:** it reads only public pages and the calls TikTok's own web app makes for a logged-out visitor; it never logs in or creates accounts; it paces like a person; it records captchas and stops using that identity instead of solving them; it does not re-implement TikTok's signatures (signed calls run TikTok's own script in a browser page) and does not compute TikTok's JavaScript challenge outside a browser; it collects account details, public posts with their counts, and public comments, not follower lists; it keys people by an HMAC of their user id and keeps personal data behind an access scope (with an optional pseudonymised mode); **by the owner's choice (2026-11-01) it deletes nothing it collects and erases no one**: no erasure requests, no suppression, no retention period, every comment of every post, and commenters' `predicted_age_group` (TikTok's inference of their age band) stored (ADR 0007 §14); **it sends no production traffic until legal signs off.**

### 5.1 TikTok's terms
- **[verified]** A terms-tracking service records a TikTok Terms of Service provision prohibiting automated extraction or scraping of TikTok data or content without written approval, naming the **"TikTok USDS Joint Venture"** (the entity running TikTok's US service since January 2026), which suggests the US terms [L1] *(snippet)*. TikTok's Advertising Terms separately bar automated scripts that collect information [L1] *(snippet)*.
- **[to verify]** TikTok's global terms are recalled to forbid using "automated scripts to collect information from or otherwise interact with the Services" and to "collect or harvest any personally identifiable information, including account names", and to say that accessing or using the services means accepting the terms (aimed at binding visitors without an account). TikTok publishes separate terms for the EEA, the UK, the US and other regions, with different contracting entities.
- **Official APIs** forbid commercial use (Research API) [O1], so they offer no compliant alternative for this purpose.

### 5.2 Case law on collecting public data without logging in
- **[verified]** *hiQ Labs v. LinkedIn*: the Ninth Circuit (April 2022) held that breaching a public website's user agreement is not, by itself, access "without authorization" under the US Computer Fraud and Abuse Act. The district court later (4 November 2022) found LinkedIn's anti-scraping terms enforceable as a contract, where hiQ had used fake logged-in profiles; the case ended with a consent judgment (USD 500,000, deletion of the data, an injunction) entered on 8 December 2022, which sets no precedent [L2][L3][L4][L5].
- **[to verify]** *Van Buren v. United States*, 593 U.S. 374 (2021): "exceeds authorized access" is a gates-up-or-down question about areas of a computer one may not enter; footnote 8 leaves open whether contract limits count.
- **[to verify]** *Meta Platforms v. Bright Data*, No. 3:23-cv-00077-EMC (N.D. Cal., 23 January 2024): Meta's terms did not prohibit logged-off scraping of public data, because Bright Data was not acting as a "user"; Meta then dropped its remaining claims.
- **[to verify]** *X Corp. v. Bright Data*, No. 3:23-cv-03698-WHA (N.D. Cal., 9 May 2024): X's contract claims were largely preempted by copyright law; dismissed with leave to amend.
- **[to verify]** *Ryanair v PR Aviation*, CJEU C-30/14, ECLI:EU:C:2015:10 (2015): where a database has neither copyright nor the sui generis right, its maker may restrict its use by contract, subject to national law.
- **Not searched:** whether TikTok or ByteDance has sued a scraper or data provider. A docket search is needed before anyone says "none".

### 5.3 Data protection
- **[to verify]** If the GDPR or UK GDPR applies: legitimate interest (Art. 6(1)(f)) with a necessity and balancing test; the CJEU held in *KNLTB* (C-621/22, 4 October 2024) that a commercial interest can be legitimate. Art. 14 requires informing people whose data was not collected from them, within a month, unless that is impossible or disproportionate (Art. 14(5)(b)), which requires other measures such as a public notice; the Polish authority fined Bisnode in 2019 for relying on that exception for scraped register data. Comments can reveal political or religious views (Art. 9).
- **[to verify]** **Territorial scope (Art. 3):** the GDPR applies to processing in the context of an EU establishment, wherever the people are; without one, only to people who are in the EU and are offered goods or services or monitored (EDPB Guidelines 3/2018). A company with no EU or UK establishment collecting data on people in Iraq is generally outside it, except for any EU-located people in the data.
- **[to verify]** Regulators on scraping: the joint statement of 12 data-protection authorities (24 August 2023) and its concluding statement (28 October 2024); the Dutch authority's 2024 guidance that scraping by private parties is "almost always" unlawful; the CNIL's 2025 recommendations, which treat robots.txt and CAPTCHAs as signals of objection to respect; EDPB Opinion 28/2024 (17 December 2024), which says public availability alone does not create reasonable expectations; Clearview AI fines in Italy, Greece, France (EUR 20 million each, 2022) and the Netherlands (EUR 30.5 million, 2024).

### 5.4 Iraq and the Kurdistan Region
- **[to verify]** No comprehensive data-protection law is known to be in force in federal Iraq; the 2005 Constitution protects privacy (Art. 17) and communications (Art. 40). Draft cybercrime laws (2011, 2019–20, 2023) were withdrawn after criticism; the Kurdistan Region has Law No. 6 of 2008 on misuse of telecommunication devices. In 2023 the Ministry of Interior opened the "Balegh" platform for reporting "indecent" content, and TikTok creators were prosecuted under the Penal Code (No. 111 of 1969). Local counsel is needed.

### 5.5 Anti-bot measures
- **[to verify]** US anti-circumvention law (17 U.S.C. §1201) and its EU counterpart (Directive 2001/29/EC, Art. 6) protect technical measures that control access to copyrighted works; no source found in this session says whether a website's bot detection, request signatures or CAPTCHAs count. The CNIL treats CAPTCHAs as an objection signal (above).
- tks's design keeps clear of the most exposed practices: it does not solve captchas, does not re-implement signatures, and does not compute the JavaScript challenge outside a browser. Whether running TikTok's own script in a browser page, and the cheaper form of sending a page-signed URL by plain HTTP (T27), are acceptable is the first question for counsel.

### 5.6 Providers
A provider that collects for us would be our processor (or an independent controller), and its collection methods become ours to justify [P5]. Sending it our account list is itself a disclosure of personal data.

### 5.7 Open points for counsel
1. Do TikTok's terms bind tks's logged-out collection in the jurisdictions that matter (the company's, the servers', the people's), and which regional terms apply to a visitor from the droplet's or the proxies' country?
2. Is running TikTok's own script in a browser page to obtain request signatures acceptable, and is sending the page-signed URL by plain HTTP (T27) different in kind? Is a browser passing TikTok's JavaScript challenge acceptable?
3. Which data-protection law applies to Iraqi accounts and commenters (and to any EU-located ones); the legal basis; how Art. 14 duties or local equivalents are met; retention periods; whether a DPIA is needed.
4. Whether public figures, brands and media accounts may be treated differently from commenters (private individuals) in scope, retention or pseudonymisation.
5. Whether the probe and Phase 0 runs (small volumes, collected data kept privately off git) need sign-off before they run, or only production does.
6. Which proxy country to use, given that TikTok's US service is now run by a separate entity under separate terms.
7. **The owner's choice to keep everything (2026-11-01):** whether collecting and keeping all data for good, with no way to erase a person or an account on request and no retention period, is acceptable under the law that applies (GDPR Art. 5(1)(e) storage limitation, Art. 17 erasure and Art. 21 objection, if it applies; local equivalents), and whether storing TikTok's inference of commenters' age band is (it can reveal minors). If counsel requires erasure or retention, both can be brought back: the code of M6b's eraser is in the history (PR #21), and `tt.retention` turns each retention step back on. Downloaded media files (videos, photos, avatars: off by default, D5) are kept until another service the owner provides deletes them (owner, 2026-11-02); tks itself deletes none, so their retention is that service's.

---

## 6. Comparison

| Method | Covers (objective §1–4) | History depth | Calls and bytes per account per day | Proxy cost | Block and captcha risk | Breaks how often; upkeep | Dependencies | Legal and terms |
|---|---|---|---|---|---|---|---|---|
| **Website, plain HTTP + browser signer** (recommended) | all: the recordings show every part served to a logged-out browser, comments and replies included | 365 days likely (T11) | 15–530 calls; 0.3–10 MB (§2.6) | ISP per IP; T20 decides | medium: TLS and header checks, IP reputation, JavaScript challenge, captchas (T5, T6, T28) | several changes a year (T25); low with variants, canary and TikTok's own script | igs's code, tls-client, Chromium for signing | terms forbid scraping; US logged-out case law favourable; data-protection questions (§5) |
| **Headless browser for everything** (fallback) | as above | same | same calls; more bytes (page assets) | same IPs; more bandwidth | medium; automation is detectable | low for signing; page-flow changes | Chromium at fleet scale (~200–400 MB per page) | as above |
| **Mobile app API** | all, richer fields | same | similar | similar | device-registration based | high: app releases change signing | reverse-engineered app internals | weakest: app reverse engineering |
| **Embeds / oEmbed** | caption, author, cover; recent posts (creator embed) | per known link | 1 call per post | low | low (documented) | low | none | documented by TikTok |
| **Research API** | most, but lagged 48 h to 10 days | archive | 1,000 calls a day for the whole project | none | none | low | approval | not eligible; non-commercial |
| **Display / Business API** | own or authorising accounts only | full for those | — | none | none | low | each account's consent | fine, but not our use case |
| **Commercial Content API** | EU ads only | from 2022 | — | none | none | low | approval | not our use case |
| **Per-call provider** | all advertised, replies included | provider's depth | billed per call | none for us | the provider's problem | the provider absorbs it; silent drops possible | the vendor | its methods become ours to justify |
| **Per-result provider** | all advertised | provider's depth | billed per row | none for us | the provider's problem | as above | the vendor | as above |

---

## 7. Recommendation and fallback

1. **Build our own tool on the logged-out website** (D11), starting with the probe (M1) so the evidence arrives before the crawler is built. Unsigned plain HTTP wherever TikTok accepts it (the `creator` list call first); TikTok's own script, in a capped browser pool, where it does not.
2. **Fallback: the browser for everything**, if the probe shows plain HTTP refused for bootstrap or signed URLs bound to their page. The code above the client does not change; the cost per call does.
3. **Comments are served to a logged-out browser** (recordings, 2026-10-31). If the probe shows a crawler cannot get them even through the browser signer, the owner decides between dropping objective §3 for now and a provider for comments only (D4), before the crawler is built.
4. **Decide at G1, after the probe**, with the measured answers to T3, T4, T7, T12, T20 (first look), T27 and T28, and the cost per account per day projected to 300, 10,000 and 300,000 accounts. If our tool cannot work, the owner may choose a per-call provider, knowing the cost curve in §4.5.
5. **Never re-implement TikTok's signatures, never solve captchas, never log in.**

---

## 8. Sources

All seen on 2026-10-31. *(snippet)* = read from a search-engine snippet, because the page is on a TikTok host (not fetched) or was blocked from this session. For open-source projects, the claims rest on their merged code, commit history and issues as read on that date.

**Website (W)**
- [W1] yt-dlp, TikTok extractor (Unlicense): https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/extractor/tiktok.py (last changed 2026-08-19)
- [W2] yt-dlp, the extractor's commit history and issues: https://github.com/yt-dlp/yt-dlp/commits/master/yt_dlp/extractor/tiktok.py ; issues https://github.com/yt-dlp/yt-dlp/issues/8620 , /12574 , /14031 , /15418 , /17393 , /17500 , /17604 ; pull request https://github.com/yt-dlp/yt-dlp/pull/17437
- [W3] gallery-dl, TikTok extractor (GPL-2.0-only): https://github.com/mikf/gallery-dl/blob/master/gallery_dl/extractor/tiktok.py ; issues https://github.com/mikf/gallery-dl/issues/7246 , /8812
- [W4] TikTok-Api by David Teather (MIT): https://github.com/davidteather/TikTok-Api
- [W5] TikTok-Api issues: https://github.com/davidteather/TikTok-Api/issues/1097 , /1255 , /1267 , /1274 , /1290 , /1311
- [W6] pytok (PolyForm-Noncommercial-1.0.0 and MIT): https://github.com/networkdynamics/pytok (README and commits of August–September 2026)
- [W7] Douyin_TikTok_Download_API (Apache-2.0): https://github.com/Evil0ctal/Douyin_TikTok_Download_API (TikTok parameters, endpoints, parser, signing notes and fixtures, September–October 2026)
- [W8] justscrapeme, TikTok web reverse-engineering notes (a vendor's README; version history of TikTok's signing script): https://github.com/justscrapeme/tiktok-web-reverse-engineering
- [W9] carcabot, tiktok-signature (MIT): https://github.com/carcabot/tiktok-signature
- [W10] go-tiktok (BSD-3-Clause): https://github.com/go-tiktok/tiktok
- [W11] HackerNoon, "What happens when you decompile TikTok's web SDK": https://hackernoon.com/what-happens-when-you-decompile-tiktoks-web-sdk-this
- [W12] TikTokDownloader (GPL-3.0): https://github.com/JoeanAmier/TikTokDownloader
- [W13] tiktok-cli (Apache-2.0): https://github.com/tamnd/tiktok-cli
- [W14] yt-dlp README, TikTok extractor arguments (`api_hostname`, `app_info`): https://github.com/yt-dlp/yt-dlp/blob/master/README.md
- [W15] yt-dlp issue #10213, 2024-06-19: https://github.com/yt-dlp/yt-dlp/issues/10213

**Official APIs (O)**
- [O1] https://developers.tiktok.com/products/research-api *(snippet)*
- [O2] https://developers.tiktok.com/doc/research-api-faq *(snippet)*
- [O3] https://developers.tiktok.com/doc/about-research-api/ *(snippet)*
- [O4] KInIT, "When research APIs close the door…", 2025: https://kinit.sk/when-research-apis-close-the-door-when-you-finally-get-it-it-is-not-what-you-have-asked-for/
- [O5] European Commission, FAQs on DSA data access for researchers, 2025-07-03: https://algorithmic-transparency.ec.europa.eu/news/faqs-dsa-data-access-researchers-2025-07-03_en
- [O6] SCL, "European Commission finds TikTok and Meta in breach of their transparency obligations under the DSA", 2025-10-24: https://www.scl.org/european-commission-finds-tiktok-and-meta-in-breach-of-their-transparency-obligations-under-the-digital-services-act/
- [O7] https://developers.tiktok.com/doc/research-api-specs-query-user-info *(snippet)*
- [O8] https://developers.tiktok.com/docs/en/research-api-specs-query-videos *(snippet)*; Cybersecurity for Democracy's client, `tiktok-research-api-helper` 0.1.3: https://pypi.org/project/tiktok-research-api-helper/
- [O9] https://developers.tiktok.com/docs/en/research-api-specs-query-video-comments *(snippet)*
- [O10] https://developers.tiktok.com/doc/research-api-codebook *(snippet)*
- [O11] Tech Policy Press, "TikTok's API guidelines are a minefield for researchers", 2023-02-22: https://techpolicy.press/tiktoks-api-guidelines-are-a-minefield-for-researchers
- [O12] AI Forensics, 2025-06-11: https://arxiv.org/abs/2506.09746
- [O13] Corso, Pierri, De Francisci Morales, February 2024: https://arxiv.org/abs/2402.13855
- [O14] Tech Policy Press, 2024-09-24: https://techpolicy.press/-researcher-data-access-under-the-dsa-lessons-from-tiktoks-api-issues-during-the-2024-european-elections
- [O15] Steel et al., April 2025: https://arxiv.org/abs/2504.13279
- [O16] https://developers.tiktok.com/docs/en/display-api-get-started and https://developers.tiktok.com/doc/tiktok-api-v2-rate-limit *(snippets)*
- [O17] https://developers.tiktok.com/docs/en/embed-videos *(snippet)*
- [O18] https://developers.tiktok.com/docs/en/embed-player *(snippet)*
- [O19] `python-tiktok` 0.1.12 (Business and Display API client): https://pypi.org/project/python-tiktok/
- [O20] https://developers.tiktok.com/docs/en/commercial-content-api-query-ads *(snippet)*
- [O21] TechCrunch, 2023-07-20: https://techcrunch.com/2023/07/20/tiktok-ads-transparency
- [O22] TechCrunch, 2021-08-31: https://techcrunch.com/2021/08/31/tiktoks-new-creator-marketplace-api-lets-influencer-marketing-companies-tap-into-first-party-data
- [O23] TechCrunch, 2024-03-04: https://techcrunch.com/2024/03/04/tiktok-rolls-out-data-portability-api-ahead-of-europes-dma-regulatory-deadline

**Providers (P)** (prices from snippets of provider pages and third-party comparisons; confirm before budgeting)
- [P1] https://www.ugcroster.com/blog/scrape-creators-pricing-explained
- [P2] https://www.xpoz.ai/blog/guides/tiktok-api-pricing-2026/
- [P3] https://scrapecreators.com/tiktok/endpoints/comment-replies
- [P4] https://scrapecreators.com/tiktok/endpoints/profile-videos
- [P5] https://scrapecreators.com/blog/web-scraping-legal
- [P6] https://scrapecreators.com/status
- [P7] https://tikhub.io/pricing
- [P8] https://dk.trustpilot.com/review/tikhub.io
- [P9] https://apify.com/scraper_creator/tiktok-video-scraper/changelog.md (2026-10-19 entry)
- [P10] https://www.tikapi.io/terms/
- [P11] https://ensembledata.com/tutorials/how-to-scrape-tiktok-user-posts-and-videos ; https://www.blotato.com/blog/tiktok-api-pricing
- [P12] https://www.xpoz.ai/blog/comparisons/best-tiktok-api-alternatives-2026/ ; https://scrapebadger.com/blog/tiktok-scraping-apis-in-2026-the-complete-deep-guide
- [P13] https://use-apify.com/docs/best-apify-actors/best-tiktok-scrapers (prices checked 2026-09-09)
- [P14] https://apify.com/clockworks/tiktok-comments-scraper
- [P15] https://apify.com/clockworks/tiktok-scraper (changelog and issues)
- [P16] https://docs.brightdata.com/products/scrapers/tiktok/introduction
- [P17] https://docs.sociavault.com/api-reference/tiktok/comments ; https://www.lobstr.io/store
- [P18] https://data365.co/presentation

**Legal (L)** (only these were seen in this session; the [to verify] citations in §5 are given for counsel to pull)
- [L1] ConductAtlas, TikTok Terms of Service provisions "Prohibition on Scraping and Data Extraction" and "Prohibition on automated scraping of platform data", and TikTok Advertising Terms "Prohibition on Automated Scripts to Collect Information" (2026) *(snippets)*: https://conductatlas.com/platform/tiktok/tiktok-terms-of-service/provision/CA-P-002457/prohibition-on-scraping-and-data-extraction ; https://conductatlas.com/platform/tiktok/tiktok-terms-of-service/provision/CA-P-018595/prohibition-on-automated-scraping-of-platform-data/ ; https://conductatlas.com/platform/tiktok-ads/tiktok-advertising-terms/provision/CA-P-069785/prohibition-on-automated-scripts-to-collect-information/
- [L2] Wikipedia, "hiQ Labs v. LinkedIn": https://en.wikipedia.org/wiki/HiQ_Labs_v._LinkedIn *(snippet)*
- [L3] Morgan Lewis, December 2022: https://www.morganlewis.com/blogs/sourcingatmorganlewis/2022/12/linkedin-v-hiq-landmark-data-scraping-suit-provides-guidance-to-data-scrapers-and-web-operators *(snippet)*
- [L4] National Law Review, "hiQ and LinkedIn reach proposed settlement in landmark scraping case": https://natlawreview.com/article/hiq-and-linkedin-reach-proposed-settlement-landmark-scraping-case *(snippet)*
- [L5] ZwillGen, "hiQ v. LinkedIn wrapped up": https://www.zwillgen.com/alternative-data/hiq-v-linkedin-wrapped-up-web-scraping-lessons-learned/ *(snippet)*
