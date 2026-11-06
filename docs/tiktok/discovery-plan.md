# Finding and adding accounts: the plan

**Status: approved by the owner on 2026-11-04, with the decisions in §1.**
- The build starts when M8 merges. Building it alongside M8 would change the same files: migrations, config, the scheduler, the API and `build-status.md`.
- **Built so far:** §17 (M8b); M10a (2026-11-05): sources, labels, blocks, paced admission, the lighter checks, the keyword list loaded, and bulk import (part A), in [ADR 0007 §18](adr/0007-tks-data-model-and-contracts.md#18-settled-in-m10a-sources-labels-admission-and-bulk-import) and [`tks-import.md`](../runbooks/tks-import.md). M10 was split in two: M10a, then M10c (2026-11-05): the entry check, the dormant list and the Iraq score (§3, §4), in [ADR 0007 §19](adr/0007-tks-data-model-and-contracts.md#19-settled-in-m10c-the-entry-check-the-dormant-list-and-the-iraq-score) and [`tks-discovery.md`](../runbooks/tks-discovery.md); the sources that call it come with M11 to M13. M10b (2026-11-05): the C6 probe, `tks-spike discover` ([`tks-discovery-probe.md`](../runbooks/tks-discovery-probe.md)), with `fakett`'s synthetic discovery answers (§14); the calls not recorded yet are T36. M11 (2026-11-06): part B (§7), in [ADR 0007 §21](adr/0007-tks-data-model-and-contracts.md#21-settled-in-m11-discovery-part-b) and [`tks-discovery.md`](../runbooks/tks-discovery.md#part-b-from-what-tks-already-collects). M14 (2026-11-06): the review page (§11), in [ADR 0007 §22](adr/0007-tks-data-model-and-contracts.md#22-settled-in-m14-the-discovery-review-page) and [`tks-discovery.md`](../runbooks/tks-discovery.md#the-review-page). M13 (2026-11-06): part D (§9), the following and followers lists, in [ADR 0007 §23](adr/0007-tks-data-model-and-contracts.md#23-settled-in-m13-discovery-part-d) and [`tks-discovery.md`](../runbooks/tks-discovery.md#part-d-following-and-followers-lists).

**What tks does today** (checked in the code, 2026-11-04; see [`build-brief.md`](build-brief.md) and [`build-status.md`](build-status.md)):
- It follows only the accounts it is given, one at a time, through `POST /v1/accounts`.
- It keeps them up to date: new posts within 3 h, counts, comments, account details and renames.
- It has no bulk import and no discovery. The brief's D1 left search, hashtag and sound pages and follower lists out of scope; the owner has now brought them in (§1).

---

## 0. The plan on one page

**The goal** (owner, 2026-11-04): find and monitor **every public, active TikTok account that is Iraqi, run by someone from or in Iraq, or followed by an Iraqi audience**. Accounts are added **automatically**.

| Part | What it does | New TikTok traffic | Milestone |
|---|---|---|---|
| **Foundations** | Where each account came from, labels, paced admission, the **entry check** and the **dormant list** (§4), the Iraq score (§3), kill switches | None | **M10** |
| **A. Bulk import** | A file of accounts becomes registered accounts, with a report | None beyond each account's normal reads | **M10** |
| **B. From data tks collects** | Accounts mentioned, replied to, or commenting on monitored posts are checked and added | One list read per new account | **M11** |
| **C. TikTok's discovery pages** | Search by the Iraq keyword list, hashtags, sounds, the For You feed, related posts | Yes | **M12**, after C6 |
| **D. Who follows whom** | The following **and followers** lists of every monitored account, new ones included | Yes, the most of all | **M13**, after C6 |
| **Review page** | Discovered accounts, the dormant list, keywords, evidence, cost, imports | None | **M14** |

**Order:**
1. M8 (running; it also marks posts that are no longer public, §17);
2. **M10** and **M10b** (the C6 probe tool);
3. M9;
4. **M11**;
5. **C6**, the owner's probe: any time after M10b; nothing waits for it except M12 and M13;
6. **M12**, **M13**, **M14**;
7. **G2**, counsel.

One build session runs at a time (brief §7).

---

## 1. The owner's decisions (2026-11-04)

1. **What to find.** All accounts that are Iraqi, owned by someone from or in Iraq, or that the Iraqi audience is interested in. §3 turns this into a score.
2. **Add automatically, with no caps in production.** Caps are for testing. The traffic budget and the machines are then the only limits (§12).
3. **No follower minimum.** An account qualifies if it is **public** and has **at least one post in the last 3 months**.
   - A private or inactive account goes on a **dedicated list**, the dormant list (§4).
   - When it is seen again, it is **not checked again until 6 months or more** have passed.
4. **Following and followers lists**, of every monitored account, **new ones included** (§9).
5. **Keep everything** that is read (§10).
6. **Build it all now**, the review page included (§11), without disturbing the M8 session.
7. **No age filter.** TikTok's predicted age band is often wrong, so accounts are kept regardless of it.

**The search keywords are fbs's Iraq list** (`configs/discovery/iraq.yaml` in `Abdullah95k/securebrand-microservices`, 2026-11-03):
- 21 groups: the country, the Kurdistan Region, the 19 governorates and their cities;
- 317 terms in English (135), Arabic (97), Sorani (48) and Kurmanji (37);
- plus `off` terms: ambiguous spellings loaded disabled.

tks gets a copy in `configs/tt/discovery/iraq.yaml`.

**The owner recorded a logged-out search session from Iraq.** The findings are in [`har-findings-search-2026-11-04.md`](har-findings-search-2026-11-04.md):
- search answered 9 pages deep, with no login wall;
- it is signed by TikTok's script, as comments are;
- its results are full posts with their authors, but no post region;
- the For You feed served logged out from Iraq is a source too.

The recording isn't in git. The build session that needs it asks the owner for the link.

---

## 2. What is reused

fbs built this pattern for Facebook after tks was copied (ADR 0004 in fbs; `internal/discovery`, `internal/mentions`, migrations `00008_discovery` and `00009_mentions`). tks ports it, as it ported the rest (D7), adapting each part to TikTok:

| fbs part | What it does | In tks |
|---|---|---|
| `discovery.Normalize` | Folds Arabic, Sorani and Kurmanji spellings (أ إ آ → ا, ة → ه, ي ى → ی, ك → ک, tatweel, zero-width joiners, harakat, Arabic-Indic digits; Latin accents, `-` and `_`) | Copied as is: keyword matching, de-duplication, Iraq text signals (§3) |
| The keyword seed (`iraq.yaml`, `fbctl discovery keywords seed`) | Loads the keywords; loading again adds only new ones and never re-enables one someone turned off | `tksctl discovery keywords seed`, the same rules |
| Keyword discovery (`discovery_keywords`, `discovery_mentions`, scheduler, `search` stage) | Searches each keyword every hour for recent posts, records each hit, adds new posters, with a daily cap | Part C, with TikTok's search call; no cap in production |
| Mentions (ADR 0004) | The persister records the accounts that new posts and comments mention, and comment authors, **only for rows it inserts for the first time**. The leader counts them; a crawler stage checks each new one. | Part B, the same three steps |
| `mentions.recheck_after: 4380h` | A not-public account is checked again only after about 6 months | The dormant list (§4), for every source |
| `source` and `discovered_at` on the schedule | Where an account came from | `account_sources` (§5.1), with the evidence |

From tks itself:
- the `creator` list (plain HTTP, unsigned, 15 posts with `locationCreated` and the author's counts) **is the entry check** (§4);
- the browser signer pool signs search and the other calls (D11);
- the operations registry (switches per call), the budget, the canary and fakett.

---

## 3. What counts as Iraqi: the Iraq score

TikTok shows no country on an account. In the recordings, `region` is the viewer's country, not the account's. So tks scores each account from what it can read, and records **why**.

| Signal | Source | Strength |
|---|---|---|
| Recent posts made in Iraq (`locationCreated: IQ`) | The entry check's list read | **Strong**: one is enough |
| Bio or nickname names Iraq, the Kurdistan Region or an Iraqi place (the keyword list, normalized) | Account | **Strong** |
| An Iraqi phone number (`+964`, `00964`, `07xx…`) or a `.iq` link in the bio | Account | **Strong** |
| Captions or hashtags name Iraqi places (#العراق, #بغداد, #هەولێر, …) in recent posts | The entry check's posts | Medium; strong when in several posts |
| **Followed by** Iraqi accounts tks knows (§9) | Followers lists | Medium; strong when many |
| **Its commenters** are Iraqi accounts tks knows | Comments | Medium; strong when a large share |
| Mentioned by, or replied to by, monitored Iraqi accounts | Part B | Medium |
| Shown to viewers in Iraq: search results and the For You feed served with `region=IQ` | Part C, only from exits TikTok places in Iraq (§8.3) | Medium |
| Writes in Arabic or Kurdish (`textLanguage`) | Posts | **Weak**: never enough alone (Arabic spans the region; Sorani is also written in Iran) |

**Rules:**
- One strong signal qualifies an account. So do two different medium ones. Weak signals only add weight.
- The thresholds are settings. The review page (§11) shows each account's evidence.
- During testing the owner checks a sample of each kind ("is this Iraqi?") and the thresholds are tuned to what they say.

The score covers all three of the owner's groups:
- **an Iraqi account**: Iraqi text, phone, link or place;
- **owned by someone in Iraq**: posts made in Iraq;
- **the Iraqi audience is interested in**: Iraqi followers, commenters and viewers. This includes non-Iraqi accounts with a large Iraqi following.

---

## 4. The entry check and the dormant list

**Every account any source finds goes through one entry check:** one read of its `creator` list (15 newest posts).

| Result | What tks does |
|---|---|
| Public, a post in the last **90 days**, Iraq score passes | **Added** to monitoring automatically, with its sources and evidence (§5.1), at the admission pace (§5.3) |
| Public, a post in the last 90 days, Iraq score doesn't pass (yet) | Stored as **not Iraqi (yet)**. Its posts and author are kept (§10). New signals can raise the score at any time, with no new read. |
| **Private** | **Dormant list** |
| **No post in the last 90 days**, including accounts that never posted | **Dormant list** |
| Banned, missing (10221, 10202) | Dormant list |

**The dormant list** (`dormant_accounts`):
- It holds the account, the reason, what was read, when it was checked, and its next check date: **the check plus 180 days**.
- **Seen again before that date:** the new signal is counted, and nothing is read.
- **Seen after that date:** one entry check. If it now passes, it is added; if not, the next date moves 180 days on.
- **Not seen again:** never checked again. Discovery is driven by being found.

**Accounts already monitored that go quiet:**
- A **discovered** account that goes private, or has no post for 90 days, **moves to the dormant list**. Its checks stop and its data stays.
- **Accounts the owner registered** (API or import) are **never moved automatically**. They get the lighter checks of §5.4. This keeps the owner's own list as given; it's one setting if the owner wants otherwise.

Cheap pre-checks avoid most reads:
- Followers lists and search results already say whether an account is private.
- Followers lists may say how many posts an account has (C6 confirms). An account shown with 0 posts goes on the dormant list without a read.

---

## 5. Foundations (M10)

### 5.1 Where each account came from

`account_sources` records for every account:
- **how** it came: `api`, `import`, `mention`, `reply`, `commenter`, `search`, `hashtag`, `sound`, `feed`, `related`, `following`, `follower`;
- which import, keyword, or account led to it;
- when;
- the evidence and the Iraq score at the time.

An account keeps all its sources. `GET /v1/accounts/{account}` shows them, and `?source=` filters by them. Any account can always be explained and removed.

### 5.2 Labels

Free labels on a registration, per API client (`list:banks`, `import:2026-11-05`). They are set on import or by a source, and the API filters by them.

### 5.3 Paced admission

**Today** (checked): a new account is due at once, and its first job is a 365-day backfill. Backfills have their own lane, so they don't delay the 2 h 45 min checks. But nothing limits how many new accounts start in an hour. Thousands of new accounts at once would use the day's proxy data within hours.

**The plan:** `tt.admission`:
- **The rate is derived from the budget (D9):** as many new accounts an hour as the day's budget allows after monitoring.
- New accounts **wait in a queue**, in this order: the owner's own (API, import), then discovered accounts by Iraq score.
- **Checks of existing accounts never wait.**
- **Testing:** `tt.discovery.max_new_per_day` caps automatic additions.
- **Production:** `0` means no cap (owner's decision 2). The queue then grows as fast as discovery finds accounts.
- The API and the review page show the queue, its estimated drain time, and **the IPs and signer pages it would take to drain it in a day** (§12).

### 5.4 Lighter checks for the owner's accounts that go quiet

**Today** (checked): a private, banned or missing account is checked as often as an active one. The plan, `tt.freshness.inactive`:

| Status | Checked |
|---|---|
| Private | Daily for 7 days, then weekly |
| Banned or not found | Weekly for 30 days, then monthly |
| Active again | Back to normal from that read |

Discovered accounts don't need this, because they move to the dormant list (§4).

### 5.5 Identity

- An account is known by its **TikTok user id**. tks registers found accounts by **`secUid`**, which a rename doesn't change.
- **An account another client already monitors** gets this client's registration added, with no second schedule (`scheduleRef` matches by user id).
- **An account the owner removed** is never added again by discovery. Removal is recorded as a block on that client.

### 5.6 Switches and permissions

**Switches:**
- `tt.discovery.enabled` turns everything off.
- Each source can be turned off.
- Each keyword can be turned off (as in fbs).
- Each new TikTok call is an operation in the registry, **off until C6 shows it works**, switched with `PUT /v1/admin/operations/{name}`.

**Permissions:** the scopes `discovery:read` and `discovery:write`. Import uses `accounts:write`.

---

## 6. Part A: bulk import (M10)

```bash
tksctl accounts import accounts.txt --label list:banks --dry-run
```

Then the same command without `--dry-run`. The API equivalent is `POST /v1/accounts/import` (1,000 lines a call). Both end with a report:
- added;
- already monitored;
- duplicates in the file;
- not accepted, with line numbers and reasons;
- queued, with an estimated finish.

`tksctl accounts import --report <batch>` later lists the accounts that turned out private, inactive or missing.

**Accepted per line:**
- `username` or `@username`;
- a profile link (any `www.`, `m.`, `?…`);
- a video link (the account is taken from it);
- a `secUid`;
- `id:<user id>`, for accounts tks has read.

Or a CSV with `account`, `labels`, `interval`, `max_age_days` and `include_media` columns. `#` comments and blank lines are skipped.

| Case | What tks does |
|---|---|
| The same account twice (case, `@`, link) | Counted once |
| Already monitored by you | Unchanged, reported; new labels added |
| Monitored by another client | Your registration added; no second schedule |
| Hidden characters from copy and paste (zero-width spaces, right-to-left marks, a byte-order mark, Windows line ends, full-width `@`) | Removed before the line is checked |
| Capital letters | Lowercased |
| Not a possible username | Not accepted; line number and reason |
| Short links (`vm.tiktok.com/…`, `tiktok.com/t/…`) | "Not supported yet". Resolving one is a request to TikTok, so C6 checks how they redirect and M12 resolves them. |
| A missing account or a typo | Registered. The first read finds TikTok's code; the account gets the lighter checks (§5.4) and is listed in the report. |
| A private account | The same |
| 100,000+ lines | Sent in chunks of 1,000; admission pace; estimated finish |
| The command stops halfway | Run it again: it is idempotent, and finished lines are skipped |
| The file should be your whole list | `--disable-missing` removes your other registrations (the data stays). It runs only with that flag and always shows a dry run first. It refuses to remove more than 10 % of the list without `--force`. |
| Two imports at once | Each is its own batch |

---

## 7. Part B: from data tks already collects (M11)

**Sources:**
1. Accounts mentioned in **captions** and **bios** of monitored accounts.
2. Accounts a monitored account **replies to** in its comments.
3. Accounts mentioned **inside comments**.
4. **Every commenter** on monitored posts.

All four already carry the person's id and `secUid`, except bios, which carry only a username and need one profile read.

**How it runs** (fbs ADR 0004, ported):
1. **The persister records** the accounts a post or comment brings in, only for rows it inserts for the first time. That is the de-duplication: a re-read, an edit or a redelivered batch records nothing again. It is one write to an append-only table, in the same transaction.
2. **The scheduler leader counts them** into per-account totals (sources, distinct monitored accounts, first and last seen) and queues each **new** account for the entry check (§4). Accounts on the dormant list wait for their date.
3. **A crawler stage runs the entry check**, through the same identities, pacing and budget as every read.

| Case | What tks does |
|---|---|
| The same commenter on thousands of posts | Recorded each time (it is evidence of the audience), checked once |
| A commenter who is private, or has never posted | Dormant list, re-checked after 6 months only if seen again |
| A famous account mentioned by many | Added if its Iraq score passes. Its Iraqi audience counts, by the owner's decision 1. |
| An account already monitored | Its evidence grows; nothing is read |
| An account the owner removed | Never added again (§5.5) |
| Renamed between being seen and being checked | Found by user id, registered by `secUid` |
| Pseudonymized mode (D8) | tks keeps no ids of people in that mode, so B can't work: tks refuses to turn it on, with a clear message |
| Many new accounts a day | The checks queue; production has no cap; the budget sets the pace (§5.3) |

---

## 8. Part C: TikTok's discovery pages (C6, then M12)

### 8.1 Known from the owner's recording

- **Logged-out search works**, 9 pages deep with no login wall. It needs TikTok's signature, which tks gets from its signer pool.
- Its results are **full posts with their authors**, but **no post region**. That's why the entry check reads the account's own list.
- **The For You feed is served logged out**, with `region=IQ` from Iraq.

### 8.2 Sources

| Source | Call | What C6 must still answer |
|---|---|---|
| **Keyword search**, all 317 Iraq terms, plus any the owner adds | `/api/search/general/full/` (recorded) | From tks's exits: answered with a signature from the pool? How deep before a login wall? Same results as from Iraq? |
| **Account search** (the accounts tab) | `/api/search/user/full/` | Everything: not recorded yet |
| **Hashtag pages**: the Iraq terms as hashtags, and hashtags that Iraqi posts use | the tag page and its post list | Everything |
| **Sound pages**: sounds used by Iraqi accounts | the music page and its post list | Everything |
| **For You feed** | `/api/recommend/item_list/` (recorded) | What a tks exit gets; whether repeated reads bring new posts |
| **Related posts** on monitored posts | the post page's related list (seen in C2) | Answered unsigned? |
| **Account suggestions and reposts** on profiles | `/api/user/list/`, `/api/repost/item_list/` (seen in the first recording) | What `/api/user/list/` is |
| Short links in imports | `vm.tiktok.com` redirects | How they redirect |

**How it runs:**
- **Sources run on timetables.** Each keyword is searched every hour for recent posts, as in fbs. The For You feed and hashtag pages run every few hours, and sound and related pages as new ones are found.
- **Calls go through what monitoring already uses:** the same identities, pacing, budget and signer pool. Each call is a registry operation with its own rate, canary check and parser drift count. A broken call pauses only its source; monitoring carries on.
- **Every author found** goes through the entry check (§4).
- **Every post found is stored** (§10).

### 8.3 Exit country

**The country matters.** From Iraq, TikTok serves `region=IQ`. tks's exits are served `region: US` (C2).

**C6 compares the same Iraqi keywords, and the For You feed, from tks's exits.**
- If the results hardly change, nothing more is needed.
- If they change a lot, "what the Iraqi audience sees" needs exits that TikTok places in or near Iraq. That's a **proxy decision for the owner**, because the owner uses no providers beyond their own proxies.
- Setting `region=IQ` in the call while the exit is in the US would make tks say it is somewhere it isn't. It's not planned without counsel.

### 8.4 Cases

| Case | What tks does |
|---|---|
| Search asks to log in after some pages | Stops there and records it; never logs in (D3) |
| Refused signatures, empty answers, captchas (10000) | As for comments: one retry on another signer page, the rate controller slows down, the canary sees it, and the source pauses after repeated blocks |
| Different results each run | De-duplicated by post and user id; evidence adds up |
| A keyword that is also a common word | The keyword can be turned off (as fbs's `off` list) |
| A hashtag with millions of posts | Its pages are read in TikTok's order, a few pages a run. New authors keep coming over runs, and "new accounts per run" is shown. |
| A call changes after a TikTok update | The canary and parser drift catch it; only that source pauses |

---

## 9. Part D: who follows whom (C6, then M13)

**Built** (M13, 2026-11-06; [`tks-discovery.md`](../runbooks/tks-discovery.md#part-d-following-and-followers-lists)). C6 run 1 answered the open question below: both lists are served logged out, unsigned; a hidden following list answers 10222 and the profile says so in advance. A re-read takes the newest people down to those already stored, then goes deeper from where the last read stopped, up to `tt.discovery.follows.max_pages`: a large account's list fills in over days. How deep TikTok lets a logged-out visitor go is still open (T39).

**What is read:** for **every monitored account, including the ones discovery adds**, its **following** list and its **followers** list.

**How:**
- **Following lists:** read in full, then re-read weekly.
- **Followers lists:** TikTok serves the newest first, so each re-read takes only the new followers since the last one, daily.
  - The first read of a large account goes as deep as TikTok serves logged out, at low priority, filling in over days. An account with millions of followers costs hundreds of thousands of pages.
- **Every person in a list** passes the cheap pre-check (private, no posts) and then the entry check (§4).
- **The links themselves are kept** (`follows`: who, whom, first and last seen). They feed the Iraq score: followed by Iraqi accounts (§3).

**The open question:**
- On the website, follower lists may be shown only to logged-in visitors. Many accounts also hide their following list; each profile says so in `followingVisibility`.
- C6 measures what a logged-out visitor gets.
- **If TikTok shows these lists only to logged-in users, tks can't read them**, because D3 says logged out only. tks never logs in without the owner changing D3, which is a decision for the owner and counsel.

**Scale:**
- This source finds the most accounts by far.
- Most followers are personal accounts that never post. The pre-check sends them to the dormant list without a read, when the list says so.
- Even so, the following and followers lists of hundreds of thousands of accounts are billions of links. They are stored partitioned, like comments.

---

## 10. Storing everything found

By the owner's decision 5, everything read is kept, as D8 already says for monitored data:
- **Posts found** by search, hashtag, sound, the For You feed and related lists are stored as posts. `discovered_via` is `SEARCH` or `HASHTAG` (both reserved since ADR 0007), plus new add-only values: `SOUND`, `FEED`, `RELATED`.
- **Their authors** are stored as accounts, whether they are added, dormant or not Iraqi (yet).
- **The posts read by an entry check** are stored too.
- **The follow links** (§9) are stored.
- **Evidence and scores** are stored, with their history.

An account becomes **monitored** only when it qualifies. The rest is stored, not followed.

---

## 11. The review page (M14)

**Built** (M14, 2026-11-06): `/review/` on `tks-api` ([`tks-discovery.md`](../runbooks/tks-discovery.md#the-review-page)). Keyword hits count from part C's searches, so they stay 0 until M12.

A web page served by `tks-api`, behind the API's own keys and scopes. Personal data appears only with `pii:read`; actions are protected against cross-site forgery.

- **Accounts**:
  - discovered and added, filtered by source, keyword, label, Iraq score and date;
  - each with its evidence ("posts made in Iraq: 11 of 15", "followed by 37 monitored Iraqi accounts") and a **remove** button, which also blocks it from coming back.
- **Dormant list**: why each account is there and its next check date.
- **Not Iraqi (yet)**: accounts close to the threshold, to tune it.
- **Keywords**: on or off, add, hits per keyword.
- **Imports**: upload a file, the dry run, the report.
- **Cost and capacity**: requests, data and accounts added per source and per day; the admission queue; the IPs and signer pages needed (§12).

The API stays the source of truth: the page only calls it.

---

## 12. Scale and cost

Phase 0 measured, per 1,000 monitored accounts:
- 2 ISP IPs;
- 2 signer pages;
- 16.3 browser hours a day;
- 6.1 GB of proxy data a day;
- 174 GB of storage.

| Monitored accounts | ISP IPs | Signer pages | Proxy data a day | Storage |
|---|---|---|---|---|
| 100,000 | 200 | 200 | 610 GB | 17 TB |
| 1,000,000 | 2,000 | 2,000 | 6.1 TB | 174 TB |

Discovery adds its own reads on top:
- entry checks;
- searches;
- the follower and following lists, the largest part.

**What "no caps" means in practice:**
- tks adds every qualifying account and admits them as fast as the budget allows.
- When the budget is the limit, the queue grows and tks says what it would take to drain it.
- Without the budget, tks would outrun its IPs and get blocked, which would harm monitoring too. The budget stays, and the owner raises it as capacity grows.

**Iraq on TikTok may be millions of public, active accounts.** The design target was hundreds of thousands (D2). Before production goes past about 300,000 accounts, the database, Kafka and the crawler fleet need a load test at the larger size. This goes in M9's load testing and is a step in the ramp.

---

## 13. Privacy and legal (for counsel; G2)

- **Larger scope (D1).** Discovery reads search, hashtag and sound pages and the For You feed. Above all, it reads followers lists: millions of private people, kept for good (D8 and decision 5).
- **No age filter (decision 7).** Accounts are added whatever TikTok predicts about age, so minors' public accounts will be monitored if they qualify. Counsel should know this explicitly.
- **Every added account records why** (§5.1). The owner can remove any account, and it then stays out.
- **Unchanged:** no logging in (D3), no re-implemented signatures (D11).
- **D15 still holds:** B, C and D stay **off in production until counsel signs off (G2)**. A needs nothing new. **G2 signed off** (owner, 2026-11-05, relayed by the M6c session): the approval covers automatic adding with no age filter and following and followers lists.
- After M8 merges, [`methods.md`](methods.md) §5 gets a discovery section for counsel. Sending it early keeps G2 from holding up M12 and M13.

---

## 14. Testing (no live TikTok requests from build sessions)

- **fakett** gains synthetic search, hashtag, sound, feed, related, suggestion, repost, following and followers answers. It can also produce login walls, empty answers, captchas and refused signatures on demand.
- **Golden fixtures for search and the feed** come from the owner's recording, scrubbed by tks's scrubber (no names, ids or text; D13). The others come from C6's bundle.
- **End-to-end tests:**
  - importing 10,000 accounts keeps the admission pace and leaves existing accounts' checks on time;
  - each source leads to an entry check, then the account is added or goes on the dormant list;
  - a dormant account isn't checked again within 180 days;
  - removed accounts stay out;
  - the test caps hold, and `0` means no cap;
  - the switches work;
  - pseudonymized mode refuses B, C and D.

---

## 15. Milestones and order

| # | What | Needs from the owner |
|---|---|---|
| M8 | (running) Canary and signer operations, plus posts no longer public (§17) | — |
| **M10** | Foundations (§5), the entry check and dormant list (§4), the Iraq score with the text, place and contact signals (§3), bulk import (§6), and `iraq.yaml` loaded. Split: **M10a** (§5, §6, `iraq.yaml`; done) and **M10c** (§3, §4) | — |
| **M10b** | The C6 probe tool: search (both tabs), hashtag, sound, feed, related, user lists, following and followers, short links, from tks's exits, capped. Plus the runbook. **Built** (2026-11-05): `make tks-discover`, [`tks-discovery-probe.md`](../runbooks/tks-discovery-probe.md); it also reads posts no longer listed since C3 (T34) and the authors' regions (§8.3) | — |
| M9 | Deploy and operations (as planned), with the load test at the larger size (§12) | — |
| **M11** | Part B, ported from fbs's mentions | — |
| **C6** | Owner: run the discovery probe on the droplet (about an hour, capped) and send the bundle | Run it |
| **M12** | Part C, ported from fbs's keyword discovery, with C6's findings | — |
| **M13** | Part D: following and followers lists, if C6 shows logged-out access | — |
| **M14** | The review page (§11) | — |
| **G2** | Counsel signs off B, C and D for production | Counsel |

Go-live (C5) doesn't wait for discovery. Part A is usable as soon as M10 merges.

---

## 16. Still open

1. **Exits in or near Iraq**, if C6 shows that TikTok serves Iraqi results only to Iraqi viewers (§8.3). This is a proxy decision.
2. ~~Follower lists behind login~~ (§9): **closed** (C6 run 1, 2026-11-05): served logged out; built in M13. How deep they go logged out is T39.
3. ~~The owner's own accounts that go quiet~~: **closed** (owner, 2026-11-05): they stay monitored with lighter checks (§4, built in M10a).
4. ~~Counsel's review~~ (§13): **closed** (owner, 2026-11-05): signed off, discovery included.

---

## 17. Posts that are no longer public (approved, in M8)

The owner approved this on 2026-11-04, and it was handed to the M8 build session.

**How a post is found and marked:**
1. **A candidate.** A list read covering the post's date returns its neighbours but not the post.
2. **A confirmation.** One read of the post page by plain HTTP.
3. **The status: "unavailable", not "deleted".** A logged-out reader can't tell a deletion from a post made private, or one TikTok removed. TikTok's code and the exit's region are recorded.

**Guards:**
- A short or empty list page marks nothing.
- Many posts vanishing at once is treated as a reading problem.
- An account that goes private or is banned keeps its posts as they were.

**Afterwards:**
- A post that comes back is visible again, with the change kept.
- Unavailable posts stop being re-read. All data stays (D8).
