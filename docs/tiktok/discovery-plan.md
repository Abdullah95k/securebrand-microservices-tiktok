# Finding and adding accounts: the plan

**Status: proposed on 2026-11-04 and waiting for the owner's approval. Nothing here is built yet.**

The owner asked for all three parts, on 2026-11-04:

1. **A.** Bulk import.
2. **B.** Suggestions from data tks already collects.
3. **C.** Discovery through TikTok's own search, hashtag and sound pages.

They asked for a plan that thinks through every case before anything is built. This document is that plan.

**What tks does today** (checked in the code, 2026-11-04; see [`build-brief.md`](build-brief.md) and [`build-status.md`](build-status.md)):
- It follows only the accounts it is given, one at a time, through `POST /v1/accounts`.
- It keeps them up to date: new posts within 3 h, counts, comments, account details and renames.
- There is no bulk import. tks has no command like fbs's `fbctl pages import`.
- There is no discovery. The brief's D1 left search, hashtag and sound pages out of scope.

---

## 0. The plan on one page

| Part | What it does | New TikTok traffic | Counsel | Owner step | Milestone |
|---|---|---|---|---|---|
| **A. Bulk import** | A file of accounts becomes registered accounts, with a report | None beyond each new account's normal backfill | No change | None | **M10** |
| **B. Suggestions from collected data** | Accounts that your accounts mention, reply to or attract comments from are ranked, checked and suggested. You approve them, or a rule adds them automatically within caps. | One small read per candidate, to check it | **Yes**: commenters are private people | None to build; counsel before use in production | **M11** |
| **C. Discovery through TikTok's discovery pages** | Keyword search, hashtags, sounds, related posts, suggested accounts, reposts, and lookalikes of your brands | **Yes**: new calls | **Yes**: a scope change (D1) | **C6**: a capped discovery probe on the droplet | **M12** |

All three parts share the foundations in §2: where each account came from, labels, paced admission, a trial tier, lighter checks for dead accounts, and kill switches. They are built in M10.

**Order:** M8 (running now; it now also marks posts that are no longer public, §12) → **M10** → M9 → **M11** → **C6** → **M12** → **G2** (counsel signs off on B and C).

- M10 comes before M9 because go-live (C5) registers 50–100 accounts and then ramps up. That needs the import and paced admission.
- One build session runs at a time (brief §7).
- Go-live (C5) does not wait for B or C.

**Your decisions** are in §11, each with a recommendation.

---

## 1. Words used here

- **Monitored account**: an account on tks's schedule, registered by an API client.
- **Candidate**: an account tks has seen somewhere but does not monitor.
- **Signal**: one piece of evidence that a candidate matters. For example: "named in a caption by one of your accounts", "commented on 9 of your accounts' posts", "found by the search for a keyword".
- **Rule**: your instruction for what to look for, which accounts qualify (filters), and what to do with them: suggest them to you, or add them automatically within caps.
- **Suggestion**: a candidate that passed a rule and waits for your decision.

---

## 2. Foundations shared by A, B and C (M10)

### 2.1 Where each account came from

A new table, `account_sources`, records for every registration:
- **how** it came: `api`, `import` or `rule`;
- the **import batch** or the **rule**;
- the **API client**;
- **who approved it, and when**;
- the **evidence**: the signals that made it a suggestion.

An account keeps all its sources. It may be imported by one client and suggested to another. `GET /v1/accounts/{account}` shows them, and `GET /v1/accounts?source=import` or `?source=rule:<id>` filters by them. Any account can always be explained ("why do we monitor this?") and removed. This also answers counsel's likely first question.

### 2.2 Labels

Free labels on a registration, per API client. Examples: `list:banks`, `city:basra`, `import:2026-11-05`, `rule:lookalikes`.
- They are set on import, on approval or by a rule.
- The API filters by them.
- They never go to TikTok.

### 2.3 Paced admission (new accounts join at a set pace)

**Today** (checked): a new account is due at once, and its first job is a 365-day backfill. Backfills already have their own lane, so they don't delay the 2 h 45 min checks. But nothing limits how many new accounts start in an hour.

**The problem:** 10,000 imported at once means 10,000 backfills of about 25 list pages each. C3 counted 381 posts per account in a year, at 15 to a page. They would use the day's proxy data within hours.

**The plan:** `tt.admission`:
- **new accounts per hour.** The default is derived from the budget (D9). C5's ramp sets it.
- **backfills running at once.**
- **New accounts wait in a queue**, in this order: approved by hand, then imported, then added by a rule.
- **Checks of existing accounts never wait.**
- **The queue slows down when the budget stretches**, and pauses before monitoring is affected.
- **The API shows each queued account's place and an estimated start time.** The import report gives the whole batch's estimated finish.

### 2.4 Trial tier for accounts a rule adds automatically

An account added automatically starts **on trial**:
- it is checked **daily**, not every 2 h 45 min, which is about 1/9 of the list traffic;
- its backfill covers **30 days**, not 365, about 1/12 of the pages;
- its comments are re-read only for posts younger than 7 days.

When you approve it, it gets the full schedule, and the backfill continues from where the trial stopped (nothing is read twice). If you don't approve it within a set time (default 30 days), checking stops. The data stays (D8) and the candidate shows as "trial ended". Only accounts you approved, or that you monitor anyway, produce new signals (§4.4), so a trial account never brings in more trial accounts.

### 2.5 Lighter checks for private, banned and missing accounts

**Today** (checked): a private, banned or missing account is checked as often as an active one. The planner backs off only after failed jobs, and these jobs don't fail. Imports and discovery will bring many such accounts. The plan, `tt.freshness.inactive`:

| Status | Checked |
|---|---|
| Private | Daily for 7 days, then weekly |
| Banned or not found (TikTok's 10221, 10202) | Weekly for 30 days, then monthly |
| Active again | Back to its normal schedule from that read |

### 2.6 Identity: the same account seen in different ways

- **A candidate is known by its TikTok user id.** Captions and comments carry the id and the `secUid` of the people in them; tks already stores both.
- **tks registers a found account by its `secUid`**, which doesn't change when the account is renamed, not by its username.
- **A rename** between "seen" and "approved" changes nothing. The username history already exists (`account_usernames`).
- **An account another client already monitors** gets your registration added, with no second schedule. `scheduleRef` already matches by user id.
- **An account you removed before** is shown as "removed by you on <date>". Rules don't suggest it again unless you ask.

### 2.7 Kill switches and permissions

**Kill switches:**
- `tt.discovery.enabled` turns all discovery off.
- Each rule has its own on/off.
- Each new TikTok call (part C) is an operation in the existing registry, **off by default**. It is switched with `PUT /v1/admin/operations/{name}`.

**Permissions:**
- New API scopes: `discovery:read` (see candidates and rules) and `discovery:write` (change rules, approve, reject).
- Import uses the existing `accounts:write`.

---

## 3. Part A: bulk import (M10)

**How you use it:**

```bash
tksctl accounts import accounts.txt --label list:banks --dry-run
```

then the same command without `--dry-run`. The API equivalent is `POST /v1/accounts/import` (up to 1,000 lines a call), and the command sends a big file in chunks. Both end with a report:
- added;
- already monitored;
- duplicates in the file;
- not accepted, with line numbers and reasons;
- queued, with an estimated finish.

`tksctl accounts import --report <batch>` later shows which accounts turned out private, banned or missing.

**Accepted per line:**
- `username` or `@username`;
- a profile link (`tiktok.com/@name`, with or without `www.` or `m.`, with any `?…`);
- a video link (`tiktok.com/@name/video/…`: the account is taken from it);
- a `secUid`;
- `id:<user id>`, for accounts tks has read.

Or a CSV with an `account` column and optional `labels`, `interval`, `max_age_days` and `include_media` columns. Lines starting with `#` and blank lines are skipped.

| Case | What tks does |
|---|---|
| The same account twice in the file (different case, with `@`, as a link) | Counted once |
| Already monitored by you | Unchanged; reported as "already monitored"; new labels added |
| Monitored by another client | Your registration added; no second schedule |
| Hidden characters from copy and paste (zero-width spaces, the right-to-left marks common in Arabic text, a byte-order mark, Windows line ends, full-width `@`) | Removed before the line is checked |
| Capital letters | Lowercased |
| Not a possible username (spaces, Arabic letters, other symbols) | Not accepted; reported with the line number |
| Short links (`vm.tiktok.com/…`, `tiktok.com/t/…`) | Reported as "not supported yet". Resolving one is a request to TikTok, so C6 checks how they redirect and M12 resolves them through the crawler. |
| A username that doesn't exist, or a typo | Registered. The first read finds TikTok's code; the account gets the lighter checks of §2.5; the report lists it. |
| A private account | The same: status private, lighter checks, listed |
| A very large file (100,000 lines or more) | Sent in chunks of 1,000; joins at the admission pace (§2.3); estimated finish shown |
| The command stops halfway (network, Ctrl-C) | Run it again: registration is idempotent, and finished lines are skipped |
| You want the file to be your full list | `--disable-missing` removes your registrations that aren't in the file (the data stays, D8). It runs only with that flag, always shows a dry run first, and refuses to remove more than 10 % of your list without `--force`. |
| Settings per account | CSV columns, checked like the API's |
| Two imports at the same time | Each is its own batch; nothing breaks |

There are no new TikTok calls and no legal change.

---

## 4. Part B: suggestions from data tks already collects (M11)

### 4.1 Sources

| Source | Already stored | Strength | Notes |
|---|---|---|---|
| **Caption mentions** by your accounts | `post_mentions` (user id, `secUid` in `people`) | Strong: your account chose to name it | Partners, branches, staff, collaborations |
| **Bio mentions** (`@name` in your accounts' bios) | `accounts.bio` | Strong | Only a username, so tks reads that profile page once to identify it |
| **Your accounts' replies** to someone in comments | `comments` (the author is your account) | Medium | |
| **Mentions inside comments** | Comment mentions (user id) | Medium | |
| **Commenters** | `comments`, `people` | Weak on its own, strong when repeated across many posts and accounts | Mostly private people: the safeguards in §4.5 |
| Duets and stitches | Not available: the list's items carry only "duet allowed" flags, no source (checked in the fixtures) | | |
| Sounds | Only the sound's author name, no id | | The sound's own page gives the author; that's part C |

### 4.2 How signals add up

- **As things arrive.** Each new mention, reply or comment the persister writes adds to a running count per candidate, source and monitored account: the count, and when it was first and last seen.
- **No rescans.** The big tables are never rescanned; at hundreds of thousands of accounts they are too large.
- **Catch-up.** A one-time job counts what is already stored.

### 4.3 Ranking, and the check before a suggestion

**The score:**
- Most weight goes to **how many different monitored accounts point to the candidate**.
- Then how often, and how recently. A signal fades over about 90 days.
- Then the source's strength: caption > bio > reply by your account > mention in a comment > comment.

Every suggestion shows **why**, for example: "named in captions by 4 of your accounts, 11 times, last 2 days ago".

**The check before suggesting:**
- tks makes one read of the candidate's newest posts. It is the `creator` list, by plain HTTP, unsigned: the same call the crawler already uses (C2).
- It gives followers, likes, number of videos, verified, private, the last post's date, and each post's **region** (`locationCreated`) and **language**.
- tks has no other way to tell an Iraqi account from another: TikTok shows no country on the account itself.
- The result is cached for 7 days, and the number of checks a day is capped (§9).

### 4.4 Filters (per rule; defaults in brackets)

- Followers from [1,000] to [no maximum]. Verified only [no]. Organisation accounts only [no].
- Region of recent posts [IQ]. Language [ar, ku, en].
- Posted within [30 days]. At least [3] posts. Account at least [30 days] old.
- **Always:** not private, not predicted under 18, not on the exclude list. The exclude list starts with TikTok's own and other global accounts, and you add to it.
- **Signals come only from accounts you monitor**, whether you added them or approved them. A trial account (§2.4) produces none, so suggestions never snowball into suggestions of suggestions.

### 4.5 Cases

| Case | What tks does |
|---|---|
| A famous global account named by many of yours | The region and follower filters, your exclude list, and "reject: not relevant", which then holds for every rule |
| Bots or spam accounts commenting everywhere | Minimum followers, posts and account age, plus "pointed to by N different accounts" |
| **A private person who comments a lot** | Under the public threshold, so never suggested |
| **Predicted under 18** (TikTok's age band on comment authors, already stored) | Never suggested |
| A private account | Not eligible; checked again monthly in case it becomes public |
| Banned or not found | Not eligible |
| Already monitored by you | Not suggested |
| Monitored by another client | Suggested as "already monitored by tks": approving adds your registration, with no backfill |
| You rejected it | Not suggested again unless its snooze has ended **and** its evidence has at least doubled, or never, if you chose "never" |
| You removed it before | Treated as rejected |
| Renamed between the suggestion and your approval | Found by user id, registered by `secUid` |
| Thousands of candidates a day | Ranked, with a daily cap on suggestions per rule; the rest wait their turn; a daily summary |
| A rule that adds automatically gets out of hand | A cap per rule [20 a day] and for all rules [100 a day]; nothing is added while the budget is stretched; added accounts start on trial; a daily summary; one switch stops it |
| Several accounts of one business | Shown together when they share a bio-link domain or a nickname |
| Pseudonymized mode (D8) | tks doesn't keep people's ids in that mode, so B can't work: tks refuses to turn B on, with a clear message |
| Several API clients | The facts about a candidate are shared; rules and decisions belong to each client |

---

## 5. Part C: discovery through TikTok's discovery pages (C6, then M12)

### 5.1 What is known, and what isn't

- **Recorded:** the logged-out web app made these calls in the owner's recordings (HAR findings §1; C2):
  - `/api/repost/item_list/` (reposts, 7–30 items);
  - `/api/user/list/` (a profile's suggestions or lists: which one is still to be confirmed);
  - "explore" recommendations;
  - related posts on a post page.
- **Never recorded:** search, hashtag pages and sound pages. Nothing is known about how they answer logged out: how far they go before asking to log in, whether they need a signature, and how often they block.
- **Region:** from the US exits every page said `region: US`. That is TikTok's US service (C2). Results for Iraqi keywords may differ by exit country.

The C6 probe answers these on the droplet before M12 builds anything.

### 5.2 Sources

| Source | What it finds | Probe questions (C6) |
|---|---|---|
| **Keyword search: accounts** | Accounts whose name or bio matches: brands, places, topics, Arabic and Kurdish keywords | Answered logged out? Pages until a login wall? Signed? Differs by exit country? |
| **Keyword search: videos** | The authors of videos about a keyword | The same |
| **Hashtag pages** | The authors posting with a hashtag | How deep it goes; its order (popular, not newest); signed? |
| **Sound pages** | The authors using a sound (a brand's jingle, an original sound of one of yours); the sound's own author | The same |
| **Related posts** on your accounts' posts | The authors of similar posts | Answered unsigned? |
| **Suggested accounts** on your accounts' profiles | TikTok's own "similar accounts" | Is `/api/user/list/` that, or the following list? |
| **Reposts** by your accounts | The authors your accounts repost | Answered in the recordings; confirm at volume |
| **Following lists** of your accounts | Whom your accounts follow | D1 excludes them. Each profile page says whether the list is shown (`followingVisibility`; all three recorded profiles have the same value, 2, whose meaning C6 confirms). Decision 4 in §11 |
| **Lookalikes** of your accounts (possible impersonation) | Accounts whose name looks like one of yours | Uses the account search with name variants (§5.4) |
| Explore page | Trending accounts by category | Bound to the exit's region; low value; optional |

**Not planned:**
- **Followers lists**: millions of private people, a lot of traffic, and a legal question.
- **Web search engines** (Google, Bing): their APIs are paid services, and scraping them breaks their terms. You use no providers.
- **TikTok's Research API**: non-commercial use only (`methods.md` §4).
- **Logging in**: never (D3).

### 5.3 How part C runs

- **As rules on a schedule.** Each rule runs on its own timetable, for example a keyword every 24 h or a hashtag every 12 h, with a cap on pages per run.
- **Through what monitoring already uses:** the same proxies, identities, pacing and budget. Calls that need a signature are signed by TikTok's own script in the existing signer pool, like comments (D11). tks never re-implements a signature.
- **New registry operations.** Each call is a new operation, off until C6 shows it works. Each has its own rate, canary check (M8) and parser drift count. A broken discovery call pauses its rule and never touches monitoring.
- **Its own budget share.** Discovery gets its own share of the day's requests and bytes (default **10 %**). When the budget stretches, discovery pauses first.
- **What is stored from a discovery read:** only what is needed to judge the candidate. That is the account's facts and the evidence: the keyword or hashtag, the post id, its rank, and when it was seen. The other accounts' posts and comments are not stored; once you approve an account, its backfill reads everything as usual. This is decision 5 in §11.

### 5.4 Lookalikes (possible impersonation)

For each account you mark as a brand (a label, or a rule's list), tks searches name variants and compares the accounts found with yours.

**Before comparing,** tks normalizes both names:
- capitals, and the `.` and `_` separators;
- digits and letters that look alike: `0`/`o`, `1`/`l`, `3`/`e`, `5`/`s`, `rn`/`m`;
- Arabic letter forms: أ إ آ ا, ى ي, ة ه, and the Persian ک and ی;
- the tatweel stretch and diacritics;
- added words such as `official`, `iq`, `real` and `_1`.

**Then it ranks them:**
- by how close the names are, and the nicknames too;
- a verified lookalike ranks low, because it is likely legitimate;
- a new account ranks high.

They are labelled **"possible impersonation"** and are **only ever suggested, never added automatically**. Comparing profile pictures can come later.

### 5.5 Cases

| Case | What tks does |
|---|---|
| Search asks to log in after some results | Stops there and records it; the rule shows "limited by TikTok"; never logs in (D3) |
| TikTok refuses the signature, answers empty, or shows a captcha (10000) | As for comments: one retry on another signer page, the rate controller slows down, the canary sees it, and the rule pauses after repeated blocks |
| Results depend on the exit's country | C6 compares US exits with others on your keywords. If Iraqi results need exits TikTok places in or near Iraq, that's a proxy decision for you; no paid service without you. |
| Different results each run | De-duplicated by user id, with evidence added up; a rule can require a candidate to appear in N runs |
| A hashtag with millions of posts | A pages-per-run cap. Its order is by popularity, so new authors come over several runs. "New authors per run" is shown, and a rule that finds nothing new slows down. |
| Keywords in Arabic or Kurdish | Sent as typed; spelling variants (§5.4) can be added as extra searches per rule |
| A keyword matches thousands of private people | The public threshold, ranking and caps (§4.4) |
| Sponsored results | Skipped when TikTok marks them |
| A discovery call breaks after a TikTok change | The canary and parser drift count catch it; only its rule pauses; monitoring carries on |
| Discovery competing with monitoring for the budget | Its share is capped; monitoring comes first |

---

## 6. Rules: what you set

Each rule has:
- a name and its API client;
- a **source** (§4.1 or §5.2);
- **inputs**: keywords, hashtags, sounds, and which of your accounts it applies to (all, or a label);
- **filters** (§4.4) and a minimum score;
- a **mode**: suggest, or add automatically;
- **caps**: per day and in total;
- a **timetable** (part C only);
- **labels** for the accounts it adds;
- on/off.

**Examples:**
- *Partners of my banks*: caption and bio mentions by accounts labelled `list:banks`, at least 5,000 followers, region IQ, **suggest**.
- *Baghdad restaurants*: account and video search for «مطعم بغداد» and "Baghdad restaurant", plus the hashtag #مطاعم_بغداد. At least 1,000 followers, posted within 30 days, **add automatically, 10 a day, on trial**.
- *Lookalikes of my brands*: lookalikes of accounts labelled `list:brands`, **suggest** (the only mode allowed).

---

## 7. Reviewing suggestions

**API:**
- **List:** sorted by score, filtered by rule, label or state.
- **Detail:** the evidence and the check's result.
- **Approve:** with labels and schedule options.
- **Reject:** with a reason (not relevant, a private person, spam, a duplicate, other) and a snooze (30 days, 90 days, never).
- **In bulk:** approve or reject many at once.

**CLI:** `tksctl discovery list | show | approve | reject | export --csv`.

**Daily summary:** suggestions, automatic additions and trials ending, through the API. It is optionally also an add-only event on a new topic, `tt.discovery.v1`, for the analysis service.

A review web page is not in this plan; the API has all it would need (decision 6 in §11).

**A candidate's states:** seen → qualified → checked → **suggested** → approved, rejected, snoozed or expired. The other states are not eligible, added automatically (on trial), and trial ended.

---

## 8. Privacy and legal (for counsel)

- **Commenters and mentioned people are private people (D8).** B suggests only accounts above a public threshold (decision 3). It never suggests accounts predicted to be under 18 or private accounts. Nothing else changes about what tks keeps.
- **Part C reads pages that D1 left out.** That is a scope change for counsel. [`methods.md`](methods.md) §5 gets a section on what discovery reads, how often, what it stores, and the safeguards.
- **Every added account records why, and who approved it** (§2.1).
- **Unchanged:** no logging in, no followers lists, no re-implemented signatures.
- **D15:** B and C stay **off in production until counsel signs off (G2)**. A needs nothing new.

---

## 9. Keeping the cost under control

Phase 0 measured, per 1,000 monitored accounts: 2 IPs, 2 signer pages and 6.1 GB of proxy data a day. Every account discovery adds costs the same, so:

- **Caps:** per rule, and for all rules.
- **The admission pace** (§2.3).
- **The trial tier** (§2.4): about 1/9 of the list traffic and 1/12 of the backfill.
- **Candidate checks:** one read each, cached 7 days, capped per day.
- **Discovery's budget share:** 10 % (§5.3).

tks reports per rule and per day: requests, bytes, candidates, suggestions, approvals, automatic additions, and the approval rate. You can see what each rule costs and whether it's worth it. This goes with D9's cost per account.

---

## 10. Testing (no live TikTok requests from build sessions)

- **fakett** gains synthetic search, hashtag, sound, related-post, suggestion and repost answers. It can also produce login walls, empty answers, captchas and refused signatures on demand.
- **End-to-end tests:**
  - importing 10,000 synthetic accounts keeps the admission pace and leaves existing accounts' checks on time;
  - a rule leads to suggestions, then approval, then the backfill;
  - automatic-addition caps and trials hold;
  - a rejected candidate isn't suggested again;
  - the kill switches work;
  - pseudonymized mode refuses B.
- **Golden fixtures** come from C6's bundle, scrubbed of real names, ids and text (D13).

---

## 11. Decisions for you (recommendation first)

1. **What should discovery look for?** For C6 and the first rules, tks needs your keywords, hashtags and brand accounts. They stay on the droplet, never in git.
   - *Recommended first rules:* (a) partners and branches of your accounts (caption and bio mentions); (b) lookalikes of your brands; (c) Iraqi accounts in your topics, through hashtags and keywords.
2. **Approve each one, or add automatically?**
   - *Recommended:* approve by default. Add automatically only on rules you mark, within caps, and on trial.
3. **The public threshold for people found through comments.**
   - *Recommended:* 1,000 followers or verified, for counsel to confirm.
4. **Following lists of your accounts** (D1 excludes them).
   - *Recommended:* include them as a source in C, only for accounts whose profile shows the list (`followingVisibility`), if logged-out TikTok serves it. Followers stay out.
5. **What to keep from discovery reads.**
   - *Recommended:* the candidate's facts and the evidence, not the full posts of accounts you don't monitor.
6. **A review page.**
   - *Recommended:* API, CLI and CSV now; a page when the analysis service exists.

**When you approve:**
- the brief's D1 is updated, with discovery sources in scope as sources for finding accounts;
- the milestones go into `build-status.md`;
- `methods.md` §5 gets the discovery section for counsel. Sending it to counsel early means G2 doesn't hold up M12.

---

## 12. Posts that are no longer public (approved, in M8)

The owner approved this on 2026-11-04, and it was handed to the M8 build session. Today a post the creator deletes stays stored as last seen.

**How a post is found and marked:**
1. **A candidate.** A list read covering the post's date returns its neighbours but not the post.
2. **A confirmation.** One read of the post page by plain HTTP, before anything is marked.
3. **The status: "unavailable", not "deleted".** A logged-out reader can't tell a deletion from a post the creator made private, or one TikTok removed. TikTok's code and the exit's region are recorded.

**Guards:**
- A short or empty list page (TikTok's usual way of blocking) marks nothing.
- If many of an account's posts vanish at once, it's treated as a reading problem, not deletions.
- An account that goes private or is banned keeps its posts as they were; the account's status says why.

**Afterwards:**
- If the post comes back, it shows as visible again, with the change kept.
- Unavailable posts stop being re-read. All data stays (D8).
- The API shows the status, since when, and the code.
