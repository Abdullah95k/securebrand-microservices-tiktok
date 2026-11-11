# tks discovery: the entry check, the dormant list and the Iraq score

How tks decides whether an account a discovery source found is added to monitoring ([`discovery-plan.md`](../tiktok/discovery-plan.md) §3 and §4; [ADR 0007 §19](../tiktok/adr/0007-tks-data-model-and-contracts.md#19-settled-in-m10c-the-entry-check-the-dormant-list-and-the-iraq-score)). The sources: **part B** (M11, built: mentions, replies, commenters; [below](#part-b-from-what-tks-already-collects)), **part C** (M12, built: keyword search, hashtags, sounds, related posts and the For You feed; [below](#part-c-tiktoks-discovery-pages)) and **part D** (M13, built: following and followers lists; [below](#part-d-following-and-followers-lists)).

## What happens to a found account

1. **Found.** A source hands tks the account (its user id, secUid and username, the source and what led to it). Its source is recorded in `tt.account_sources` with the evidence.
   - Already monitored: nothing more; the source is evidence.
   - Removed by any client (`DELETE /v1/accounts/{account}`): `blocked`; never read, never added.
   - The source already shows it private, or with no post: straight onto the **dormant list**, with no read.
   - Otherwise it waits for its **entry check** (`pending`).
2. **The entry check.** The scheduler's leader sends waiting accounts to their check, the most promising first, at most `tt.discovery.entry.batch` a tick and `max_running` at once, in the backfill lane (when the budget pauses backfills, entry checks wait too; monitoring never does). The crawler reads the first page of the account's post list (15 newest posts, by plain HTTP, unsigned) and, only when needed, its profile page. Everything read is stored. Nothing else is read.
3. **The decision**, once what was read is stored:

   | The check found | The account |
   |---|---|
   | Private, banned or gone | **Dormant list** (`private`, `banned`, `gone`) |
   | No post at all | **Dormant list** (`never_posted`) |
   | Newest post older than 90 days (`active_for`) | **Dormant list** (`inactive`) |
   | Nothing (errors), 3 times (`max_attempts`) | **Dormant list** (`unreadable`) |
   | Public, active, and the Iraq score passes | `qualified`, then **added** |
   | Public, active, the score does not pass | `not_iraqi` (yet): kept, re-scored with no read when a new signal arrives |

4. **Added.** The leader registers it to the discovery client (`tt.discovery.client`) with origin `discovery`. Its first read, a 365-day backfill, waits in the admission queue behind the owner's accounts, by score ([`tks-import.md`](tks-import.md#the-admission-queue)).
5. **Goes quiet.** A discovered account that turns private, banned or gone, or has no post for 90 days, moves to the dormant list: its checks stop, its data stays. Accounts the owner registered are never moved (they get the lighter checks of M10a).

**The dormant list.** An account on it is read again only when a source finds it again **after** its next check date (its check plus 180 days, `recheck_after`). Found earlier, the finding is counted and nothing is read. Not found again, it is never read again.

## Part B: from what tks already collects

[`discovery-plan.md`](../tiktok/discovery-plan.md) §7; [ADR 0007 §21](../tiktok/adr/0007-tks-data-model-and-contracts.md#21-settled-in-m11-discovery-part-b). No new kind of TikTok request: the people come from the posts, bios and comments tks reads for the accounts it monitors; each new one costs one entry check.

| Source | Who | Carries |
|---|---|---|
| `mention` | named with @ in a monitored account's caption, its bio, or a comment on its posts | id and secUid (caption, comment); a bio only the username, so the entry check reads the profile page |
| `reply` | a person whose comment the monitored account answered on its own post | id, secUid and username (from the stored comment) |
| `commenter` | everyone who commented on a monitored account's post | id, secUid and username |

1. **The persister records** each person in `tt.discovery_sightings` (one row per sighting: source, where, the monitored account, the person, the post and comment), **only for a post or comment it writes for the first time, or a bio that changed**, in the same transaction. A re-read, an edit or a redelivered batch records nothing again. Only accounts on the schedule count (an entry check's posts bring no one in), and only sources switched on. The account itself, and its own comments and replies, are never recorded.
2. **The leader counts** what it has not counted yet, `tt.discovery.count_batch` (5,000) at a time, for up to 10 seconds a tick: one finding per person, source and monitored account, with how often it was seen. The rows are kept (`counted_at` set).
3. Each found account then follows [the steps above](#what-happens-to-a-found-account): already monitored, it gains the source; new, it gets one entry check; dormant, it waits for its date. The same commenter on a thousand posts is recorded each time and read once.

**Scoring.** Mentions and replies count as "mentioned or replied to by monitored accounts" (medium; strong from 10 different monitored accounts); commenting counts as weak. A mention inside someone else's comment on a monitored post counts as a mention by that post's account: it is evidence of the Iraqi audience around it.

**Known gaps.** A mention in a comment carries only ids; tks fills the username from what it stored for the person, if anything, and otherwise the entry check needs an identity that has loaded a page already (a fresh one fails once and the check is tried again). A discovered account's first 15 posts are stored by its entry check, before it is monitored, so their captions bring no one in; its later posts and all its comments do.

## Part C: TikTok's discovery pages

[`discovery-plan.md`](../tiktok/discovery-plan.md) §8; [ADR 0007 §27](../tiktok/adr/0007-tks-data-model-and-contracts.md#27-settled-in-m12-discovery-part-c). tks reads the posts on TikTok's discovery pages and checks their authors. C6 run 2 ([findings](../tiktok/c6-findings-2026-11-06.md)) showed every one of these calls served to a logged-out visitor **only signed** (by TikTok's own script, through the signer, on a profile page: every signed call answered). Six new calls on the account crawlers, all signed: `search_general`, `challenge_detail`, `challenge_list`, `music_list`, `related_list`, `recommend_list`.

| Source | What is read | Targets | How often | Notes |
|---|---|---|---|---|
| `hashtag` | a hashtag's posts (about 30 a page) | the hashtags at least `min_accounts` (3) monitored accounts used in the last `window` (30 days), the `top` (200) most used, minus `skip` (fyp and its kind); with `keywords`, also every one-word term of the Iraq list | `every` (6 h), up to `max_pages` (5) | **Build first** (C6 run 2: Arabic and Sorani posts, 422 new authors from 8 hashtags, from US exits). The hashtag's id comes from the monitored posts that use it, else from its detail call |
| `sound` | a sound's posts (about 20 a page) | the sounds at least 3 monitored accounts used in the last 30 days, the 100 most used | weekly, up to 3 pages | From US exits the sounds C6 read were not Iraqi |
| `related` | a post's related posts (one page) | each post monitored accounts made in the last `window` (72 h), the most played first, up to `per_tick` (50) new ones a tick | once | Drifts away from Iraq from US exits |
| `search` | a keyword's search results (12 a page) | every enabled keyword of the list (`tksctl discovery keywords`) | `every` (6 h), up to 5 pages | **Off until `tt.discovery.search.tier` names a tier TikTok places in or near Iraq**: from US exits TikTok answers 2483, "Please login" (T37). Turning a keyword off stops its search |
| `feed` | the For You feed (4 to 8 new posts a read) | one | `every` (3 h), `reads` (6) reads | American from US exits (C6 run 2): switch it on only with `tt.discovery.feed.tier` set to an Iraq-placed tier |
| `repost` | an account's reposts tab (16 posts a page) | every monitored account; with `reposts.candidates`, each candidate the entry check has read (posts of its own or not), up to `candidates_per_tick` (100) new ones a tick | monitored accounts every `every` (7 days), up to `max_pages` (3), the first read up to `first_pages` (20); a candidate once | M19 (owner D-047). Unsigned, on the ISP tier, on the account's own profile page. Every repost is kept (`tt.reposts`, `GET /v1/accounts/{account}/reposts`); each reposted post's author is a find. No audience signal: a found author qualifies by its own signals |

**How a read goes.** The leader keeps one row per target in `tt.discovery_targets` (source, key, state, next read, what each read did) and sends due ones to be read, the sources taking turns (a backlog of one source, such as related's posts, cannot hold the others back) and within a source never-read first, at most `pages.batch` (50) a tick and `pages.max_running` (20) at once, in the backfill lane (the budget pauses them with the backfills; monitoring never waits). `pages.day_pages` caps all part C pages in a UTC day (0: none). Each read bootstraps its identity on a monitored account's profile page (the page C6 signed these calls on; a related read on the post's own author), so nothing runs before tks monitors an account. A read stops at the end of the list (`end`), its page cap (`cap`), a page with no post (`empty`), a cursor that does not move (`stuck`), a hashtag or sound TikTok does not know (`gone`), or a status code twice (`refused`, with the code: search's 2483). A read that fails is tried again after `pages.retry_after` (6 h).

**What is kept.** Every post found is stored like a monitored account's (with its counts, music and media metadata; no comments, no subtitle files, no media downloads), with `discovered_via` `search`, `hashtag`, `sound`, `feed` or `related` (a post keeps the way it was first found), and its author as an account. The persister records each author in `tt.discovery_sightings`, **once per source, target and post**, with the region TikTok served the page in; the leader counts them as for part B. The `via` of a find is the keyword's id (so the review page's keyword hits count it), the hashtag's name, the sound's id, the post's id, or the feed's day.

**Scoring.** A find served with region `IQ` (an exit TikTok places in Iraq) counts as "shown to viewers in Iraq" (medium; strong from 10 different keywords, hashtags, sounds, posts or feed days). From US exits finds carry `US` and add no signal: their authors qualify on what their entry check reads (bio, phone, link, captions) and the audience signals.

**Turning it on.** In `tt.discovery.sources`: `hashtag: true` first; then `sound` and `related`. `search` and `feed` only with an Iraq-placed tier (the owner's proxy decision, [`discovery-plan.md` §8.3](../tiktok/discovery-plan.md#83-exit-country)):

```yaml
tt:
  discovery:
    search:
      tier: iraq        # a tier of tt.proxy.tiers whose exits TikTok places in or near Iraq
    feed:
      tier: iraq
```

**Watching it.** `tks_discover_pages_total{source,result}`, `tks_discover_reads_total{source,stop}`, `tks_discover_posts_total{source}`, `tks_discover_targets{source,state}`, `tks_discover_targets_due{source}`, `tks_discovery_sightings_total{source}`. Targets: `SELECT source, state, last_stop, count(*) FROM tt.discovery_targets WHERE enabled GROUP BY 1, 2, 3;`. Many `refused` searches mean the tier is placed outside Iraq (T37); many `error` pages, a call that changed (T25): switch that source off while it is fixed, the others carry on.

**Known gaps.** The account cards a search shows (type 4) and account search are not read (empty from US exits, T37). The canary does not check part C's calls; a drift shows as `tks_discover_pages_total{result="error"}`. Whether a hashtag's or sound's list is served newest first or ranked is T43: re-reads may mostly see the same posts.

## Part D: following and followers lists

[`discovery-plan.md`](../tiktok/discovery-plan.md) §9; [ADR 0007 §23](../tiktok/adr/0007-tks-data-model-and-contracts.md#23-settled-in-m13-discovery-part-d). For **every monitored account** (the owner's and the ones discovery added, once admitted), tks reads its **following** list (who it follows; source `following`) and its **followers** list (who follows it; source `follower`). C6 run 1 showed both served to a logged-out visitor, unsigned, from US exits. One new call: `/api/user/list/` (operation `user_list`, unsigned), on the account crawlers.

| List | Read | Skipped without a call |
|---|---|---|
| Following | every `following_every` (a week) | when the profile hides it (`followingVisibility` 2, about half of the profiles C6 read): the list is `hidden` |
| Followers | every `followers_every` (a day) | — |
| Both | | when the account is private, banned or gone: `gone` |

**How a read goes.** TikTok serves the newest people first (T41). The first read walks the list from the top. Every later read walks from the top **until a page holds nobody already stored** (`known_pages`, 1), then, if the list was never read to its end, goes on **from where the last read stopped**, so a large account's list fills in over days. A read fetches at most `max_pages` pages of `page_size` people (20 × 30 = 600); `day_pages` caps all reads in a UTC day (0: none). Reads run in the backfill lane, so the budget pauses them with the backfills; monitoring never waits for them. A read that fails is tried again after `retry_after` (6 h). A list TikTok will not serve a logged-out visitor (any other status code, twice) is recorded `refused` with the code and read again at its next interval; nothing fails.

**What is kept.** Every person read is a link in `tt.follows` (the account, the direction, the person; first and last seen; what the list showed: private, posts, followers, following), kept for good; the person's name and avatar are in `tt.people`. TikTok does not say when someone followed: the times are when tks first and last saw the link. Unfollows are not detected (a link no longer listed keeps its last-seen time).

**Who is checked.** A link **new to its list** records a sighting (as part B's do, in the same transaction), and the leader counts it: a monitored account gains the source with no read; a person the list shows **private** or with **no post** goes onto the dormant list with no read (the cheap pre-checks: most followers are personal accounts that never post); anyone else gets one entry check. Being followed by monitored accounts is a medium signal of the Iraq score (strong from 10 of them); following a monitored account is weak.

**Switches** (all under `tt.discovery`, with `enabled: true`):

```yaml
tt:
  discovery:
    sources: {following: true, follower: true}
    follows:
      following_every: 168h
      followers_every: 24h
      retry_after: 6h
      max_pages: 20        # one read
      day_pages: 0         # all reads in a UTC day; 0 = no cap
      page_size: 30
      known_pages: 1
      batch: 100           # lists sent to be read per tick
      max_running: 50      # reads running fleet-wide
```

Turning a source off stops its reads at the next tick; what was read stays.

**Looking at it.** The links of one account (key with `data:read`; who each person is needs `pii:read`):

```bash
curl -s -H "Authorization: Bearer $KEY" "$TKS/v1/accounts/id:<user id>/follows?direction=follower&limit=100"
```

`list` in the answer is the list's state (`ok`, `hidden`, `gone`, `refused` with `status_code`), when it was read, whether it was read to its end and why the last read stopped; `next_cursor` pages back. In SQL: `SELECT direction, state, last_stop, count(*) FROM tt.follow_lists GROUP BY 1, 2, 3`. Metrics: `tks_follow_pages_total{direction,result}`, `tks_follow_reads_total{direction,stop}` (crawler), `tks_follows_stored_total{direction,result}` (persister), `tks_follow_lists{direction,state}` and `tks_follow_lists_due{direction}` (leader). Many `result="refused"` or `"error"` pages mean TikTok changed the call or stopped serving it logged out: the canary does not check this call yet, so look at `tks_follow_pages_total` first. A `follow_lists_due` that keeps growing means `max_running`, `day_pages` or the budget hold the reads back.

## The Iraq score

Each kind of signal counts once. One **strong** signal passes; so do two **different medium** ones; **weak** signals never pass alone or with one medium one.

| Signal | Strength |
|---|---|
| A post made in Iraq (`locationCreated: IQ`) among the posts read | strong, but **never fires**: TikTok's web answers don't carry the field (T39) |
| Iraq, the Kurdistan Region or an Iraqi place in the bio or nickname (the enabled keywords) | strong |
| An Iraqi phone number in the bio (`+964`, `00964`, `07[3-9]…`) | strong |
| A `.iq` link | strong |
| Iraqi places in captions or hashtags | medium; strong in 3 of the posts read |
| Followed by monitored accounts (their following lists) | medium; strong from 10 |
| Mentioned or replied to by monitored accounts | medium; strong from 10 |
| Shown to viewers in Iraq (search, hashtag, sound, feed or related finds served with region IQ) | medium; strong from 10 |
| Posts in Arabic or Kurdish | weak |
| Comments on, or follows, monitored accounts | weak |

The numbers are `tt.discovery.score` (weights, `pass`, `caption_posts_strong`, `audience_strong`, `regions`, `languages`). During testing, check a sample of each kind with the owner ("is this Iraqi?") and tune them. A keyword turned off (`PATCH /v1/discovery/keywords/{id}`) stops counting within 5 minutes.

## Switches

All in `tt.discovery` (off by default):

```yaml
tt:
  discovery:
    enabled: true
    sources: {mention: true, reply: true, commenter: true, hashtag: true, sound: true, related: true, following: true, follower: true}   # parts B (M11), C (M12) and D (M13); search and feed need a tier
    search: {tier: ""}         # part C's search: a tier TikTok places in or near Iraq (T37); empty = off
    max_new_per_day: 0         # a cap for testing; 0 = no cap
    count_batch: 5000          # part B's sightings the leader counts at a time
```

Discovery cannot be turned on with `privacy.pii_mode: pseudonymized`: it needs people's ids.

## The review page

`tks-api` serves a web page at **`/review/`** ([`discovery-plan.md`](../tiktok/discovery-plan.md) §11, [ADR 0007 §22](../tiktok/adr/0007-tks-data-model-and-contracts.md#22-settled-in-m14-the-discovery-review-page)). It holds no data: it asks for an API key, keeps it in that browser tab only, and calls `/v1` with it, so the key's scopes decide what it shows and does.

| Scope | On the page |
|---|---|
| `discovery:read` | needed to sign in: every tab, read only |
| `discovery:write` | remove accounts, turn keywords on and off, add keywords |
| `accounts:write` | the Imports tab: upload, dry run, earlier imports and their reports |
| `pii:read` | usernames (else user ids and secUids) |

A key for a person reviewing discovery:

```bash
tksctl apikey create --name reviewer --scopes discovery:read,discovery:write,accounts:write,pii:read
```

To open it on a cluster without an Ingress (the chart's Ingress, when on, routes `/review` and `/v1`):

```bash
kubectl port-forward svc/tks-api 8080:80
```

Then open `http://localhost:8080/review/` and paste the key.

**Tabs:**
- **Accounts**: every found account, filtered by state, source, keyword (`via`: the keyword's id for search, the monitored account's user id for part B), label, Iraq score and the date first found, latest first or highest score first. Each shows its evidence ("posts made in Iraq: 11 of 15"), the sources that found it, and a **Remove** button.
- **Dormant list**: why each account is there, what its entry check read, and its next check date.
- **Not Iraqi (yet)**: the public, active accounts closest to the threshold, with their evidence; the threshold is shown (`tt.discovery.score.pass`).
- **Keywords**: on or off, add, and how many accounts each keyword's searches found (search is part C, M12: 0 until it runs). A count links to those accounts.
- **Imports**: paste or pick a file (up to 1,000 lines; larger files go through `tksctl accounts import`), labels, a dry run first; earlier imports with their counts, and each one's report of accounts that turned out private, quiet or gone.
- **Cost and capacity**: the switches, the daily test cap, found accounts by state, the admission queue with what draining it within a day takes (requests, proxy IPs, signer pages), and per UTC day the accounts found and added by source and discovery's own reads (entry checks and part D's list reads: jobs, requests, data).

**Remove** (`POST /v1/discovery/accounts/{id}/remove`) blocks the account for the caller, so no source adds it back; if discovery had added it, discovery's registration goes and its checks stop. An account another client registered itself stays monitored for that client (the page says so). Its data stays. To bring one back, register it (`POST /v1/accounts`): the caller's registration lifts its own block.

**Safety:** the page's Content-Security-Policy runs only its own script, nothing can frame it, and it writes what the API returns as text. The API refuses a change a browser marks as sent from another site (`Sec-Fetch-Site`), and sends no CORS headers, so no other site can use a key. Programs are not affected. Dates on the page are in Iraq time; the per-day figures are UTC days.

## Looking at it

The found accounts, by state, with each one's reason, score, evidence and dates (key with `discovery:read`; usernames need `pii:read`):

```bash
curl -s -H "Authorization: Bearer $KEY" "$TKS/v1/discovery/accounts?state=dormant&limit=100"
```

`state=not_iraqi&order=score` lists the accounts closest to the threshold; `source=`, `via=`, `label=`, `min_score=`, `max_score=`, `since=` and `until=` filter (as on the review page); `counts` gives every state's total; `next` is the cursor for `&after=`. `GET /v1/discovery/stats?days=7` gives the review page's cost and capacity figures. In SQL: `SELECT * FROM tt.dormant_accounts`.

**Part B:** `tks_discovery_sightings_total{source}` (persister), `tks_discovery_sightings_counted_total`, `tks_discovery_sightings_backlog` (leader). A backlog that keeps growing means the leader counts slower than the persister records (raise `count_batch`, or look at the leader's errors). In SQL: `SELECT how, found_in, count(*) FROM tt.discovery_sightings GROUP BY 1, 2`.

**Metrics:** `tks_discovery_findings_total{source,state}`, `tks_entry_checks_total{state}`, `tks_discovered_accounts_total`, `tks_dormant_moves_total{reason}`, `tks_discovery_candidates{state}` (leader). Many `entry_checks_total{state="pending"}` (retries) or a growing `discovery_candidates{state="checking"}` mean the checks fail: look at the account stage's errors (entry checks run on its topics). A `pending` count that never falls while backfills are paused is the budget at work (`GET /v1/admission`).
