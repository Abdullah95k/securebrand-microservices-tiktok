# tks probe: specification (first build milestone, M1)

The probe is the first thing tks builds. It answers the open questions that need real TikTok traffic ([open-questions.md](open-questions.md)), measures what each request costs, and collects everything it reads so the owner can see it. It is the TikTok counterpart of igs's `igs-spike probe` ([igs runbook](https://github.com/Abdullah95k/securebrand-microservices-igs/blob/9463b91/docs/runbooks/igs-probe.md)), and copies its shape: one Go command, a dry run against a built-in fake, a report and CSVs, a bundle to download.

The owner runs it on the **Phase 0 droplet**, through the **ISP proxy tier**, in two passes:
1. **Test pass (checkpoint C2):** 3 to 5 accounts, to check that every step works and to answer the questions that need few samples.
2. **Full pass (checkpoint C3):** the whole list (a few hundred Iraqi accounts), back 365 days, to measure depth, block rates and cost at a realistic volume.

The build session writes the code, its tests and the runbook (`docs/runbooks/tks-probe.md`); it never runs the probe against TikTok.

---

## 1. What it does, per account

Each step records its outcome (class, HTTP status, reason, latency, wire bytes, items) in `report.json`, and every value it reads in the CSVs.

| # | Step | Request | Answers |
|---|---|---|---|
| 1 | **Load the profile page** with a fresh identity (ISP exit, Chrome fingerprint) | `GET /@<username>` | T1 (user data in the page), T2 (client values, cookie names), T3 (plain HTTP enough?), T5, T6 (captcha?), bytes (T21) |
| 2 | **Read the account** from the page: id, `secUid`, username, nickname, bio, bio link, avatar, verified, private, follower, following, total-likes, video and likes-given counts. Save the avatar | — (and `GET` the avatar image) | Objective §1 |
| 3 | **User detail call**, unsigned | `GET /api/user/detail/?uniqueId=…` (plus the web client's standard parameters) | T24 (cheap daily refresh?), T4 |
| 4 | **Video list, unsigned**, first page; then the same with `count` 5, 15 and 30 | `GET /api/post/item_list/?secUid=…&count=…&cursor=0` | T4, T9 (items complete?), T10 (pinned), T11 (page size), T21 (bytes per count) |
| 5 | If step 4 fails: **the other list variant**, unsigned | `GET /api/creator/item_list/?secUid=…&cursor=<now ms>&type=1&count=…` (the shape yt-dlp is reported to use) | T4 |
| 6 | **Walk the list** with whichever variant worked: follow the cursor until `hasMore` is false, a wall, `--max-list-pages`, or posts older than `--max-age-days` (judged by the post id's time when T8 holds, else `createTime`) | as step 4 or 5 | T8, T11 (depth, cursor meaning), T6 (walls with depth) |
| 7 | **Cursor portability:** replay one list cursor from a second identity on another exit | as step 6 | T22 |
| 8 | **Without cookies:** repeat the first list call with no `msToken`, then with no cookies at all | as step 4 | T2, T4 |
| 9 | **Load the newest post's page** (and the newest photo post's, if any) | `GET /@<username>/video/<id>` (`/photo/<id>`) | T1 (full post with counts), T16, T19, bytes |
| 10 | **Comments, unsigned**: for the 3 newest posts that have comments, read up to `--max-comment-pages` pages | `GET /api/comment/list/?aweme_id=<id>&count=20&cursor=0` | T12 (served logged out? page size, cursor), T13 (order: compares `create_time` across pages) |
| 11 | **Replies, unsigned**: for the first 3 comments with `reply_comment_total` > 0, read up to 3 pages | `GET /api/comment/list/reply/?item_id=<id>&comment_id=<cid>&count=20&cursor=0` | T14, T15 (Σ(1 + replies) vs the post's count, when a post's comments are read to the end) |
| 12 | **Media sample:** download the newest post's cover; with `--videos N`, download the smallest video rendition of the N newest posts | the URLs the post carries | T18 (do links work without cookies? expiry in the URL), bytes |
| 13 | **Signed calls** (only with `--signer browser`): open the profile page in headless Chromium on the same exit and fingerprint, then repeat steps 3, 4, 10 and 11 as `fetch` calls inside the page. Record the parameters TikTok's script added to each call and the outcome. Then replay each signed URL, exactly as the page sent it, by plain HTTP from a second identity with the same user agent | the same calls, from the page; then the replay | T7 (does TikTok's own script sign them, and do they then work?), T27 (is a signed URL portable?), cost of a signed call (browser time, bytes) |
| 14 | **oEmbed** (only with `--oembed`): one post | `GET /oembed?url=<post link>` | T26 |

Rules that keep the probe safe and comparable:
- **Pacing like a person:** one request every ~3 seconds per identity (`--rps 0.3`), as igs.
- **Captchas are recorded, not solved.** A captcha retires the identity and is counted per step; a new identity continues. `--max-captchas` (default 10 per run) stops the run, so a bad exit list does not burn IPs.
- **A stop is a stop:** Ctrl-C writes what was read, marks the current account `stopped`, and counts it for no question (igs's fix of 2026-10-31).
- **Credentials never appear** in the report or errors (`://***@`).
- **Unsigned first.** The probe only uses the browser when told to, so every answer about unsigned access stands on its own.

## 2. What it writes

For each tier, `phase0/out-tt/<tier>/` (git-ignored, files 0600) holds:

| File | Holds |
|---|---|
| `accounts.csv` | One row per account: link, id, `secUid`, username, nickname, bio, bio link, verified, private, counts, avatar file, how far the list went (pages, posts, oldest date, why it stopped) |
| `posts.csv` | One row per post read: link, id, kind (video or photo), caption, hashtags, mentions, posting time, the time the id encodes, duration, cover link and file, music (id, title, author, original), pinned, ad and other labels, plays, likes, comments, shares, saves |
| `comments.csv` | Comments and replies read: post, comment id, parent id, author (id, username, nickname), text, time, likes, reply count, page number |
| `images/avatars/`, `images/covers/` | Avatars, and covers of the newest `--covers` posts per account (default 12) |
| `videos/` | The sampled video files (only with `--videos`) |
| `raw/` | TikTok's raw answers, to check their shape: per account the first answer of each call kind and every answer that failed or held no items; the page HTML of the first 3 accounts; with `--signer browser`, the signed request URLs |
| `report.json` | Everything above, plus every request's outcome, timing and size, and the answers under `summary` (below) |

and `phase0/out-tt/<tier>.tar.gz` packs the folder for downloading.

**`summary` in `report.json`** answers each question over all accounts:
- `bootstrapped`, `page_data_found` (per scope), `cookies` (names and counts), `client_values_found` (T1–T3);
- per call kind and variant, unsigned and signed: requests, classes, empty answers, mean latency and wire bytes (T4, T7, T21, T24);
- `captchas` by step, `walls` by step and depth (T6);
- `id_time_matches` / `id_time_mismatches` with the largest gap (T8);
- `items_with_stats`, `items_with_music`, `pinned_seen`, `ad_seen`, `photo_seen` (T9, T10, T16, T19);
- list: `ended`, `capped`, `age_stops`, `walled`, the deepest date reached, the cursor's apparent meaning, page sizes honoured per `count` (T11);
- comments: `served`, the order verdict (`newest_first`, `not_time_ordered`, per post), page sizes, replies served, and the Σ(1 + replies) ratio for posts read to the end (T12–T15);
- `cursor_portable` / `cursor_not_portable` (T22);
- media links: downloaded with and without cookies, sizes, expiry found in the links (T18);
- `stopped_early`.

## 3. Command and Makefile targets

```
tks-spike probe --accounts FILE [--config FILE] [--tier NAME] [--out-dir DIR]
                [--rps 0.3] [--max-list-pages 40] [--max-age-days 0]
                [--max-comment-pages 5] [--covers 12] [--videos 0]
                [--signer none|browser] [--oembed] [--max-captchas 10]
                [--fingerprint chrome_152]
tks-spike probe --dry-run
```

| Target | What it runs |
|---|---|
| `make tks-probe-dryrun` | The whole probe against the built-in `fakett`, no traffic. Its summary must say `bootstrapped 3/3` |
| `make tks-probe` | The test pass: `phase0/tt-accounts.txt`, ISP tier, `--videos 1 --signer browser --oembed`, output `phase0/out-tt/` |
| `make tks-probe-all` | The full pass: `phase0/tt-accounts-all.txt`, ISP tier, `--max-age-days 365 --max-list-pages 100 --covers 3 --videos 0`, signer as the test pass showed it is needed, output `phase0/out-tt-all/` |
| `make tks-browser-check` | Checks that Chromium is installed for `--signer browser` and prints the one command to install it if not |

The accounts file holds one account per line: `username`, `@username` or a profile link; `#` starts a comment. The owner prepares it on the Mac and copies it to the droplet with `scp` (the runbook gives the exact command); no editing on the droplet.

A full pass at 0.3 requests per second takes about 3 to 6 minutes per busy account (a year of posts is 10 to 40 list pages, plus the comment and page steps), so a few hundred accounts take a day or more inside `tmux`.

## 4. `fakett`, the fake TikTok it rehearses against

The first slice of `internal/fakett` serves a seeded world (3 accounts of 60 posts, comments and replies, one photo post, one pinned post, one ad) with:
- profile and post pages that embed the rehydration document and set cookies;
- the five calls of §1 with TikTok's parameters, cursors and status codes;
- a small page script that signs `fetch` calls, so the browser signer is tested end to end where Chromium exists (CI skips it otherwise);
- faults: signature required (empty 200 when unsigned), captcha on the Nth page load or call, rate limits, a list wall at a depth, cursors bound to the identity, comments ordered by relevance, and media links that need cookies.

## 5. Acceptance criteria for M1

| Check | How |
|---|---|
| The client and the probe work against `fakett` over both transports (in-process and real HTTP), as igs's `igtest` does | `go test ./internal/tt/... ./internal/fakett/...` |
| Every fault in §4 is detected and reported in the right summary field | one test per fault |
| `make tks-probe-dryrun` reaches `bootstrapped 3/3`, writes every file of §2, and the bundle | CI job |
| The browser signer test passes where Chromium exists | CI job with Chromium (Playwright's build), skipped elsewhere |
| A Ctrl-C mid-walk writes a partial report with `stopped_early` | test |
| No credential, and no file outside `phase0/`, is written | test |
| The copied fbs and igs suites stay green (`make test`, `make test-integration`, `make e2e`) | CI |
| The runbook `docs/runbooks/tks-probe.md` gives the owner numbered steps with one command per block, `scp` from the Mac for the accounts file and the downloads, and no file editing on the droplet | review |
