# Logged-out Instagram: findings from the owner's recordings (2026-10-30)

On 2026-10-30, logged-out Instagram on desktop Chrome served everything below:
- profiles;
- post and reel details, with like and comment counts;
- carousels and video files;
- highlights;
- **every top-level comment, through a dedicated logged-out GraphQL query** (up to 372 comments on one post in one session, with no wall).

It served neither **replies** nor **reel view counts**. In the browser UI, the **profile grid stopped after 24 posts**, although the server still reported more.

**Source:** 9 HAR files recorded by the owner in a logged-out Chromium-based browser on macOS (Chrome 152 user agent), totalling about 300 MB and 1,794 requests:
- 3 profile sessions (a brand, a TV channel, a government office);
- 4 feed posts, a carousel among them;
- 2 reels.

The raw files stay out of git. This note holds only request shapes, field names and counts: no commenter names, usernames or comment text.

**Caveats:**
- Chrome's HAR export strips cookies, so cookie names and `Set-Cookie` behaviour are not visible here.
- Everything is one person on one network on one day. Phase 0 must confirm it at scale, through proxies.

---

## 1. Operations the logged-out web app uses

All are `POST`, `application/x-www-form-urlencoded`, persisted queries (`doc_id` + `variables`). doc_ids rotate, so these are seeds for the registry, valid on 2026-10-30.

| Purpose | Friendly name | Endpoint | doc_id | Variables | Returns |
|---|---|---|---|---|---|
| Profile | `PolarisLoggedOutDesktopWWWProfileRootContentQuery` | `/api/graphql` | `27981003384861049` | `username` | `data.xig_user_by_username` |
| Profile grid, first page | `PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery` | `/api/graphql` | `29483846714553007` | `username`, `first: 12`, `__relay_internal__pv__PolarisSEOIsCrawlerrelayprovider: false` | `data.xig_user_by_username.polaris_ordered_timeline_connection` |
| Profile grid, next pages | `PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery_connection` | `/api/graphql` | `28904005159232689` | `id` (the user object's `id`, 17841…, **not** its `pk`; see the correction below), `after` (90-char cursor), `first: 12`, the same provider flag | `data.node.polaris_ordered_timeline_connection` |
| Comments | `PolarisLoggedOutDesktopWWWPostCommentsPaginationQuery` | `/api/graphql` | `27837656105907150` | `media_id` (media pk), `after` (142-char cursor), `first: 10` | `data.xig_polaris_media.comments_connection` |
| Highlights | `PolarisStoriesV3HighlightsLoggedOutPageQuery` | `/graphql/query` | `39683663864580775` | `reel_ids: ["highlight:<id>"]`, `initial_reel_id`, `first: 1`, `apply_new_highlights_filter`, a community-notes provider flag | root field `xdt_api__v1__feed__reels_media__connection` (header `x-root-field-name`) |
| UI only (no data needed) | `PolarisLoggedOutDynamicDialogQuery` (login prompt), `PolarisLoggedOutDesktopWWWAYMLQuery` ("accounts you may like"), `fetchPolarisLoggedOutExperimentQuery`, `QuickPromotionSupportIGSchemaBatchFetchQuery` | `/api/graphql` | — | — | — |

**Corrections from the owner's probe runs (2026-10-31, droplet, ISP and residential):**
- The user object carries two ids: `pk` (for example 9 digits) and `id` (17 digits starting 17841). The grid's `_connection` query resolves `id`; given the `pk`, it answers `{"data":{"node":null}}` with no error.
- The comments query with `"after": ""` answers its connection with three `field_exception` errors (on `edges`, `geoblocked_comment_notices` and `page_info`) beside an empty list. A first page needs `"after": null`, or the post page's embedded comments.
- The first grid page answered in one part, with `has_next_page: true` and an `end_cursor`.
- With the user's `id`, the grid kept paging past 24 posts: 5 of 5 profiles to the probe's cap of 396 posts, no wall (ISP).
- A media `pk` encodes its creation time: `(pk >> 23) + 1314220021721` is the time in Unix milliseconds, within about a minute of the post page's `taken_at`.

The page HTML of a profile, post or reel already embeds the first page of data as Relay JSON (`<script type="application/json" data-sjs>`):
- **profile page:** the first 12 grid posts;
- **post or reel page:** the full media object, plus the first 10–12 comments.

A crawler can therefore start from the HTML and only call GraphQL for further pages. That is the same pattern as fbs.

## 2. Request format

**Form fields**, in the browser's order:
- Identity and routing: `av=0`, `__d=www`, `__user=0`, `__a=1`, `__req`.
- Session and revision tokens: `__hs`, `dpr`, `__ccg=EXCELLENT`, `__rev`, `__s`, `__hsi`, `__comet_req=7`, `lsd`, `jazoest`, `__spin_r`, `__spin_b=trunk`, `__spin_t`.
- Client-state blobs: `__dyn`, `__csr`, `__hblp`, `__hsdp`, `__sjsp`.
- Query: `__crn`, `fb_api_caller_class=RelayModern`, `fb_api_req_friendly_name`, `variables`, `server_timestamps=true`, `doc_id`.

This is the same family as fbs's form. Unlike fbs, Instagram's browser also sends `__dyn`, `__csr`, `__hblp`, `__hsdp` and `__sjsp`. Phase 0 must test whether requests work without them.

**Headers** (alphabetical in the HAR): `x-asbd-id: 359341` (the same value as fbs's seed), `x-csrftoken`, `x-fb-friendly-name`, `x-fb-lsd`, `x-ig-app-id: 936619743392459`, `x-ig-max-touch-points`, plus Chrome's client hints, `origin`, `referer` and `sec-fetch-*`. The highlights query adds `x-bloks-version-id` and `x-root-field-name`.

**Tokens:** every 200 page HTML (profile, post, reel) carried all of these:
- `LSD`;
- `csrf_token`;
- the app id `936619743392459`;
- `server_revision` / `__spin_r`;
- `haste_session`;
- `hsi`;
- `rollout_hash`;
- `__comet_req=7`;
- `jazoest`.

`DTSGInitialData` was absent, as on logged-out Facebook. So **a plain HTTP page load is enough to bootstrap**, and the browser fallback should rarely be needed. `instagram.com/<user>` answers 301 to `www.instagram.com/<user>/`.

**Responses:** plain JSON, one object per line, under `{"data":…, "extensions":{"server_metadata":…,"is_final":true}}`. There is no `for (;;);` prefix. Responses are labelled `text/javascript`. The fbs reader (`SplitParts`) handles this shape.

## 3. What data is available logged out

| Entity | Available | Not available |
|---|---|---|
| **Profile** (`xig_user_by_username`) | `pk`, `id`, `username`, `full_name`, `biography`, `bio_links[]` (title, url), `is_verified`, `is_private`, `is_memorialized`, `follower_count`, `following_count`, `profile_pic_url`, `linked_fb_info` (the linked Facebook page's name and id), `has_any_clips`, `latest_reel_media`, the highlights list (`lox_highlights_connection`: id, title, cover) | `all_media_count` is null (no post total) |
| **Grid item** (`polaris_ordered_timeline_connection`, 12 per page) | `pk`, `code` (shortcode), `media_type`, `product_type`, `caption.text`, `accessibility_caption`, `display_uri`, owner | `taken_at`, counts, video URLs: these need the post page |
| **Post or reel** (page HTML, media object) | `pk`, `code`, `taken_at`, `media_type` (1 photo, 2 video, 8 carousel), `product_type` (`feed`, `clips`, `carousel_container`), `like_count`, `comment_count`, `like_and_view_counts_disabled`, `commenting_disabled_for_viewer`, `caption` (text, created_at), `caption_is_edited`, `usertags`, `location`, `coauthor_producers` (collab posts), `image_versions2` candidates, `video_versions` (3 renditions), `video_dash_manifest`, `has_audio`, `clips_metadata` (music and original sound), `carousel_media[]` (9 children, each with its own pk, images and usertags), `original_width`/`height`, `accessibility_caption`, `ai_label_info`, `related_topic_pills` | **play and view counts** (no field named `*play*` or `*view_count*` anywhere), share counts, paid-partnership flag (not seen) |
| **Comment** (`comments_connection`, 10 per page, newest first) | `pk`, `id`, `text`, `created_at`, `comment_like_count`, `user` (`pk`, `id`, `username`, `is_verified`, `profile_pic_url`, `is_unpublished`), `is_covered`, `restricted_status`, `has_translation`, `giphy_media_info`, `parent_comment_id` (always null: top-level only) | **replies:** `child_comment_count` was null on all 618 comments seen, and no replies query appeared. The owner's test agrees: replies need login |
| **Highlights** | the highlights page query works logged out | current **stories** need login (owner's test) |
| **Not tested** | — | tagged posts, follower and liker lists (expected to need login), the embed page (works per the owner's test, but not recorded) |

## 4. Comment coverage

The comment count on a post includes replies, which logged-out visitors cannot see. So "complete" for a logged-out read means every top-level comment.

| Post | Type | Comment count | Collected | Read to the end? | Collected ÷ count |
|---|---|---|---|---|---|
| reel A | clips | 42 | 40 | yes (`has_next_page: false`) | 95% |
| post B | feed | 84 | 65 | yes | 77% |
| post C | feed | 52 | 39 | yes | 75% |
| reel D | clips | 13 | 11 (embedded only) | not paginated | 85% |
| carousel E | carousel_container | 199 | 91 | no (stopped by the owner) | 46% so far |
| post F | feed | 1,980 | 372 (36 pages) | no (stopped by the owner) | 19% so far |

- Every read that reached the end stopped cleanly with `has_next_page: false`. The shortfall matches hidden replies, not truncation.
- Post F paged 36 times in about 2 minutes (about 3.4 s per request) **without a wall**.
- **Implication for fbs's deletion check:** fbs requires Σ(1 + reply_count) = comment_count. With reply counts hidden, that can never balance logged out. The completeness and deletion formulas need an Instagram variant: for example, treat a read that ends with `has_next_page: false` as complete for top-level comments.

## 5. The profile grid stops at 24 posts in the UI

In all three profile sessions, the browser loaded exactly 24 posts: 12 embedded in the HTML and one `_connection` page of 12. Then the logged-out UI stopped loading more. The owner could dismiss the login prompt, but scrolling no longer fetched posts. The last response still said `has_next_page: true` with an `end_cursor`.

**The biggest open question for backfills** is whether a crawler that calls the `_connection` query with that cursor keeps getting pages, or is walled after 24. Test it first in Phase 0. If it is walled, alternatives to measure are:
- the reels tab (`has_any_clips`);
- a fresh identity per page of 24;
- reaching older posts by shortcode from other sources.

Incremental crawls (every 3 hours, newest posts first) only need the first page or two, so they are not affected.

## 6. Pacing observed

| Session | Duration | GraphQL calls | Walls |
|---|---|---|---|
| post F (comments) | 121 s | 42 | none |
| carousel E | 96 s | 13 | none |
| profiles | 15–76 s | 9–21 | only the dismissible login prompt in the UI |

Every request returned HTTP 200. Per-IP limits at crawler scale are unknown and are the main Phase 0 measurement.

## 7. What changes in the plan

- **Client:** the same design as fbs:
  - bootstrap from page HTML;
  - POST persisted queries with `lsd`, `x-csrftoken`, `x-ig-app-id`, `x-asbd-id` and `x-fb-friendly-name`;
  - read NDJSON responses.

  The TLS/HTTP2 fingerprint profile must match a current Chrome: the recordings show Chrome 152, while fbs pins `chrome_146`.

  Two differences: Instagram uses two endpoints (`/api/graphql` and `/graphql/query`), and its browser sends extra client-state fields whose necessity Phase 0 must test.
- **Stages:**
  - profile (HTML + `ProfileRootContentQuery`);
  - grid pages (`_connection`);
  - **post detail (the post page HTML is needed for `taken_at`, counts and media)**;
  - comments (pagination query, 10 per page);
  - highlights (optional).

  There is no replies stage logged out.
- **Requests per post:** 1 post page plus ⌈top-level comments ÷ 10⌉ − 1 comment pages. Comments dominate the request budget on busy posts, so the "fetch comments only when the count went up" rule (owner's D2) matters a lot.
- **Data model:** no reply rows, no view counts. Carousels have children, and collab posts have co-authors. Keep `like_count` nullable: `like_and_view_counts_disabled`.
- **Scope answers for the owner:** replies, current stories, reel view counts, and probably follower and liker lists are **not reachable logged out**. Highlights are.
