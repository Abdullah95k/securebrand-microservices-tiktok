# tks and the shared contract: the mapping (input to M15)

tks's review of the shared contract's draft v0.1 (`Abdullah95k/securebrand-contracts`), written for the master session's REQ-014 on 2026-11-06, kept here as the design input for **M15** (adopting the contract). **The contract's v1.0.0 is authoritative where it differs from this note.**

## What the master took into v0.2 (its reply of 2026-11-06)

| # | Change | For tks |
|---|---|---|
| C1 | `COUNT_EXACTNESS_ROUNDED` means "approximate"; `LOWER_BOUND` only for real floors | Rounded counts as in Q5; `displayed` stays empty |
| C2 | `provenance.exit_country` is where the platform placed the reader (TikTok's page region when served, else the exit's configured country), on every event | M15 step 4 |
| C3 | Scope is the registry's decision. A private account stays `FULL` with status `PRIVATE`, never `PROFILE_ONLY`. New value `NOT_MONITORED` for accounts known only from content | Comment authors and mentioned accounts: `NOT_MONITORED` |
| C4 | `DELETED` for a post only when the platform says deleted; `UNAVAILABLE` with the code otherwise; `DELETED` for comments absent from a complete re-read | As Q6 |
| C5 | The kind is `playlist` everywhere (`tt:playlist:<id>`) | |
| C10 | `CaptionTrack.source` (uploader, platform speech-to-text, machine translation) replaces `auto_generated` | M17 (subtitles) |
| C11 | `Media.bitrate_bps`, shared, instead of `tt.bitrate` | |
| C14 | `delete_after` from a producer setting, 72 hours by default, at least 24 | M15 |
| C16 | `Audio.status`, optional | |
| C17 | `OUTCOME_EXCLUDED`, for accounts the owner removed | Discovery's `blocked` |
| C20 | `tt.status_code` and `tt.category_code` dropped; `tt.hashtag_ids` and `tt.effects` wait until something collects them | |
| C21 | Every row of §3 below goes into the field matrix | |

**Agreed design:**
- content events are emitted from the persister, produced before its commit, with the same `event_id` on a repeat;
- SHA-256 `event_id` and `content_hash`;
- `by_post_owner` set in the mapper.

**Not part of M15:** `tt.media.*` topics are retired later, and only with the owner's approval at the time (standing instruction 1).

---

## The review (REQ-014)

Written by the tks build session on 6 November 2026 (Iraq time). It answers REQ-014 from the master session.

- **What I reviewed:** `Abdullah95k/securebrand-contracts`, PR #1, branch `claude/securebrand-platform-integration-4iiwlr`, commit `3a75c8b`: `README.md`, every proto, `docs/field-matrix.md` (the tt column), `samples/post.tt.json` and `samples/media_ready.tt.json`, `registry/extras.yaml`, `registry/topics.yaml`, `registry/platforms.yaml`.
- **tks's side:** `Abdullah95k/securebrand-microservices-tiktok`, `main` after PR #45 (`793b237`).
- **Read only.** Nothing changed for this review.
- All examples are synthetic.

### Summary

**The draft fits TikTok well.** Every field tks collects has a place, either a shared field or a listed extras key. All nine tks-specific answers are "right", some with a correction.

**Six things to change:**

1. **Rounded counts are not lower bounds.** `COUNT_EXACTNESS_ROUNDED` says "value is the lower bound". TikTok's rounded numbers have 3–4 significant digits, and whether TikTok rounds down or to the nearest value is unknown. Define ROUNDED as "approximate" and keep `LOWER_BOUND` for real floors. Also, tks never has a displayed text like "12.5K": TikTok's web JSON gives numbers. The tt sample's `"displayed": "12.5K"` should go.
2. **TikTok does serve subtitles.** The matrix and "Gaps that matter to analysis" say TikTok has no captions. In fact, list items carry `video.subtitleInfos`: WebVTT tracks with expiring URLs, of two kinds:
   - **ASR**, TikTok's own speech-to-text, mostly Arabic;
   - **MT**, machine translations.

   In tks's scrubbed fixtures, **14 of 30 videos** have them, and 12 have an Arabic ASR track. It's a small sample. Each track is 2–5 KB. This is free speech-to-text for analysis. `CaptionTrack` needs a way to tell ASR from machine translation (proposal in §4). tks doesn't collect them yet.
3. **Duplicates in `registry/extras.yaml`.**
   - `tt.status_code` duplicates `Status.reason`.
   - `tt.category_code` duplicates `Post.category`; the sample uses the extras key.

   Keep the shared fields and drop both keys.
4. **One name for playlists.** The kind is "playlist" in `registry/platforms.yaml` (`entity_kinds`) and "collection" in `common.proto`'s comments (Envelope `kind`, ID kinds).
5. **The volume rules need change detection.** Content is published only on change, and metrics on change or daily. tks's events can't feed that statelessly: tks publishes every read, and its event IDs come from the content hash alone. The mapper therefore belongs **in tks's persister**, which already knows when a row changed. My earlier "small stateless mapper" estimate grows (§6).
6. **`exit_country` should say where TikTok placed the reader.** That is the `region` TikTok's page reports to the identity, which can differ from the proxy provider's stated country. It should also be set on **every** event, not only on status reads. tks needs a small change to carry it (§5).

---

### 1. Mapping, per message

Notes on the tables:
- "tks" is the current `tt.v1` field.
- **agree** means the mapping is as the draft has it, or as the obvious reading of it.

#### Envelope and Provenance (on every message)

| tks | Contract | Verdict |
|---|---|---|
| (entity key) | `id` = `tt:<kind>:<native id>`; `native_id` as text | agree |
| `Observation.event_id` (xxh3 of entity and content hash) | `event_id` | **change.** tks's id ignores the observation time, so two reads with the same content share it. The mapper derives SHA-256(`<id>\|<observed_at µs>\|<content_hash>`), as the draft recommends. A redelivered `tt` event gives the same id |
| `Observation.observed_at` | `observed_at`; `version` = µs of it | agree |
| `Observation.content_hash` (xxh3-128 over the message, counts included for posts) | `content_hash` = hex SHA-256 of content fields, not counts | **change.** The mapper computes SHA-256 over the contract's content fields. tks's hash isn't reused |
| key-only deletion events (comments) | `partial = true` with the status | agree |
| `query_name`, `doc_id` (`creator`, `web-signed`…) | `provenance.source_operation`, `source_variant` | agree |
| `parser_version` | `provenance.parser_version` | agree |
| `job_ids` | `provenance.job_ids` | agree |
| `mode` (full, incremental, refresh, recrawl) | `provenance.mode` | agree. Backfill reads map to `CRAWL_MODE_BACKFILL` (tks marks them by job kind) |
| — | `provenance.collector` `tks`, `collector_version` | agree (build info) |
| — | `provenance.acquisition` | agree: always `ACQUISITION_SCRAPED_LOGGED_OUT` (imports: `ACQUISITION_IMPORT`) |
| raw answers of failed parses in `tks-raw` | `provenance.raw_ref` | agree. Needs a 1 % sample mode for D-004 (§5) |
| `Post.status_region` (status reads only) | `provenance.exit_country` | **change:** on every event (§5) |

#### Account → `content.account.v1`

| tks (`tt.v1.Account`) | Contract | Verdict |
|---|---|---|
| `user_id` | `envelope.native_id`; id `tt:account:<user_id>` | agree |
| `sec_uid` | `alt_ids["sec_uid"]` | agree |
| `username` | `handle` | agree |
| `nickname` | `display_name` | agree |
| — | `url` = `https://www.tiktok.com/@<username>` (built) | agree |
| `organization` (`isOrganization`) | `is_business`; `type` = ORGANIZATION when true | **change.** The matrix marks `type` "`organization` **A**". But when it's false, TikTok doesn't say person or page: leave `type` unset (rule 4), never PERSON |
| `bio` | `bio.text` (spans: none, TikTok gives no positions for bios) | agree |
| `bio_link` | `links[]` with `source = LINK_SOURCE_BIO` | agree |
| `bio_link_risk` | extras `tt.bio_link_risk` | agree |
| `verified`, `private` | `verified`, `is_private` | agree |
| `language` | `platform_language` | agree |
| `created_at` | `created_at` (EXACT) | agree |
| `username_modified_at`, `nickname_modified_at` | `handle_changed_at`, `name_changed_at` (EXACT) | agree |
| `avatar_media_id` | `picture` (role PROFILE_PICTURE) | agree |
| `status` (active, private, banned, unavailable; code) | `status` (VISIBLE, PRIVATE, BANNED, UNAVAILABLE; `reason` = TikTok's code, e.g. 10221) | agree |
| owner's or discovered account; private | `scope` FULL; **PROFILE_ONLY while private** | **question:** does scope follow the account's state, or only the registry's decision? tks would set PROFILE_ONLY while an account is private, since only its profile can be read |
| discovery score (discovered accounts) | `relevance.score`, `matched_rules` | agree |
| `counts` | `counts` (see snapshot) | agree |
| (not stored yet) `followingVisibility` | extras `tt.following_visibility` | agree (X) |
| — | `contact`, `categories`, `stated_country`, `location`, `cover`, `keywords` | not on TikTok: unset |

#### Account counts → `content.account_snapshot.v1` (and `Account.counts`)

| tks | Contract | Verdict |
|---|---|---|
| `followers`, `following`, `videos`, `hearts`, `likes_given`, `friends` | `followers`, `following`, `posts`, `likes`, `likes_given`, `friends` | agree |
| `counts.exact` (per observation) | `Count.exactness` per count | agree. The profile page's `statsV2` → EXACT. A list's `authorStatsV2` → ROUNDED for every count, since all list follower counts end in 000 (T31). See Q5 |
| `verified`, `private`, `username`, `nickname`, `status` | the snapshot fields | agree |

#### Post → `content.post.v1`

| tks (`tt.v1.Post`) | Contract | Verdict |
|---|---|---|
| `post_id` | native id; `tt:post:<id>` | agree |
| `user_id` | `account_id` | agree |
| `kind` video / photo | `type` SHORT_VIDEO, subtype `video` / `type` POST, subtype `photo` | agree (as the rules table) |
| — | `url` = `https://www.tiktok.com/@<username>/video/<id>` (`/photo/` for photo posts) | agree (built) |
| `created_at` | `published_at` (EXACT; a scheduled post: when it went public) | agree |
| photo posts' `imagePost.title` (served, not read) | `title` | **X**: tks doesn't read it yet; small |
| `caption` | `text.text` | agree |
| `text_language` | `text.platform_language` (`un` → empty) | agree |
| `text_ranges` | `text.spans` (UTF-16), mention spans with `AccountRef` (`user_id`, `sec_uid`, handle) | agree |
| `hashtags` | `hashtags` (lower-case names; ids not kept) | agree. tks also has hashtag ids: drop them, or `extras` if anyone wants them |
| mentions | `mentions[]` | agree |
| `region` | `created_in_country` | agree (Q3) |
| `music_id` (+ title, author, original from `tt.music.v1`) | `audio` (`tt:audio:<id>`, title, artist, original) | agree |
| `duration_ms`, `width`, `height` | `video.duration_ms`, `width`, `height` | agree |
| `cover_media_id`, `photos` | `media[]` (roles: Q7) | agree |
| `labels.ai_generated` | `labels.ai_generated` | agree |
| (served, not read) `AIGCDescription` | `labels.ai_label_text` | **X** |
| `labels.aigc_label_type` | extras `tt.aigc_label_type` | agree |
| `labels.warnings` | `labels.warnings` | agree. The matrix lists them under age_restricted/members_only/family_safe; they belong only in `warnings` |
| `labels.anchors_json` | extras `tt.anchors` | agree |
| `ad` | `labels.ad` | agree |
| `pinned`, `comments_disabled` | `flags.pinned`, `flags.comments_disabled` | agree |
| caption edits (new event, latest kept) | `flags.edited`, `edited_at` | agree. The mapper sets `edited` when the caption changes from the previous version (persister knows) |
| `category` | `category` (the code as text, "112") | agree; **drop extras `tt.category_code`** |
| `playlist_id` | `playlists[]` (`tt:playlist:<id>`) | agree (playlists themselves: X) |
| `counts` | `counts` (Engagement: views = plays, likes, comments, shares, saves, reposts) | agree |
| `status`, `status_code` | `status.availability`, `status.reason`, `status.since` | agree (Q6); **drop extras `tt.status_code`** |
| `discovered_via` | extras `tt.discovered_via` | agree |
| `comments_state`, `comments_collected`, `replies_collected`, `completeness`, `comment_count_read`, `comments_synced_at` | `coverage` (`comments_state`, collected, reported, completeness, `replies_collected`, `synced_at`; `replies_available = true`) | agree; `comments_disabled` → `COMMENTS_STATE_DISABLED` |
| (served, not read) `video.subtitleInfos` | `captions[]` | **X**, and a proposed field (§4) |
| — | `relevance` | agree. The mapper sets `region-IQ` from `created_in_country` and language rules (`lang-ckb`). Keyword ids need the shared list |
| — | `tagged`, `coauthors`, `links`, `uploader_tags`, `platform_topics`, `poll`, `video.live_status`, `chapters` | not on TikTok logged out: unset |
| — | `reshare` (duet, stitch) | **unknown**, not X: no duet or stitch source id was seen in 253 posts (only `duetEnabled`, `duetDisplay`…) |
| — | `location` | **unknown**: no `poiInfo` seen in the recordings |

#### Post counts → `content.metrics.v1`

| tks (`tt.v1.PostMetrics`) | Contract | Verdict |
|---|---|---|
| `post_id` | `envelope.id` `tt:post:<id>`, `target_kind` post, `parent_id` the account | agree |
| `captured_at` | `envelope.observed_at` | agree |
| `counts.*` | `counts.*` with exactness: **≥ 10,000 → ROUNDED, below → EXACT** (T31) | agree, with the ROUNDED definition fix |
| published every read today | publish on change, plus once a day | agree. The persister already stores metrics only on change; it emits then, and the daily walk gives the daily one |
| comment `like_count`, `reply_count` | `Metrics` with `target_kind` comment, `parent_id` the post, key the post | agree. Emitted when a whole re-read finds them changed |

#### Comment and reply → `content.comment.v1`

| tks (`tt.v1.Comment`) | Contract | Verdict |
|---|---|---|
| `comment_id` | native id; `tt:comment:<id>` | agree |
| `post_id`, `user_id` | `post_id`, `account_id` | agree |
| `parent_comment_id` (TikTok `reply_id`) | `root_comment_id` | agree (Q4) |
| `reply_to_comment_id` (TikTok `reply_to_reply_id`, when not 0 and not the root), else `parent_comment_id` | `parent_comment_id` | agree (Q4) |
| `depth` 0 / 1 | `depth` | agree |
| `author` (`user_id`, `sec_uid`, `username`, `nickname`, `avatar_url`, `verified`) | `author.account` (id, `alt_ids.sec_uid`, handle, display_name, picture_url, verified); `type` unset | agree |
| `author.predicted_age_group` | extras `tt.predicted_age_group` | agree (Q9) |
| `text`, `text_ranges`, `language` | `text` (spans UTF-16, `platform_language`) | agree |
| `created_at` | `published_at` (EXACT) | agree |
| `image_media_ids` | `attachments` (role ATTACHMENT, type IMAGE) | agree |
| `pinned_by_creator`, `liked_by_creator` | `flags.pinned`, `flags.liked_by_creator` | agree |
| (derived) | `flags.by_post_owner` | agree (Q8) |
| `edit_count`, `edited_at` | `flags.edited`, `edited_at` | agree |
| `stick_position` | extras `tt.stick_position` | agree |
| `labels` (`label_list` texts) | extras `tt.comment_labels` | agree. TikTok's labels look like badges ("Creator", "Friends"), so `author.badges` may fit once their meaning is checked |
| `status` visible / deleted, `deleted_at` | `status` VISIBLE / **DELETED**, `since` | agree. **For comments tks does say deleted**: absent from a complete re-read (a later sighting restores it) |
| `like_count`, `reply_count` | `counts.likes`, `counts.replies` | agree |
| `redacted` | — | not mapped (always false; erasure is the platform's, dormant) |

#### Media → `content.media.v1`, and the hand-off (`securebrand.media.v1`)

| tks (`tt.v1.Media`, `common.v1.MediaReady`…) | Contract | Verdict |
|---|---|---|
| `media_id` (UUIDv5) | `tt:media:<uuid>` | agree |
| owner ids | `owner_id`, `owner_kind` | agree |
| `kind` | `type`, `role`, `platform_kind` | agree (Q7) |
| `variant` (original, sd, hd, thumbnail) | `variant` | agree |
| `position`, `width`, `height`, `duration_ms`, `size_bytes`, `mime`, `source_url`, `source_expires_at` | the same | agree |
| `bitrate` | — | extras `tt.bitrate` (on media) |
| `MediaReady` (tks's `common.v1`) | `securebrand.media.v1.MediaReady` on `content.media.ready.v1` | agree. tks moves off its `common.v1` media types |
| — | `delete_after`, `required_consumers` | **question.** tks sets no deletion time: the owner said on 2 Nov that another service deletes the files. The contract has the producer set a hard deadline. tks can set it from a setting; its value is the owner's (R5) |
| — | `object.perceptual_hash` | can be added for images (cheap). Not for video |
| ledger consumes `tt.media.processed.v1`, `tt.media.deleted.v1` | consumes `content.media.processed.v1`, `content.media.deleted.v1` | agree. tks's `tt.media.*` hand-off topics are then unused by anyone; tks asks before retiring them (rule 1) |

#### Audio → `content.audio.v1`

See Q1.

#### Coverage → `collection.coverage.v1`

| tks (`common.v1.JobEvent`) | Contract `JobCoverage` | Verdict |
|---|---|---|
| `job_id`, `status`, `kind`, `subject_ref`, `subject_id`, `created_at`, `finished_at`, `counters`, `completeness`, `errors` | the same; `errors` → `reasons[]` with counts | agree. tks's counter names map to the shared keys (`requests`, `bytes` from `wire_bytes`, `posts`, `comments`, `replies`, `blocked`), the rest as `tt.<name>` |
| (in the task, not the event) the window | `window_from`, `window_to` | X; small |

#### Discovery → `discovery.candidate.v1`

| tks (`discovery_candidates`, not published yet) | Contract `Candidate` | Verdict |
|---|---|---|
| `account_ref` (`sec:`/`u:`), `user_id`, `username` | `account` (AccountRef) | agree |
| sources (commenter, mention, hashtag, sound, related, search, followers, following, import) | `source` | agree: all have values |
| `score`, `evidence` | `relevance`, `evidence[]` | agree |
| `state` (pending, checking, qualified, added, not_iraqi, dormant, blocked) and dormant `reason` | `outcome` (PENDING, ADDED, NOT_RELEVANT, DORMANT…), `outcome_reason` | agree. `qualified` → PENDING until added; `blocked` (the owner removed it) → **no value**. Proposal: `OUTCOME_EXCLUDED` (removed by the owner), or `NOT_RELEVANT` with reason `excluded` |

---

### 2. The mapping rules in README.md that apply to TikTok

| Rule | Verdict |
|---|---|
| TikTok videos → `POST_TYPE_SHORT_VIDEO` | right |
| TikTok photo posts → `POST_TYPE_POST`, subtype `photo` | right |
| Replies on TikTok: root = `reply_id`; parent = `reply_to_reply_id`, else the root | right. `reply_to_reply_id` = 0 means "none", so the root. Inline quoted replies follow the same rule (Q4) |
| Live streams → `POST_TYPE_LIVE` | right in general; tks collects no lives (a profile's `roomId` is dropped) |
| `version` = `observed_at` µs | right, and stateless. **But publishing "only on change" needs state** (§6) |
| Absent is not zero; deletions are partial events | right; tks already works this way |
| `sb-type` / `sb-contract` headers, platform topics stay | right. tks's `tt.*` keep `tks-type` |
| Keys: comments and metrics by post | right; tks already keys comments by post |

---

### 3. Corrections to the tt column of the field matrix

| Row | Matrix says | Should say |
|---|---|---|
| Account `type` | `organization` **A** | `is_business` from `isOrganization` **A**; `type` **N** (ORGANIZATION only when true) |
| Account `picture` | **A** (expires in hours) | right |
| Post `title` | — | photo posts' `imagePost.title`: **R**, **X** (not read) |
| Post `location` | not served (unknown) | right; say "unknown" |
| Post `reshare` | duets and stitches: **X** | **unknown**: no duet or stitch source seen in 253 posts |
| Post `labels.ai_label_text` | (not listed) | `AIGCDescription` **X** |
| Post `labels.age_restricted, members_only, family_safe` | warnings **R** | **—** for those three; `labels.warnings` **R** |
| Post `playlists` | `playlist_id` **S** | **S** (30 %: 75 of 253) |
| Post `captions` | — | **`video.subtitleInfos`: S–O, X** (not collected). 14 of 30 videos in tks's fixtures (a small sample), ASR Arabic in 12, WebVTT 2–5 KB, URLs expire. And in "Gaps that matter to analysis", TikTok has captions |
| Post `flags.edited` | (not listed) | caption change detected: **A** (by tks) |
| Post `video.width/height` | (not listed) | **A** |
| Counts `views` | plays **A** (rounded from 10,000) | right; `displayed` is never set (numbers only) |
| Comment `flags.by_post_owner` | derivable, not stored: **X** | derivable by the mapper: **A** once adopted |
| Comment `status: deleted` | **A** (when the count still rises) | right |
| Audio | `Music` **A** (title, author, original, duration, album, cover) | add: copyrighted **O**, commercial **O**; `uses` **X** (sound pages, discovery); `author` as an account **N** (TikTok gives a name only) |
| `provenance.exit_country` | `status_region` **S** | **A** once tks carries it on every event (X today) |
| `provenance.raw_ref` | failures only | right; the 1 % sample is X |

---

### 4. Fields with no place

| tks field | Proposal |
|---|---|
| `video.subtitleInfos` (not collected yet) | Shared field `Post.captions`, as it is. `CaptionTrack` needs one more field: **`string source`**, with values `asr` (speech-to-text), `mt` (machine translation), `uploader`, `platform_auto`. `auto_generated` alone can't tell a translation from the original speech. Optional: `string url` with `url_expires_at`, for collectors that reference before downloading |
| `Media.bitrate` | extras **`tt.bitrate`** on media (YouTube has bitrates too: a candidate for a shared field later) |
| `Post.hashtags[].id` (TikTok hashtag ids) | extras **`tt.hashtag_ids`** on post, only if analysis or discovery wants them (hashtag pages are keyed by name, so probably not) |
| `effectStickers`, `stickersOnItem` (served, not read) | extras **`tt.effects`** on post, if collected |
| `Music.commerce` | `Audio.commercial` (exists) |
| Discovery state `blocked` | `OUTCOME_EXCLUDED`, or a reason (§1, discovery) |
| `followingVisibility` | extras `tt.following_visibility` (listed) |

**Drop from `registry/extras.yaml`:**
- `tt.status_code`: use `Status.reason`.
- `tt.category_code`: use `Post.category`.

**Keep, as listed:**
- `tt.bio_link_risk`
- `tt.following_visibility`
- `tt.aigc_label_type`
- `tt.anchors`
- `tt.discovered_via`
- `tt.stick_position`
- `tt.comment_labels`
- `tt.predicted_age_group`

---

### 5. What tks can't fill, and what would change how it collects

**Can't fill (TikTok doesn't serve it logged out):**
- account `type` (person or page);
- `contact`, `stated_country`, `location`, `cover`, `keywords`;
- post `tagged`, `coauthors`, `links`, `poll`, `chapters`;
- `Count.displayed`;
- comment author type.

**Unset without loss:**
- `Audio.author` as an account (TikTok gives a name only);
- `Audio.uses` until discovery reads sound pages.

**Changes tks makes to collection:**

| Change | What it involves | Size |
|---|---|---|
| **The exit's country on every event** | Each identity knows the `region` TikTok's page reported for it. tks carries it to every `tt` event (a field on `Observation`, or in the source the emitter stamps). An addition, compatible | S |
| **A 1 % raw sample** (D-004) | tks keeps failures today (`tt.capture_raw: failures`); add a `sample` mode with a rate | S |
| **Subtitles** | Read `video.subtitleInfos` from list items (free: already in every answer); fetch each WebVTT file before its URL expires (one CDN request, 2–5 KB per track); publish `Post.captions`. **Not yet measured:** whether the files need the `tt_chain_token` cookie, as video files do. The next probe run can check it | M |
| **Read what's served and dropped** | photo-post `imagePost.title`, `AIGCDescription`, `followingVisibility` | S |
| **Media deadline** | `delete_after` from a setting, when the owner gives the value (R5) | S |

---

### 6. The effort to adopt it (updated)

My earlier estimate was "about one milestone, a small stateless mapper". Two things in the draft change that:
1. Content events are published **only when the content changed**, and `Metrics` **only on change plus once a day**.
2. tks publishes every read, and its event ids come from the content hash only.

A stateless mapper would either publish every read (which breaks the volume rule) or need its own store of hashes.

**Recommended: emit `content.*` from tks's persister.** It already decides, row by row, whether a post, comment, account or metrics row changed (its upserts compare content hashes and write metrics only on change).

| Step | What | Size |
|---|---|---|
| 1 | Depend on the contract module (`gen/go`); move the media hand-off off tks's `common.v1` onto `securebrand.media.v1` (`content.media.*` topics); the ledger consumes the contract's processed and deleted topics | S |
| 2 | A content emitter in the persister: for each changed row, map and produce in the same unit of work. **Order:** produce (acks all) before the database transaction commits. If the produce fails, roll back and retry from Kafka; if the commit fails after the produce, the repeat carries the same `event_id`, and consumers ignore it. Metrics: on change, plus the daily walk's read as the heartbeat | M |
| 3 | The mapping per entity (§1), extras keys, `by_post_owner`, relevance rules (`region-IQ`, language), exactness rules | M |
| 4 | Exit country on every `tt` event; the 1 % raw sample | S |
| 5 | Conformance in CI over the e2e run against `fakett` (every entity, deletions, partial events); samples | S |
| 6 | Helm: topics in the shared cluster, the `sb-*` headers; nothing registered by tks | S |

**Total: one milestone of about 5–7 build days** (was "a few days"). Subtitles are a separate M if wanted.

All of it is additive: `tt.*` stays as it is (rule 1), and nothing is registered in a shared registry by tks.

---

### 7. The questions for tks

**Q1. Audio vs `tt.music.v1`.**

| `tt.music.v1` | `content.audio.v1` | Note |
|---|---|---|
| `music_id` | id `tt:audio:<id>` | |
| `title` | `title` | |
| `author_name` | `artist` | |
| — | `author` (AccountRef) | **N**: TikTok gives the name only; for an original sound it is usually, not always, the poster |
| `original` | `original` | |
| `duration_ms` | `duration_ms` | |
| `album` | `album` | |
| `cover_media_id` | `cover` (role AUDIO_COVER) | |
| `copyrighted` | `copyrighted` | |
| `commerce` | `commercial` | |
| — | `url` | built: `https://www.tiktok.com/music/-<id>` |
| — | `uses` | **X**: only the sound's own page gives it (discovery, M11) |

Missing: nothing tks collects. **One addition proposed: `Status status`** on Audio. TikTok removes or mutes sounds for copyright, and a post's sound then disappears. It's optional.

**Q2. Account IDs:** `native_id` = `user_id`, `alt_ids["sec_uid"]`. **Right.**
- `user_id` is numeric and stable.
- `sec_uid` is what TikTok's calls take; whether it survives a rename is still open (T17), so it must stay an alternate id, never the key.
- The same holds in every `AccountRef`: mention spans and comment authors carry both.

**Q3. Country fields.**
- **`region` → `created_in_country`: right.** TikTok gives an ISO alpha-2 code (`IQ`), sometimes empty.
- **`status_region` → `provenance.exit_country`: right in kind, but too narrow.**
  - `status_region` exists only on status reads.
  - `exit_country` should be on every event, and should be **the region TikTok placed the reader in** (the page's `region` for that identity): TikTok serves by that, not by the provider's label.
  - tks adds it to every event (§5).

**Q4. Threading.** **Right, and it holds for the inline quoted replies too.**
- A comment page quotes some replies under their comment (`reply_comment`).
- tks parses each quoted reply as a reply in its own right: its `reply_id` names the top-level comment, and its `reply_to_reply_id` the reply it answers.
- So root = `reply_id`, and parent = `reply_to_reply_id` when it isn't 0, else the root. Depth is 1 for every reply (TikTok has two levels).
- A quoted reply and the same reply read later from the reply list carry the same `cid`, `reply_id` and `reply_to_reply_id`, so they get the same id and threading. tks's parser test covers the quoted case (`internal/tt/parse/parse_test.go`: depth 1, parent the quoting comment, `reply_to_reply_id` 0 → none).

**Q5. Rounded counts:** **yes, tks can set exactness per count.**

| Counts | Rule |
|---|---|
| Account counts from the profile page (`statsV2`) | all EXACT |
| Account counts from a list item (`authorStatsV2`) | all ROUNDED: every follower count seen there ended in 000 |
| Post and comment counts | ≥ 10,000 → ROUNDED; below → EXACT (T31) |

Two caveats:
1. ROUNDED should mean "approximate", not "lower bound" (Summary, point 1).
2. `displayed` stays empty: TikTok's JSON gives numbers.

**Q6. Unavailable posts.**

| Case | Contract | Verdict |
|---|---|---|
| 10216 | `AVAILABILITY_PRIVATE`, `reason` "10216" | **right** |
| 10204 and any other code | `AVAILABILITY_UNAVAILABLE`, `reason` = the code | **right** |
| `since` | the first read in that state | **right** |

- tks never says "deleted" for a post: 10204 is what TikTok answers for a post its author deleted **and** for one TikTok removed. A logged-out reader can't tell them apart.
- **Note 1:** tks marks only 10216 today. 10204 is counted but not yet marked (T34; C6 run 1 found 6 of 6 posts at 10204). Marking it is a one-line setting (`tt.posts.unavailable.codes`).
- **Note 2:** for **comments**, tks does say deleted (absent from a complete re-read) → `AVAILABILITY_DELETED`.

**Q7. Media kinds:** **right.**

| tks kind | Contract role | Type |
|---|---|---|
| cover, origin_cover, dynamic_cover | `MEDIA_ROLE_COVER`, with `platform_kind` | IMAGE |
| music cover | `MEDIA_ROLE_AUDIO_COVER` | IMAGE |
| video | CONTENT | VIDEO |
| photo | CONTENT | IMAGE, with position |
| avatar | PROFILE_PICTURE | IMAGE |
| comment image | ATTACHMENT | IMAGE |

The dynamic cover is an animated WebP: type IMAGE with its mime, not GIF.

**Q8. `by_post_owner`:** **yes, the mapper sets it** from fields every comment carries.
- It is true when the author's `user_id` equals the post's `user_id`.
- In pseudonymized mode (not used), the author keys are compared instead.
- No extra request, no state.

**Q9. `predicted_age_group` as extras `tt.predicted_age_group` on the comment:** **no technical problem.**
- It is a property of the author, repeated on each of the author's comments. That's fine; putting it on the author instead isn't possible, since `Author` has no extras.
- It is absent on about two-thirds of authors: the key is then left out, never set to "".
- The value is TikTok's band label as served.
- The owner and legal decide whether it is carried (README open point 2). tks has stored it since 1 November by the owner's decision.
