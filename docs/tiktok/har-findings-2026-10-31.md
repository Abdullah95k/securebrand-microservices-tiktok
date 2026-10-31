# Logged-out TikTok: findings from the owner's recordings (2026-10-31)

On 2026-10-31, logged-out TikTok on desktop Chrome served a public account's page with its details, its whole post list page by page, every post's counts, top-level comments **and replies**. This note records what the recordings show, for the open questions ([open-questions.md](open-questions.md)) and the probe ([probe-spec.md](probe-spec.md)).

**Source:** 5 HAR files recorded by the owner on 2026-10-31 between 21:57 and 22:01 UTC, in a logged-out Chrome on macOS, from Iraq (the pages say `region=IQ`, `tz_name=Asia/Baghdad`), about 207 MB and 2,149 requests:
- 2 profile sessions, each scrolled through several pages of the grid: **account A**, a large TV channel (millions of followers, thousands of posts, posting every 15 minutes or so), and **account B**, a public figure (millions of followers, hundreds of posts);
- 3 post sessions, each a post opened from a grid with its comments scrolled and several reply threads opened.

The raw files stay in `.local/har/` (git-ignored) and are never committed. This note holds only request shapes, field names and counts: no usernames, names, bio links or comment text.

**Caveats:**
- The export **stripped cookies**: no request carries a `cookie` header and no response a `set-cookie` header, although the option to keep them was in the recording guide. Cookie names stay open (T2).
- The post sessions opened posts by clicking (single-page navigation), so **no post page HTML** was recorded (T1 for post pages stays open).
- One person, one network in Iraq, one evening, one browser that ran TikTok's script. Whether a crawler without that script gets the same is the probe's question (T4).

---

## 1. Calls the logged-out web app made

All on `www.tiktok.com`, all `GET`, all HTTP 200.

| Call | Count | What it returned |
|---|---|---|
| `/@<username>` (the profile page) | 2 | Server-rendered HTML with the account (§2) |
| `/api/post/item_list/` | 16 | The post list, 14–19 posts a page (§3) |
| `/api/comment/list/` | 35 | Top-level comments, up to 20 a page (§4) |
| `/api/comment/list/reply/` | 21 | Replies, up to 3 a page (§4) |
| `/api/user/playlist/`, `/api/story/item_list/`, `/api/repost/item_list/`, `/api/user/collection_list/`, `/api/user/list/` | 1 or 2 each | Profile tabs and suggestions; not needed |
| `/api/feedback/v1/newest_reply/`, `/api/compliance/settings/`, `/api/share/settings/`, `/api/global-footer/graphql`, … | a few | UI settings; not needed |

Other hosts: `mssdk-sg.tiktok.com` (`/web/resource`, `/web/report`: TikTok's script, which answers with a new `msToken` in an `x-ms-token` header), `mcs-sg.tiktokv.com` and `mon*.tiktokv.com` (telemetry: about 180 posts per profile session), `*.ttwstatic.com` (scripts), `p16/p19-common-sign.tiktokcdn.com` (images), `v16/v19-webapp-prime.tiktok.com` (video files). The `-sg` hosts suggest the session was served by TikTok's Singapore cluster (T23).

**The list call the browser used is `/api/post/item_list/`**, the one public sources report as needing `X-Gnarly`. The `/api/creator/item_list/` call that yt-dlp and gallery-dl use did not appear: the web app does not use it, so whether it answers a crawler is still the probe's question (T4).

## 2. Request format

**Query parameters**, in the browser's order, on every API call:

`WebIdLastTime`, `aid=1988`, `app_language`, `app_name=tiktok_web`, `browser_language`, `browser_name=Mozilla`, `browser_online`, `browser_platform`, `browser_version` (the user agent without "Mozilla/"), `channel=tiktok_web`, `cookie_enabled`, then the call's own parameters in alphabetical order with the rest (`count`, `coverFormat=2`, `cursor`, `data_collection_enabled=false`, `device_id`, `device_platform=web_pc`, `focus_state`, `from_page`, `history_len`, `is_fullscreen`, `is_page_visible`, `language`, `odinId`, `os`, `priority_region` (empty), `referer`, `region=IQ`, `root_referer`, `screen_height`, `screen_width`, `secUid` or `aweme_id`, `tz_name`, `user_is_login=false`, `video_encoding`, `webcast_language`), and last, in this order, **`X-Dynosaur`, `msToken`, `X-Bogus`, `X-Gnarly`**.

- **The parameters are alphabetical** (case-sensitive, capitals first), with the four signature parameters appended at the end.
- **`X-Bogus` was the constant `1`** on every call; `X-Gnarly` was 332 characters, `X-Dynosaur` 412–508 characters. This matches one public source's report of September 2026 ([methods.md](methods.md) §2.3).
- **`msToken`** was empty on the first list call of a session, then 152–172 characters. It arrives in `x-ms-token` response headers (the profile page's and the script's own calls).
- **`device_id` and `odinId`** are 19-digit numbers, **`WebIdLastTime`** a 10-digit Unix time. **They are the profile page's `wid`, `odinId` and `webIdCreatedTime`** (checked on both profile sessions, the same on every call of a session), and the calls' `region` and `language` are the page's too: one page load gives an identity every value an unsigned call needs. The page also reports `botType: "others"` for this browser; the probe records what a crawler gets.
- **Later list pages add** `clientABVersions`, `enable_cache`, `from_page`, `needPinnedItemIds` and `post_item_list_request_type`.
- **The recording browser** was Opera on macOS (Chromium 152: `sec-ch-ua` has `"Chromium";v="152", "Not?A_Brand";v="24"`), sending `browser_platform=MacIntel`, `os=mac`, `screen_width=1440`, `screen_height=932`, `tz_name=Asia/Baghdad`, `video_encoding=dash`.
- **Headers** on API calls are only Chrome's standard ones (`accept`, `accept-encoding`, `accept-language`, `priority`, `referer`, the three `sec-ch-ua*`, three `sec-fetch-*`, `user-agent`): no custom `x-` header. TikTok's page responses carry `x-akamai-request-id`, so Akamai fronts the site (T5).

## 3. The account

**The profile page** was server-rendered (about 405 KB of HTML, 92 KB on the wire), with `<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">` holding, under `__DEFAULT_SCOPE__`, the scopes `webapp.app-context`, `webapp.biz-context`, `webapp.i18n-translation`, `seo.abtest`, `webapp.user-detail` and `webapp.a-b`. No `SIGI_STATE`, `__NEXT_DATA__` or creator-embed state. **T1 confirmed for profile pages** (both loads server-rendered; how often a shell is served is for the probe).

- **`webapp.app-context`:** `language`, `region`, `appId`, `appType`, `wid`, `webIdCreatedTime`, `odinId`, `nonce`, `botType`, `requestId`, `clusterRegion`, `abTestVersion`, `csrfToken`, `userAgent`, `encryptedWebid`, `host`.
- **`webapp.user-detail`:** `userInfo` (`user`, `stats`, `statsV2`, `itemList`), `shareMeta`, `statusCode` (0), `statusMsg`, `needFix`. **`itemList` is empty**: the page embeds no posts.
- **`userInfo.user`:** `id` (18 or 19 digits), `shortId`, `uniqueId`, `nickname`, `avatarLarger`, `avatarMedium`, `avatarThumb`, `signature` (bio), **`createTime`** (when the account was created), `verified`, `secUid` (76 characters), **`bioLink`** (`{link, risk}`), `privateAccount`, `secret`, **`uniqueIdModifyTime`** and **`nickNameModifyTime`** (when the username and nickname last changed), `isOrganization`, `commerceUserInfo`, `ttSeller`, `language`, `roomId`, settings (`commentSetting`, `duetSetting`, `stitchSetting`, `downloadSetting`, `followingVisibility`, `profileTab`, …) and more.
- **`stats`** holds rounded numbers (`followerCount` 15,900,000); **`statsV2` holds the exact counts as strings** (15,858,019): `followerCount`, `followingCount`, `heart`, `heartCount`, `videoCount`, `diggCount`, `friendCount`.

Every field of objective §1 is there, including the bio link. `uniqueIdModifyTime` dates a rename even between two daily reads.

## 4. Posts

**`/api/post/item_list/`** (`count=16`): 16 pages, 253 posts. Each answer has `cursor`, `extra`, `hasMore`, `itemList`, `log_pb`, `statusCode` (0), `status_code`, `status_msg`, and sometimes `filter_reasons`.

- **Cursor:** a time in milliseconds (the first call sends `0`; the next cursor is about the last post's `createTime` × 1000). `hasMore` stayed true on every page recorded. **T11 confirmed for this call.**
- **Page size:** 14–19 posts for `count=16`. The first page of account A held 19: its **3 pinned posts first**, then 16 by date. Account B's pages that held 14 or 15 posts carried `filter_reasons: {"filterReasons":[1]}`: posts withheld from this viewer, reason unknown (**T30**, new).
- **Bytes:** 37–59 KB on the wire per page (330–506 KB decompressed), about 3 KB on the wire per post. **T21** (for a browser).
- **Depth:** account B's six pages reached back about five months; account A's ten pages covered about one day (it posts every 15 minutes or so). No wall in either session. 365 days is for the probe (T11).

**Each post** (all 253) carries: `id`, `desc`, `createTime`, `video`, `music`, `author`, `authorStats`, `authorStatsV2`, `stats`, `statsV2`, `isAd`, `AIGCDescription`, `ShowAIGC`, `CategoryType`, `textLanguage`, `itemCommentStatus`, `privateItem`, `secret`, `forFriend`, `shareEnabled`, `duetDisplay`, `stitchDisplay`, `officalItem`, `originalItem`, `isReviewing`, `isProhibited`, `item_control`, `creatorAIComment`, `penaltyContext`, `collected`, `digged`; and where they apply: `challenges` and `textExtra` (60 posts with hashtags), `contents` (166), `playlistId` (75), `duetEnabled` and `stitchEnabled` (171), `effectStickers`, `stickersOnItem`, **`isPinnedItem`** (4, all `true`), **`imagePost`** (1: `cover`, `images`, `shareCover`, `title`), `aigcLabelType`, `anchors`, `warnInfo`.

- **Counts:** `stats` has `playCount`, `diggCount`, `commentCount`, `shareCount`, `collectCount`; `statsV2` the same as strings plus **`repostCount`**. **T9 confirmed**: a new post costs nothing beyond the list call.
- **Pinned:** `isPinnedItem: true` on pinned posts, which come first, out of date order. **T10 confirmed.**
- **Ads and labels:** `isAd` is on every post (all `false` in this sample); `AIGCDescription` and `ShowAIGC` on every post, `aigcLabelType` on one. No paid-partnership field appeared. **T16 partly confirmed.**
- **Photo posts:** one post carried `imagePost` instead of a playable video. **T19 confirmed in the list.**
- **Hashtags and mentions:** `textExtra` entries carry `hashtagName`, `start`, `end`, `type`, `subType`, `isCommerce`, `awemeId`, and for mentions `userId`, `secUid`, `userUniqueId`; `challenges` carry `id`, `title`, `desc` and covers.
- **Music:** `id`, `title`, `authorName`, `original`, `duration`, `album`, `playUrl`, covers, `isCopyrighted`, `is_commerce_music`, `private`.
- **Video:** `duration`, `width`, `height`, `cover`, `originCover`, `dynamicCover`, `zoomCover`, `playAddr`, `downloadAddr`, `PlayAddrStruct`, `bitrateInfo`, `bitrateAudioInfo`, `codecType`, `definition`, `format`, `size`, `subtitleInfos`, `VQScore`, and more.
- **The author** object (`id`, `uniqueId`, `nickname`, `secUid`, `signature`, avatars, `verified`, `privateAccount`, settings) repeats on every post, with `authorStats`.
- **Ids carry time:** for 253 of 253 posts, `id >> 32` is within 13 seconds of `createTime` (usually 4–10 seconds earlier), except a few up to 6.5 hours earlier, most likely posts scheduled ahead (an item has `scheduleTime` in public sources). **T8 confirmed**, with a margin of a few hours for the backfill's age cut-off.

## 5. Comments and replies

**Top-level comments are served logged out. T12 confirmed** (for a browser in Iraq).

- **`/api/comment/list/`**: `aweme_id`, `count=20`, `cursor` (an offset: 0, 20, 40, …). Answers: `comments`, `cursor`, `has_more`, **`total`**, `status_code`, `reply_style`, `has_filtered_comments`, `alias_comment_deleted`, `top_gifts`, and once `has_fold_comment`/`fold_comment_cursor`. Pages held 10–20 comments for `count=20`.
- **Order: TikTok's ranking, not time.** `create_time` was out of order on almost every page. **T13 confirmed**: the refresh strategy is the "re-read to the cap" one (ADR 0007 §5).
- **The browser fetched every comments page twice** (two identical calls a few milliseconds apart), so a reader must count unique comment ids.
- **Replies are served logged out. T14 confirmed.** `/api/comment/list/reply/`: `item_id`, `comment_id`, `count=3`, `cursor` (offset). Replies carry `reply_id` (the top-level comment's id), `reply_to_reply_id` (0 or the reply answered) and `thread_id`.
- **Completeness:** one post was read to the end (`has_more: 0`): 143 unique top-level comments, Σ(1 + `reply_comment_total`) = 202 against `total` = 210, so **96 %**. The other two were stopped early. **T15 partly confirmed.**
- **Each comment** carries `cid`, `text`, `create_time`, `digg_count`, `reply_comment_total`, `reply_comment` (inline previews), `reply_id`, `reply_to_reply_id`, `aweme_id`, `comment_language`, `stick_position`, `author_pin`, `is_author_digged`, `label_list`, `text_extra`, `image_list`, `status`, `fold_status`, `no_show`, `share_info`, `thread_id`, and more.
- **Each comment's author** carries `uid`, `sec_uid`, `unique_id`, `nickname`, `avatar_thumb` and many empty profile fields. **`uid` was present on all 706 author objects. T17 partly confirmed** (the numeric id is always there; `secUid` across renames is still open).
- **Privacy:** some author objects (234 of 706) carry **`predicted_age_group`**, TikTok's inference of the person's age band. tks must **never store** it (ADR 0007 §6).

## 6. Media

- **Images** (`p16/p19-common-sign.tiktokcdn.com`) are signed links (`x-expires`, `x-signature`) that lasted **5 to 48 hours**; covers came as AVIF or JPEG, 3–15 KB each.
- **Video files** (`v16/v19-webapp-prime.tiktok.com`) are signed links (`expire`, `signature`, `policy`) that lasted **48 hours**, 8–10 MB per file, fetched whole. The export hides whether they needed cookies. **T18 partly confirmed** (links signed and expiring; cookies still open). With downloads switched on (D5), a video costs about as much as 200 list pages, so downloads stay off by default and use the smallest rendition when on.

## 7. What changes in the plan

- **The objective is reachable by a browser, comments and replies included.** The biggest open risk from the public research (comments needing login) did not happen in this session.
- **The method decision now hinges on T4:** every call the web app made carried `X-Gnarly` and `X-Dynosaur` from TikTok's script. The probe's unsigned steps (the `creator` list call, then the app's own calls without signatures) decide whether the browser signer is needed for lists and comments, or only as the fallback.
- **The probe builds requests the way the recordings show:** parameters alphabetical with the signature parameters last, `device_id`/`odinId`/`WebIdLastTime` from the page's app context, `region` from the page, no custom headers, `count=16` for lists, `count=20` for comments and `count=3` for replies (larger counts tried as a separate step).
- **Comments are re-read to the cap, not newest first** (T13). The comment cap and the age tiers (ADR 0007 §4) are what bound the cost.
- **Busy accounts are very busy:** account A posts about every 15 minutes, close to 100 posts a day. A 365-day backfill of such an account is thousands of posts, about 15–16 per list page, so hundreds of list pages, plus comments; the comment cap matters most there.
- **The data model gains fields:** account `createTime`, `uniqueIdModifyTime`, `nickNameModifyTime`, `isOrganization`; post `repostCount`, `ShowAIGC`/`aigcLabelType`, `textLanguage`, `CategoryType`, `playlistId`; comment `author_pin`, `is_author_digged`, `reply_to_reply_id`, `image_list`. It drops nothing. `predicted_age_group` is never stored.
- **New question T30:** what the list's `filterReasons` withholds, and whether the probe from a European exit sees the same.
