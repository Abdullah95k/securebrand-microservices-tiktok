# What a logged-out TikTok search session showed (owner's recording, 2026-11-04)

The owner recorded one search session on 2026-11-04, between 22:39:46 and 22:40:44 UTC: 483 requests in Chrome on macOS, from Iraq.

- The recording itself is a 68.6 MB HAR. It was read in the build session's scratch space only. It is **not in git** and never will be (D13).
- This summary holds **no** keyword, username, name, id, link, caption or comment.
- The session was **logged out**: every call carried `user_is_login=false`.
- The browser was in Iraq: `region=IQ`, `tz_name=Asia/Baghdad`.

It answers several of the questions the discovery plan ([`discovery-plan.md`](discovery-plan.md) §5) left for the C6 probe.

## 1. Calls

All on `www.tiktok.com`, all `GET`, all HTTP 200.

| Call | Count | What it is |
|---|---|---|
| `/api/recommend/item_list/` | 1 | The For You feed shown before the search (`from_page=fyp`, `count=12`): 8 full posts |
| `/api/search/suggest/guide/` | 1 | Suggestions while typing (79 bytes) |
| `/api/search/general/preview/` | 1 | The suggestion list under the search box (`sug_list`) |
| `/api/search/general/full/` | 9 | **The search results**, page after page as the owner scrolled |
| `/api/feedback/v1/newest_reply/` | 6 | Site notices; not needed |

The other hosts were the usual ones from the first recording ([`har-findings-2026-10-31.md`](har-findings-2026-10-31.md)): telemetry (`mcs-sg.tiktokv.com`, `mon*.tiktokv.com`), images and video on the CDNs, and TikTok's login script assets, which load on every page.

Not in this recording: the account-only search tab, hashtag pages, sound pages, and followers or following lists. The C6 probe still covers them.

## 2. The search call

- **Parameters.** The same set as the profile's calls (`device_id`, `odinId`, `WebIdLastTime`, `region`, `tz_name`, `user_is_login=false`, …). Then the call's own:
  - `keyword`;
  - `offset` and `cursor`: 0, 12, 24, … 96;
  - `count=12`;
  - `search_source=normal_search`, and `search_id` from the second page on;
  - `is_non_personalized_search=0`;
  - `web_search_code` (a fixed JSON of client flags);
  - `device_type=web_h265`.
- **Signed by TikTok's script.** Every page carried `X-Dynosaur`, `msToken`, `X-Bogus` and `X-Gnarly`, as on the comment calls. tks's signer pool signs it the same way, inside a TikTok page (D11). Nothing is re-implemented. Whether a plain call with that signature is answered from tks's exits is a C6 question, as it was for comments.
- **Depth.** All **9 pages were answered with no login wall**, about one page a second. The last answer still said `has_more: 1`; the owner stopped scrolling there. How far it goes before TikTok asks to log in is still a C6 question.
- **Size.** 230–280 KB per page before compression.

## 3. What a search answer carries

- **`data`.** A list of results; most pages had 12, and the later pages 15, 24 and 28.
  - **`type: 1`**: a post. Its `item` is the **same full post object as the account's own list**: caption, time, counts (`statsV2`), music, hashtags and mentions (`textExtra`), video, AI label, `isAd`. The `author` comes with id, `secUid`, username, nickname, bio (`signature`), verified and private flags, plus `authorStatsV2`.
  - **`type: 4`**: an account card, `user_list` (2 accounts on the first page). Each has `user_info` with `follower_count`, nickname, avatar and verification, and the account's posts and sounds.
- **`cursor`, `has_more`, `extra.search_request_id`, `log_pb`.** `ad_info` was empty: no sponsored results in this session.
- **One keyword over 9 pages gave 141 posts by 117 different accounts.**
  - All the authors were public.
  - Followers ranged from 40 to 7 million (median about 7,000); 24 of the 141 posts were by accounts with under 1,000 followers.
  - Post languages (`textLanguage`): Arabic 56, undetected 45, English 32, others 6.
- **No search result carries `locationCreated`**, the region a post was made in. The account's own post list carries it (C2), so tks still needs one read of a new account's list to tell where it posts from ([`discovery-plan.md`](discovery-plan.md) §3).

## 4. The For You feed, logged out

`/api/recommend/item_list/` gave 8 full posts to a logged-out viewer in Iraq. The call was signed the same way, with `region=IQ`.

These are the posts TikTok recommends to Iraqi viewers, so their authors fit "accounts the Iraqi audience is interested in" ([`discovery-plan.md`](discovery-plan.md) §3). None carried `locationCreated` either.

## 5. What this changes in the plan

- Logged-out search works, at least 9 pages deep, and needs TikTok's signature, which tks already gets from its signer pool.
- A search page gives everything needed to judge a post and to store it: the full post and its author. Only the region needs the extra list read.
- **The country question is real.** This session ran from Iraq and was served `region=IQ`. tks's exits were served `region: US` (C2). C6 must compare what Iraqi keywords and the For You feed return from tks's exits with what they return from Iraq.
