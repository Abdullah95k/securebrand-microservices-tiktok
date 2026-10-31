# 3. Parsing, public data model, fixtures and fakefb

[← Index](README.md)

Repository: this repository at commit `265ae3a`. `path:line` references are relative to the repository root and point at that commit.
Scope of this note: `internal/fb/parse`, `testdata/fb/**`, `api/proto/securebrand/fb/v1/*.proto` (+ the internal task proto), `internal/topics`, `internal/fakefb`, `cmd/fakefb`, `internal/fixtures`, `docs/contracts/kafka-schemas.md`. Where the parsers' outputs are consumed elsewhere (crawl stages, emitter, finalizer) I cite those call sites briefly so the behaviour is understandable end to end; the HTTP client, doc_id registry, crawl pipeline and persistence are covered in other notes.

All `file:line` references are to the repository at commit `265ae3a`. The in-scope unit tests (`go test ./internal/fb/parse ./internal/fixtures ./internal/fb/htmlx ./internal/fb/graphql`) pass at that commit.

---

## 0. TL;DR for the Instagram team

1. **The parsers don't use fixed JSON paths.** Facebook's Comet/Relay GraphQL responses are treated as an arbitrary tree. A generic walker (`parse.Collect`) collects every object whose `__typename` is in a wanted set (`Story`, `Comment`, `Feedback`, `Photo`, `Video`, ...) and records each one's nearest collected ancestor. Ownership ("does this Feedback belong to this Story or to a Comment quoted inside it?") is decided by that ancestor chain, not by paths. Each field is then read with a list of candidate paths (`First`), or with a depth-first key search that stops at nested entities (`Find`/`FindWhere`/`FindAll` with `skip` typenames).
2. **Drift detection is built into the parser contract.** Every parser returns a `Report` with `Missing` (required fields: the response is drifted, and the crawler classifies it as `SchemaDrift`) and `Optional` (only metrics). Very little is required: post id + creation time, comment id + created time, and page id + name.
3. **Ids are decoded from Facebook's base64 graph ids:** `S:_I<actor>:<post>:...` for stories, `comment:<post>_<comment>` for comments, `feedback:<post>[_<comment>]` for feedback. Media get deterministic UUIDv5 ids per (fb media id, variant).
4. **Testing is golden-file based.** Each fixture directory is `testdata/fb/<query>/<case>/{response.ndjson|response.html, meta.json, expected.json}`. Three kinds are mixed: synthetic (`source: fakefb`), handcrafted variant shapes (`source: handcrafted`), and real captures scrubbed of PII. `make golden` rewrites `expected.json`, and `fbs-spike check-fixtures` parses captured fixtures without comparing.
5. **The scrubber pseudonymises real responses.** It walks the JSON (including JSON serialized inside strings) while preserving key order. It pseudonymises people's ids and names consistently, blanks text people wrote (keeping UTF-16 length so mention offsets stay valid), and strips signed CDN parameters.
6. **`fakefb` is a deterministic synthetic Facebook** (seeded world plus ground truth). It serves page HTML with tokens, the `/api/graphql/` endpoint in Facebook's streamed `@stream`/`@defer` NDJSON shape, a signed-URL media host, a relay JS bundle that declares doc_ids, and an admin API. Faults can be injected: rate limits, login walls, doc_id rotation, schema drift, JS-only tokens, partial-field errors and latency. The e2e tests compare against its ground truth to assert 100% completeness.
7. **Public output is protobuf on Kafka** (`fb.page.v1`, `fb.post.v1`, `fb.post_metrics.v1`, `fb.comment.v1`, `fb.media*.v1`, `fb.job.v1`, `fb.erasure.v1`). Values are plain protobuf (no Confluent framing) with an `fbs-type` header. Subjects are registered in a Schema Registry as `BACKWARD_TRANSITIVE`, and `buf breaking` runs in CI.

The walker, the `Report` contract, the golden/fixture framework, the scrubber, the signed-URL expiry, the deterministic media ids, the event-id hashing and the fake-server pattern all carry over to Instagram almost unchanged. What needs Instagram-specific work is the entity typenames/keys, id codecs, pagination signals and the protos (section 13).

---

## 1. Package map

| Path | Role |
|---|---|
| `internal/fb/graphql/parts.go` | Splits a response body into JSON parts (`for (;;);` prefix, NDJSON stream) and extracts errors. Out of scope but every parser input passes through it. |
| `internal/fb/htmlx/tokens.go` | Regex extraction of tokens and page id from HTML, plus `JSONBlobs` (every `<script type="application/json">`). Used by `parse.Page`. |
| `internal/fb/parse/walk.go` | Generic tree walker: `Collect`, `First`, `Find`, `FindWhere`, `FindAll`, `FindPageInfo`. |
| `internal/fb/parse/fields.go` | `Report`, number/time helpers, `ParseCount`, graph-id codecs, `MediaID`, `URLExpiry`, `Ranges`, `Hashtags`, `Reactions`. |
| `internal/fb/parse/timeline.go` | Timeline parser → `[]PostBundle{Post, Media}` + `PageInfo`. |
| `internal/fb/parse/comments.go` | Comments/replies parser → `[]CommentBundle{Comment, Media}` + `[]ThreadRef` + `PageInfo` + `TotalCount`. |
| `internal/fb/parse/media.go` | Media node → one `Media` per downloadable variant. |
| `internal/fb/parse/page.go` | Page profile from server HTML. |
| `internal/fb/parse/*_test.go` | Golden runner, shape regression tests, field helper tests. |
| `testdata/fb/<query>/<case>/` | Fixtures: 6 cases across `page_html`, `timeline`, `comments`, `replies`. |
| `api/proto/securebrand/fb/v1/*.proto` | Public event contract (package `securebrand.fb.v1`). |
| `api/proto/securebrand/fbs/task/v1/tasks.proto` | Internal task envelope (not public, but holds `max_depth`, video policy, cursors). |
| `api/proto/embed.go` | `embed.FS` of the public protos, used to register them with the Schema Registry. |
| `internal/topics/{topics,schemas}.go` | Topic names, task-topic routing, topic → proto file map. |
| `internal/fakefb/{world,render,server,fixtures}.go` | Synthetic world, Facebook-shaped rendering, HTTP server with faults, fixture writer. |
| `cmd/fakefb/main.go` | Standalone fakefb binary. |
| `internal/fixtures/{scrub,ojson,write}.go` | PII scrubber, order-preserving JSON codec, scrub-before-write fixture writer. |
| `docs/contracts/kafka-schemas.md` | Public topic and Schema Registry contract. |

Dependencies of note: `github.com/tidwall/gjson` for lazy JSON access (every part is a `gjson.Result`), `github.com/google/uuid` for UUIDv5, and `protojson` for golden normalisation.

---

## 2. From HTTP body to parser input

`graphql.SplitParts` (`internal/fb/graphql/parts.go:20-46`):
- trims whitespace and a leading `for (;;);` (Facebook's anti-JSON-hijacking prefix);
- decodes consecutive JSON documents with `json.Decoder`. Facebook streams `@defer`/`@stream` payloads as one JSON document per line (fakefb separates them with `\r\n`, `render.go:378-388`);
- on a malformed part it skips to the next newline and continues, so one bad part does not lose the rest (`parts.go:31-41`).

`graphql.HasData` (`parts.go:51-63`) says a response is partial (not failed) when any part's `data` has a non-null field. `{"data":{"node":null}}` next to an error counts as a failed request. `graphql.Errors` (`parts.go:66-91`) normalises both GraphQL `errors[]` (code in `code` or `extensions.code`) and the legacy `{"error":<int>,"errorSummary","errorDescription"}` form.

The parsers never reassemble `@stream`/`@defer` parts by their `path`. Each part is walked as an independent tree. This works because:
- a `@stream` part carries a whole edge (`{"label":"...$stream$...","path":[...,"edges",i],"data":{"node":{"__typename":"Story",...},"cursor":...}}`), so the typename walker finds the Story without the path;
- the deferred `page_info` part (`{"label":"...$defer$..._page_info","data":{"page_info":{...}}}`) is special-cased in `FindPageInfo` (section 3.3).

**Consequence:** a deferred part that carries only a fragment of an entity (for example `{"label":"..._feedback$defer","path":[...,"node","feedback"],"data":{...}}` without the Story around it) is not attached to its Story. The timeline parser then falls back to whatever `feedback` is inside the Story itself, and records `story.feedback`/`story.comment_count` as optional-missing. fakefb's `FieldErrors` fault exercises this case with an error-only deferred part (`server.go:331-335`).

---

## 3. Parsing strategy: shape-tolerant tree walking

Package doc (`internal/fb/parse/walk.go:1-6`): *"Parsers locate entities by `__typename` instead of fixed paths and try several candidate paths per field, so they survive the frequent A/B shape changes Facebook ships. Missing required fields are reported so the caller can classify the response as schema drift."*

### 3.1 `Collect`: typename-driven entity discovery with an ancestor tree

`Collect(parts, want...) []*Node` (`walk.go:40-50`, walker `walk.go:60-94`):
- Depth-first over every part, every array and every object (depth capped at `maxWalkDepth = 200`, `walk.go:58`).
- An object whose `__typename` (a string) is in `want` becomes a `Node{Typename, V, Parent}`. `Parent` is the nearest enclosing collected node, not the JSON parent (`walk.go:13-18, 83-85`). The walk continues inside it with the node as the new parent, so nested entities are found too. Results are in document order.
- `Node.Ancestor(typenames...)` returns the nearest ancestor of any of the given typenames (`walk.go:21-30`). All ownership decisions use it. For example, a `Feedback` belongs to a Story only if `x.Ancestor("Story","Comment") == storyNode` (`timeline.go:133`). That excludes the Feedback of a comment preview quoted inside the story.
- **De-duplication** (`walk.go:72-82`). The key is `typename|id`, prefixed by the parent's key, so the key encodes the whole collected-ancestor path. An object with an `id` seen before at the same place is skipped, which handles parts that repeat an object. The same object quoted elsewhere (a shared post inside the story that shares it, a highlighted copy of a comment) has a different key, so it is returned again: "the first copy must not hide the one in its own place". Objects without an `id` are never de-duplicated, and two id-less siblings of one typename share a key prefix for their children.
- Regression tests: `TestTimelineSharedPostInSamePage` (`walk_test.go:13-45`) covers a page sharing its own post, in both orders. `TestCommentsQuotedTwice` (`walk_test.go:63-108`) covers a highlighted comment plus its copy in the list, with a reply nested only under the second copy.

### 3.2 Field access helpers

- `First(v, paths...)` (`walk.go:97-105`): the first gjson path that exists and is not `null`. Every field has several candidate paths, ordered from the shape seen most often to legacy shapes.
- `Find(v, key, skip...)` / `FindWhere(v, key, ok, skip...)` (`walk.go:110-153`): depth-first search for the first non-null value under `key`. It does not descend into nested objects whose `__typename` is in `skip` (`walk.go:123-131`), so a Story's `Find` never picks a field out of a nested shared Story or a quoted Comment. `FindWhere` adds a predicate. The timeline uses it to find a `message` object that actually has `text` (`timeline.go:112-116`), because `comet_sections.message` is a wrapper without `text` in some shapes.
- `FindAll(v, key, skip...)` (`walk.go:157-189`): every non-null value under `key`, used for `attachments` arrays (`timeline.go:202`) and for social-context `text` lines (`page.go:87`).
- Keys are escaped for gjson metacharacters (`walk.go:191-203`).

### 3.3 Connections, edges, cursors: `FindPageInfo`

`FindPageInfo(parts, keys, skip...) PageInfo{HasNext, EndCursor, Found}` (`walk.go:205-269`):
- `keys` are connection names: timeline `timeline_list_feed_units`, `timeline_feed_units`, `posts` (`timeline.go:29`); comments `comments`, `comment_rendering_instance_for_feed_location`, `display_comments`; replies `replies_connection`, `replies` (`comments.go:55-58`).
- For any object having one of those keys whose value is an object, its `page_info` is a candidate (`walk.go:240-244`). The **shallowest** candidate wins (`walk.go:220-226, 229`). Subtrees of `skip` typenames are not searched: `Story` for the timeline, `Comment` for comments and replies. A comment's inline `replies_connection.page_info` therefore never overrides the top-level comments `page_info`. The handcrafted fixture `comments/variant_graph_ids` tests exactly this (its inline reply has `has_next_page:true, end_cursor:"rc1"`, and the result is `HasNext:false`).
- **Deferred `page_info` parts** (`walk.go:255-265`): a part with `data.page_info` is accepted at depth 0 (authoritative) when its `label` is empty or contains one of the connection keys, case-insensitively.
- `has_next_page` and `end_cursor` are read as-is. `end_cursor: null` becomes `""`. Per-edge `cursor` values are ignored: only `page_info.end_cursor` drives pagination.
- Edges themselves are never addressed. Entities are found by typename wherever they sit (`edges[].node`, streamed `data.node`, highlighted copies...).

### 3.4 The `Report` contract and parser versions

`Report{Missing, Optional}` (`fields.go:20-35`); `OK()` means no required field is missing.

| Parser | Required (→ `Missing`) | Optional (→ `Optional`, metrics only) |
|---|---|---|
| `Timeline` | `story.post_id`, `story.creation_time` (the story is dropped) | `story.permalink` (synthesised), `story.feedback`, `story.reactions`, `story.comment_count`, `page_info` (when posts were found without one) |
| `Comments` | `comment.id` (comment dropped), `comment.created_time` (comment kept, `created_at` nil) | `comment.author`, `comment.feedback` |
| `Page` | `page.id`, `page.name` | `page.followers` |

How callers use it:
- **Crawl stages:** `!rep.OK()` → classify as `detect.SchemaDrift` and stop the segment (`internal/crawl/stage/page.go:191-193`, `post.go:119-121`; replies `post.go:399-402`). `rep.Optional` feeds `metrics.ParseMissingField{query,field}` (`page.go:188-190`).
- **Registry sync** (doc_id validation): missing required fields → verdict `Drift` (`internal/registrysync/sync.go:706-715`).
- **Shadow parsing:** a candidate parser runs next to the active one on a sampled share of live traffic. Outcomes are `same|differs|drift|error`, and differing responses are captured raw for replay/fixtures (`internal/crawl/shadow/shadow.go:1-8, 63-110`). The active parser's output is what gets emitted.
- **Replay:** `fbctl replay` re-parses captured raw failures with the current parsers and turns them into scrubbed fixtures (`internal/replay/replay.go:1-5`).

Parser versions are constants recorded in `Observation.parser_version`: `TimelineVersion = 1` (`timeline.go:14`), `CommentsVersion = 1` (`comments.go:13`), `PageVersion = 1` (`page.go:15`).

### 3.5 Id decoding

All in `fields.go`:
- `decodeB64ID` tries Std, RawStd, URL and RawURL base64 (`fields.go:90-97`).
- **Story → post id:** `PostIDFromStoryID`. The id decodes to `S:_I<actor_id>:<post_id>[:...]` and the parser takes the third colon field (`fields.go:100-112`). Test: `"UzpfSTEwMDo1NTU6NTU1"` (= `S:_I100:555:555`) → 555 (`fields_test.go:17-23`).
- **Comment graph id → comment id:** `CommentIDFromGraphID`. The id decodes to `comment:<post>_<comment>` and the parser takes the part after the last `_` (`fields.go:115-126`).
- **Feedback id:** `FeedbackID(postID) = base64("feedback:<post>")` (`fields.go:129-131`). The inverse `FeedbackTarget(id) → (post, comment)` handles `feedback:<post>` and `feedback:<post>_<comment>` (`fields.go:136-151`). The crawler synthesises a post's feedback id when the story did not carry one (`internal/crawl/stage/page.go:303`). The feedback id is the `id` variable of the comments query (`configs/queries.seed.yaml:64`) and of the replies query, which also needs the `expansionToken` (`queries.seed.yaml:112,121`).
- **Media id:** `MediaID(fbMediaID, variant)` = UUIDv5 (SHA-1) in namespace `6f1c1f7e-8d9b-5c2a-9e0f-3b1f6a7c2d10` over `fbMediaID + "|" + variant` (`fields.go:154-160`). It is deterministic across scrapes, so a media item is downloaded once. If the media node has no `id`, the URL is used as the "fb media id" (`media.go:41-44`). The `timeline/variant_nested` link preview is an example: its `fb_media_id` is the signed URL, so that id changes whenever Facebook re-signs the URL.

### 3.6 Scalars

- `Int64` accepts a JSON number or a numeric string (`fields.go:38-47`), because Facebook sends many ids and counts as strings. `Unix` turns seconds into a UTC `Timestamp`, or nil when ≤ 0 (`fields.go:50-56`).
- `ParseCount` turns human counts into numbers: `"1,234 followers"` → 1234, `"1.2M"` → 1,200,000, `"3,4K"` → 3400, `"2.5B"`. With a K/M/B suffix a comma is treated as the decimal separator; without a suffix both `,` and `.` are stripped as thousands separators (`fields.go:58-86`, test `fields_test.go:5-15`).

---

## 4. Page HTML parsing

`parse.Page(html, pageID)` (`page.go:25-133`) runs on the server-rendered page HTML that the crawler fetches during bootstrap.

1. **Page id.** If the caller did not pass one, `htmlx.PageID` tries these regexes in order: `"pageID":"<n>"`, `"page_id":"<n>"`, `"delegate_page":{"id":"<n>"`, `fb://page/?id=<n>`, `fb://page/<n>`, `fb://profile/<n>`, `"userID":"<5+ digits>"` (`internal/fb/htmlx/tokens.go:94-116`). The `fb://page/<id>` value usually comes from the `<meta property="al:android:url">` app-link tag.
2. **Embedded JSON.** `htmlx.JSONBlobs` returns every `<script type="application/json" ...>` body that is valid JSON (`tokens.go:118-130`). Facebook preloads GraphQL results this way, inside `{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{...}}]], ["RelayPrefetchedStreamCache","next",[],["adp_<Query>RelayPreloader_<n>",{"__bbox":{"complete":true,"result":{"data":{"user":{...}}}}}]]]}`. **Nothing parses `require`, `ScheduledServerJS` or `__bbox` structurally.** The typename walker simply finds the `User`/`Page` object wherever it is nested (`page.go:39-43`).
3. Only collected `User`/`Page` objects whose `id` equals the page id count (`mine`). Each field takes the first non-empty value across them (`page.go:44-51`).
4. **Meta-tag fallbacks:** `og:title`, then `<title>` minus ` | Facebook`, for the name; `og:url` for the URL (`page.go:17-21, 114-125`).
5. **Tokens** (`lsd`, `fb_dtsg`, `_js_datr`, `__spin_r/b/t`, `hsi`, `server_revision`, `haste_session`, `__comet_req`, consent-wall markers) are extracted by regex in `htmlx.ExtractTokens` (`tokens.go:54-92`). The bootstrap uses them; the page parser does not. The fixture HTML shows the shapes: `["LSD",[],{"token":"..."},323]`, `["SiteData",[],{...},317]`, `["CookieCoreConfig","set",null,[["_js_datr","...",1800000]]]`.

Field mapping is in section 5.2. Note that `Page` always returns `status = PAGE_STATUS_ACTIVE` (`page.go:35`). The crawler emits `PAGE_STATUS_UNAVAILABLE` itself when the first timeline page comes back with `node: null` (`stage/page.go:196-199, 321-331`).

---

## 5. Entity by entity: proto fields and where they come from

Notation: `a|b|c` means `First(v, "a","b","c")`, i.e. candidate paths in priority order. `Find(k)` means a depth-first key search that skips nested Story/Comment. Protos are proto3; `optional` marks presence-tracked scalars. Generated Go is in `gen/go/securebrand/fb/v1` (package `fbv1`).

### 5.1 Common types (`api/proto/securebrand/fb/v1/common.proto`)

**`CrawlMode`** (`common.proto:10-20`): `UNSPECIFIED`, `FULL` (first scrape/backfill), `INCREMENTAL` (new posts and posts whose counts changed), `REFRESH` (counts-only refresh), `RECRAWL` (full comment pass on an aged post to capture edits/deletions).

**`Observation`** (`common.proto:25-35`) is attached to every public entity message by the emitter, not by the parser.

| Field | Type | Source |
|---|---|---|
| `event_id` | string | hex of xxh3-128(`<entity>:<id>` + `|` + content_hash) (`internal/crawl/emit/emit.go:52-66`) |
| `observed_at` | Timestamp | emitter clock |
| `job_ids` | repeated string | the job |
| `query_name` | string | registry query name (`timeline`, `comments`, `comments_newest`, `replies`, `page_html`) |
| `doc_id` | string | GraphQL doc_id used |
| `parser_version` | uint32 | `parse.*Version` |
| `content_hash` | bytes | xxh3-128 over the deterministic proto marshal of the message with `obs` cleared and **signed URLs normalised** (query dropped when it has `oe=` or `_nc_`, `internal/crawl/emit/normalize.go:14-60`). An unchanged re-scrape therefore yields a byte-identical event even though CDN signatures rotate. |
| `mode` | CrawlMode | task mode |

**`TextRange`** (`common.proto:38-46`), from `message.ranges[]` / `body.ranges[]` via `Ranges()` (`fields.go:180-195`):

| Field | Source (each range `x`) |
|---|---|
| `offset`, `length` (int32) | `x.offset`, `x.length`. Passed through unchanged, so they are in **UTF-16 code units** as Facebook sends them. |
| `entity_type` | `x.entity.__typename` (`Hashtag`, `Page`, `User`, `ExternalUrl`, ...) |
| `entity_id` | `x.entity.id` |
| `url` | `x.entity.url|external_url|profile_url` |
| `name` | `x.entity.name` |

**`Reactions`** (`common.proto:48-54`), from a Feedback object via `Reactions()` (`fields.go:246-260`):

| Field | Source |
|---|---|
| `total` (int64) | `reaction_count.count|reactors.count|i18n_reaction_count|reaction_count` |
| `by_type` (map<string,int64>) | edges of `top_reactions.edges|reaction_breakdown.edges|important_reactors.edges`. Count is `reaction_count|count|node.reaction_count`. The name comes from the fixed reaction-id table (`fields.go:223-231`): like `1635855486666999`, love `1678524932434102`, care `613557422527858`, haha `115940658764963`, wow `478547315650144`, sad `908563459236466`, angry `444813342392137`. Otherwise it is the lowercased `reaction_type|localized_name|name`, else `"unknown"` (`fields.go:233-243`). |
| `complete` (bool) | `total == 0` or the breakdown sums exactly to `total` (`fields.go:258`). A logged-out client only sees the top 3 types (fakefb mimics this with `TopReactions` default 3, `server.go:76-78`), so a post with more than 3 reaction types is incomplete. Stored as `posts.reactions_complete` (`migrations/00001_init.sql:57`). |

**`ImageRef`** (`common.proto:56-60`): `url`, `width`, `height`. Only `url` is filled by the page parser.

### 5.2 Page → `fb.page.v1` (`page.proto:17-37`)

| Field | Type | Source in HTML / embedded JSON (`page.go`) |
|---|---|---|
| `obs` | Observation | emitter |
| `page_id` | int64 | caller or `htmlx.PageID` regexes (`page.go:27-33`). **Required.** |
| `requested_ref` | string | set by the crawler: `"v:nike"` / `"id:123"` (`stage/page.go:104`) |
| `vanity` | string | `vanity|username` (`page.go:53`). The persister falls back to `requested_ref` `v:` (`internal/persist/writer.go:379-380`). |
| `url` | string | `url|profile_url`, else `og:url` (`page.go:54, 121-125`) |
| `name` | string | `name`, else `og:title`, else `<title>` minus ` \| Facebook` (`page.go:52, 114-120`). **Required.** |
| `categories` | repeated string | first of `delegate_page.category_name|category_name|page_category`, plus unique entries of `category_type_list|categories` (each `text|name`, or a bare string) (`page.go:67-77`) |
| `intro` | string | `profile_intro_card.bio.text|intro_card.bio.text|bio.text|about.text` (`page.go:55`) |
| `followers_count` | optional int64 | `follower_count|followers_count|subscribers.count`, else any `text` under the page object containing "follower", run through `ParseCount` (`page.go:78-99`). Optional-missing if absent. |
| `likes_count` | optional int64 | `page_likers.global_likers_count|likes_count|fan_count`, else a `text` containing "like" (but not "talking") through `ParseCount` (`page.go:82-99`) |
| `verified` | optional bool | `is_verified|is_blue_verified` (`page.go:63-66`) |
| `profile_picture` | ImageRef | `profilePicLarge.uri|profile_picture.uri|profilePic160.uri|profilePicNormal.uri` (`page.go:56-58`) |
| `cover_photo` | ImageRef | `cover_photo.photo.image.uri|cover_photo.photo.viewer_image.uri|cover_photo.image.uri` (`page.go:59-61`) |
| `contact` | map<string,string> | `email`: `email|page_about_fields.email`; `phone`: `phone_number|formatted_phone_number|page_about_fields.phone`; `website`: `website|websites.0|page_about_fields.website`; `address`: `address.full_address|single_line_address` (`page.go:100-112`) |
| `extra_json` | string | never set by the parser (reserved for unmodelled fields) |
| `status` | PageStatus | `ACTIVE` from the parser; `UNAVAILABLE` from the crawler on `node:null`. `RESTRICTED` is never produced. |

`PageStatus` enum: `UNSPECIFIED, ACTIVE, UNAVAILABLE, RESTRICTED` (`page.proto:9-14`).

### 5.3 Post → `fb.post.v1` (`post.proto:38-69`)

The parser works on each top-level `Story` node, i.e. one with no Story ancestor (`timeline.go:36-45`). It collects `Story`, `Comment`, `Feedback` and the media typenames in one `Collect` call (`timeline.go:35`), and `skip = {Story, Comment}` for every `Find` (`timeline.go:74`).

| Field | Type | Source (`timeline.go`) |
|---|---|---|
| `post_id` | int64 | `post_id`, else `PostIDFromStoryID(id)`, else `Find(post_id)` (`76-86`). **Required.** |
| `page_id` | int64 | caller (the crawled page) |
| `created_at` | Timestamp | `creation_time|comet_sections.context_layout.story.comet_sections.metadata.0.story.creation_time`, else `Find(creation_time)` (`87-95`). **Required.** |
| `story_id` | string | Story `id` (base64 graph id) |
| `permalink` | string | `permalink_url|url|comet_sections.context_layout.story.comet_sections.metadata.0.story.url`, else `Find(permalink_url)`, else synthesised `<base>/<page_id>/posts/<post_id>` with optional `story.permalink` (`103-110`) |
| `type` | PostType | see the decision order below |
| `text` | string | `message|comet_sections.content.story.message`. If that has no `text`, `FindWhere(message, has text)`. Then `.text` (or the string itself) (`112-122`). |
| `text_ranges` | repeated TextRange | `message.ranges` |
| `hashtags` | repeated string | `Hashtags(text, ranges)`, lowercased and unique (`fields.go:197-220`). See 6.2. |
| `links` | repeated LinkAttachment | every `attachments` array under the story (`FindAll`, skipping nested Story/Comment) → `linkAttachment` (`202-209`). See 6.1. |
| `shared_post_id` | optional int64 | first collected `Story` whose **direct collected parent** is this story: its `post_id`, else decoded from `id` (`153-166`). In practice this is `attached_story`. |
| `shared_post_url` | string | that nested story's `permalink_url|url` |
| `is_pinned` | optional bool | `Find(is_pinned)` (`168-171`) |
| `is_paid_partnership` | optional bool | true if `Find(is_paid_partnership)` is true **or** a `branded_content_post_info` object exists; otherwise unset (`172-178`; proto comment `post.proto:54`) |
| `author_id`, `author_name` | string | `actors.0|comet_sections.context_layout.story.comet_sections.actor_photo.story.actors.0` → `.id`, `.name` (`125-128`) |
| `reactions` | Reactions | merged Feedback (see below); empty `Reactions` with optional `story.reactions` if none (`250-253`) |
| `comment_count` | optional int64 | Feedback `comment_rendering_instance.comments.total_count|comments_count_summary_renderer.feedback.comment_rendering_instance.comments.total_count|total_comment_count|comment_count.total_count|comments.total_count` (`229-236`). Total including replies (`post.proto:58`). |
| `share_count` | optional int64 | Feedback `share_count.count|i18n_share_count|share_count` (`237-242`) |
| `view_count` | optional int64 | Feedback `video_view_count|video_view_count_renderer.feedback.video_view_count|play_count`, else Story `video_view_count|play_count`, else `Find(play_count)` (`143-151, 243-248`) |
| `feedback_id` | string | first owned Feedback's `id` (`221-223`) |
| `media_ids` | repeated string | `Media.media_id` of owned media with variant `original`, `sd` or `link_preview`. `hd` and `thumbnail` are excluded (`197-199`). |
| `extra_json` | string | not set |
| `status` | PostStatus | always `VISIBLE` from the parser (`101`). `UNAVAILABLE` exists (`post.proto:23-27`) and is handled by the persister but not produced here. |

**Feedback merging** (`timeline.go:130-142, 215-257`). Every `Feedback` node whose nearest Story/Comment ancestor is this story is merged in document order; each field takes the first value found. If there is none, the parser falls back to `Find(feedback)` within the story. In real Comet shapes the Feedback sits deep, at `comet_sections.feedback.story.story_ufi_container.story.feedback_context.feedback_target_with_context.comet_ufi_summary_and_actions_renderer.feedback` (as rendered by fakefb, `render.go:186-188`). The typename walk finds it without that path. A Feedback inside a Comment quoted in the story (comment preview) is **excluded** by the ancestor rule. The handcrafted variant puts `reaction_count: 99999` there, and the expected total stays 1500 (`testdata/fb/timeline/variant_nested`).

**`PostType`** (`post.proto:10-21`) is decided in this order (`timeline.go:300-323`):
1. `LIVE` if `Find(is_live_streaming)` is true;
2. `EVENT` if `Find(event)` is an object with `__typename: Event`;
3. `REEL` if the permalink contains `/reel/`;
4. `VIDEO` if ≥1 owned `Video` node;
5. `ALBUM` if >1 owned `Photo`;
6. `PHOTO` if exactly 1 owned `Photo`;
7. `SHARE` if `shared_post_id` is set;
8. `LINK` if any external link;
9. `STATUS` otherwise.

**`LinkAttachment`** (`post.proto:29-35`): `url, title, description, source, image_url` (see 6.1).

### 5.4 PostMetrics → `fb.post_metrics.v1` (`post.proto:72-82`)

The parser does not build it. The emitter derives one sample from every emitted `Post` (`internal/crawl/emit/emit.go:74-87`): `post_id, page_id, post_created_at (= created_at), captured_at (= now), reactions, comment_count, share_count, view_count`. Because `captured_at` is part of the hashed content, every sample has a new `event_id`, which gives a time series of engagement.

### 5.5 Comment / reply → `fb.comment.v1` (`comment.proto:48-74`)

`Comments(parts, CommentContext)` (`comments.go:61-148`) handles both the comments query and the replies query. `CommentContext` (`comments.go:16-27`) holds `PostID, PageID, PostCreatedAt, PageAuthorID, BaseDepth, ParentCommentID, RootCommentID, Replies`.

| Field | Type | Source (`comments.go`) |
|---|---|---|
| `comment_id` | int64 | `legacy_fbid|comment_id`, else `CommentIDFromGraphID(id)` (`153-160`). **Required**; the comment is dropped without it. |
| `post_id`, `page_id`, `post_created_at` | | from `CommentContext` |
| `graph_id` | string | `id` (base64 `comment:<post>_<comment>`) |
| `parent_comment_id`, `root_comment_id` | optional int64 | see Depth and nesting below |
| `depth` | int32 | see Depth and nesting below |
| `author` | Author | `author` object: `id`, `name`, `url|profile_url|uri`, `avatar_url` = `profile_picture_depth_0.uri|profile_picture_depth_1.uri|profile_picture.uri`, `verified` = `is_verified|is_blue_verified`, `type` = `__typename` (`User`/`Page`) (`204-215`). Optional-missing if absent. |
| `text` | string | `body|preferred_body|body_renderer` → `.text` (`220-224`) |
| `text_ranges` | repeated TextRange | that body's `.ranges` |
| `attachments` | repeated Attachment | one per media variant found under the comment (see below) (`108-119`) |
| `created_at` | Timestamp | `created_time|creation_time` (`161-164`). **Required** (the comment is still emitted). |
| `edited` | optional bool | `edit_history.count|is_edited`: true if the bool is true or the count > 0 (`225-228`) |
| `is_op` | optional bool | `is_author_original_poster|author_original_poster`, else `author.id == PageAuthorID` when the caller knows the page id (`229-235`). The crawl stages always set `PageAuthorID = page_id` (`stage/post.go:72-73`); the golden runner does not, so `is_op` is absent in the handcrafted fixture's expected output. |
| `reactions` | Reactions | `feedback` → `Reactions()`; empty if there is no feedback (`237-249`) |
| `reply_count` | optional int64 | `feedback.replies_fields.total_count|replies_fields.count|total_reply_count|replies_connection.count|display_replies_count` (`241-245`) |
| `feedback_id` | string | `feedback.id` (base64 `feedback:<post>_<comment>`), the id for the replies query |
| `url` | string | `url|comment_action_links.0.comment.url` (`172`) |
| `redacted` | bool | set by the emitter when the author is suppressed (erasure) (`emit.go:113-118`) |
| `extra_json` | string | not set |
| `status` | CommentStatus | unspecified (= visible) for crawled comments. `DELETED` is published by the job finalizer (see 6.10). |

**`Author`** (`comment.proto:10-21`) adds `author_key` (bytes) = HMAC-SHA256(secret, author id) (`internal/gdpr/suppression.go:19-27`), set in `Emitter.Privatize` (`emit.go:108-124`). With `pii_mode=pseudonymized` the emitter blanks `id/name/url/avatar_url` and keeps only `author_key`. A suppressed (erased) author also loses the text, ranges, attachments and URL, and `redacted=true` is set.

**`Attachment`** (`comment.proto:23-32`): `type` = `gif|sticker|video|photo` from the media kind (`comments.go:253-264`). `media_id`, `url` (= `source_url`), `width` and `height` come from the Media. `preview_url` is never set, and the comments parser never produces `type: "link"`.

**`CommentStatus`** (`comment.proto:34-44`): `UNSPECIFIED`, `VISIBLE`, `DELETED`.

**Depth and nesting** (`comments.go:175-202`):
- A comment nested under another collected `Comment` in the same response (an inline reply): `depth = parent.depth + 1`, `parent_comment_id = parent.id`, `root_comment_id = parent.root ?? parent.id`.
- A top-level comment of the response: `depth = BaseDepth` (0 for comments, the thread depth for replies). For the comments query, an explicit `depth` field overrides it. The parent is `cc.ParentCommentID`, else `comment_direct_parent.legacy_fbid|comment_parent.legacy_fbid`. Parent and root are set only when depth > 0, with root `cc.RootCommentID ?? parent`.

**Duplicates** (`comments.go:78-106`). The same comment quoted twice (highlighted plus in the list) is emitted once, from its first copy. Replies nested under a later copy are re-parented onto the first copy (`canon` map).

**Thread references** (`comments.go:35-44, 122-143`). For each emitted comment with `reply_count > inline replies seen under it`, the parser returns a `ThreadRef{CommentID, RootCommentID, Depth (of the parent), FeedbackID, ExpansionToken, ReplyCount, InlineReplies}`, sorted by comment id. The expansion token is read from `feedback.expansion_info.expansion_token|feedback.replies_connection.expansion_token`, else `expansion_info.expansion_token` on the comment.

**`TotalCount`**: the first part with `data.node.comment_rendering_instance_for_feed_location.comments.total_count|data.node.display_comments.count|data.node.comments.total_count` (`comments.go:68-74`).

**`NodeNull`**: no comments and every part's `data.node|user|page` is null or absent (`comments.go:144-146`, `timeline.go:55-69`). The crawler treats it as "post unavailable" on the first page (`stage/post.go:124-127`).

### 5.6 Media → `fb.media.v1` (`media.proto:25-50`)

`MediaFromNode(node, typename, Owner)` (`media.go:34-117`) returns one `Media` per downloadable variant. Media typenames: `Photo`, `Video`, `Sticker`, `GenericAttachmentMedia`, `Gif` (`media.go:10`). A media node belongs to the nearest Story/Comment ancestor (`timeline.go:186`, `comments.go:110`). An album's sub-attachments are found by typename, not through `all_subattachments.nodes` paths.

| Field | Type | Source |
|---|---|---|
| `media_id` | string | `MediaID(fb_media_id, variant)` (UUIDv5) |
| `fb_media_id` | string | node `id`, else the URL (`media.go:41-44`) |
| `kind` | MediaKind | see the variant table below |
| `variant` | string | `original | sd | hd | thumbnail | link_preview` |
| `owner_type` | string | `post` / `comment` (`page` exists in the comment, but the parser never builds page media) |
| `owner_id` | int64 | post id or comment id |
| `post_id`, `page_id` | int64 | owner context |
| `comment_id` | optional int64 | for comment media |
| `source_url` | string | the signed CDN URL |
| `source_expires_at` | Timestamp | `URLExpiry(url)`: the hex `oe` query parameter as unix seconds (`fields.go:163-177`; test `fields_test.go:33-41`, `oe=6A1B2C3D`) |
| `width`, `height` | int32 | per typename (below) |
| `duration_ms` | int64 | video: `playable_duration_in_ms`, else `length_in_second|playable_duration` × 1000 (`media.go:79-82`) |
| `mime` | string | `"video/mp4"` for sd/hd video; otherwise empty |
| `alt_text` | string | `accessibility_caption|alt_text|accessibility_label` (Facebook's automatic "May be an image of ...") (`media.go:36`) |
| `dash_manifest_url` | string | `dash_manifest_url|dash_manifest_urls.0.manifest_url` |
| `thumbnail_url` | string | video: `preferred_thumbnail.image.uri|thumbnailImage.uri|thumbnail_image.uri|image.uri` |
| `post_created_at` | Timestamp | not set by the parser (field 20; available for the persister) |
| `obs` | Observation | emitter |

**Variant/kind table** (`media.go:62-115`):

| Typename | Variant(s) | URL path(s) | Kind |
|---|---|---|---|
| `Photo` | `original` | `photo_image|image|viewer_image|large_share_image|preview_image|blurred_image` → `.uri/.width/.height` | `PHOTO`, or `COMMENT_PHOTO` on a comment |
| `Video` | `sd` | `browser_native_sd_url|playable_url|videoDeliveryLegacyFields.browser_native_sd_url` | `VIDEO`; `REEL` if the post permalink has `/reel/` or the node has `is_reel`; `COMMENT_VIDEO` on a comment |
| `Video` | `hd` | `browser_native_hd_url|playable_url_quality_hd|videoDeliveryLegacyFields.browser_native_hd_url` | same |
| `Video` | `thumbnail` | the thumbnail path above | `THUMBNAIL` (no width/height) |
| `Sticker` | `original` | `image|sticker_image|preview_image` | `STICKER` |
| `GenericAttachmentMedia`/`Gif` with `animated_image|animated_gif` | `original` | `.uri` | `COMMENT_GIF` |
| `GenericAttachmentMedia`/`Gif` otherwise | `link_preview` | `image|large_share_image` | `THUMBNAIL` on a post, `COMMENT_PHOTO` on a comment |

Video width/height come from `width|original_width` and `height|original_height`.

`MediaKind` enum (`media.proto:10-22`): `UNSPECIFIED, PHOTO, VIDEO, REEL, THUMBNAIL, COMMENT_PHOTO, COMMENT_GIF, STICKER, COMMENT_VIDEO, COVER, PROFILE_PIC`. **`COVER` and `PROFILE_PIC` are never produced** today; the page parser only fills `ImageRef`s.

Which variants are downloaded is decided downstream by `VideoPolicy` (`tasks.proto:29-35`; `internal/crawl/stage/deps.go:206-222`). `HD` takes `hd`, or `sd` when there is no hd. `SD`/unspecified takes `sd`. `THUMBNAIL` takes thumbnails. `NONE` takes no video and no thumbnails. Every other kind is always taken. All variants are still published on `fb.media.v1` as discovered metadata (`deps.go:188-194`).

The handoff messages (`MediaReady`, `MediaProcessed`, `MediaDeleted`, `S3Object`, `ProcessStatus`, `DeleteReason`; `media.proto:52-119`) belong to the download/analysis contract (`docs/contracts/media-handoff.md`). `media_id` is the key; `MediaReady.event_id` is `<media_id>:ready:<attempt>`; objects are deleted on ack by every required consumer or at `delete_after` (default 72h).

### 5.7 Job → `fb.job.v1` (`job.proto:9-46`)

`JobEvent{event_id, job_id, status, kind (on_demand|scheduled|backfill), page_ref, page_id, created_at, finished_at, counters map, completeness, errors}`. It is published by the scheduler's finalizer when a job reaches a terminal state. `COMPLETED` or `PARTIAL` means all of the job's data is queryable.

`JobStatus`: `UNSPECIFIED, QUEUED, RUNNING, COMPLETED, PARTIAL (some posts truncated or tasks failed permanently), FAILED, CANCELLED`.

`Completeness{posts, posts_complete, posts_truncated, median_ratio, min_ratio, comments_reported, comments_collected}`. The per-post ratio is `min(1, (top-level + replies collected) / Facebook's comment_count)` (`internal/scheduler/finalizer.go:394-405`).

**`comments_state`** is a DB/API column, not a proto field: `none | partial | complete | truncated` (`migrations/00001_init.sql:68`). The finalizer sets it from the job's per-post state; pending/failed become `partial` (`finalizer.go:370-404`). The e2e test asserts `complete` and ratio 1 against fakefb ground truth (`internal/e2e/e2e_test.go:163-165`).

### 5.8 Erasure → `fb.erasure.v1` (`erasure.proto:9-25`)

`ErasureEvent{event_id, request_id, subject_type (AUTHOR|PAGE), author_key, page_id, requested_at, completed_at}`. It is produced by `internal/gdpr/erasure.go:69-130`. An author erasure adds the key to the suppression set, nulls the comment text/author, sets `redacted`, and deletes media. A page erasure deletes the page's posts, comments and media and disables the schedule. Every consumer must delete the subject's data when it receives the event.

### 5.9 Internal task envelope (for context)

`securebrand.fbs.task.v1.Task` (`tasks.proto:96-123`) has a deterministic `task_id`, `lineage_id`+`seq` for pagination streams, `lane` (P0 on-demand, P1 scheduled, P2 backfill), `kind` (PAGE/POST/THREAD/MEDIA), `mode`, `cursor` (resume point), `limits`, and a `scope` oneof:
- `PageScope`: watermark, posts_seen, consecutive_old.
- `PostScope`: feedback_id, reported_comment_count, comments_watermark.
- `ThreadScope`: comment_id, root_comment_id, `depth` (of the replies being fetched), feedback_id, expansion_token, reported_reply_count.
- `MediaScope`: the `Media` message.

`Limits` (`tasks.proto:37-47`) holds `max_age_seconds, max_posts, max_comments_per_post, max_depth, include_comments/replies/media/comment_media, video_policy`.

---

## 6. Special handling

### 6.1 Links and the `l.facebook.com` shim

`linkAttachment` (`timeline.go:259-280`) handles each element `a` of every `attachments` array in the story:
- The attachment object is `styles.attachment|attachment|style_type_renderer.attachment`, or `a` itself.
- The raw URL is `story_attachment_link_renderer.attachment.web_link.url|web_link.url|url|target.url`.
- `unwrapLinkShim` (`timeline.go:282-293`): if the host is `l.facebook.com` or `lm.facebook.com` and the path is `/l.php`, the parser takes the `u` query parameter. The URL decodes it: `https%3A%2F%2Fnews.example.org%2Fstory%3Fid%3D9` → `https://news.example.org/story?id=9` in `timeline/variant_nested`.
- URLs that fail to parse, have no host, or point at Facebook hosts are dropped: `facebook.com`, `*.facebook.com`, `fb.com`, `*.fbcdn.net`, `fb.watch` (`timeline.go:295-298`). Internal attachments such as photos, albums and shared Facebook posts therefore never become links.
- `title` = `title_with_entities.text|title|name`; `description` = `description.text|description`; `source` = `source.text|source`; `image_url` = `media.image.uri|media.large_share_image.uri`.
- The link's preview image is separately collected as a `GenericAttachmentMedia` → `link_preview` Media (kind `THUMBNAIL`) when the node has that typename.

### 6.2 Entity ranges and hashtags

Ranges are copied 1:1 (section 5.1). `Hashtags` (`fields.go:200-220`) first slices each `Hashtag` range out of the text, then adds every regex match of `#([\p{L}\p{N}_]+)`. Both are lowercased and de-duplicated, in order.

**Observed bug (verified with a standalone copy of the function).** Ranges are sliced on `[]rune(text)`, but Facebook offsets are UTF-16 units (the scrubber explicitly preserves UTF-16 length for this reason, `scrub.go:398-401`, test `scrub_test.go:191`). With an astral-plane character such as an emoji before the hashtag, the slice is shifted. `Hashtags("😀 #tag more words", [{offset:3,length:4,Hashtag}])` returns `["tag ", "tag"]`, a spurious entry with a trailing space. fakefb's `hashtagRanges` also uses rune offsets (`render.go:208-226`), so the synthetic fixtures cannot surface it. The same unit question applies to anything else that consumes `text_ranges` downstream.

### 6.3 Shared posts

The share's `attached_story` is a nested `Story`. It is not emitted as a post (`timeline.go:37`); it only sets `shared_post_id`/`shared_post_url` (`153-166`). Its media are owned by the nested story, not the share (`Ancestor("Story","Comment") == n`), so a share of a photo post is typed `SHARE`, not `PHOTO`. If the shared post is also on the page's timeline, both are parsed and the original keeps its media (`walk_test.go:13-45`). Note that `shared_post_id` is set from the first nested Story whose direct collected parent is the share, so any nested Story counts.

### 6.4 Pinned and paid-partnership flags

`is_pinned` is a present-or-absent tri-state from `Find(is_pinned)`. The timeline stage excludes pinned posts from the "consecutive old posts" stop counter, because a pinned post is old but comes first (`stage/page.go:205-211`). `is_paid_partnership` is only ever `true` or unset (`timeline.go:172-178`).

### 6.5 `reactions_complete`

See `Reactions.complete` in 5.1. The API exposes it as `reactions_complete` (`internal/api/data.go:32`).

### 6.6 Edited flag and versions

`edited` comes from `edit_history.count > 0` or `is_edited` (`comments.go:225-228`). Edits are also detectable across observations: every observation of a comment is published, so a later record with the same `comment_id` and different `text` is an edit (`docs/contracts/kafka-schemas.md:25`). The store keeps `edit_count`/`edited_at`.

### 6.7 OP flag

See 5.5. fakefb renders page-authored comments with `author.__typename = "Page"`, `author.id = page id`, and `is_author_original_poster: true` (`render.go:250-264`; `world.go:269-272`, 4% of comments).

### 6.8 `author_key` (HMAC)

`AuthorKey(secret, id) = HMAC-SHA256(secret, authorID)`, nil for an empty id (`internal/gdpr/suppression.go:19-27`). It is stable and pseudonymous, and it is the join key for authors (`comment_authors.author_key`), for erasure (`fb.erasure.v1.author_key`) and for suppression. It is always present in the API, while names require the `pii:read` scope (`internal/api/data.go:159-237`).

### 6.9 Signed URLs

The `oe` (hex unix expiry) parameter becomes `Media.source_expires_at`. Signed parameters are removed before content hashing (`emit/normalize.go:45-60`), so rotating signatures do not create new events. The scrubber replaces signed fbcdn URLs entirely (section 9).

### 6.10 Deleted-comment detection inputs

The parser itself does not detect deletions; it supplies the inputs:
- During a **full pass** (`RECRAWL`, or `FULL` on a post crawled before), each page's top-level comments are recorded as `(comment_id, reply_count)` (`stage/post.go:151-155, 240-248`), and the last `TotalCount` is recorded as Facebook's reported count (`post.go:148-150, 176-180`).
- The finalizer only trusts a **complete** read: Σ(1 + reply_count) of the seen comments must be within `DeletionCountSlack × reported` of the reported count (`internal/scheduler/finalizer.go:289-310`, slack check at `304-305`).
- Stored, visible, non-redacted top-level comments first seen before the pass started and not seen in it are published as `Comment{status: DELETED}` with only ids, `post_created_at`, `depth` and `obs` set (`emit.go:96-106`). Their replies are marked too. A later sighting restores the comment. fakefb's `DeleteComment`, `WithholdComments` (still counted, but missing from lists, so the read is partial and skipped) and `HideComment` drive the e2e test `TestCommentEditsAndDeletions` (`internal/e2e/changes_test.go:27`).

### 6.11 Video duration and alt text

See the Media table in 5.6. Duration is normalised to milliseconds. `alt_text` is Facebook's automatic caption. The scrubber blanks it in real captures because it can name people.

---

## 7. Pagination

**Cursors.** These are opaque strings taken from `page_info.end_cursor`. Real Facebook cursors look like `AQHR...` (the handcrafted fixture uses `"AQHRnext"`). fakefb's are base64url JSON `{"k":<t|c|r>,"s":<scope id>,"o":<offset>}` (`render.go:280-302`), e.g. `eyJrIjoidCIsIm8iOjUsInMiOjEwMDA2NDAwMDAwMDAwMH0`. The variable names come from the registry seed (`configs/queries.seed.yaml`):
- timeline `cursor` (+ `count`, `id` = page id);
- comments `commentsAfterCursor` (+ `id` = post feedback id, `commentsIntentToken`);
- replies `repliesAfterCursor` (+ `id` = comment feedback id, `expansionToken`).

**End-of-feed** (in the stages, using the parser's `PageInfo`):
- **Timeline** stops when `!HasNext || EndCursor == ""`, when `ConsecutiveOld >= crawler.consecutive_old_stop` (posts older than the age cutoff or the incremental watermark, pinned excluded), or when `max_posts` is reached (`stage/page.go:205-217, 251-255`). A first page with `NodeNull` makes the page unavailable.
- **Comments** stop on `!HasNext || EndCursor == ""` (exhausted), on `max_comments_per_post`, or, in incremental newest-first mode, after `incrementalOlderStop = 5` consecutive comments older than the watermark (`stage/post.go:30, 158-166`).
- **Replies** stop on `!HasNext || EndCursor == ""` (ThreadStage loop in `internal/crawl/stage/post.go`, after `post.go:409`).

**Comment ordering.** The `commentsIntentToken` variable selects it (`queries.seed.yaml:47-102`):
- `RANKED_UNFILTERED_CHRONOLOGICAL_REPLIES_INTENT_V1`: "All comments", the most complete logged-out view, used for full reads (query `comments`).
- `RECENT_ACTIVITY_INTENT_V1`: newest first, used by incremental re-scrapes (`comments_newest`). If the reported count grew more than the incremental pass found, a full `RECRAWL` follows (`post.go:195-203`).
- `RANKED_FILTERED_INTENT_V1`: "Most relevant", which hides some comments. It is only used by the Phase 0 spike for comparison.

fakefb implements all three (`server.go:482-493`). The default is oldest first and complete; "filtered" drops the roughly 30% marked `Filtered`.

**Reply nesting and `max_depth`.** The comments response includes an inline preview reply (fakefb: the first reply, with its own `page_info`). That reply is emitted at depth 1, and the parent becomes a `ThreadRef` when `reply_count > inline`. Each `ThreadRef` becomes a THREAD task only if `parent_depth + 1 < max_depth`. `max_depth` defaults to 3, and the API accepts 1-5 (`stage/post.go:303-327`; `internal/api/jobs.go:45-49`). With the default, replies at depth 1 and 2 are fetched and depth-3 threads are not; inline replies already in a response are always emitted. A replies response is parsed with `Replies=true`, `BaseDepth = ThreadScope.depth`, and parent/root from the thread. Replies that themselves have replies produce further `ThreadRef`s (nested threads, `post.go:409`). In incremental mode a thread whose `reply_count` did not grow is skipped.

---

## 8. Golden-fixture testing

**Layout.** `testdata/fb/<query>/<case>/` with:
- `response.ndjson` (GraphQL, possibly multi-part, possibly with a `for (;;);` prefix) or `response.html` (page);
- `meta.json`: `page_id`, `post_id`, `parent_comment_id` (replies), `source` (`fakefb | handcrafted | capture...`), free-text `note`, `__scrubbed: true`, `__scrubber: <version>` for real captures, and `captured_at`;
- `expected.json`: the normalised parser output.

**Runner.** `TestGolden` (`internal/fb/parse/golden_test.go:37-83`):
- Iterates every query directory and case. It **fails if `meta.__scrubbed` is not true** (`57-59`), which forces every committed fixture through the scrubber or marks it synthetic.
- Dispatches by directory name (`run`, `85-128`):
  - `page_html` → `Page(html, 0)`, so the page id is extracted from the HTML;
  - `timeline` → `Timeline(SplitParts(body), meta.page_id, "https://www.facebook.com")`;
  - `comments` → `Comments(..., {PostID, PageID})`;
  - `replies` → the same plus `Replies=true, BaseDepth=1, ParentCommentID=RootCommentID=meta.parent_comment_id`.
- Required fields missing → test failure. The output is `protojson` with `UseProtoNames` (compact, int64 as strings), plus `page_info` (Go struct fields `HasNext/EndCursor/Found`), `threads`, `total_count` and `optional_missing`. It must equal `expected.json` byte for byte (after trimming).
- `-update` rewrites the files: `make golden` = `go test ./internal/fb/parse -run TestGolden -update` (`Makefile:30-32`). The intended workflow is to change the parser, run `make golden`, and review the `expected.json` diff in the PR.
- Principle (`golden_test.go:34-36`): *"Every parser version must pass every fixture of its query, old shapes included."* New shapes are added as new case directories, so old ones keep regression-testing.

**Current fixtures:**

| Case | Source | What it covers |
|---|---|---|
| `page_html/fakefb_basic` | fakefb | tokens script, `RelayPrefetchedStreamCache` header preloader, social-context "1.8M likes / 2.3M followers", `delegate_page.category_name`, og tags |
| `timeline/fakefb_first_page` | fakefb | 5 posts in 1 initial part, 4 `@stream` parts and 1 `@defer` page_info part: pinned album, photo, share, album, video (sd/hd/thumbnail), alternating A/B shapes |
| `timeline/variant_nested` | handcrafted | *"post_id only in graph id; deep message; summary-renderer comment count; shim link; deferred page_info; comment preview inside story"*. Has the `for (;;);` prefix and a Page mention range. |
| `comments/fakefb_first_page` | fakefb | 25 top-level comments plus 4 inline replies, gif and sticker attachments, 219 total, threads with expansion tokens |
| `comments/variant_graph_ids` | handcrafted | *"no legacy_fbid; creation_time; page author; sticker; inline reply with nested page_info; total_reply_count"* |
| `replies/fakefb_first_page` | fakefb | 10 depth-1 replies (page size), `comment_direct_parent`, a nested thread ref at depth 1 |

**Regeneration of synthetic fixtures.** `make fixtures` = `go test -tags fixturegen ./internal/fakefb -run TestWriteFixtures` (`Makefile:34-36`; `internal/fakefb/fixtures_gen_test.go:1-20`) uses a fixed clock (2026-10-01 12:00 UTC), `DefaultSpec` with 6 posts per page, a fixed media signing key and base URL `https://www.facebook.com`. `WriteFixtures` (`internal/fakefb/fixtures.go:14-76`) writes:
- `page_html` for page 0;
- `timeline` with `count=5`;
- `comments` for the post with the most comments, in "unfiltered chronological" order;
- `replies` for that post's busiest thread.

All are marked `source: fakefb, __scrubbed: true`. The committed `page_html` fixture predates the relay-bundle `<script src>` tag that `pageHTML` now emits (`render.go:331-335`), so re-running `make fixtures` would change it slightly. Then run `make golden`.

**Checks on captured fixtures (no expected output).** `spike.CheckFixtures(root)` (`internal/spike/fixturecheck.go:39-113`) runs the same dispatch as the golden test. A fixture is OK when there is no read error, no missing required field, **and at least one item parsed** ("an unrecognised shape usually parses to nothing rather than to missing fields", `fixturecheck.go:25-28`). CLI: `fbs-spike check-fixtures --dir testdata/fb [--json]` (`cmd/fbs-spike/main.go:313-322`). It works without a Go toolchain from the static `fbs-spike` binaries (`make spike-bin`).

**Capture sources.**
- `fbs-spike capture` records page HTML, a timeline page, a comments page and a replies page of real pages.
- `fbs-spike har --fixtures-dir` imports a browser HAR. The fixture is the largest successful response that carries data, never an error (`docs/runbooks/phase0-spike.md:168`).
- `fbctl replay --fixtures-dir` turns drifted production responses into fixtures.

All of them go through `fixtures.Write`, which scrubs before writing (`internal/fixtures/write.go:13-29`), so a raw response never touches disk. `fbctl fixtures scrub --dir` re-scrubs a directory (`cmd/fbctl/commands.go:291-296`).

**Scrub safety test.** `TestScrubbedFixturesStillParse` (`internal/spike/scrubparse_test.go:15-76`) treats every committed fixture as a raw capture, scrubs it, and asserts that `CheckFixtures` still passes with the same item counts.

---

## 9. The fixture scrubber (`internal/fixtures`)

**Entry points.**
- `ScrubDir(dir, keep)` (`scrub.go:26-60`). It skips fixtures already scrubbed by this version or later, and synthetic ones (`source: fakefb|handcrafted`, `scrub.go:39-41, 76-79`). Fixtures from older versions are scrubbed again; `scrubVersion = 3` (`scrub.go:64`). Kept ids are `keep` plus `meta.page_id`. It rewrites `response.ndjson` / `response.html` and stamps `__scrubbed: true, __scrubber: 3`.
- `Scrub(b, keep)` (`scrub.go:172-185`) has three modes:
  1. **HTML** (starts with `<`): every `<script type="application/json">` block is decoded and walked as JSON, then re-encoded with HTML escaping; the rest of the page is scrubbed by pattern (`scrub.go:222-244`).
  2. **JSON documents** (optional `for (;;);` prefix, NDJSON): decoded and walked (`scrub.go:194-219`).
  3. **Fallback** for truncated or unparseable bodies (e.g. a cut-off HAR body): pattern scrub. Every `"text":"..."` is blanked, names are replaced by regex, and nothing is dropped (`scrub.go:180-184`; test `scrub_test.go:276-285`).

**Two phases.**
1. `collect` finds **people's ids** (`scrub.go:277-301`):
   - the `id` of any object with `__typename == "User"` or any `__is*` key equal to `"User"` (Facebook interleaves `__isNode`, `__isActor`, ...) (`isUser`, `scrub.go:255-262`);
   - `owning_profile.id`;
   - the numeric id in `facebook.com/people/<name>/<id>` URLs;
   - all of the above inside **JSON serialized within strings**, recursively (`serialized`, `scrub.go:265-274`);
   - ids in `keep`, non-numeric ids, and the page are ignored (`person`, `scrub.go:247-253`).
   
   Regexes do the same for non-JSON text (`idPatterns`, `scrub.go:136-142`).
2. `transform` (`scrub.go:314-347`) rewrites values:
   - **Names:** keys `name, short_name, alternate_name, first_name, last_name, display_name, username` → `"Person " + 6 digits of sha256(name)`, except on the page's own object (the object id is in `keep`) (`scrub.go:114-115, 323-324`).
   - **Authored text** is blanked when it is under one of the authored keys (`body, body_renderer, preferred_body, message, translated_body_for_viewer, translation, description, title_with_entities, snippet, profile_status_text`), or anywhere inside a `Comment` (or under a key `comment`), or in a text object whose `ranges` mention a non-page `User` (`scrub.go:119-121, 325-326, 350-362`). Post text under `message` is blanked too; the page's UI text such as "1.2M followers" or "Photos" survives (`scrub_test.go:238-257, 290-304`).
   - **Captions:** `accessibility_caption`, `alt_text` are blanked.
   - **Profile links** on User objects (`facebook.com/...` but not fbcdn) → `https://www.facebook.com/scrubbed.profile`. Post permalinks and the page's link survive.
   - Serialized JSON strings are recursed and re-encoded.

**Final pass** `finish` (`scrub.go:377-396`):
- `/people/<name>/<id>` → `facebook.com/people/scrubbed/<id>`;
- every **whole** digit run equal to a collected person id → `pseudo(id)` = `sha256("id"+id)` mod 9e12 + 1e11 (`scrub.go:156-159`). Whole numbers only, so a short id inside a longer page id or post id is untouched (`scrub_test.go:78-94`);
- every signed fbcdn URL (plain, `\/`, `\\\/`, `&`, `&amp;`, single-quoted, unquoted attribute) → `https://<host>/scrubbed/<file>`, dropping `oh`, `oe`, `_nc_*`, `stp` and the path (`scrub.go:147-148`; tests `scrub_test.go:98-124`).

**Text blanking** `blank` (`scrub.go:401-411`) replaces letters, digits and marks with `x`, repeated to the character's **UTF-16 length**. Whitespace, punctuation and emoji are kept: `"Ali Hassan 😀 علي"` → `"xxx xxxxxx 😀 xxx"`, still 17 UTF-16 units. Mention offsets therefore stay valid.

**Order-preserving JSON** (`ojson.go`). An `object{keys, vals}` decoder/encoder keeps key order and `json.Number`s as written (`ojson.go:14-101`). It is needed because the order-sensitive regexes in `htmlx` (e.g. `"delegate_page":{"id":"<n>"`) must still match after scrubbing. Script blocks are re-encoded with HTML escaping (`ojson.go:105-111`). A truncated stream is an error (`unexpected`, `ojson.go:96-101`), so truncated bodies fall back to pattern mode.

**What survives** (runbook `docs/runbooks/phase0-spike.md:239-245`): the page's own name, links and text, real structure, post and comment ids, permalinks, timestamps and counts. Pseudonyms are deterministic (`scrub_test.go:25-27, 89-93`).

---

## 10. fakefb: the synthetic Facebook

### 10.1 World model (`internal/fakefb/world.go`)

`Spec` (`world.go:18-29`) holds `Seed, Pages, PostsPerPage, PostSpacing, MaxComments, ReplyProbability, MaxReplies, Depth2Prob, AttachmentProb, Now`. `DefaultSpec` is 2 pages × 12 posts, 20h spacing, ≤60 comments, reply probability 0.25, ≤14 replies, depth-2 probability 0.2, attachment probability 0.1 (`world.go:32-35`).

Generation (`NewWorld`, `world.go:125-165`) is deterministic, using PCG seeded with `(seed, seed^0x9e3779b97f4a7c15)`:
- **Authors:** 200 people, id `100_000_000_000 + i*7919`, names from fixed lists, every 37th verified.
- **Pages:** id `100_064_000_000_000 + i*1_000_003`, vanity `brand<i>`, name `Brand <i> Official`, category cycling through 3 values, followers random up to 5M, likes = 80% of followers, even-numbered pages verified, a fixed intro.
- **Posts** (`addPostLocked`, `world.go:202-246`): created at `now - (j+1)·spacing - rand(1h)`; the oldest post is **pinned**.
  - Type from a weighted list: status×2, photo×2, album, video, reel, link×2, share.
  - Text: random brand words plus `#<word> #brand`.
  - Media: photo 1, album 2-5, video/reel 1 video (5-175s) with views ≤500k.
  - Link: `https://shop.example.com/products/<n>`.
  - Share: the previously generated post. Because posts are generated newest first, that is actually a *newer* post, despite the code comment; harmless, but visible in the fixture (share created 09-29 shares a photo from 09-30).
  - Reactions up to 5000 split across types; shares up to 300.
- **Comments:** heavy-tailed count `MaxComments · r³` (`world.go:230-241`). Each comment (`newComment`, `world.go:259-290`) has:
  - a random author, text and reactions;
  - `Filtered` 30% (hidden from "most relevant"), `Edited` 5%;
  - 4% authored by the page (`IsOP`);
  - an attachment (photo/sticker/gif) with probability `AttachmentProb`;
  - replies with `ReplyProbability` at depth 0 and `Depth2Prob` at depth 1. Maximum depth is 2; each level has 1..`MaxReplies` replies.
- **Ids** increase by 1-97 from `1_200_000_000_000 + 1_000_000` (`world.go:167-170`).
- **Ground truth:** `TruthFor(pageID, since)` returns post ids, created time, type, total non-hidden comments including replies, top-level count, all comment ids and media count (`world.go:323-375`).
- **Mutators** (concurrency-safe), used by scheduling and change tests:
  - `Mutate(page, newPosts, newCommentsOnLatest)` (`379-405`);
  - `AddComments(post, n)` (`409-426`);
  - `DeleteComment(id)` removes it with its replies (`430-461`);
  - `WithholdComments(post, n)` leaves the first n out of every list but keeps them counted, like Facebook's partial logged-out lists (`465-476`);
  - `HideComment(id, bool)` removes a top-level comment from lists, counts and truth (`480-489`);
  - `EditComment(id, text)` (`492-501`).

### 10.2 Endpoints (`server.go:155-180`)

| Route | Behaviour |
|---|---|
| `GET /<vanity>`, `GET /<numeric id>`, `GET /profile.php?id=`, `GET /` (first page) | Page HTML (`page`, `server.go:219-267`). It issues a fresh `lsd` (11 chars) bound to the identity's `datr`: the `datr` cookie, or a random 24-char value returned as `_js_datr` in the HTML, which the client must turn into a `datr` cookie as Facebook's JS does. Unknown page → 404 "This content isn't available right now". `/login*` → a bare login form. |
| `POST /api/graphql/` | Form-encoded GraphQL (`graphql`, `server.go:278-346`), dispatched on `fb_api_req_friendly_name`: `ProfileCometTimelineFeedRefetchQuery`, `CommentsListComponentsPaginationQuery`, `Depth1CommentsListPaginationQuery`. Responses use `Content-Type: text/html; charset="utf-8"` like Facebook (`server.go:271`). |
| `POST /cookie/consent/` | Sets `fbs_consent=1` and returns `for (;;);{"payload":null}`. |
| `GET /media/<name>?oe=<hex>&oh=<sig>` | A fbcdn-like host (`server.go:546-585`). `oh` must equal `HMAC-SHA256(key, name|oe)[:24 hex]`, else 403 "Bad URL hash". An expired `oe` gives 403 "URL signature expired". The body is deterministic pseudo-random bytes: 20-80 kB JPEG, 150-400 kB MP4, GIF by extension. |
| `GET /rsrc.php/...` | A JS "relay bundle" declaring each accepted doc_id the way Facebook's bundles do: `__d("<Friendly>_facebookRelayOperation",[],(function(a,b,c,d,e,f){e.exports="<doc_id>"}),null);` (`server.go:689-707`). The bundle path is content-addressed (`/rsrc.php/v3/<fnv64>/relay.js`) and changes when doc_ids rotate (`server.go:141-153`). This lets doc_id discovery be tested end to end. |
| `/__fakefb/pages` | Lists pages. |
| `/__fakefb/truth/<page_id>?since=RFC3339` | Ground truth. |
| `POST /__fakefb/mutate` | `{page_id,new_posts,new_comments}` |
| `POST /__fakefb/faults` | A `Faults` JSON object (replaces the fault configuration). |
| `POST /__fakefb/rotate` | `{friendly_name, doc_id}` |
| `/__fakefb/stats` | Per-identity requests, GraphQL count, `min_interval_seconds` (to assert pacing), per-query counts, rate-limited count. |

**GraphQL validation order** (mimicking Facebook's error codes, `server.go:296-325`):
1. `lsd` unknown, or `x-fb-lsd` header ≠ form `lsd` → `for (;;);{"error":1357001,"errorSummary":"Sorry, something went wrong",...}`.
2. `x-fb-friendly-name` header ≠ form friendly name → `{"errors":[{"code":1357004,...}]}`.
3. `doc_id` not accepted for that friendly name → `1675012 PersistedQueryNotFound`.
4. Rate-limit fault → `for (;;);{"error":1675004,"errorSummary":"Rate limit exceeded",...}`.
5. Login-wall fault → `1357005 "Please log in to continue."`.
6. Bad `variables` JSON → `1357004`.
7. An unknown operation → `1675012`.

### 10.3 How replies mimic Facebook

**Timeline** (`server.go:375-429`):
- Ordering is pinned first, then newest first; the page size is `count` (default 3).
- Part 1 is `{"data":{"node":{"__typename":"User","id":...,"timeline_list_feed_units":{"edges":[edge0]}}},"extensions":{"is_final":false}}`.
- Each further edge is an `@stream` part: `{"label":"ProfileCometTimelineFeed_user$stream$ProfileCometTimelineFeed_user_timeline_list_feed_units","path":["node","timeline_list_feed_units","edges",i],"data":edge}`.
- The last part is the `@defer` page_info: `{"label":"...$defer$..._timeline_list_feed_units$page_info","path":["node","timeline_list_feed_units"],"data":{"page_info":{...}},"extensions":{"is_final":true}}`.
- Unknown page → `{"data":{"node":null}}`.
- The optional `FieldErrors` fault appends `{"label":"ProfileCometTimelineFeed_feedback$defer",...,"data":null,"errors":[{"message":"A server error field_exception occured...","severity":"ERROR",...}]}`.

**Story shape** (`render.go:130-206`):
```json
{"__typename":"Story","id":"<b64 S:_I<page>:<post>:<post>>","post_id":"<post>",
 "actors":[{"__typename":"User","id":"<page>","name":"...","url":"..."}],
 "comet_sections":{
   "content":{"story":{"message":{"text":"...","ranges":[{"offset":..,"length":..,"entity":{"__typename":"Hashtag",...}}]},
                       "attachments":[{"styles":{"attachment":{"media":{...}}}}]}},
   "context_layout":{"story":{"comet_sections":{"metadata":[{"__typename":"CometFeedStoryMinimizedTimestampStrategy",
                       "story":{"creation_time":<unix>,"url":"<permalink>"}}]}}},
   "feedback":{"story":{"story_ufi_container":{"story":{"feedback_context":{"feedback_target_with_context":
       {"comet_ufi_summary_and_actions_renderer":{"feedback":{"__typename":"Feedback","id":"<b64 feedback:<post>>",
        "reaction_count":{"count":N},"i18n_reaction_count":"N","top_reactions":{"edges":[...top 3...]},
        "share_count":{"count":N},"comment_rendering_instance":{"comments":{"total_count":N}},"video_view_count":N}}}}}}}}},
 "is_pinned":true, "attached_story":{"__typename":"Story",...}}
```
- Odd-indexed stories are an **A/B variant** with top-level `creation_time` and `permalink_url` (`render.go:191-195`).
- Albums use `styles.attachment.all_subattachments{count,nodes[{media}]}`.
- Links use `url: "https://l.facebook.com/l.php?u=<url>&h=AT0"`, `title_with_entities`, `description`, `source` and a `GenericAttachmentMedia` image.
- Reels have permalink `/reel/<id>`.
- Media nodes (`render.go:106-127`):
  - Photo: `photo_image{uri,width,height}` + `accessibility_caption`;
  - Video: `browser_native_sd_url/hd_url`, `playable_url[_quality_hd]`, `length_in_second`, `preferred_thumbnail.image.uri`;
  - Sticker: `image`;
  - Gif: `GenericAttachmentMedia.animated_image`.
- Media URLs are `<base>/media/<id>_<variant>.<ext>?oe=<hex now+TTL>&oh=<sig>`.

**Comments** (`server.go:463-510`):
- `data.node` is a `Feedback` with `comment_rendering_instance_for_feed_location.comments{edges, page_info, total_count}`, 25 per page.
- `total_count` counts withheld comments (not hidden ones), so the count can exceed the list, as on Facebook.
- The intent token picks the ordering (section 7).

**Replies** (`server.go:512-544`):
- They require `expansionToken == b64("exp:<comment id>")`; otherwise they return an "invalid expansion token" error.
- `data.node` is a `Feedback` with `replies_connection{edges, page_info}`, 10 per page.

**Comment shape** (`render.go:232-278`): `__typename: Comment`, `id` = b64 `comment:<post>_<id>`, `legacy_fbid`, `depth`, `created_time`, `body{text,ranges}`, `author{__typename User|Page, id, name, url, is_verified, profile_picture_depth_0.uri}`, `is_author_original_poster`, `edit_history.count`, `url` `<base>/<post>?comment_id=<id>`, `comment_direct_parent{id, legacy_fbid}`, `attachments[].style_type_renderer.attachment.media`. Its `feedback` holds:
- `id` = b64 `feedback:<post>_<id>`;
- `reactors.count`, `top_reactions`;
- `replies_fields{total_count = all descendants, count = direct}`;
- `expansion_info.expansion_token`;
- an inline `replies_connection` with the first reply when `InlineReply` is on.

**Page HTML** (`render.go:306-365`):
- `<title>Name | Facebook</title>`, `og:title`, `og:url`, `al:android:url fb://page/<id>`;
- the relay bundle `<script src>`;
- an optional consent banner (`data-cookiebanner` + `/cookie/consent/` form);
- a `ScheduledServerJS` JSON script with `LSD`, `SiteData`, `CurrentUserInitialData{USER_ID:"0"}` and `CookieCoreConfig _js_datr`;
- the profile header as `RelayPrefetchedStreamCache` → `adp_ProfileCometHeaderQueryRelayPreloader_1` → `__bbox.result.data.user{...}`;
- a `pageID` script;
- optionally, the page's own first timeline `fetch()`, which is what a render-and-intercept browser captures.

### 10.4 Fault injection

`Faults` (`server.go:23-39`); counters are per identity (`datr`).

| Fault (JSON / CLI flag) | Effect |
|---|---|
| `rate_limit_every` / `--rate-limit-every N` | Every Nth GraphQL request per identity → error 1675004 |
| `login_wall_after` / `--login-wall-after N` | After N GraphQL requests per identity → 1357005 "Please log in" |
| `latency` / `--latency d` | Sleep `d/2 + rand(d)` on every request |
| `consent_wall` / `--consent-wall` | Cookie banner on page HTML until `/cookie/consent/` sets `fbs_consent` |
| `drift` | Rename `message` → `msg_v2`. An optional field goes missing: text is empty, no drift. |
| `drift_required` | Rename `creation_time` → `created_v2`. A required field goes missing → `SchemaDrift`. |
| `js_only_tokens` | Tokens are inserted by a script via `atob`, so an HTTP bootstrap fails and the browser path is needed |
| `media_ttl` | TTL of signed media URLs (default 7 days) |
| `field_errors` | Adds a partial `field_exception` error part after timeline pages |
| doc_id rotation | `--docids rotated` (`RotatedDocs`: `"7316"+id[4:]`, `server.go:712-726`), `RotateDoc`, `AcceptDocs`, admin `/rotate`. `OmitFromBundle` hides a friendly name from the bundle, as if it were lazily loaded. |

`cmd/fakefb` (`cmd/fakefb/main.go:17-67`) flags: `--addr :8090 --seed 42 --pages 5 --posts 30 --max-comments 120 --rate-limit-every --login-wall-after --latency --consent-wall --docids seed|rotated`. Accepted doc_ids come from the embedded `configs/queries.seed.yaml`, and `InlineReply` is on. Drift, JS-only, field-error and TTL faults are only reachable through `/__fakefb/faults` or in-process `Options`.

### 10.5 Where fakefb is used

- **Client-layer tests:** `internal/fb/fbtest.New` starts an in-process fakefb behind `httptest` with the real session pool, registry and GraphQL client (`internal/fb/fbtest/fbtest.go:44-99`). Tests cover the login wall, rate limits, field errors, doc rotation and drift (`internal/fb/fbtest/client_test.go:143-240`).
- **Browser bootstrap** with JS-only tokens (`internal/fb/browser/browser_test.go`).
- **Registry sync:** doc_id rediscovery from bundles, required-field drift and omitted bundle entries (`internal/registrysync/sync_test.go`).
- **e2e** (`internal/e2e/*`, `make e2e`, `make test-integration`):
  - `TestEndToEnd` asserts post and comment counts equal to `TruthFor`, completeness 1.0, depth-2 replies present, and the PII split;
  - `TestCommentEditsAndDeletions` covers edits and deletions;
  - schedule, registry rotation, chaos (restarts and faults) and browser tests;
  - `make loadtest` and `make soak` against large or changing worlds.
- **Phase 0 rehearsal:** `make phase0-dryrun` runs `phase0/dryrun.sh`. It builds and starts `bin/fakefb --docids rotated --pages 5`, then runs `fbs-spike phase0 --tiers direct`. The rehearsal (preflight, doc_id discovery, capture plus fixture check, smoke, measurement run, report, bundle) only passes if every step works, including rediscovering the rotated doc_ids.
- **Synthetic fixtures** via `make fixtures`.

---

## 11. Kafka topics and schema contract

**Public topics** (`internal/topics/topics.go:10-21`; `docs/contracts/kafka-schemas.md:6-17`). Names get `kafka.topic_prefix` prepended.

| Topic | Key | Value |
|---|---|---|
| `fb.page.v1` | page id | `Page` |
| `fb.post.v1` | post id | `Post` |
| `fb.post_metrics.v1` | post id | `PostMetrics` |
| `fb.comment.v1` | **post id**, so a post's thread stays ordered within a partition | `Comment` (including DELETED tombstone-like records) |
| `fb.media.v1` | media id | `Media` |
| `fb.media.ready.v1` / `.processed.v1` / `.deleted.v1` | media id | `MediaReady` / `MediaProcessed` (produced by the analysis service) / `MediaDeleted` |
| `fb.job.v1` | job id | `JobEvent` |
| `fb.erasure.v1` | erasure request id | `ErasureEvent` |

`topics.Entity = [Page, Post, Comment, Media]` are the topics the persister consumes; the finalizer waits for it to catch up on these (`topics.go:31`).

**Internal task topics:** `fbs.task.{page,post,thread}.{p0,p1,p2}` by kind and lane (`topics.Task`, `topics.go:37-53`), `fbs.task.media`, the retry topics `fbs.task.retry.{30s,5m,1h}` and `fbs.task.dlq` (`topics.go:22-26`).

**Wire format** (`kafka-schemas.md:32-38`):
- plain protobuf bytes, with **no** Confluent magic byte or schema id;
- header `fbs-type` = full message name (e.g. `securebrand.fb.v1.Comment`);
- `content-type: application/x-protobuf` (`internal/platform/kafka/producer.go:23-31`).

Consumers decode by `fbs-type`.

**Schema Registry** (`kafka-schemas.md:40-55`):
- `fbctl schemas apply` registers the embedded protos (`api/proto/embed.go`) using `topics.ValueSchemas` (`internal/topics/schemas.go:8-19`).
- Each `<topic>-value` subject holds the **whole proto file** declaring the value message (so `fb.post.v1-value` and `fb.post_metrics.v1-value` both hold `post.proto`). `common.proto` gets its own subject, which the others reference; `google/protobuf/*` are built in.
- Apply checks every changed schema first and registers nothing unless all pass. Changed imports are staged under temporary `fbs-check-…` subjects.
- Default compatibility is **`BACKWARD_TRANSITIVE`**, set only when the subject has no level of its own; an operator-set level is kept.
- Registration is idempotent.
- A Helm hook (`hooks.schemas: true`) and a Compose `schemas` service run it before upgrades, so an incompatible change fails the deploy.

**Change rules** (`kafka-schemas.md:57-62`):
- only add fields with new numbers; never change a type or number; `reserved` removed numbers;
- CI runs `buf breaking` (FILE rules);
- a real break needs a new topic and package (`fb.comment.v2`, `securebrand.fb.v2`) produced alongside v1.

**Comment versions and deletions** (`kafka-schemas.md:21-30`):
- every observation is published;
- an edit is a later record with different text;
- a deletion record has `status=DELETED` and only keys;
- a re-sighting is a later ordinary record;
- `event_id` is content-derived, except a deletion's, which is unique per job. Deduplicate on `event_id` only within the same status.

---

## 12. Gaps, quirks and things to know before copying

1. **Hashtag offsets: rune vs UTF-16 mismatch** (6.2). `Hashtags` can produce a shifted, space-suffixed duplicate when an emoji precedes a hashtag range. fakefb cannot surface it.
2. **No `@defer` path stitching** (section 2). Deferred fragments without their parent entity are ignored. That is fine for the shapes seen so far, but a structural limit.
3. **Media id falls back to the URL** when a media node has no `id` (`media.go:41-44`). That id changes whenever the URL is re-signed, which defeats "download once" for link previews.
4. **Social-context count parsing scans every `text` under the page object** (`page.go:87-99`). A bio such as "We like shoes since 1990" appearing before the real likes line would be read as 1990 likes, because document order decides.
5. **Unused contract surface:** `extra_json` (Page/Post/Comment), `Attachment.preview_url`, `Attachment.type: "link"`, `MediaKind_COVER/PROFILE_PIC`, `PageStatus_RESTRICTED`, `Media.post_created_at` and `ImageRef.width/height` are never filled by the parsers.
6. **`i18n_reaction_count`** (e.g. `"1.2K"`) is read by `Int64`, which returns 0 for non-numeric strings. It only matters when both structured counts are absent.
7. **The golden runner does not pass `PageAuthorID`**, so it never exercises the `is_op` fallback.
8. **The fakefb share points at a newer post**, and the committed `page_html` fixture lags the current renderer by one `<script src>` tag.

---

## 13. Instagram adaptation notes

> **Everything in this section is a suggestion by the author of this note, not a fact from the codebase.** Instagram field names below are from general knowledge of Instagram's web responses and must be confirmed with a Phase-0-style capture (HAR import + `check-fixtures`) before you build on them. Instagram changes its shapes and logged-out access rules often, and much of the data (comments especially) may need a session.

### 13.1 What is generic and can be lifted almost as-is

| Component | Why it's generic | Suggested change |
|---|---|---|
| `parse/walk.go` (`Collect`, `First`, `Find*`, ancestor tree, de-dup keys) | Pure JSON-tree utilities | **Generalise `Collect` to a predicate.** Instagram's private v1 API objects (`/api/v1/...`) have **no `__typename`**, and the newer GraphQL `xdt_*` shapes use typenames like `XDTMediaDict` inconsistently. Suggested: `CollectBy(parts, func(gjson.Result) (kind string, ok bool))` with a classifier such as "has `pk` and `media_type`" → media, "has `pk` and `text` and `created_at`" → comment, "has `pk` and `username`" → user. Keep the typename collector as one classifier. |
| `FindPageInfo` | Connection + `page_info` search with skip and deferred parts | Instagram GraphQL uses `page_info{has_next_page,end_cursor}`, so it works as-is with keys like `xdt_api__v1__feed__user_timeline_graphql_connection`, `edge_owner_to_timeline_media`, `edge_media_to_parent_comment`, `edge_threaded_comments`. The v1 REST endpoints instead return top-level `more_available` + `next_max_id` (feeds) and `has_more_comments`/`next_min_id` (comments). Add a pluggable "pagination extractor" returning the same `PageInfo`. |
| `Report{Missing,Optional}` + drift classification + metrics | Platform-neutral contract | Reuse unchanged. Keep the required set minimal (media pk + taken_at; comment pk + created_at; profile id + username). |
| `Int64`, `Unix`, `ParseCount`, `URLExpiry`, `MediaID` (UUIDv5), `Ranges`-style helpers | Generic | `URLExpiry` applies directly: Instagram CDN URLs (`*.cdninstagram.com`, `*.fbcdn.net`) carry the same hex `oe=` and `oh=`. Give `MediaID` a separate namespace UUID per platform. Fix the UTF-16 offset issue before reusing `Hashtags`. |
| `graphql.SplitParts` / `HasData` / `Errors` | Instagram web GraphQL also streams and prefixes | Likely reusable. Also handle v1 REST errors such as `{"status":"fail","message":"Please wait a few minutes before you try again.","require_login":true}` and `checkpoint_required`. |
| Golden framework (`testdata/<platform>/<query>/<case>/{response.*, meta.json, expected.json}`, `-update`, `__scrubbed` gate, variant cases, `check-fixtures`, the scrub-then-parse test) | Platform-neutral | Copy wholesale; parametrise the runner's dispatch table. |
| `internal/fixtures` scrubber + `ojson` | JSON-walk design is generic | Swap the person detectors for Instagram's. A person is any object with `username` + `pk|id` (+ `full_name`, `profile_pic_url`) that isn't the scraped profile. Name keys: `full_name`, `username`. Authored text: `caption.text`, `text` (comments), `biography`, `accessibility_caption`. Profile links: `instagram.com/<username>`. Signed-URL regex: `cdninstagram\.com|fbcdn\.net`. Keep the UTF-16-preserving `blank`, whole-number id pseudonymisation and the version/re-scrub logic. Note that **usernames are identifiers too**: they need consistent pseudonymisation like ids, not just blanking, because cross-references (mentions, tags) use them. |
| Emitter `stamp` (content hash minus signed URL params → deterministic `event_id`), `author_key` HMAC, suppression and erasure | Platform-neutral | Reuse. Key `author_key` on the Instagram user pk (stable), not the username (mutable). |
| fakefb pattern (seeded world + ground truth + Facebook-shaped renderers + faults + admin API + signed media host + fixture writer) | The design is generic; the renderers are not | Build `fakeig` with the same skeleton (section 13.4). |
| Kafka contract machinery (embedded protos, `ValueSchemas`, registry apply/check, `fbs-type` header, `buf breaking`) | Generic | Reuse with a new package and topics (section 13.5). |

### 13.2 What needs Instagram-specific equivalents

- **Entity detection and field candidate lists** (the whole of `timeline.go`, `comments.go`, `media.go`, `page.go`).
- **Id codecs** replacing `PostIDFromStoryID`/`FeedbackID`/`CommentIDFromGraphID`:
  - media `pk` (numeric, ~19 digits; still fits int64, but consider string ids or uint64 for headroom);
  - media `id` = `"<media_pk>_<owner_pk>"`;
  - **shortcode ↔ pk**: base64 with alphabet `A-Za-z0-9-_` over the pk, used in `/p/<code>/` and `/reel/<code>/` URLs. This is the analogue of decoding story ids, and it lets a permalink be turned into a pk without a request;
  - comment `pk`; user `pk`/`id`.
- **Page HTML bootstrap:** Instagram profile pages embed data differently (historically `window._sharedData`, later `<script type="application/json" data-sjs>` with `require`/`ScheduledServerJS`/`__bbox` exactly like Facebook Comet, plus `PolarisProfilePage...` preloaders). `htmlx.JSONBlobs` + `Collect` will likely work; the `PageID` regexes need Instagram versions (`"profile_id":"<n>"`, `"user_id":"<n>"`, `instagram://user?username=`). The tokens differ (`csrftoken` cookie, `X-IG-App-ID` header such as `936619743392459`, `lsd`, `X-ASBD-ID`); this is the transport team's concern.
- **Comment threading:** Instagram has two levels only (comment → child comment; replies to replies are flattened with an @mention). Model `depth ∈ {0,1}`. Thread refs come from `child_comment_count` > `preview_child_comments` length, paginated by a child-comments endpoint/query.
- **Reactions:** Instagram has only likes. Comments have `comment_like_count`; posts `like_count` (`edge_liked_by.count`/`edge_media_preview_like.count` in older shapes). `like_and_view_counts_disabled` can hide them.

### 13.3 Proposed entity mapping (Facebook → Instagram)

**Profile ↔ Page**

| fb `Page` | Suggested Instagram source |
|---|---|
| `page_id` | user `pk` / `id` |
| `vanity` | `username` |
| `name` | `full_name` |
| `intro` | `biography` (+ `bio_links[].url`) |
| `url` | `https://www.instagram.com/<username>/` |
| `categories` | `category_name` / `business_category_name` |
| `followers_count` | `follower_count` / `edge_followed_by.count` |
| `likes_count` | no equivalent. Suggest new fields `following_count` (`edge_follow.count`) and `media_count` (`edge_owner_to_timeline_media.count`). |
| `verified` | `is_verified` |
| `profile_picture` | `profile_pic_url_hd` / `hd_profile_pic_url_info.url` |
| `cover_photo` | none |
| `contact` | `business_email`, `business_phone_number`/`contact_phone_number`, `external_url`, `business_address_json` |
| new fields | `is_private`, `is_business_account`/`is_professional_account` |
| `status` | `ACTIVE` / `UNAVAILABLE` (404 / `user: null`) / `RESTRICTED` (private) |

**Post / reel / carousel ↔ Post**

| fb `Post` | Suggested Instagram source |
|---|---|
| `post_id` | media `pk` (keep `id` `<pk>_<owner>` as `graph_id`, and add a `shortcode` field from `code`/`shortcode`) |
| `created_at` | `taken_at` / `taken_at_timestamp` |
| `permalink` | `https://www.instagram.com/p/<code>/` or `/reel/<code>/` |
| `type` | from `media_type` (1 image, 2 video, 8 carousel) + `product_type` (`feed`, `clips` = reel, `carousel_container`, `igtv`). Suggested enum: `IMAGE, VIDEO, CAROUSEL, REEL` (+ `IGTV`, `STORY` if ever needed). No SHARE/LINK/EVENT/LIVE. |
| `text` | `caption.text` / `edge_media_to_caption.edges[0].node.text`. **No ranges**: extract `#hashtags` and `@mentions` by regex (offsets in UTF-16 if you emit ranges). |
| `hashtags` | regex over the caption (fixed version of `Hashtags`) |
| `links` | usually none in captions (links are not clickable); maybe `bio_links` for the profile only |
| `shared_post_id` | none (possibly a remix/`original_media` reference for reels) |
| `is_pinned` | `timeline_pinned_user_ids` non-empty / `clips_tab_pinned_user_ids` |
| `is_paid_partnership` | `is_paid_partnership` / `sponsor_tags` / branded content fields |
| `author_id/name` | `user.pk` / `user.username` (+ `coauthor_producers[]`: a new repeated field for collab posts) |
| `reactions` | replace with `like_count` (optional int64) + `likes_hidden` flag. Or reuse `Reactions{total=like_count, by_type={"like":n}, complete=true}` to keep consumers uniform. |
| `comment_count` | `comment_count` / `edge_media_to_comment.count` (+ `comments_disabled`) |
| `share_count` | not generally public (possibly `reshare_count` on some reels responses; verify) |
| `view_count` | reels: `play_count` / `ig_play_count` (and `view_count` / `video_view_count` for legacy video). Suggest separate `play_count` and `view_count` fields, since they mean different things. |
| `feedback_id` | not needed: comments are addressed by media pk |
| `media_ids` | carousel children + cover |
| new | `location{pk,name}`, `usertags[]` (user pks; PII), `music/audio` id for reels, `accessibility_caption` |

**PostMetrics.** Same idea: `like_count`, `comment_count`, `play_count`, `view_count`, `captured_at`.

**Comment / child comment ↔ Comment/reply**

| fb `Comment` | Suggested Instagram source |
|---|---|
| `comment_id` | comment `pk` |
| `parent_comment_id`, `root_comment_id`, `depth` | `parent_comment_id` (child comments) → depth 1; root = parent (two levels) |
| `author` | `user{pk, username, full_name, is_verified, profile_pic_url}`. `type` is always `User`; keep `author_key` = HMAC(pk). |
| `text`, `text_ranges` | `text`; ranges by regex for @mentions/#tags |
| `created_at` | `created_at` / `created_at_utc` |
| `reactions` | `comment_like_count` |
| `reply_count` | `child_comment_count` |
| `is_op` | `user.pk == media owner pk` |
| `edited` | Instagram comments are not editable; leave unset |
| `attachments` | GIF/sticker comments where present (`giphy_media_info`?), otherwise none |
| new | `is_pinned` (`is_pinned`/`has_liked_comment` variants; verify), `is_hidden`/restricted flags if exposed |

Thread refs: `child_comment_count > len(preview_child_comments)` → paginate child comments. Comment ordering: Instagram v1 supports `can_support_threading=true` and sort orders (`popular` vs chronological); pick the most complete one, like `RANKED_UNFILTERED_...`, and validate with a fake "withheld/filtered" model as fakefb does.

**Media variants ↔ Media**

| Variant | Suggested Instagram source |
|---|---|
| `original` (image) | largest of `image_versions2.candidates[]` (`url,width,height`) / `display_url` / `display_resources[]` |
| `thumbnail` | for video: the image candidate (cover frame); optionally the smallest image candidate as a preview |
| `sd` / `hd` (video) | `video_versions[]` (each has `type`, `width`, `height`, `url`). Map by height (e.g. ≤720 → sd, max → hd), or emit one variant per rendition with the height in the variant name. `video_dash_manifest` → `dash_manifest_url`. |
| carousel | `carousel_media[]` / `edge_sidecar_to_children.edges[].node`. Each child is its own media (`fb_media_id` = child `pk`/`id`) with its own variants. Owner is the parent post. |
| `duration_ms` | `video_duration` (seconds float) × 1000 |
| `alt_text` | `accessibility_caption` |
| `source_expires_at` | `oe=` via `URLExpiry` (unchanged) |
| kinds | `IMAGE, VIDEO, REEL, THUMBNAIL, CAROUSEL_ITEM?, PROFILE_PIC` (comment media is rare) |

**Job / Erasure.** Reuse unchanged (subject = author key over the Instagram user pk, or profile id).

### 13.4 fakeig: suggested shape

Copy `internal/fakefb` structurally:
- **World:** profiles (public/private), posts with media_type/product_type mix, carousels, reels with play counts, comments with child comments (max depth 1), hidden like counts, comments disabled, pinned posts, collab co-authors, withheld/filtered comments, deletions and edits (none for Instagram comments), plus `TruthFor`.
- **Renderers:** both the GraphQL (`xdt_*`/`edge_*`) shape and the v1 REST shape, if the scraper will use both. Alternate A/B variants per item, as fakefb does with `alt` stories.
- **Endpoints:** a profile HTML page with tokens and preloaded user JSON, the profile-info JSON endpoint, `/graphql/query` (doc_id + friendly name validated against a seed), the comments and child-comments endpoints, and a signed CDN host with `oe`/`oh` HMAC and expiry.
- **Faults:** 429/`Please wait a few minutes` every N requests, a login redirect (`/accounts/login/`) or `require_login` after N requests, `checkpoint_required`, doc_id rotation with a content-addressed bundle, renamed-field drift (optional and required), latency, JS-only tokens.
- **Admin:** `/__fakeig/{profiles,truth,mutate,faults,rotate,stats}`.
- `WriteFixtures` + a `fixturegen` build tag for synthetic goldens.

### 13.5 Contract suggestions

- New proto package `securebrand.ig.v1` and topics `ig.profile.v1`, `ig.post.v1`, `ig.post_metrics.v1`, `ig.comment.v1` (key = post pk, for per-post ordering), `ig.media.v1`, with the same wire format, `fbs-type` header and `BACKWARD_TRANSITIVE` registry flow.
- Factor the platform-neutral types (`Observation`, `TextRange`, `ImageRef`, `CrawlMode`, `S3Object`, the media handoff messages, `JobEvent`, `ErasureEvent`) into a shared `securebrand.common.v1` *for new code*. Leave `fb.v1` untouched: moving its messages is a breaking change by the repo's own rules (`kafka-schemas.md:57-62`).
- Alternatively, reuse `fb.media.ready.v1`/`processed`/`deleted` for Instagram media if the analysis service should see a single stream; an `owner_platform` field would then be an additive change.
- Use string ids or keep int64 deliberately. Instagram media pks are ~3.5-4e18 today; int64 max is 9.22e18. Store shortcodes alongside.
