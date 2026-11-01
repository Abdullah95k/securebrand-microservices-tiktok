# tks golden fixtures: from a real answer to a committed test

tks's parsers (`internal/tt/parse`) are tested against **golden fixtures**: real TikTok answers, scrubbed, under `testdata/tt/<kind>/<case>/`. Each holds the answer (`response.json`, or `response.html` for a page), a `meta.json` and the parsers' expected output (`expected.json`). Every parser version must pass every fixture, old shapes included.

Real answers come from the owner's recordings (HARs) and the probe's `raw/` folder. **They hold personal data and never enter git** (build brief §7): they stay in `.local/` (git-ignored). Only the scrubbed copy is written into `testdata/tt`.

## What the scrubber does

`internal/tt/scrub` (version in `scrub.Version`, recorded in each `meta.json`):
- **People and their content get pseudonyms, consistently.** Usernames become `user` + 8 hex digits, nicknames `Person` + 6 hex digits, secUids keep their `MS4wLjABAAAA` prefix and length. Every id (users, posts, comments, music, hashtags, subtitles, the browser's device id) gets new digits; an id that carries a time in its top 32 bits (T8) keeps that time, so a fixture's id times and `createTime`s still agree, and every pseudonym fits int64. The same value gets the same pseudonym everywhere in an answer: in its fields, in share links, inside nested JSON.
- **Text people wrote is blanked**, keeping its length in UTF-16 units (TikTok's offsets for hashtags and mentions): captions, comments, bios, titles, hashtags. Letters and digits become `x`; punctuation, `#`, `@`, spaces and emoji stay. A nickname quoted in a text is blanked even when it is only emoji.
- **Media links lose their signatures.** A signed link keeps its host, a stable file name, its type and its expiry (`x-expires`, `expire`), which the parsers read. Bio links and links to other sites become `https://example.invalid/scrubbed`.
- **Pages are cut down** to one rehydration script with the account and post scopes and the app context's values the client reads. The rest of a page (its title, meta tags, other scopes) names the account and is never kept.
- **Tokens and file handles** (`csrfToken`, `nonce`, video `Uri`, file hashes) and TikTok's inferences about people (`predicted_age_group`) become `scrubbed`.
- `--items` keeps a few list items or comments, chosen to cover the shapes the parsers read (pinned, photo, ad, mention, hashtag, labels; inline replies, images, pins).

How we know it works:
- `go test ./internal/tt/scrub` scrubs answers with a distinctive value in every place TikTok puts personal data and proves none survives; it also guards the committed fixtures (scrubbed by the current version, no signed link, no age band). CI also fails on a signed TikTok link under `testdata/tt`.
- With real answers at hand, the same package scrubs each one, reads both copies with the parsers, and checks that no username, nickname, secUid, id, text or signed link of the original is in the scrubbed copy, and that both read the same (records, id times, text lengths, counts):

```bash
TKS_SCRUB_REAL_DIR=$PWD/.local/realset go test -count=1 -run Real -v ./internal/tt/scrub
```

`.local/realset` is any folder of raw answers: HAR bodies or probe `raw/` files. The test names files by number, never by their names, which may hold a username.

## Adding a fixture

1. Put the HAR or the raw answer under `.local/` (never anywhere git sees).
2. Add it. From a raw answer (its kind is guessed):

```bash
go run ./cmd/tksctl fixtures add --from .local/raw/creator_list.json --name probe_first_page
```

   From a HAR, name the kind and which of its answers (from 1):

```bash
go run ./cmd/tksctl fixtures add --from .local/har/post1.har --kind comments --entry 3 --name har_third_page
```

   Kinds: `profile_page`, `post_page`, `post_list` (the web app's), `creator_list`, `user_detail`, `comments`, `replies`. **Name the case by what it shows (`har_pinned_first`), never by the account.**
3. Write its expected output and review the diff:

```bash
go test ./internal/tt/parse -run Golden -update
```

4. Run the scrubber's tests, and the real-answer check above if you have the originals:

```bash
go test ./internal/tt/scrub ./internal/tt/parse
```

## When the scrubber learns a rule

Raise `scrub.Version`, then scrub every fixture again and rewrite the goldens:

```bash
go run ./cmd/tksctl fixtures scrub
```

```bash
go test ./internal/tt/parse -run Golden -update
```

Pseudonyms change when a fixture is scrubbed again; they stay consistent within each fixture.
