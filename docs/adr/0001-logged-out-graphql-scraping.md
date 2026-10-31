# ADR 0001: Logged-out scraping of Facebook's web GraphQL over plain HTTP

Status: accepted (Phase 0 validation pending)

## Context
We need posts, every comment and reply, and media of any public page, at
hundreds of thousands of pages per day, self-hosted. Options were the Graph
API (Page Public Content Access is rarely granted and restricts commenter
data), a paid scraping API, a headless browser per page, or calling the web
client's GraphQL endpoint directly.

## Decision
* Call `POST /api/graphql/` with the web client's `doc_id`, variables and
  tokens (`lsd`, `jazoest`, `__spin_*`, ...) bootstrapped from the page HTML,
  using `bogdanfinn/tls-client` so TLS, HTTP/2 and header order match the
  Chrome version in the User-Agent. A browser is used only as a fallback
  transport (Phase 2), never per page.
* **Logged out only.** No accounts, no fake or warmed accounts, no ban
  evasion. "All comments" means everything Facebook serves to logged-out
  clients with the unfiltered ordering, and coverage is measured per post
  (`completeness`, `comments_state`).
* Query names, doc_ids and variable templates are data (registry table,
  hot-reloaded), because Facebook rotates them.
* Parsers find entities by `__typename` with candidate field paths and report
  missing required fields as schema drift instead of silently dropping data.

## Consequences
* 50-100x cheaper per page than a browser; CPU is not the bottleneck,
  proxies and pacing are.
* Coverage is bounded by what logged-out clients receive. Phase 0 measures it;
  the system reports truncation rather than hiding it.
* Doc_id and shape churn is an operational task (registry updates, golden
  fixtures from `fbs-spike capture`).
* Legal review (ToS, GDPR legitimate interest, DPIA) is required before
  production use.
