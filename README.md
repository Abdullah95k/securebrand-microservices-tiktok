# tks: public TikTok account scraper

tks monitors a list of public TikTok accounts (media, brands, public figures) without logging in: each account's details, its videos and photo posts with their metrics, their comments, and media metadata. It publishes `tt.*.v1` Kafka topics, keeps everything queryable in its own Postgres schema, and serves an HTTP API, with the same privacy protections as its siblings: an HMAC key on the stable user id, personal data behind a scope, and erasure.

It joins two scrapers built the same way:
- **fbs**, logged-out Facebook pages: [`Abdullah95k/securebrand-microservices`](https://github.com/Abdullah95k/securebrand-microservices);
- **igs**, logged-out Instagram profiles: [`Abdullah95k/securebrand-microservices-igs`](https://github.com/Abdullah95k/securebrand-microservices-igs).

**Status:** building the probe (milestone M1). The code is a copy of igs's (and through it fbs's), seeded on 2026-10-31: see [`UPSTREAM.md`](UPSTREAM.md). The copied pipeline's own README is [`docs/fbs-readme.md`](docs/fbs-readme.md). The plan, the evidence behind it, and the prompt that starts every build session are in [`docs/tiktok/`](docs/tiktok/build-brief.md):

| Document | What it holds |
|---|---|
| [`build-brief.md`](docs/tiktok/build-brief.md) | Decisions, architecture, milestones with acceptance criteria, owner checkpoints, rules, the service's acceptance criteria, the build-session prompt |
| [`build-status.md`](docs/tiktok/build-status.md) | Where the build stands and what the owner needs to do next |
| [`methods.md`](docs/tiktok/methods.md) | Every way to get the data, compared, with sources; the recommendation, the fallback, and notes for the legal team |
| [`open-questions.md`](docs/tiktok/open-questions.md) | The hypotheses about TikTok (T1, T2, …) and how each gets answered |
| [`recording-guide.md`](docs/tiktok/recording-guide.md) | What to record in Chrome, step by step |
| [`probe-spec.md`](docs/tiktok/probe-spec.md) | The first build milestone: the probe the owner runs on the droplet |
| [`reuse-plan.md`](docs/tiktok/reuse-plan.md) | What tks takes from igs |
| [ADR 0006](docs/tiktok/adr/0006-tks-code-organisation.md), [ADR 0007](docs/tiktok/adr/0007-tks-data-model-and-contracts.md) | Code organisation; data model and contracts |

**Never commit** recordings (`.har`), probe output, proxy credentials, or any real username, name or comment text. Recordings go under `.local/har/` and probe output under `phase0/`, both git-ignored; CI fails on a committed `.har`.
