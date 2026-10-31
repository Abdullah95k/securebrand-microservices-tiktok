# Upstream: igs and fbs

This repository (tks, the TikTok account scraper) owns a copy of the code of igs, the Instagram scraper, which owns a copy of the code of fbs, the Facebook page scraper. The decision and its reasons are in [ADR 0006](docs/tiktok/adr/0006-tks-code-organisation.md). tks work never changes igs's or fbs's repositories.

- **Upstream repositories:** igs, [`Abdullah95k/securebrand-microservices-igs`](https://github.com/Abdullah95k/securebrand-microservices-igs); fbs, [`Abdullah95k/securebrand-microservices`](https://github.com/Abdullah95k/securebrand-microservices).
- **Copied from:** igs `9463b91` (main, 2026-10-31), with its full history, merged into tks's `main` (`git merge --allow-unrelated-histories`), so `git blame` reaches back through igs into fbs (igs was copied from fbs at `f1377a9`).
- **Seed changes:** the Go module renamed to `github.com/abdullah95k/securebrand-microservices-tiktok` (protobuf code regenerated); the Instagram-only code removed (`internal/ig`, `internal/fakeig`, `cmd/igs-spike`, `configs/ig`, `docs/instagram`, `docs/runbooks/igs-probe.md`, the `igs-probe*` and `igs-spike-bin` Makefile targets, `phase0/ig-profiles.example.txt`); igs's README moved to [`docs/fbs-readme.md`](docs/fbs-readme.md). The copied Facebook code stays as the regression net of the copied pipeline until tks's own suites replace it (ADR 0006 §1).
- **Names on shared infrastructure** are still igs's (`igs:` keys, `igs_` metrics, `IGS_` env) until M1b renames them to tks's; until then no tks binary runs on shared infrastructure (ADR 0006 §5).

To port an igs change: `git remote add igs https://github.com/Abdullah95k/securebrand-microservices-igs`, `git fetch igs`, then `git cherry-pick <sha>` while the paths still match, or port it by hand. Record it below either way. Fetching is read-only; never push to igs's or fbs's repositories.

A cherry-picked change conflicts on any import line it touches, because the module paths differ. Resolve those by keeping this repository's paths, then run `go build ./...`; if it adds or changes a `.proto` file, run `buf generate` afterwards.

## Ported from igs or fbs

| Date | Upstream commit | What | tks PR |
|---|---|---|---|

## tks fixes igs or fbs may want

Fixes made in tks's copy of shared code that igs or fbs still lack. Their own sessions decide whether to port them.

| Date | tks PR | Defect | Upstream location |
|---|---|---|---|
