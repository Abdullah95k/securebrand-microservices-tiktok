# syntax=docker/dockerfile:1.7
# One image for every tks binary; each Deployment picks its command.
FROM golang:1.27-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w -X main.version=${VERSION}" -o /out/ ./cmd/...

# `--target browser`: the same binaries with Chromium, for tks-signer, whose
# pages sign the comment and reply calls (build brief D11, M9d), for the
# probes, and for tt.browser's bootstrap fallback.
FROM debian:12-slim AS browser
RUN apt-get update \
 && apt-get install -y --no-install-recommends chromium ca-certificates fonts-liberation tini \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 65532 --create-home --home-dir /home/nonroot nonroot
COPY --from=build /out/ /usr/local/bin/
ENV TKS_BROWSER_BINARY=/usr/bin/chromium HOME=/tmp
USER 65532:65532
EXPOSE 8080 9090
# tini reaps Chromium's orphans under `docker run`; in Kubernetes the chart
# shares the pod's process namespace instead (its command bypasses this).
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/tks-signer"]

# `--target backup`: tksctl with PostgreSQL 18's pg_dump and pg_restore,
# for the backup CronJob (`tksctl backup run`; M16, runbook
# docs/runbooks/tks-backup.md). The client must be the server's major
# version or newer.
FROM postgres:18-alpine AS backup
COPY --from=build /out/tksctl /usr/local/bin/tksctl
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/tksctl"]
CMD ["backup", "run"]

# The default image: static binaries only.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/tks-api"]
