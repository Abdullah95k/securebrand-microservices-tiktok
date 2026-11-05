SHELL := /bin/bash
GO ?= go
IMAGE ?= tks:dev
BROWSER_IMAGE ?= $(IMAGE)-browser
PG_DSN ?= postgres://tks:tks@localhost:5432/tks?sslmode=disable
TEST_ENV = TKS_TEST_PG_DSN="$(PG_DSN)" TKS_TEST_REDIS_ADDR=localhost:6379 TKS_TEST_KAFKA_BROKERS=localhost:19092 TKS_TEST_SCHEMA_REGISTRY=http://localhost:18081
COMPOSE = docker compose -f deploy/compose/docker-compose.yml

.PHONY: all build test lint vet generate golden fixtures infra-up infra-down test-integration e2e loadtest soak docker docker-prebuilt compose-up compose-down spike-report phase0 phase0-dryrun phase0-trim phase0-rate spike-bin tks-probe tks-probe-all tks-probe-dryrun tks-browser-check tks-phase0 tks-phase0-dryrun tks-signers tks-signers-dryrun tks-signers-browsers tks-discover tks-discover-dryrun tks-spike-bin chart-check chart-smoke clean

all: generate build test

build:
	$(GO) build ./...

## Unit tests (no infrastructure). Integration tests skip themselves.
test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

lint: vet
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; ran go vet only"
	@command -v buf >/dev/null && buf lint || echo "buf not installed; skipped proto lint"

## Regenerate protobuf code (requires buf + protoc-gen-go).
generate:
	buf generate

## Regenerate parser golden files after an intended parser change.
golden:
	$(GO) test ./internal/fb/parse -run TestGolden -update

## Regenerate the synthetic fakefb fixtures.
fixtures:
	$(GO) test -tags fixturegen ./internal/fakefb -run TestWriteFixtures

## Start Postgres, Redpanda and Valkey for integration tests.
infra-up:
	$(COMPOSE) up -d postgres redpanda valkey
	@until docker exec tks-postgres-1 pg_isready -U tks >/dev/null 2>&1; do sleep 1; done

infra-down:
	$(COMPOSE) down -v

## Integration tests against real Postgres/Redpanda/Valkey (make infra-up first).
test-integration:
	$(TEST_ENV) $(GO) test -count=1 -timeout 25m ./...

## Load tests against fakefb (opt-in; prints a report). Size with
## TKS_LOADTEST_PAGES, _POSTS, _COMMENTS, _ROWS, _LATENCY; TKS_LOADTEST_OUT=file.
loadtest:
	$(TEST_ENV) TKS_LOADTEST=1 $(GO) test -count=1 -v -timeout 45m -run 'TestLoad' ./internal/e2e/

## Soak test (opt-in): scheduled crawls of a changing fakefb world with
## component restarts. TKS_SOAK_DURATION (20m), TKS_SOAK_PAGES (6),
## TKS_SOAK_RESTART_EVERY (90s, 0 = none), TKS_SOAK_OUT=file for the report.
soak:
	$(TEST_ENV) TKS_SOAK=1 $(GO) test -count=1 -v -timeout 6h -run 'TestSoak' ./internal/e2e/

## Full pipeline end-to-end test only.
e2e:
	$(TEST_ENV) $(GO) test -count=1 -v ./internal/e2e/... ./internal/tt/e2e/...

## The default image (static binaries) and the crawler's, with Chromium for
## the browser signer (--target browser).
docker:
	docker build -f deploy/docker/Dockerfile -t $(IMAGE) .
	docker build -f deploy/docker/Dockerfile --target browser -t $(BROWSER_IMAGE) .

## Build binaries on the host and package them (no Go builder image needed).
docker-prebuilt:
	CGO_ENABLED=0 GOOS=linux $(GO) build -trimpath -ldflags="-s -w" -o bin/ ./cmd/...
	docker build -f deploy/docker/Dockerfile.prebuilt -t $(IMAGE) .

HELM ?= helm
KUBECONFORM ?= kubeconform
KUBECONFORM_ARGS = -strict -summary -kubernetes-version 1.30.0 -schema-location default \
	-schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

## Lint the Helm chart and validate its renders (defaults, every optional
## object, the staging example) against the Kubernetes and CRD schemas.
chart-check:
	$(HELM) lint deploy/helm/tks
	$(HELM) lint deploy/helm/tks -f deploy/helm/tks/examples/fakett.yaml
	set -o pipefail; $(HELM) template tks deploy/helm/tks | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	set -o pipefail; $(HELM) template tks deploy/helm/tks --set keda.authSecret=tks-keda-kafka,tlsFiles.existingSecret=tks-tls,alerts.enabled=true,dashboards.enabled=true,serviceMonitor.enabled=true,api.ingress.enabled=true,networkPolicy.enabled=true,hooks.schemas=true,image.browserTag=browser \
	  | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	set -o pipefail; $(HELM) template tks deploy/helm/tks -f deploy/helm/tks/examples/fakett.yaml | $(KUBECONFORM) $(KUBECONFORM_ARGS)

## Run the chart's hooks and Deployments in Docker exactly as rendered, then
## real backfills against fakett, comments and replies signed by the
## crawlers' Chromium pages (make infra-up and make docker first).
chart-smoke:
	python3 scripts/chart-smoke.py --image $(IMAGE) --browser-image $(BROWSER_IMAGE) --helm $(HELM)

## The local stack against fakett (deploy/compose/docker-compose.yml).
compose-up: docker
	TKS_IMAGE=$(IMAGE) TKS_BROWSER_IMAGE=$(BROWSER_IMAGE) $(COMPOSE) up -d

compose-down:
	$(COMPOSE) down -v

spike-report:
	$(GO) run ./cmd/fbs-spike report --in spike-results.jsonl

PHASE0_CONFIG ?= phase0/proxies.yaml
PHASE0_PAGES ?= phase0/pages.csv
PHASE0_TIERS ?= isp,residential
PHASE0_OUT ?= phase0/out
PHASE0_ARGS ?=

## Phase 0 against real Facebook (docs/runbooks/phase0-spike.md). Fill in
## phase0/proxies.yaml and phase0/pages.csv first. Resume with PHASE0_ARGS="--from run".
phase0:
	@test -f $(PHASE0_CONFIG) || { echo "missing $(PHASE0_CONFIG): cp phase0/proxies.example.yaml $(PHASE0_CONFIG), fill it in, chmod 600"; exit 2; }
	@test -f $(PHASE0_PAGES) || { echo "missing $(PHASE0_PAGES): cp phase0/pages.example.csv $(PHASE0_PAGES) and fill it in"; exit 2; }
	$(GO) run ./cmd/fbs-spike phase0 --config $(PHASE0_CONFIG) --pages $(PHASE0_PAGES) --tiers $(PHASE0_TIERS) --out-dir $(PHASE0_OUT) $(PHASE0_ARGS)

TRIM_TIER ?= isp
TRIM_ARGS ?=
## Payload experiment: what each change to the timeline and comments queries'
## variables does to response size and parsing, on the first 3 pages
## (docs/runbooks/phase0-spike.md, "Cost experiments"). TRIM_ARGS="--each" also
## measures every provider flag alone.
phase0-trim:
	$(GO) run ./cmd/fbs-spike trim --config $(PHASE0_CONFIG) --pages $(PHASE0_PAGES) --registry $(PHASE0_OUT)/registry.yaml \
	  --tier $(TRIM_TIER) --out $(PHASE0_OUT)/trim.jsonl $(TRIM_ARGS)

RATE_TIER ?= isp
RATES ?= 0.5 1.0
RATE_PAGES ?= 30
RATE_ARGS ?= --orderings comments --timeline-pages 10
## Rate experiment: the same RATE_PAGES pages at each per-identity rate in
## RATES (requests per second per IP), one result file and report each.
phase0-rate:
	@mkdir -p $(PHASE0_OUT)
	@grep -v '^#' $(PHASE0_PAGES) | grep -v '^[[:space:]]*$$' \
	  | awk -F, -v n=$(RATE_PAGES) 'NR == 1 && tolower($$1) == "page" { print; next } c < n { print; c++ }' > $(PHASE0_OUT)/rate-pages.csv
	@for r in $(RATES); do \
	  echo "== $(RATE_TIER) at $$r requests/s per IP"; \
	  $(GO) run ./cmd/fbs-spike run --config $(PHASE0_CONFIG) --pages $(PHASE0_OUT)/rate-pages.csv \
	    --registry $(PHASE0_OUT)/registry.yaml --tier $(RATE_TIER) --rps $$r --out $(PHASE0_OUT)/rate-$$r.jsonl $(RATE_ARGS) || exit 1; \
	  $(GO) run ./cmd/fbs-spike report --in $(PHASE0_OUT)/rate-$$r.jsonl > $(PHASE0_OUT)/rate-$$r.txt; \
	  grep -E 'block rate|bootstrap|VERDICT|requests' $(PHASE0_OUT)/rate-$$r.txt | head -n 8; \
	done

## Rehearse Phase 0 against a local fakefb (no proxies, no Facebook, about a minute).
phase0-dryrun:
	GO=$(GO) phase0/dryrun.sh

TKS_PROBE_ACCOUNTS ?= phase0/tt-accounts.txt
TKS_PROBE_ACCOUNTS_ALL ?= phase0/tt-accounts-all.txt
TKS_PROBE_TIERS ?= isp
TKS_PROBE_OUT ?= phase0/out-tt
TKS_PROBE_ARGS ?=
# Accounts the full pass reads at once, each through its own exit IP (the
# owner chose 10 on 2026-11-01; capped at the tier's IP count).
TKS_PROBE_PARALLEL ?= 10

## tks probe against real TikTok, test pass (checkpoint C2, docs/runbooks/tks-probe.md):
## 3 to 5 accounts, one run per tier in TKS_PROBE_TIERS (the owner's ISP tiers;
## "direct" = this machine's own IP), with a video sample, the browser signer
## and the embeds. Each writes $(TKS_PROBE_OUT)/<tier>/ and <tier>.tar.gz.
tks-probe:
	@test -f $(TKS_PROBE_ACCOUNTS) || { echo "missing $(TKS_PROBE_ACCOUNTS): copy your accounts file there (docs/runbooks/tks-probe.md)"; exit 2; }
	@for t in $$(echo $(TKS_PROBE_TIERS) | tr , ' '); do \
	  if [ $$t != direct ] && [ ! -f $(PHASE0_CONFIG) ]; then echo "missing $(PHASE0_CONFIG) for tier $$t (docs/runbooks/tks-probe.md)"; exit 2; fi; \
	  $(GO) run ./cmd/tks-spike probe --accounts $(TKS_PROBE_ACCOUNTS) $(if $(wildcard $(PHASE0_CONFIG)),--config $(PHASE0_CONFIG)) \
	    --tier $$t --out-dir $(TKS_PROBE_OUT)/$$t --max-list-pages 60 --videos 1 --signer browser --oembed $(TKS_PROBE_ARGS) || exit 1; \
	done

## tks probe, full pass (checkpoint C3): the whole list, back a year, TKS_PROBE_PARALLEL
## accounts at a time, no covers (they work, and would make the bundle hundreds of MB), no videos.
tks-probe-all:
	@test -f $(TKS_PROBE_ACCOUNTS_ALL) || { echo "missing $(TKS_PROBE_ACCOUNTS_ALL): copy your full accounts file there (docs/runbooks/tks-probe.md)"; exit 2; }
	@for t in $$(echo $(TKS_PROBE_TIERS) | tr , ' '); do \
	  $(GO) run ./cmd/tks-spike probe --accounts $(TKS_PROBE_ACCOUNTS_ALL) $(if $(wildcard $(PHASE0_CONFIG)),--config $(PHASE0_CONFIG)) \
	    --tier $$t --out-dir $(TKS_PROBE_OUT)-all/$$t --max-age-days 365 --max-list-pages 100 --covers 0 --videos 0 --count-checks 0 --depth-checks 100000 --depth-days 365 --parallel $(TKS_PROBE_PARALLEL) $(TKS_PROBE_ARGS) || exit 1; \
	done

## Rehearse the probe against a built-in fake TikTok: no traffic, no proxies.
tks-probe-dryrun:
	$(GO) run ./cmd/tks-spike probe --dry-run --out-dir $(TKS_PROBE_OUT)-dryrun

TKS_PHASE0_ACCOUNTS ?= phase0/tt-accounts-all.txt
TKS_PHASE0_TIERS ?= isp
TKS_PHASE0_OUT ?= phase0/out-tt-phase0
TKS_PHASE0_ARGS ?=
TKS_PHASE0_MEM ?= 3G
TKS_PHASE0_CPU ?= 200%
# Phase 0 runs in a systemd scope named tks-phase0 where systemd runs the
# machine: past TKS_PHASE0_MEM the kernel stops the run, not the machine (the
# pilot server froze in C4's second attempt). systemctl stop tks-phase0.scope
# ends it and every browser in it.
TKS_PHASE0_CAP = systemd-run --scope --quiet --unit=tks-phase0 -p MemoryMax=$(TKS_PHASE0_MEM) -p MemorySwapMax=0 -p CPUQuota=$(TKS_PHASE0_CPU)

## tks Phase 0 against real TikTok (checkpoint C4, docs/runbooks/tks-phase0.md),
## capped at TKS_PHASE0_MEM of memory and TKS_PHASE0_CPU of CPU where systemd runs the machine:
## 150 accounts spread over TKS_PHASE0_ACCOUNTS, the crawler's work at its pace
## through TKS_PHASE0_TIERS, then a per-IP rate ladder; report.txt says GO or
## NO-GO with the cost per account per day. Resume with TKS_PHASE0_ARGS="--from run";
## price it with TKS_PHASE0_ARGS="--from report --ip-month-usd 3".
tks-phase0:
	@test -f $(TKS_PHASE0_ACCOUNTS) || { echo "missing $(TKS_PHASE0_ACCOUNTS): copy your accounts file there (docs/runbooks/tks-phase0.md)"; exit 2; }
	@test -f $(PHASE0_CONFIG) || { echo "missing $(PHASE0_CONFIG) (docs/runbooks/tks-phase0.md)"; exit 2; }
	$(GO) build -o bin/tks-spike ./cmd/tks-spike
	$(if $(wildcard /run/systemd/system),$(TKS_PHASE0_CAP)) ./bin/tks-spike phase0 --accounts $(TKS_PHASE0_ACCOUNTS) --config $(PHASE0_CONFIG) \
	  --tiers $(TKS_PHASE0_TIERS) --out-dir $(TKS_PHASE0_OUT) $(TKS_PHASE0_ARGS)

## Rehearse Phase 0 against a built-in fake TikTok whose variants are rotated
## (the creator list needs a signature): no traffic, no proxies, under a minute.
tks-phase0-dryrun:
	$(GO) run ./cmd/tks-spike phase0 --dry-run $(TKS_PHASE0_ARGS)

TKS_SIGNERS_ACCOUNTS ?= phase0/tt-accounts-all.txt
TKS_SIGNERS_TIER ?= isp
TKS_SIGNERS_OUT ?= phase0/out-tt-signers
TKS_SIGNERS_MEM ?= 2G
TKS_SIGNERS_CPU ?= 150%
TKS_SIGNERS_ARGS ?=
# The cap: a systemd scope named tks-signers (systemctl stop tks-signers.scope
# ends it and every browser in it).
TKS_SIGNERS_CAP = systemd-run --scope --quiet --unit=tks-signers -p MemoryMax=$(TKS_SIGNERS_MEM) -p MemorySwapMax=0 -p CPUQuota=$(TKS_SIGNERS_CPU)

## Compare signer browsers against real TikTok (docs/runbooks/tks-signers.md):
## Lightpanda, chrome-headless-shell and Chrome, each as itself, a few accounts'
## comments each. The run is capped by systemd at TKS_SIGNERS_MEM of memory and
## TKS_SIGNERS_CPU of CPU (the kernel stops it there, not the machine), and its
## own guard stops a browser at 1200 MB or when the machine has under 1500 MB.
tks-signers:
	@test -f $(TKS_SIGNERS_ACCOUNTS) || { echo "missing $(TKS_SIGNERS_ACCOUNTS) (docs/runbooks/tks-signers.md)"; exit 2; }
	@test -f $(PHASE0_CONFIG) || { echo "missing $(PHASE0_CONFIG) (docs/runbooks/tks-signers.md)"; exit 2; }
	@test -d /run/systemd/system || { echo "no systemd here: the run must be capped (docs/runbooks/tks-signers.md)"; exit 2; }
	$(GO) build -o bin/tks-spike ./cmd/tks-spike
	$(TKS_SIGNERS_CAP) ./bin/tks-spike signers --accounts $(TKS_SIGNERS_ACCOUNTS) --config $(PHASE0_CONFIG) \
	  --tier $(TKS_SIGNERS_TIER) --out-dir $(TKS_SIGNERS_OUT) $(TKS_SIGNERS_ARGS)

## Rehearse the comparison against a built-in fake TikTok: no traffic, no proxies,
## under the same cap where systemd runs the machine.
tks-signers-dryrun:
	$(GO) build -o bin/tks-spike ./cmd/tks-spike
	$(if $(wildcard /run/systemd/system),$(TKS_SIGNERS_CAP)) ./bin/tks-spike signers --dry-run $(TKS_SIGNERS_ARGS)

## Download Lightpanda and chrome-headless-shell into ~/tks-browsers (nothing system-wide).
tks-signers-browsers:
	bash scripts/tks-signers-browsers.sh

TKS_DISCOVER_ACCOUNTS ?= phase0/tt-accounts-all.txt
TKS_DISCOVER_TIER ?= isp
TKS_DISCOVER_OUT ?= phase0/out-tt-discover
# The full pass's posts (C3): those no longer listed are read (T34).
TKS_DISCOVER_KNOWN ?= phase0/out-tt-all/isp/posts.csv
# Optional: links to posts you know were deleted or made private, and short links.
TKS_DISCOVER_GONE ?= phase0/tt-gone-posts.txt
TKS_DISCOVER_SHORT ?= phase0/tt-short-links.txt
TKS_DISCOVER_MEM ?= 3G
TKS_DISCOVER_CPU ?= 200%
TKS_DISCOVER_ARGS ?=
# The cap: a systemd scope named tks-discover (systemctl stop tks-discover.scope
# ends it and every browser in it).
TKS_DISCOVER_CAP = systemd-run --scope --quiet --unit=tks-discover -p MemoryMax=$(TKS_DISCOVER_MEM) -p MemorySwapMax=0 -p CPUQuota=$(TKS_DISCOVER_CPU)

## The discovery probe against real TikTok (checkpoint C6, docs/runbooks/tks-discovery-probe.md),
## about an hour, capped at TKS_DISCOVER_MEM of memory and TKS_DISCOVER_CPU of CPU where systemd
## runs the machine: search (posts and accounts), hashtags, sounds, the For You feed, related
## posts, following and followers lists, reposts, posts no longer listed (T34), short links, and
## the pages' own calls in a browser. Writes $(TKS_DISCOVER_OUT)/ and its bundle.
tks-discover:
	@test -f $(TKS_DISCOVER_ACCOUNTS) || { echo "missing $(TKS_DISCOVER_ACCOUNTS): copy your accounts file there (docs/runbooks/tks-discovery-probe.md)"; exit 2; }
	@test -f $(PHASE0_CONFIG) || { echo "missing $(PHASE0_CONFIG) (docs/runbooks/tks-discovery-probe.md)"; exit 2; }
	$(GO) build -o bin/tks-spike ./cmd/tks-spike
	$(if $(wildcard /run/systemd/system),$(TKS_DISCOVER_CAP)) ./bin/tks-spike discover --accounts $(TKS_DISCOVER_ACCOUNTS) --config $(PHASE0_CONFIG) \
	  --tier $(TKS_DISCOVER_TIER) --out-dir $(TKS_DISCOVER_OUT) $(if $(wildcard $(TKS_DISCOVER_KNOWN)),--known-posts $(TKS_DISCOVER_KNOWN)) \
	  $(if $(wildcard $(TKS_DISCOVER_GONE)),--gone-posts $(TKS_DISCOVER_GONE)) $(if $(wildcard $(TKS_DISCOVER_SHORT)),--short-links $(TKS_DISCOVER_SHORT)) $(TKS_DISCOVER_ARGS)

## Rehearse the discovery probe against a built-in fake TikTok: no traffic, no proxies, about a minute.
tks-discover-dryrun:
	$(GO) run ./cmd/tks-spike discover --dry-run $(TKS_DISCOVER_ARGS)

## Is a Chromium installed for the probe's browser signer? Prints how to install one if not.
tks-browser-check:
	$(GO) run ./cmd/tks-spike browser-check

## tks probe: static binaries the owner can run without a Go toolchain.
tks-spike-bin:
	@for t in linux/amd64 linux/arm64 darwin/arm64 darwin/amd64; do \
	  os=$${t%/*}; arch=$${t#*/}; \
	  echo "bin/tks-spike-$$os-$$arch"; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build -trimpath \
	    -ldflags="-s -w -X main.version=$$(git describe --always --dirty 2>/dev/null || echo dev)" \
	    -o bin/tks-spike-$$os-$$arch ./cmd/tks-spike || exit 1; \
	done

## Static fbs-spike binaries in bin/, to run Phase 0 on a machine without Go or Docker.
spike-bin:
	@for t in linux/amd64 linux/arm64 darwin/arm64 darwin/amd64; do \
	  os=$${t%/*}; arch=$${t#*/}; \
	  echo "bin/fbs-spike-$$os-$$arch"; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build -trimpath \
	    -ldflags="-s -w -X main.version=$$(git describe --always --dirty 2>/dev/null || echo dev)" \
	    -o bin/fbs-spike-$$os-$$arch ./cmd/fbs-spike || exit 1; \
	done

clean:
	rm -rf bin/
