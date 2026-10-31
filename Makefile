SHELL := /bin/bash
GO ?= go
IMAGE ?= fbs:dev
PG_DSN ?= postgres://fbs:fbs@localhost:5432/fbs?sslmode=disable
TEST_ENV = IGS_TEST_PG_DSN="$(PG_DSN)" IGS_TEST_REDIS_ADDR=localhost:6379 IGS_TEST_KAFKA_BROKERS=localhost:19092 IGS_TEST_SCHEMA_REGISTRY=http://localhost:18081
COMPOSE = docker compose -f deploy/compose/docker-compose.yml

.PHONY: all build test lint vet generate golden fixtures infra-up infra-down test-integration e2e loadtest soak docker docker-prebuilt compose-up compose-down spike-report phase0 phase0-dryrun phase0-trim phase0-rate pilot-up pilot-register pilot-registry pilot-report pilot-down pilot-clean spike-bin chart-check chart-smoke clean

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
	@until docker exec fbs-postgres-1 pg_isready -U fbs >/dev/null 2>&1; do sleep 1; done

infra-down:
	$(COMPOSE) down -v

## Integration tests against real Postgres/Redpanda/Valkey (make infra-up first).
test-integration:
	$(TEST_ENV) $(GO) test -count=1 ./internal/persist/... ./internal/e2e/... ./internal/spike/... ./internal/registrysync/... ./internal/platform/schemareg/...

## Load tests against fakefb (opt-in; prints a report). Size with
## IGS_LOADTEST_PAGES, _POSTS, _COMMENTS, _ROWS, _LATENCY; IGS_LOADTEST_OUT=file.
loadtest:
	$(TEST_ENV) IGS_LOADTEST=1 $(GO) test -count=1 -v -timeout 45m -run 'TestLoad' ./internal/e2e/

## Soak test (opt-in): scheduled crawls of a changing fakefb world with
## component restarts. IGS_SOAK_DURATION (20m), IGS_SOAK_PAGES (6),
## IGS_SOAK_RESTART_EVERY (90s, 0 = none), IGS_SOAK_OUT=file for the report.
soak:
	$(TEST_ENV) IGS_SOAK=1 $(GO) test -count=1 -v -timeout 6h -run 'TestSoak' ./internal/e2e/

## Full pipeline end-to-end test only.
e2e:
	$(TEST_ENV) $(GO) test -count=1 -v ./internal/e2e/...

docker:
	docker build -f deploy/docker/Dockerfile -t $(IMAGE) .

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
	$(HELM) lint deploy/helm/fbs
	$(HELM) lint deploy/helm/fbs -f deploy/helm/fbs/examples/fakefb.yaml
	set -o pipefail; $(HELM) template fbs deploy/helm/fbs | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	set -o pipefail; $(HELM) template fbs deploy/helm/fbs --set keda.authSecret=fbs-keda-kafka,tlsFiles.existingSecret=fbs-tls,alerts.enabled=true,dashboards.enabled=true,serviceMonitor.enabled=true,api.ingress.enabled=true,networkPolicy.enabled=true,hooks.schemas=true \
	  | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	set -o pipefail; $(HELM) template fbs deploy/helm/fbs -f deploy/helm/fbs/examples/fakefb.yaml | $(KUBECONFORM) $(KUBECONFORM_ARGS)

## Run the chart's hooks and Deployments in Docker exactly as rendered, then
## real backfills against fakefb (make infra-up and make docker first).
chart-smoke:
	python3 scripts/chart-smoke.py --image $(IMAGE) --helm $(HELM)

compose-up:
	$(COMPOSE) up -d

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

PILOT_COMPOSE = $(COMPOSE) -f deploy/compose/pilot.yml
# deploy/compose/pilot.yml mounts these paths, whatever PHASE0_* say.
PILOT_PROXIES = phase0/proxies.yaml
PILOT_OUT = phase0/out
PILOT_SINCE ?= 24h
PILOT_PROJECT ?= 5000
PILOT_COST_PER_GB ?= 4
PILOT_FBCTL = $(PILOT_COMPOSE) run --rm --no-deps -T --entrypoint /usr/local/bin/fbctl api
## Pilot: the real pipeline against real Facebook on this machine, to measure
## the steady state (docs/runbooks/pilot.md). Needs Docker, phase0/proxies.yaml
## and the Phase 0 registry. Start with the Phase 0 run finished.
pilot-up:
	@test -f $(PILOT_PROXIES) || { echo "missing $(PILOT_PROXIES)"; exit 2; }
	@test -s $(PILOT_OUT)/registry.yaml || { echo "missing $(PILOT_OUT)/registry.yaml: finish Phase 0 first"; exit 2; }
	@command -v docker >/dev/null || { echo "Docker is not installed: docs/runbooks/pilot.md, step 1"; exit 2; }
	@printf 'registry_sync:\n  canary_pages: [%s]\n' \
	  "$$(grep -v '^#' $(PHASE0_PAGES) | grep -v '^[[:space:]]*$$' \
	    | awk -F, 'NR == 1 && tolower($$1) == "page" { next } c < 3 { print $$1; c++ }' | sed 's/.*/"&"/' | paste -sd, -)" \
	  > $(PILOT_OUT)/pilot-canary.yaml
	$(MAKE) docker
	$(PILOT_COMPOSE) up -d
	@echo "up; register the pages with: make pilot-register"

## Registers every page of phase0/pages.csv; each is backfilled (a week), then crawled every 3 hours.
pilot-register:
	@key=$$($(PILOT_FBCTL) apikey create --name pilot --scopes pages:write,jobs:write,data:read \
	  | sed -n 's/.*"api_key": *"\([^"]*\)".*/\1/p'); \
	test -n "$$key" || { echo "could not create an API key"; exit 1; }; \
	$(PILOT_COMPOSE) run --rm --no-deps -T -v $(CURDIR)/$(PHASE0_PAGES):/pages.csv:ro --entrypoint /usr/local/bin/fbctl api \
	  pages import --api http://api:8080 --key "$$key" /pages.csv

## Makes every query of phase0/out/registry.yaml active, over what registry-sync promoted.
pilot-registry:
	$(PILOT_COMPOSE) run --rm --no-deps -T -v $(CURDIR)/$(PILOT_OUT)/registry.yaml:/etc/fbs/registry.yaml:ro \
	  --entrypoint /usr/local/bin/fbctl api registry import --file /etc/fbs/registry.yaml

## Requests, bytes and blocks per page per day over the last PILOT_SINCE, projected to PILOT_PROJECT pages.
pilot-report:
	@$(PILOT_FBCTL) report load --since $(PILOT_SINCE) --pages $(PILOT_PROJECT) --cost-per-gb $(PILOT_COST_PER_GB)

## Stops the pilot and keeps its database; pilot-clean also deletes it.
pilot-down:
	$(PILOT_COMPOSE) down
pilot-clean:
	$(PILOT_COMPOSE) down -v

## Rehearse Phase 0 against a local fakefb (no proxies, no Facebook, about a minute).
phase0-dryrun:
	GO=$(GO) phase0/dryrun.sh

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
