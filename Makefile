SHELL := /bin/sh

CONTAINER_TOOL ?= docker
IMG ?= s3-lease-e2e:dev
PLATFORMS ?= linux/amd64,linux/arm64
E2E_COMPOSE_FILE ?= test/e2e/compose.yaml
E2E_PROJECT ?= s3-lease-e2e
S3_LEASE_E2E_PORT ?= 8333
S3_LEASE_E2E_ENDPOINT ?= http://127.0.0.1:$(S3_LEASE_E2E_PORT)

.DEFAULT_GOAL := help

.PHONY: help all fmt fmt-check vet build test test-race test-aws e2e-up e2e-down e2e e2e-race docker-build docker-buildx

help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

all: fmt vet build test ## Format, vet, build, and run unit tests.

fmt: ## Format Go source files.
	gofmt -w $$(find . -type f -name '*.go' -not -path './vendor/*')

fmt-check: ## Verify that Go source files are formatted.
	@files="$$(gofmt -l $$(find . -type f -name '*.go' -not -path './vendor/*'))"; \
	if [ -n "$$files" ]; then echo "unformatted Go files:"; echo "$$files"; exit 1; fi

vet: ## Run standard static analysis.
	go vet ./...

build: ## Build all packages.
	go build ./...

test: ## Run non-E2E tests and compile checks.
	go test ./...

test-race: ## Run all non-E2E tests with the race detector.
	go test -race ./...

test-aws: ## Run the real-AWS compatibility gate (requires AWS environment variables).
	go test -tags=awss3 -count=1 -timeout=20m ./test/awss3/...

e2e-up: ## Start the disposable SeaweedFS fixture.
	$(CONTAINER_TOOL) compose -p $(E2E_PROJECT) -f $(E2E_COMPOSE_FILE) up -d --wait

e2e-down: ## Stop SeaweedFS and remove its disposable data.
	$(CONTAINER_TOOL) compose -p $(E2E_PROJECT) -f $(E2E_COMPOSE_FILE) down --volumes --remove-orphans

e2e: ## Run tagged tests against a disposable SeaweedFS fixture.
	@set -eu; \
	trap '$(MAKE) --no-print-directory e2e-down' EXIT INT TERM; \
	$(MAKE) --no-print-directory e2e-up; \
	$(MAKE) --no-print-directory docker-build; \
	CONTAINER_TOOL='$(CONTAINER_TOOL)' E2E_PROJECT='$(E2E_PROJECT)' \
		E2E_COMPOSE_FILE='$(abspath $(E2E_COMPOSE_FILE))' S3_LEASE_E2E_ENDPOINT='$(S3_LEASE_E2E_ENDPOINT)' \
		S3_LEASE_E2E_CANDIDATE_IMAGE='$(IMG)' go test -tags=e2e -count=1 -timeout=10m ./test/e2e/...

e2e-race: ## Run tagged host-side tests with the race detector.
	@set -eu; \
	trap '$(MAKE) --no-print-directory e2e-down' EXIT INT TERM; \
	$(MAKE) --no-print-directory e2e-up; \
	$(MAKE) --no-print-directory docker-build; \
	CONTAINER_TOOL='$(CONTAINER_TOOL)' E2E_PROJECT='$(E2E_PROJECT)' \
		E2E_COMPOSE_FILE='$(abspath $(E2E_COMPOSE_FILE))' S3_LEASE_E2E_ENDPOINT='$(S3_LEASE_E2E_ENDPOINT)' \
		S3_LEASE_E2E_CANDIDATE_IMAGE='$(IMG)' go test -race -tags=e2e -count=1 -timeout=15m ./test/e2e/...

docker-build: ## Build and load the current-platform E2E candidate image.
	$(CONTAINER_TOOL) buildx build --load -t $(IMG) .

docker-buildx: ## Push a multi-platform candidate image (IMG must name a registry).
	@case "$(IMG)" in *.*/*|*:*/*|localhost/*) ;; *) echo "IMG must be registry-qualified (for example registry.example.com/team/s3-lease-e2e:tag)" >&2; exit 2;; esac
	$(CONTAINER_TOOL) buildx build --platform $(PLATFORMS) --push -t $(IMG) .
