# Urutau — Go engine.
# Common usage: make all (lint + test + build).
# Tools live in ./bin (buf, golangci-lint); proto plugins are `go tool`
# pinned in go.mod — nothing is installed by hand.

GO ?= go
BIN := $(CURDIR)/bin

BUF_VERSION ?= v1.72.0
GOLANGCI_LINT_VERSION ?= v2.13.2

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X github.com/maltzsama/urutau/internal/version.Version=$(VERSION) \
	-X github.com/maltzsama/urutau/internal/version.Commit=$(COMMIT) \
	-X github.com/maltzsama/urutau/internal/version.Date=$(DATE)

.PHONY: all bootstrap build test lint proto tidy clean docker e2e-up e2e-down e2e-test

all: lint test build

bootstrap:
	GOBIN=$(BIN) $(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	GOBIN=$(BIN) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/urutau ./cmd/urutau
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/urutau-coordinator ./cmd/coordinator
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/urutau-worker ./cmd/worker

test:
	$(GO) test -race ./...

lint:
	$(BIN)/golangci-lint run

proto:
	$(BIN)/buf lint
	$(BIN)/buf generate

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin dist

docker:
	docker build -f build/Dockerfile -t urutau:dev .

E2E_COMPOSE := test/e2e/docker-compose.yml

e2e-up:
	docker compose -f $(E2E_COMPOSE) up -d --wait

e2e-down:
	docker compose -f $(E2E_COMPOSE) down

e2e-test: e2e-up
	URUTAU_E2E=1 $(GO) test -count=1 -v ./test/e2e
