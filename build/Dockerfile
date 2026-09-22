# syntax=docker/dockerfile:1

# Build stage: five static Go binaries (no CGO) — the collapsed CLI plus the
# coordinator, worker, operator and history server. One image carries all
# five; only the container command differs per Pod (the operator clones the
# image for the coordinator StatefulSet and every worker Deployment). The Go
# image is pinned to the same patch the go.mod directive requires — a floating
# tag breaks the build whenever the toolchain patch lags the go directive.
ARG GO_VERSION=1.26.5
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
# Version metadata for urutau version (defaults keep ad-hoc builds honest).
ARG VERSION=dev
ARG COMMIT=unknown
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    set -e; \
    ldflags="-s -w \
      -X github.com/maltzsama/urutau/internal/version.Version=${VERSION} \
      -X github.com/maltzsama/urutau/internal/version.Commit=${COMMIT}"; \
    for pair in urutau:urutau urutau-coordinator:coordinator urutau-worker:worker urutau-operator:operator urutau-history-server:history-server; do \
      name="${pair%%:*}"; dir="${pair##*:}"; \
      CGO_ENABLED=0 go build -trimpath -ldflags="$ldflags" -o "/out/$name" "./cmd/$dir"; \
    done

# Final image: static distroless, no shell, non-root user. The default
# entrypoint is the collapsed CLI; the operator/coordinator/worker Pods
# override `command` with their own binary.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot
ENTRYPOINT ["urutau"]
