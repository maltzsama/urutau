# syntax=docker/dockerfile:1

# Build stage: three static Go binaries (no CGO). The Go image is pinned to
# the same patch the go.mod directive requires — a floating tag breaks the
# build whenever the toolchain patch lags the go directive.
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
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w \
      -X github.com/maltzsama/urutau/internal/version.Version=${VERSION} \
      -X github.com/maltzsama/urutau/internal/version.Commit=${COMMIT}" \
      -o /out/urutau ./cmd/urutau && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w \
      -X github.com/maltzsama/urutau/internal/version.Version=${VERSION} \
      -X github.com/maltzsama/urutau/internal/version.Commit=${COMMIT}" \
      -o /out/urutau-coordinator ./cmd/coordinator && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w \
      -X github.com/maltzsama/urutau/internal/version.Version=${VERSION} \
      -X github.com/maltzsama/urutau/internal/version.Commit=${COMMIT}" \
      -o /out/urutau-worker ./cmd/worker

# Final image: static distroless, no shell, non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot
ENTRYPOINT ["urutau"]
