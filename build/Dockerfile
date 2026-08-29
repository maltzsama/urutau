# syntax=docker/dockerfile:1

# Build stage: three static Go binaries (no CGO).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/urutau ./cmd/urutau && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/urutau-coordinator ./cmd/coordinator && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/urutau-worker ./cmd/worker

# Final image: static distroless, no shell, non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot
ENTRYPOINT ["urutau"]
