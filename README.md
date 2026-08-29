# Urutau

A CDC engine written in Go with Python authoring: replicates MySQL/Postgres
into Iceberg **reflecting state** (upsert by PK, first-class UPDATE/DELETE),
where a single replication connection per source feeds N workers writing in
parallel — no Kafka in the data path, recoverable from total catastrophe.

> This repository holds the **Go engine** (coordinator, workers, CLI,
> operator). The Python SDK/planner lives in its own repository.

## Language

All code, comments, and documentation in this repository are **English**.

## Map

| Path | Role |
| --- | --- |
| `cmd/urutau` | CLI (`plan`, `run --local`, `status`, `resume`, …) |
| `cmd/coordinator` | coordinator binary (reader, router, supervisor) |
| `cmd/worker` | worker binary (Iceberg writer) |
| `internal/spec` | resolvedSpec + single server-side validation |
| `internal/position` | position contract (GTID/LSN, `Compare`/`Contains`) |
| `internal/source` | sources: MySQL (`go-mysql`), Postgres (`pgx`) |
| `internal/coordinator` | reader/router loops, flight budget, supervisor |
| `internal/worker` | batcher, per-PK collapse, serial committer |
| `internal/sink` | Iceberg writes (upsert/equality delete) |
| `internal/transport` | gRPC control + Arrow Flight; generated in `internal/transport/pb` |
| `internal/eventlog` | per-run-id JSONL in S3 |
| `api/v1alpha1` | CDCPipeline CR types (pending) |
| `proto/` | coordinator↔worker wire contract |

## Development

```sh
make bootstrap   # buf + golangci-lint pinned into ./bin
make build       # bin/urutau, bin/urutau-coordinator, bin/urutau-worker
make test        # go test -race ./...
make lint        # golangci-lint
make proto       # buf lint + generate (generated code is committed)
```

Go ≥ 1.26. No `protoc` needed — generation uses `buf` with the
`protoc-gen-go`/`protoc-gen-go-grpc` plugins pinned as `go tool`.

## Status

Skeleton. Next step: the `iceberg-go` spike: append +
equality delete + read back in Trino and watch the old version actually
disappear.
