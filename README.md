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
| `internal/change` | row change event, per-key collapse, batch |
| `internal/position` | position contract (GTID/LSN, `Compare`/`Contains`) |
| `internal/source` | sources: MySQL (`go-mysql`), Postgres (`pgx`) |
| `internal/coordinator` | reader/router loops, flight budget, supervisor |
| `internal/worker` | per-table batcher + serialized committer |
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

## E2E spike

The spike proves the Iceberg write path by reading it back with
Trino (trusting the read, not the commit return). Stack: RustFS (S3) +
Polaris (REST catalog) + Trino (Iceberg REST catalog).

```sh
make e2e-test   # compose up --wait, then URUTAU_E2E=1 go test ./test/e2e
make e2e-down   # tear the stack down
```

It exercises append, equality delete, the `cdc.position` snapshot/table
properties, the collapsed worker end to end (inline YAML → batcher →
collapse → delete-then-append commits), and the full MySQL pipeline:
binlog → DBLog snapshot → stream → Iceberg, with resume after downtime.
All verified through Trino. Key finding: in `iceberg-go` v0.6.0 an append
and an equality delete staged in one transaction produce **two** snapshots
— the delete gets the higher sequence number and applies to the freshly
appended file too. A correct upsert is therefore delete-then-append
(separate commits), never append-then-delete in a single commit.

## Status

The MySQL source is in: one binlog reader per source (canal), DBLog
snapshot with the caught-up proof (never a timer), per-key collapse and
serialized commits in the worker, and resume from the cumulative GTID set
committed atomically with the data. Every run can also record a JSONL
audit trail in S3 (`internal/eventlog`, `run --eventlog s3://bucket/prefix`):
lifecycle and commit events, one object per run. Next: Postgres source, or
the multi-worker split.
