# Columnar migration — QUARANTINE closing plan

The columnar data plane (PR #42) replaced the row-oriented pipeline with
`*dataplane.Batch` end to end, but the QUARANTINE bridges remain: the
sources and sinks still round-trip through the row layer. This document
lists every `QUARANTINE` point, groups them by milestone, and defines the
measurable closing criterion.

## The residual cost

Today the pipeline does MORE work than the old row path on every batch:
`rowchange → serialize IPC → deserialize → columnar → (process) → each sink
unpacks back to rowchange`. The CR-045/CR-069 gain materializes only when
the bridges die.

## Inventory — 21 QUARANTINE points

### Milestone A — sinks consume RecordBatch directly (close 7 points)

Sinks first: the output unpack happens THREE times (one per sink), the
input bridge once. Closing the sinks removes 3 conversions at once.

| File | Point |
|------|-------|
| `internal/sink/iceberg/writer.go` | `dataRecord`/`project` row projection (kept for tests) |
| `internal/sink/iceberg/writer.go` | remaining unpack paths |
| `internal/sink/clickhouse/writer.go` | `unpackBatch` (batch → rowchange) |
| `internal/sink/couchbase/writer.go` | `unpackBatch` + append fallback |
| `internal/plugin/sink.go` | `unpackBatch` (plugin sink) |
| `internal/runner/runner.go` | `AddWindowRows` row→batch bridge |

Iceberg and ClickHouse have no excuse — both are columnar natively
(Parquet / ClickHouse columnar insert). Couchbase is the structural
exception the CR-069 §4.1 already predicted: key-document, one JSON per
row. Its bridge is permanent by design.

### Milestone B — sources build Arrow directly (close 4 points)

| File | Point |
|------|-------|
| `internal/source/mysql/adapter.go` | `sourcepull.Puller` bridging |
| `internal/source/kafka/kafka.go` | `sourcepull.Puller` bridging |
| `internal/sourcepull/pull.go` | the whole bridge |
| `internal/plugin/source.go` (via Puller) | DoGet stream bridging |

The decoders (mysql canal, pgoutput, kafka) populate RecordBuilders
directly. This is the M4 "sources build Arrow" milestone.

### Milestone C — worker row-decode dies (close 11 points)

The worker's batcher decodes batches to rows for drift/enrich/window. These
become columnar operators.

| File | Point |
|------|-------|
| `internal/worker/worker.go` | 11 QUARANTINE points (row-decode, `bridge upsert`, `decodeToChanges`, snapshot partition, append rewrite) |
| `internal/enrich/batch.go` | `EnrichBatch` decode→join→re-encode |

### Coordinator (2 points)

| File | Point |
|------|-------|
| `internal/coordinator/coordinator.go` | `changesFromReader` (Next → rows for the per-change pump) |

The coordinator's distributed pump becomes batch-based.

## Criterion

The `QUARANTINE` count must fall with every commit that claims to close a
milestone:

```
git grep -c QUARANTINE -- '*.go' | awk -F: '{s+=$2} END {print s}'
```

- Today: 21
- After Milestone A: ≤ 14
- After Milestone B: ≤ 10
- After Milestone C: ≤ 3 (Couchbase's structural bridge + Iceberg test
  projection)

Stable between commits = milestone deferred, not worked.