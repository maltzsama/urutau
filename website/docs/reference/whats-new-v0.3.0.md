---
sidebar_position: 9
---

# What's new in v0.3.0

Released 2026-09-21. [Full changelog](https://github.com/maltzsama/urutau/compare/v0.2.0...v0.3.0).

## Postgres CDC (full production readiness)

The Postgres source received a complete rewrite. Snapshot and CDC are now
two well-separated phases with independent retry, projection, and
filtering — no more shared code paths that conflated the two.

**Snapshot:**

- **CTID chunking** with `workers > 1` — range-partitions the table by
  CTID so multiple workers scan in parallel, each assigned a disjoint
  range.
- **Concurrent row scan** with transient retry — rows are fetched in
  parallel inside each chunk, and transient errors (statement canceled,
  serialization failure) are retried per-row instead of restarting the
  whole chunk.
- **Column projection and structured filter** — only the columns the
  pipeline needs are read, and the optional `filter` clause is pushed
  down into the `SELECT`.

**CDC:**

- **Incremental mode** — the worker
  resumes from the last committed position via `pg_replication_origin`,
  without re-running a full snapshot.
- **Slot validation** — on startup the worker checks that the replication
  slot exists and is not invalidated, and fails loud if it is.
- **`commit_ts` capture** — each row carries the source transaction
  timestamp, giving you a wall-clock timestamp for downstream ordering
  and audit.

**Connectivity:**

- **Nested connection config** — `snapshotUri` / `cdcUri` now accept the
  same structured form as the main `uri`, with independent TLS, SSH,
  `maxThreads`, and `retryCount`.
- **SSH in distributed mode** — workers
  dial through the SSH bastion, not just the coordinator.
- **Schema discovery** — column types
  and nullability are read from `information_schema` at boot, removing
  the need for hand-written schema hints.

See [Sources: Postgres](../sources/postgres.md) for the full reference.

## MySQL hardening

- **Server preflight** — on startup the
  source checks that `log_bin` is on, `binlog_format` is `ROW`, and
  `binlog_row_image` is `FULL`, failing loud if any precondition is
  missing.
- **Purged-binlog guard** — if the
  requested GTID position has already been purged, the worker surfaces a
  clear error instead of hanging forever.
- **TLS** — both the replication (binlog) and query connections support
  TLS now, via the standard `tls=true` query parameter.
- **`commit_ts` capture** — like Postgres, each row now carries the
  source transaction timestamp.
- **Snapshot/CDC filter + projection** — `filter` and `columns` are
  pushed down into the snapshot `SELECT` and the CDC row image, reducing
  I/O on the source.

See [Sources: MySQL](../sources/mysql.md) for the full reference.

## Iceberg write correctness

A concentrated bug-hunt on the Iceberg sink fixed ~20 correctness,
hygiene, and idempotency issues — the most impactful:

- **Schema evolution** — add-column and safe type promotions are now
  opt-in via `schemaEvolution: true` per table.
- **Sort order on primary key** — tables are created with a sort order
  matching the primary key, improving downstream read performance.
- **Append-mode deletes** — `__op = NULL` is now treated as an insert
  (not silently dropped), and delete rows with before-images are written
  correctly.
- **Empty batch safety** — an empty batch no longer advances the
  position or persists a stale snapshot state.
- **Config resolution** — multi-level namespaces, empty credentials, and
  silent fallbacks all behave correctly now.
- **Staged commit idempotency** — a retry of the same staged cycle no
  longer creates duplicate data files.
- **Position walk-back** — on resume the coordinator walks the branch
  ancestry, not the flat snapshot list, so it finds the committed
  position even after compaction prunes old snapshots.

See [Sinks: Iceberg](../reference/sinks.md#iceberg) for the full reference.

## Kafka field projection

The Kafka source now lets you pick which payload fields land as columns
— in raw, JSON, and Avro modes.
Previously every field in the payload was mapped; now you can narrow the
projection at the source level, reducing downstream schema width.

See [Sources: Kafka](../sources/kafka.md) for the full reference.

## Operator: Server-Side Apply and serverId uniqueness

- **Server-Side Apply** — the operator applies every managed
  object (StatefulSet, Service, ConfigMap, RBAC) under a named field
  manager (`--field-manager`, default `urutau-operator`). Fields it does
  not own — mutating-webhook defaults, API-server-assigned `clusterIP` —
  are left untouched, and a reconcile that changes nothing writes nothing.
- **`serverId` uniqueness** — the
  admission webhook rejects a pipeline whose `source.serverId` duplicates
  an existing pipeline's on the same MySQL source within a namespace.
- **Un-brick on spec change** — updating a
  `CdPipeline` spec now correctly reconciles the coordinator, instead of
  stalling.
- **Secret key validation** — the
  operator verifies that referenced secret keys exist before
  provisioning the coordinator.
- **RBAC** — status
  subresource and finalizer RBAC rules are split, and the GC window is
  documented.

See [Deploy on Kubernetes](../guides/deploy-kubernetes.md) and
[Architecture: Operator](../architecture/operator.md) for details.

## Maintenance improvements

- **Completion-time throttle** — the
  `interval` on every maintenance operation is now measured from the
  **previous run's completion**, not from a fixed start-time schedule.
  A run that takes longer than its interval does not immediately re-fire.
- **Per-operation isolation** — a failing
  operation (e.g. compaction) no longer starves the others; each runs on
  its own timer.
- **`CheckInterval` gate** — the shared
  `CheckInterval` is ignored when `enabled: false`, avoiding confusing
  log noise.

See [Table maintenance](../guides/table-maintenance.md) for details.

## Worker: onDelete and schema drift

Workers now propagate `onDelete` events and report schema drift over the
gRPC wire. Previously,
delete events were silently dropped when the sink did not support them,
and schema drift was only detected locally.

## Coordinator hardening

A concentrated bug-hunt fixed deadlocks, data races, panics, and leaks:

- **Budget deadlock** — `flowBudget.acquire`
  deadlocked on an oversized first batch; fixed with serialized
  admission.
- **Ack leak** — an unparsable ack
  position leaked the flow budget forever.
- **Take panic** — `splitByOwner` panicked
  on an unexpected datum.
- **Data races** — worker
  epoch and reset window were accessed without synchronization.
- **Snapshot watchdog** — a worker that
  attaches to the snapshot phase but stops draining is now timed out
  (default 10m) instead of wedging the run forever.
- **Session-loss redelivery** — batches
  that were delivered to a worker but not yet acked are redelivered on
  reconnect, closing the gap between a dropped session and a restart.
- **Dashboard restart guard** — restarting a
  worker from the dashboard now checks the in-flight window first.

## Dashboard fixes

- **SSE write error** — the event stream
  now aborts cleanly on a write error instead of leaking the goroutine.
- **Fields map clone** — `Events.Record`
  clones the caller's fields map, preventing data races on the SSE
  encoder.
- **Cyclic attribute** —
  `jsonSafeValue` bounds its recursion depth to prevent a stack overflow
  on cyclic attribute maps.
- **Deadline log** — a failed
  `SetWriteDeadline` is now logged instead of silently swallowed.

## Dataplane fixes

- **Empty primary key guard** —
  `Collapse` rejects upsert mode with an empty PK list at validation
  time, not at commit time.
- **Narrow integer keys** — `EncodeKey`
  covers `int8`, `int16`, `int32` widths, not just `int64`.

## Eventlog fixes

- **Close/emit race** — `Emit` races with
  `Close` are now bounded by an inflight WaitGroup.
- **Buffer bound** — the in-memory
  event buffer has a cap to prevent unbounded growth.
- **`Close` error** — `Close` now returns
  an error instead of silently discarding it.
- **Backlog cap** — the per-emit backlog
  is capped to prevent a slow consumer from consuming unbounded memory.

## Iceberg maintenance fixes

- **Starvation fix** — a failing compaction no longer blocks snapshot
  expiry and orphan cleanup.
- **Retry budget** — the maintenance retry budget is widened past the
  writer's commit cadence to avoid false-positive failures.
- **Orphan cleanup** — `cleanOrphans` now retries `LoadTable` like
  compaction and expiry do.
