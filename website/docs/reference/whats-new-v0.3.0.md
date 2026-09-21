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
  range ([#151](https://github.com/maltzsama/urutau/issues/151)).
- **Concurrent row scan** with transient retry — rows are fetched in
  parallel inside each chunk, and transient errors (statement canceled,
  serialization failure) are retried per-row instead of restarting the
  whole chunk.
- **Column projection and structured filter** — only the columns the
  pipeline needs are read, and the optional `filter` clause is pushed
  down into the `SELECT`.

**CDC:**

- **Incremental mode** ([#157](https://github.com/maltzsama/urutau/issues/157)) — the worker
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
  `maxThreads`, and `retryCount` ([#148](https://github.com/maltzsama/urutau/issues/148)-[#150](https://github.com/maltzsama/urutau/issues/150),
  [#165](https://github.com/maltzsama/urutau/issues/165)-[#166](https://github.com/maltzsama/urutau/issues/166)).
- **SSH in distributed mode** ([#170](https://github.com/maltzsama/urutau/issues/170)) — workers
  dial through the SSH bastion, not just the coordinator.
- **Schema discovery** ([#152](https://github.com/maltzsama/urutau/issues/152)) — column types
  and nullability are read from `information_schema` at boot, removing
  the need for hand-written schema hints.

See [Sources: Postgres](../sources/postgres.md) for the full reference.

## MySQL hardening

- **Server preflight** ([#182](https://github.com/maltzsama/urutau/issues/182)) — on startup the
  source checks that `log_bin` is on, `binlog_format` is `ROW`, and
  `binlog_row_image` is `FULL`, failing loud if any precondition is
  missing.
- **Purged-binlog guard** ([#183](https://github.com/maltzsama/urutau/issues/183)) — if the
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
  correctly ([#186](https://github.com/maltzsama/urutau/issues/186), [#187](https://github.com/maltzsama/urutau/issues/187)).
- **Empty batch safety** — an empty batch no longer advances the
  position or persists a stale snapshot state ([#196](https://github.com/maltzsama/urutau/issues/196)).
- **Config resolution** — multi-level namespaces, empty credentials, and
  silent fallbacks all behave correctly now ([#189](https://github.com/maltzsama/urutau/issues/189),
  [#190](https://github.com/maltzsama/urutau/issues/190), [#199](https://github.com/maltzsama/urutau/issues/199)).
- **Staged commit idempotency** — a retry of the same staged cycle no
  longer creates duplicate data files.
- **Position walk-back** — on resume the coordinator walks the branch
  ancestry, not the flat snapshot list, so it finds the committed
  position even after compaction prunes old snapshots.

See [Sinks: Iceberg](../reference/sinks.md#iceberg) for the full reference.

## Kafka field projection

The Kafka source now lets you pick which payload fields land as columns
— in raw, JSON, and Avro modes ([#143](https://github.com/maltzsama/urutau/issues/143)).
Previously every field in the payload was mapped; now you can narrow the
projection at the source level, reducing downstream schema width.

See [Sources: Kafka](../sources/kafka.md) for the full reference.

## Operator: Server-Side Apply and serverId uniqueness

- **Server-Side Apply** ([#249](https://github.com/maltzsama/urutau/issues/249),
  [#251](https://github.com/maltzsama/urutau/issues/251)) — the operator applies every managed
  object (StatefulSet, Service, ConfigMap, RBAC) under a named field
  manager (`--field-manager`, default `urutau-operator`). Fields it does
  not own — mutating-webhook defaults, API-server-assigned `clusterIP` —
  are left untouched, and a reconcile that changes nothing writes nothing.
- **`serverId` uniqueness** ([#252](https://github.com/maltzsama/urutau/issues/252)) — the
  admission webhook rejects a pipeline whose `source.serverId` duplicates
  an existing pipeline's on the same MySQL source within a namespace.
- **Un-brick on spec change** ([#248](https://github.com/maltzsama/urutau/issues/248)) — updating a
  `CdPipeline` spec now correctly reconciles the coordinator, instead of
  stalling.
- **Secret key validation** ([#250](https://github.com/maltzsama/urutau/issues/250)) — the
  operator verifies that referenced secret keys exist before
  provisioning the coordinator.
- **RBAC** ([#254](https://github.com/maltzsama/urutau/issues/254), [#257](https://github.com/maltzsama/urutau/issues/257)) — status
  subresource and finalizer RBAC rules are split, and the GC window is
  documented.

See [Deploy on Kubernetes](../guides/deploy-kubernetes.md) and
[Architecture: Operator](../architecture/operator.md) for details.

## Maintenance improvements

- **Completion-time throttle** ([#244](https://github.com/maltzsama/urutau/issues/244)) — the
  `interval` on every maintenance operation is now measured from the
  **previous run's completion**, not from a fixed start-time schedule.
  A run that takes longer than its interval does not immediately re-fire.
- **Per-operation isolation** ([#247](https://github.com/maltzsama/urutau/issues/247)) — a failing
  operation (e.g. compaction) no longer starves the others; each runs on
  its own timer.
- **`CheckInterval` gate** ([#246](https://github.com/maltzsama/urutau/issues/246)) — the shared
  `CheckInterval` is ignored when `enabled: false`, avoiding confusing
  log noise.

See [Table maintenance](../guides/table-maintenance.md) for details.

## Worker: onDelete and schema drift

Workers now propagate `onDelete` events and report schema drift over the
gRPC wire ([#264](https://github.com/maltzsama/urutau/issues/264), [#272](https://github.com/maltzsama/urutau/issues/272)). Previously,
delete events were silently dropped when the sink did not support them,
and schema drift was only detected locally.

## Coordinator hardening

A concentrated bug-hunt fixed deadlocks, data races, panics, and leaks:

- **Budget deadlock** ([#209](https://github.com/maltzsama/urutau/issues/209)) — `flowBudget.acquire`
  deadlocked on an oversized first batch; fixed with serialized
  admission.
- **Ack leak** ([#210](https://github.com/maltzsama/urutau/issues/210)) — an unparsable ack
  position leaked the flow budget forever.
- **Take panic** ([#211](https://github.com/maltzsama/urutau/issues/211)) — `splitByOwner` panicked
  on an unexpected datum.
- **Data races** ([#207](https://github.com/maltzsama/urutau/issues/207), [#208](https://github.com/maltzsama/urutau/issues/208)) — worker
  epoch and reset window were accessed without synchronization.
- **Snapshot watchdog** ([#206](https://github.com/maltzsama/urutau/issues/206)) — a worker that
  attaches to the snapshot phase but stops draining is now timed out
  (default 10m) instead of wedging the run forever.
- **Session-loss redelivery** ([#235](https://github.com/maltzsama/urutau/issues/235)) — batches
  that were delivered to a worker but not yet acked are redelivered on
  reconnect, closing the gap between a dropped session and a restart.
- **Dashboard restart guard** ([#205](https://github.com/maltzsama/urutau/issues/205)) — restarting a
  worker from the dashboard now checks the in-flight window first.

## Dashboard fixes

- **SSE write error** ([#225](https://github.com/maltzsama/urutau/issues/225)) — the event stream
  now aborts cleanly on a write error instead of leaking the goroutine.
- **Fields map clone** ([#226](https://github.com/maltzsama/urutau/issues/226)) — `Events.Record`
  clones the caller's fields map, preventing data races on the SSE
  encoder.
- **Cyclic attribute** ([#227](https://github.com/maltzsama/urutau/issues/227)) —
  `jsonSafeValue` bounds its recursion depth to prevent a stack overflow
  on cyclic attribute maps.
- **Deadline log** ([#228](https://github.com/maltzsama/urutau/issues/228)) — a failed
  `SetWriteDeadline` is now logged instead of silently swallowed.

## Dataplane fixes

- **Empty primary key guard** ([#218](https://github.com/maltzsama/urutau/issues/218)) —
  `Collapse` rejects upsert mode with an empty PK list at validation
  time, not at commit time.
- **Narrow integer keys** ([#220](https://github.com/maltzsama/urutau/issues/220)) — `EncodeKey`
  covers `int8`, `int16`, `int32` widths, not just `int64`.

## Eventlog fixes

- **Close/emit race** ([#229](https://github.com/maltzsama/urutau/issues/229)) — `Emit` races with
  `Close` are now bounded by an inflight WaitGroup.
- **Buffer bound** ([#230](https://github.com/maltzsama/urutau/issues/230)) — the in-memory
  event buffer has a cap to prevent unbounded growth.
- **`Close` error** ([#231](https://github.com/maltzsama/urutau/issues/231)) — `Close` now returns
  an error instead of silently discarding it.
- **Backlog cap** ([#233](https://github.com/maltzsama/urutau/issues/233)) — the per-emit backlog
  is capped to prevent a slow consumer from consuming unbounded memory.

## Iceberg maintenance fixes

- **Starvation fix** — a failing compaction no longer blocks snapshot
  expiry and orphan cleanup.
- **Retry budget** — the maintenance retry budget is widened past the
  writer's commit cadence to avoid false-positive failures.
- **Orphan cleanup** — `cleanOrphans` now retries `LoadTable` like
  compaction and expiry do.
