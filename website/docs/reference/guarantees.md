---
sidebar_position: 1
---

# Delivery guarantees

The promises the engine makes, written down so you can depend on them.
Everything here is implemented today; this page is the reference a user or
an operator reads to know what to expect.

If a behavior you rely on is not on this page, do not assume it.

## Delivery: at-least-once, never at-most-once

After a crash, a batch that was not durably committed is **replayed**.
Duplicates are possible; **loss is not**. Resume re-reads from the last
committed position, and a batch already covered by the destination is
skipped rather than re-applied.

The consequence for destinations: writes must be **idempotent by key**. An
upsert of the same key with the same value is a no-op; a delete of an
already-deleted key is success, not an error. Every built-in sink satisfies
this — see [Sinks](sinks.md).

## Atomicity: per-table, never across tables

A commit is **per-table**. A batch never mixes tables, and the position
advances per table. A crash between two tables' commits leaves one advanced
and one not — resume replays the unadvanced one.

There is **no cross-table transaction**, and none is promised.

## Ordering: per-table, arrival order

Within one table, changes are applied in **arrival order**, and the
last-write-wins collapse picks the last operation per key. The batch buffer
preserves arrival order across source batches.

Across tables there is **no ordering guarantee**.

### Why a replayed insert cannot resurrect a deleted row

The destination does not know the order between a delete in one commit and
an insert in another. The engine does, by **position**: each batch carries a
watermark, commits apply sequentially per table, and a batch whose high
position is already committed is dropped. Data and delete files are written
in the order the positions dictate — never the order batches happen to
arrive at the sink.

## Deletes

A delete is a first-class change, not a tombstone you have to filter:

- With `upsert`, a delete removes the row at the primary key (Iceberg: an
  equality delete; ClickHouse: a tombstone hidden from `FINAL`; Couchbase:
  a `Remove`).
- Deletes are **idempotent** — applying the same delete twice is success.

The mechanics differ per sink; see [Sinks](sinks.md).

## Metadata columns

Every batch carries a fixed tail of context columns — `__op`, `__pos`,
`__commit_ts`, `__ingest_ts`, `__snapshot`, `__phase` — alongside the data,
aligned by construction.

A context column reaches the destination table **only if you name it**, per
column, with the name you want:

```yaml
tables:
  - source: shop.orders
    target: raw.orders
    metadata:
      - {from: commit_ts, as: committed_at}
      - {from: phase, as: source_phase}
```

A context column with no `metadata` entry is discarded by omission — there
is no separate "strip" step; not materializing it is the default. The full
catalog of names is in [Sinks](sinks.md#cross-cutting).

## Poison batch: terminal, no dead-letter queue

A batch the sink cannot commit is a **terminal** error: the run stops
rather than skip the batch, because skipping would silently lose data.

Recovery is manual and explicit: fix the cause (declare the missing column,
fix the cast) and **resume from the last committed position**. The run never
advances past a batch it could not write.

A dead-letter queue with a manual skip valve is a **v2 feature**, not a v1
gap — see [Roadmap](roadmap.md).

## Snapshot and backfill

A table with no committed position is **backfilled** (a chunked `SELECT`)
before live streaming resumes.

- Snapshot rows whose key a live event also touched take the upsert path, so
  they are not duplicated; untouched keys are appended.
- A worker session lost during the snapshot fails the run, so it restarts
  and re-snapshots cleanly. A partially applied snapshot is never trusted.

## Position and resume

The committed position lives **in the destination**, written atomically with
the data. Resume folds use the minimum safe position — an undefined order is
an error, never an arbitrary pick.

The one case the sink cannot cover is an external plugin sink that has no
position capability of its own; the store for that case is designed but not
implemented. When both a sink position and an out-of-sink store exist, the
sink wins. See [Position](../architecture/state-position.md).

## Related

- [Pipeline specification](pipeline-spec.md) — the fields these behaviors
  are configured with.
- [Sinks](sinks.md) — how each destination implements idempotency.
- [Enrichment](enrichment.md) — the join contract.
