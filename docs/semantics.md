# Pipeline semantics — the contract

> Implicit promises made explicit. Each rule below is a behavior the code
> already implements; this page is the reference a user or an operator reads
> to know what to expect (and a future contributor reads to avoid breaking
> it).

## Delivery: at-least-once, never at-most-once

The pipeline guarantees **at-least-once**: after a crash, a batch that was
not durably committed is replayed. Duplicates are possible; **loss is not**.
This is why resume re-reads from the last committed position and why the
worker skips batches already covered by the sink (`covered` / position
index).

The consequence for sinks: writes must be **idempotent by key** — an upsert
of the same key with the same value is a no-op, and an equality delete of an
already-deleted key is success, not an error.

## Cross-table atomicity: NONE

A commit is **per-table**. A batch never mixes tables (the wire schema is
per-table), and the position advances per table. A crash between two tables'
commits leaves one advanced and one not — resume replays the unadvanced one.
There is **no cross-table transaction** and none is promised.

## Ordering: per-table, arrival order

Within one table, changes are applied in **arrival order**, and the
last-write-wins collapse picks the last operation per key. The worker's
batch buffer preserves arrival order across source batches (the
granularity-insensitivity property); the columnar enrich join preserves row
order (it is a broadcast lookup, never a re-sort).

Across tables there is **no ordering guarantee**.

### The order rule for upserts across batches

Iceberg does not know the order between a delete in one commit and an insert
in another. The system does, by **position**: the watermark per batch (the
`__pos` of the last row received), commits applied sequentially per table
(the coordinator's FIFO), and the replay skip (`covered` in
`batchReceiver` — a batch whose high position is already committed is
dropped, never re-applied). A sink writes its data and delete files in the
order the positions dictate — **never the order batches happen to arrive at
the sink.** This is why a replayed insert cannot resurrect a row a newer
delete removed.

## Delete image contract (RV-11)

A delete's row image lives in different places depending on where the change
came from:

- **wire-decoded** (through `DecodeBatch`): the image is in **`After`** — the
  wire carries the before image in the flat columns (CR-021), and `Before`
  is nil;
- **in-process** (a source decoder that filled it): the image may be in
  **`Before`**.

Consumers selecting a delete's image MUST handle both: **prefer `Before`
when non-empty, else `After`**. Two consumers that assumed one location
produced mirrored data-loss bugs (enrich #4, plugin sink RV-02).

## Pipeline context columns

Every wire batch carries a fixed tail of metadata columns —
`__op`, `__pos`, `__commit_ts`, `__ingest_ts`, `__snapshot`, `__phase` —
**born as columns at the source** (the encoder that turns decoded events
into the RecordBatch). They ride the same RecordBatch as the data, aligned
by construction. Nothing downstream injects them.

`__phase` is `"snapshot"` for rows from a DBLog chunk `SELECT` and
`"stream"` for live events — an axis orthogonal to `__op` (a snapshot row is
semantically an insert).

A context column reaches the target table **only if the operator names it**,
per column, with their own name:

```yaml
tables:
  - source: shop.orders
    target: raw.orders
    metadata:
      - {from: commit_ts, as: committed_at}
      - {from: phase, as: source_phase}
```

Every sink projects by the target table's own columns. A context column with
no `metadata` entry is **discarded by omission** — the projection simply
never includes it. There is no code path that "strips" context; not
materializing it is the default.

## Enrich: columnar broadcast join

The enrich stage (CR-069) is a **columnar** broadcast hash join: Arrow in,
Arrow out, no per-row `rowchange` on the path. The reference table is read
whole into a worker-local map, swapped atomically on a periodic re-read; a
whole RecordBatch matches against it in one pass. Reference columns land
**typed** (Int64, Float64, Timestamp, …) — the destination type is derived
from the first non-null value seen in the reference load. A reference column
that is NULL in every row falls back to a String placeholder that the next
non-null load corrects.

**Cold start is per batch, not per row.** Before the first reference load:
`onColdStart: drop` drops the whole batch; `buffer` and `pass` both let
every row through as a miss (reference columns NULL). The row path's per-row
cold-start buffer — parking events until the reference warmed — is gone.

## Table-name convention

The name fields are easy to confuse, and the confusion has caused real bugs:

- **source side**: a table is named by its **SOURCE** (`db.table`).
- **`ChunkRequest.Table`** is the **SOURCE** table (what to `SELECT`).
- **`BatchMeta.Table`** and **`dataplane.Batch.Table`** are the **TARGET**
  table (where the batch is written).
- routing and the worker registry are keyed by **TARGET**; the canonical
  schema map is keyed by **SOURCE** and resolved to target when a batch or
  marker is built.

## Poison event / poison batch

There is **no automatic dead-letter queue in v1**. A batch the sink cannot
commit is a **terminal** error: the run stops rather than skip the batch
(skipping would silently lose data). Recovery is manual and explicit:
declare the missing column / fix the cast and **resume from the last
committed position**. The run never advances past a batch it could not
write; if the event is genuinely unprocessable, the operator fixes or drops
it at the source and resumes.

A DLQ (with a manual skip valve) is a **feature for v2**, not a v1 gap.

## Snapshot / backfill

A table with no committed position is backfilled (DBLog chunk `SELECT`)
before live streaming resumes. Snapshot rows whose key a live event touched
take the upsert path (equality delete) to avoid duplication; untouched keys
are pure-appended. A worker session lost during the snapshot fails the run
so it restarts and re-snapshots cleanly (CD-5) — a partially applied
snapshot is never trusted.

## Position / resume

The committed position lives **in the sink**, written atomically with the
data (design §17.3). `internal/state` (bbolt) is the exception for external
plugin sinks that cannot persist a position themselves; when both exist, the
sink wins (see `docs/state-position.md`). Resume folds use `position.MinSafe`
— an undefined order is an error, never an arbitrary pick (P1).

## Registered for v2 (not v1 gaps)

These are deliberate deferrals, recorded so they are not rediscovered as
debt. None is a correctness gap in v1.

- **Operator image/S3 planner**: the operator supports inline definitions
  only; an image/S3 source is not implemented.
- **ADD COLUMN propagation**: a schema change requires declare-and-resume;
  live propagation is not automatic.
- **Dead-letter queue** (and multi-destination DLQ): a poison batch is
  terminal in v1; a DLQ with a manual skip valve is a v2 feature.
- **Wildcard enrich drift** (issue #56a): with `select: ["*"]` the reference
  destinations are only known at load time, so a miss before the first
  non-empty load injects no columns and the table's schema can drift between
  the first batches and the first load. Governed by the cold-start policy or
  by declaring the reference columns explicitly. A synchronous pre-boot load
  (or `Stage.AddRefColumnsFromSnapshot` feeding the resolved types into the
  schema owners after warm-up) would close it.
- **`enrich_miss` as a materializable column**: the columnar join marks a
  left-join miss by leaving the reference columns NULL — it does not emit a
  dedicated `__enrich_miss` wire column, so the `enrich_miss` metadata key
  cannot be materialized yet. Adding it is a 7th wire metadata column, the
  same shape as `__phase`.
