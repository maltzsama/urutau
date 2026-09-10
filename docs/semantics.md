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
granularity-insensitivity property); the enrich stage re-sorts its output by
arrival sequence so a multi-reference cold start cannot reorder events
(enrich #5).

Across tables there is **no ordering guarantee**.

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
