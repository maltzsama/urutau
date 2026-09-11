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
order (a membership test plus a positional gather, never a re-sort).

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

The enrich stage (CR-069) is a **columnar** broadcast join: Arrow in, Arrow
out, no per-row `rowchange` on the path, no `map[string]any` inside the
join. The reference is read whole into a worker-local **typed Arrow table**
(join key as column 0, projected columns after it) plus a `key → row`
index; the table is swapped atomically on a periodic re-read. A whole
RecordBatch matches against it with the `is_in` compute kernel (hash
membership); the matched reference columns are gathered in one Go pass
(arrow-go v18.7.0 has no `index_in` / `if_else` kernel — that pass is the
executor's only `//allow:rowloop`). Everything else — `__op == delete`,
`is_not_null`, `and`/`or`/`not`, the final `FilterRecordBatch` — is a
kernel. Reference columns land **typed**, straight from the loader's Arrow
arrays; there is no "first non-null value" type inference any more.

### The loader: two rows, imposed by `database/sql`

The SQL reference loader (`loader_sql.go`) has exactly two row-shaped
loops, co-located in one loop body and marked in the source:

- **Row 1 — `rows.Scan`.** The driver writes into `[]any` targets, one per
  column, one call per source row.
- **Row 2 — the value→builder switch (`appendTyped`).** `Scan` hands back
  the driver's natural Go type per cell; something has to route it to a
  typed Arrow builder.

Both die together when an Arrow-native driver (ADBC) lands. No other row
loop exists in the loader.

The loader also appends an `ORDER BY` on the join key when the query has
none (wrapping the query in a subselect if it already ends in `LIMIT` /
`GROUP BY` / `UNION`), so the duplicate-key check downstream can compare
adjacent rows.

### Join-key type: matched at boot or the reference fails loud

The reference's join-column Arrow type and the event's join-column Arrow
type must be **equal**. A mismatch (e.g. reference `uint64`, event
`int64`) is a **boot-time failure** naming both types — the reference
never goes hot, the run does not start enriching against it. There is no
automatic cast and no width-collapsing coercion: **the operator casts in
the reference query** so both sides agree.

### Duplicate join key: the error cites the column and the count

A reference whose join key repeats is rejected. The message names the
**column** and **how many** duplicate rows were seen — never a specific
value, never a row position (both leak reference data into logs and shift
between loads).

### Join grammar

`joinType` accepts `left` (alias `left outer`), `inner`, `left semi`,
`left anti`.

- **left / left outer:** a miss passes with the reference columns NULL and
  bumps the miss counter.
- **inner:** a miss drops the row.
- **left semi:** a hit is kept, **without** the reference columns in the
  output (a semi join emits no reference columns — `select` is rejected
  as spec error).
- **left anti:** a miss is kept, **without** the reference columns.
- **A delete (`__op == OpDelete`) always survives** every join type — it
  bypasses the lookup entirely, and its reference columns are NULL.

### Cold start is per batch, not per row

Before the first reference load: `onColdStart: drop` drops the whole
batch; `buffer` and `pass` both let every row through as a miss (reference
columns NULL, or — for semi — every non-delete dropped, for anti —
everything kept). The row path's per-row cold-start buffer is gone.

### Wildcard select: no schema drift (issue #56)

With `select: ["*"]` the reference's destination column names are only
knowable by actually running the reference query — there is no static
name to declare ahead of time. Every schema owner (coordinator, the
collapsed runner, the worker) closes this at boot: a wildcard reference's
first load runs **synchronously**, before the table's wire/sink schema is
finalized and before `EnsureTable` creates the sink table, so the real
columns are present from the first batch — there is no drift window.

If that first load fails (the reference database is unreachable, the
query is invalid), pipeline boot fails loudly, naming the reference and
the underlying cause — never a silent fallback to an empty schema.

An **explicit-select** reference is unaffected: its destination names are
static (known from the config, no I/O needed), so it keeps loading fully
asynchronously — the pipeline boots without waiting for it, and the
cold-start policy governs whatever arrives before its first refresh
completes, exactly as before this fix.

In distributed mode (coordinator + worker), the coordinator's synchronous
wildcard load and the worker's own long-lived load are two separate
queries against the same reference — the two processes share no
connection. This is an accepted, disclosed cost of the split, not a bug.

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
- **`enrich_miss` as a materializable column**: the columnar join marks a
  left-join miss by leaving the reference columns NULL — it does not emit a
  dedicated `__enrich_miss` wire column, so the `enrich_miss` metadata key
  cannot be materialized yet. Adding it is a 7th wire metadata column, the
  same shape as `__phase`.
