---
sidebar_position: 5
---

# Enrich internals: the columnar join

How the broadcast reference join (`internal/enrich`) is built, and the
invariants a contributor must not break. The user-facing grammar lives in
[Enrichment](../reference/enrichment.md); this page is the design note.

## Columnar, not row-by-row

The stage is **columnar**: Arrow in, Arrow out, no per-row `rowchange` on
the path and no `map[string]any` inside the join.

The reference is read whole into a worker-local **typed Arrow table** (join
key as column 0, projected columns after it) plus a `key → row` index. The
table is swapped atomically on each periodic re-read, so an in-flight batch
finishes against the old image.

A whole `RecordBatch` matches against it with the `is_in` compute kernel
(hash membership). The matched reference columns are gathered in one Go pass
— arrow-go v18.7.0 has no `index_in` / `if_else` kernel, so that pass is the
executor's only `//allow:rowloop`. Everything else — `__op == delete`,
`is_not_null`, `and`/`or`/`not`, the final `FilterRecordBatch` — is a kernel.

Reference columns land **typed**, straight from the loader's Arrow arrays.
There is no "first non-null value" type inference.

## The loader: exactly two row-shaped loops

The SQL reference loader (`loader_sql.go`) has exactly two row loops,
co-located in one loop body and marked in the source:

- **`rows.Scan`** — the driver writes into `[]any` targets, one per column,
  one call per source row.
- **the value→builder switch (`appendTyped`)** — `Scan` hands back the
  driver's natural Go type per cell; something has to route it to a typed
  Arrow builder.

Both die together when an Arrow-native driver (ADBC) lands. No other row
loop exists in the loader.

The loader also appends an `ORDER BY` on the join key when the query has
none (wrapping the query in a subselect if it already ends in `LIMIT` /
`GROUP BY` / `UNION`), so the duplicate-key check downstream can compare
adjacent rows.

## Boot-time type and key checks

- **Join-key type must match.** The reference's join-column Arrow type and
  the event's join-column Arrow type must be **equal**. A mismatch (e.g.
  reference `uint64`, event `int64`) is a **boot-time failure** naming both
  types — the reference never goes hot. There is no automatic cast; the
  operator casts in the reference query so both sides agree.
- **Duplicate join keys are rejected.** The message names the **column** and
  **how many** duplicate rows were seen — never a specific value, never a
  row position (both would leak reference data into logs and shift between
  loads).

## Cold start is per batch

Before the first reference load: `onColdStart: drop` drops the whole batch;
`buffer` and `pass` both let every row through as a miss. There is no
per-row cold-start buffer or drain queue.

## Wildcard select closes schema drift at boot

With `select: ["*"]` the reference's destination column names are only
knowable by running the reference query — there is no static name to declare
ahead of time.

Every schema owner (coordinator, collapsed runner, worker) closes this at
boot: a wildcard reference's first load runs **synchronously**, before the
table's wire/sink schema is finalized and before `EnsureTable` creates the
sink table. The real columns are present from the first batch; there is no
drift window.

If that first load fails (reference database unreachable, invalid query),
pipeline boot fails loudly, naming the reference and the cause — never a
silent fallback to an empty schema.

An **explicit-select** reference is unaffected: its destination names are
static, so it keeps loading fully asynchronously, and the cold-start policy
governs whatever arrives before its first refresh.

In distributed mode, the coordinator's synchronous wildcard load and the
worker's own long-lived load are two separate queries against the same
reference — the two processes share no connection. This is an accepted,
disclosed cost of the split, not a bug.

## Related

- [Enrichment](../reference/enrichment.md) — the user-facing grammar.
- [Enriching a stream](../guides/enrich-reference-table.md) — the how-to.
