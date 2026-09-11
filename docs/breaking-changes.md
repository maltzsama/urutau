# Breaking changes

There is no CHANGELOG file; this page records changes that break wire
compatibility, spec compatibility, or a documented contract. Ordered newest
first.

## Onda 2 — enrich Arrow-native inside the join (`feat/perf-onda2-enrich-arrow-native`)

The enrich join no longer decodes a join key per row, computes a string
`joinKey`, and probes a Go map. The `map[string]map[string]any` snapshot
image and the `joinKey` / `intKey` helpers are **gone**. The reference is a
typed Arrow table plus a `key → row` index; the batch matches with the
`is_in` kernel and one marked Go gather pass.

### `Loader.Load` returns an Arrow RecordBatch

```go
type Loader interface {
    Load(ctx context.Context) (arrow.RecordBatch, error)
    Close() error
}
```

was `([]map[string]any, error)`. `NewSQLLoader` gained an `onRef` argument
(the join column, so the loader can append an `ORDER BY`).

### `enrich.New` takes the event schema

`enrich.New(cfgs, eventSchema core.Schema, log)` — was
`enrich.New(cfgs, eventColumns []string, log)`. It needs the join
column's type to check it against the reference at boot. Call sites in
`runner` and `worker/remote` pass the resolved source schema;
`introspectAll` now returns `sourceSchemas map[string]core.Schema` in
place of `sourceCols []string`.

### Join-key type mismatch fails at boot

If the reference's join column and the event's join column are different
Arrow types, the reference **never goes hot** and the error names both
types. No automatic cast. Cast in the reference query. The old
signed/unsigned width-collapsing (`joinKey` treated `uint64` and `int64`
as the same key space) is gone with it.

### semi / anti joins

`joinType` accepts `left semi` and `left anti` in addition to `left`,
`left outer`, `inner`. Both emit **no reference columns** — `select` on a
semi/anti join is a spec error. A delete always survives any join type.

### `Enricher` takes a context

`EnrichBatch(ctx context.Context, b *dataplane.Batch, primaryKey []string)`
— and `Stage.ColumnarJoin(ctx, b)`. The context threads the allocator into
the compute kernels.

### `dataplane.Collapse` splits by kernel

The `__op` split loop (a `[]bool` builder per row) is now `equal(__op,
OpDelete)` + `not`. The composite-PK hashing and last-write-wins grouping
loops stay — no kernel gives the row-index map — and are marked
`//allow:rowloop`. `filter.go` (`EvaluatePredicate`, `SplitByOp`,
`TransitionMatrix`, …) is untouched: it has no production callers.

## Onda 1 — Arrow end to end (`feat/arrow-onda1-columnar-enrich`)

The data plane is now Arrow from the source encoder to the sink projection.
`rowchange.Change` exists only as a decode-boundary DTO inside the built-in
source decoders; nothing downstream of the encoder is row-shaped.

### Wire schema: `__phase` is a sixth metadata column

`transport.WireMetadataFields()` grew from 5 to 6. Every wire batch now
carries `__phase` (`String`, nullable) after `__snapshot`:
`"snapshot"` for DBLog chunk rows, `"stream"` for live events.

**Impact:** a worker on the new schema cannot decode a batch produced by an
old coordinator, and vice versa. The deploy must be all-or-nothing across
coordinator and workers — not a rolling upgrade.

`dataplane.AddMetadata` is **removed**. It had no production callers; its
only job (injecting `__phase`) now belongs to the source encoder.

### Enrich: columnar broadcast join, row path deleted

`enrich.Stage.Enrich([]rowchange.Change)` and its helpers are **removed**.
The seam is `enrich.Stage.ColumnarJoin(*dataplane.Batch)`; `EnrichBatch`
delegates to it and no longer needs the `primaryKey` argument (kept for
call-site compatibility, ignored).

- **Non-string reference columns work natively.** A reference query
  returning `int`, `float`, `time`, `[]byte` no longer fails the load — the
  destination column lands typed. The `CAST(col AS CHAR)` workaround is no
  longer needed (and the FT-2 fail-loud check is gone).
- **Cold start is per batch.** `onColdStart: buffer` no longer parks
  individual events until the reference warms; it now behaves like `pass`
  (every row a miss). `onColdStart: drop` drops the whole batch.
  `bufferLimits` is still validated as spec grammar but its value is
  ignored.
- A reference column's Arrow type is derived from the first non-null value
  in a load. An all-NULL column gets a String placeholder that a later
  non-null load replaces — a sink that has already created the column as
  String must tolerate the type change, or the first reference load must be
  synchronous.

### `dataplane.BatchFromChangeBatch` removed

The row-to-wire bridge is gone. `transport.RecordFromChanges(rows, cs,
alloc)` builds the wire `arrow.RecordBatch` directly;
`transport.MergeSchema` / `transport.InferSchemaFromChanges` are the schema
helpers for schema-less or schema-growing producers.

### Postgres live CDC

The postgres source's `stream` embedded a nil `*sourcepull.Puller` and had
no `Next` — live replication never worked. It is now wired the same way
MySQL is (per-table introspection + `Puller`). This is a fix, not a
regression, but it is the first release where postgres live CDC runs.

### Still deferred

- **Issue #56a** (wildcard enrich drift): `select: ["*"]` still cannot
  extend the wire schema after the first load. Documented exception; see
  `docs/semantics.md`.
- **`enrich_miss` metadata**: the columnar join marks a miss with NULL
  reference columns, not a dedicated `__enrich_miss` wire column, so the
  metadata key is not materializable yet.
