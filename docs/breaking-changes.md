# Breaking changes

There is no CHANGELOG file; this page records changes that break wire
compatibility, spec compatibility, or a documented contract. Ordered newest
first.

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
