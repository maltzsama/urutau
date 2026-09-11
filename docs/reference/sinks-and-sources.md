# Sources, sinks, and feature detail

> **Staleness notice:** this page was carried over from the old README
> almost verbatim. Nested `Struct`/`List`/`Map` columns, the Kafka
> `raw`/Confluent-Avro decoders, and the Kafka connection bugs mentioned as
> "roadmap"/"known limitations" below have since shipped (see issue #17 and
> the PRs that closed it) — the **Roadmap** and **Known limitations**
> sections need a pass to catch up with the code. Treat the per-sink
> behavior descriptions above them as current; treat the two sections at
> the bottom as due for a rewrite, not as ground truth.

## Sources

- **MySQL** (`go-mysql`/canal, GTID, heartbeat), **Postgres** (`pgx`,
  pgoutput, LSN slot), **Kafka** (franz-go, manual partition assignment,
  debezium-json/raw/avro decoders) — one replication reader per source,
  mapped through the canonical type system. Kafka registers
  `Capabilities{Stream: true}` (no snapshot capability); the runner skips
  DBLog and streams directly from the committed offset.

## Sinks

- **Iceberg:** upsert via equality delete, delete-then-append as two
  separate commits (see the [E2E spike](../architecture/overview.md#e2e-spike)
  for why), position committed as both a snapshot property (audit trail)
  and a table property (O(1) resume, survives compaction).
- **ClickHouse:** upsert via `ReplacingMergeTree(seq, is_deleted)`
  (`ORDER BY` the declared primary key), append via plain `MergeTree`. One
  `INSERT` per batch — upserts as rows, deletes as tombstones hidden from
  `FINAL` reads. Resume reads the position from the data itself —
  `argMax(position, seq)` — never a separate control table. **The
  atomicity guarantee is narrower than Iceberg's**: the default table has
  no `PARTITION BY`, which is what makes a batch atomic (ClickHouse
  guarantees atomicity per insert, per partition — not across a
  multi-partition write). Partitioning is opt-in and weakens that
  guarantee: a batch crossing a partition boundary commits as multiple
  parts, no longer all-or-nothing. Tombstone physical cleanup is operator
  maintenance (`OPTIMIZE ... FINAL CLEANUP`); reads are correct under
  `FINAL` regardless of whether cleanup has run.
- **Couchbase:** key-document writes where upsert-by-key IS the native
  operation — one collection per table, one control document per
  collection carrying the committed position (a single O(1) `Get` on
  resume, never a scan or aggregation). Documents hold data fields at the
  top level and pipeline metadata under a reserved `_urutau` sub-object,
  so a data field named `op` never collides with the metadata `op`. Nested
  canonical types (`Struct`/`List`/`Map`) land as native JSON — the one
  sink where nesting is not a special case. Deletes are `Remove`,
  immediate, not tombstones. **The atomicity trade is a mode, not a
  caveat**: `commitMode: fast` (default) writes data first and the control
  document last — a crash in between leaves the position un-advanced and
  the restart replays the batch, which is idempotent because every
  mutation is keyed by the row's primary key. `commitMode: atomic` wraps
  data and control document in a distributed ACID transaction, closing the
  window at the cost of transaction overhead per batch. Every write
  acknowledges at synchronous-durability `majority`; on a single node that
  requires a 0-replica bucket (which is what the sink creates) —
  `DurabilityImpossible` is a loud error, not a silent downgrade.

## Cross-cutting features

- **Metadata columns:** closed catalog — CDC (`op`, `commit_ts`,
  `ingest_ts`, `position`, `source_table`, `phase`) and transport-native
  (`stream`, `shard`, `sequence`, `msg_ts`, `msg_key`, `headers`) — landed
  as nullable columns at the end of the canonical schema, renamed per-table
  via `metadata`.
- **Per-column cast:** explicit type overrides with a closed matrix —
  widening always, to-string always, narrowing/parsing never except
  explicit temporal reinterpretation (`timestamptz(assume_utc)`).
  Unmappable source types bypass the cast rather than silently coercing.
- **DBLog snapshot:** generic in `internal/snapshot` — chunk by primary
  key, low/high watermarks, and a caught-up **proof** that closes each
  window (never a timer; `windowTimeout` is a pathology detector, not a
  trigger). Skipped for sources without snapshot capability (Kafka).
- **Worker:** per-key collapse in upsert mode, pass-through in append
  mode, strictly serialized commits per table, over a sink-agnostic
  contract.
- **Distributed split:** coordinator (control plane + Flight data plane,
  flow budget, supervisor) and workers, with the two streams
  lifecycle-coupled on one connection — a dead channel is detected by both
  sides, never a silent zombie.
- **Supervision:** a worker that stops acking is reset (epoch bump,
  session cancel); resets beyond the cap within the window terminate the
  job — crashloops stay dead until a human clears them.
- **Driver registry:** self-registration from `init()`, blank-imported by
  `internal/builtin`. A missing registration fails loudly: the error lists
  every registered kind and points at the blank-import, and
  `TestAllDriversRegistered` fails in CI if a built-in driver silently
  stops registering.
- **Operator:** CDCPipeline CRD, coordinator StatefulSet reconciler,
  validating webhook, envtest suite.

## Enrichment

Broadcast hash join against small reference tables (`internal/enrich`) —
the reference is read whole into worker RAM, every event matches in O(1)
against the map, and a periodic full re-read swaps the map atomically
(in-flight events finish on the old image). No lookup per event, no
shuffle, no windowed state.

**Projection is explicit**: `select` is required and lists the reference
columns the event receives — nothing unselected ever lands in the sink.
**Column namespacing follows Spark DataFrame semantics**: unrenamed
columns are automatically prefixed with `{table}.{column}` to prevent
silent collisions when multiple references inject columns with the same
name. `as` renames at load time and overrides the prefix.

`joinType: left|inner` per reference (inner drops on a miss; left passes
with NULL reference columns and `enrich_miss` when the table declares that
metadata column); `onColdStart: buffer|pass|drop` governs events that
arrive before the first reference load, with the buffer bounded by both
events (`maxEvents`) and time (`maxWait`) — an evacuated event follows the
join type, so there is never a third miss policy.

**Enrichment is point-in-time, and pipelines with it say so**: `statusz`
reports `enrichment: point-in-time` — the enriched columns are not
reproducible by replay (the reference is a snapshot, not CDC), while
source columns and position stay deterministic either way. A pipeline
without `enrich` runs the identical code path it always did.

Full semantics (join grammar, cold-start details, wildcard-select
behavior): [`docs/reference/semantics.md`](semantics.md#enrich-columnar-broadcast-join).

**Single reference example:**
```yaml
tables:
  - name: orders
    columns: [id, customer_id, amount]
    enrich:
      - table: customers
        select: [name, tier]
        on: {customer_id: id}
        joinType: left
```
Source row: `{id: 1, customer_id: 42, amount: 100}`
Output: `{id: 1, customer_id: 42, amount: 100, customers.name: "Ana", customers.tier: "gold"}`

**Multi-reference example:**
```yaml
enrich:
  - table: customers
    select: [name]
    on: {customer_id: id}
  - table: products
    select: [name, category]
    on: {product_id: id}
```
Output: `{..., customers.name: "Ana", products.name: "Laptop", products.category: "electronics"}`
Both `customers.name` and `products.name` coexist — no silent collision.

**Renaming with `as`:**
```yaml
enrich:
  - table: customers
    select: [name, tier]
    on: {customer_id: id}
    as: {"customers.name": "client_name"}
```
Output: `{..., client_name: "Ana", customers.tier: "gold"}` — `name` renamed
to `client_name`, `tier` keeps prefix.

## Known limitations

> See the staleness notice at the top — this list has not been updated to
> reflect nested-column and Kafka-Avro work that has since shipped.

This list is expected to shrink, not grow, before 0.1.0:

- A terminated pipeline (`status.terminated` set) is terminal by design —
  the operator parts ways and nothing recreates the job; a new run means a
  new CR.
- Composite (nested) columns are not yet mappable into ClickHouse:
  `KindList`/`KindMap`/`KindStruct` hit the escape valve and require an
  explicit cast today (Iceberg and Couchbase take them natively). Native
  `Array`/`Map`/`Tuple` mapping is a planned follow-up.
- Iceberg tables with a `map` column cannot be written by `iceberg-go`
  v0.6.0 (`AppendTable` rejects the composite record); tracked upstream,
  worked around by an explicit cast to string.
- The ClickHouse sink's atomicity is per-insert-per-partition, not
  per-commit like Iceberg's; see the ClickHouse section above and don't
  opt into `PARTITION BY` without reading that paragraph.

If you hit any of these, please open an issue rather than working around
them silently — they're tracked, not forgotten.

## Roadmap

> See the staleness notice at the top.

In: nested canonical types (`Struct`/`List`/`Map`) and `KindFixedBinary`;
Kafka `raw` and Confluent-Avro decoders (the Avro decoder is the first
real consumer of the nested types); transport-native metadata columns
(`stream`, `shard`, `sequence`, `msg_ts`, `msg_key`, `headers`); the
append-idempotent write mode keyed on the transport's own monotonic
coordinate. Ahead: Kinesis as a source (which, unlike Kafka, reshards —
closing/parent-child shard lineage the position contract will need to
model), native ClickHouse mapping for the nested types
(`Array`/`Map`/`Tuple`), and the post-0.1.0 sync modes declared in the
contract (`incremental`, `backfill-only`).
