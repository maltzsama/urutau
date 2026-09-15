---
sidebar_position: 5
---

# Known limitations and roadmap

## Known limitations

- **`Map` columns can't reach Iceberg.** `iceberg-go` v0.6.0's
  `AppendTable` rejects a `Map` column at write time. `Struct` and `List`
  work; cast a `Map` column to `string` (JSON) until upstream fixes this.
  See [Sinks: Iceberg](sinks#iceberg).
- **A terminated pipeline is terminal by design.** Once
  `status.terminated` is set, the operator stops reconciling and nothing
  recreates the job — a new run means a new CR, not a restart of the old
  one.
- **ClickHouse's atomicity is per-insert-per-partition, not per-commit**
  like Iceberg's. Don't opt into `PARTITION BY` without reading
  [Sinks: ClickHouse](sinks#clickhouse) first.

If you hit any of these, open an issue rather than working around them
silently — they're tracked, not forgotten.

## Roadmap

**Kinesis as a source.** Unlike Kafka, Kinesis reshards — closing and
parent-child shard lineage is a position-contract problem Kafka doesn't
have, and it isn't modeled yet.

**Post-0.1.0 sync modes** (`incremental`, `backfill-only`) — declared as a
future part of the contract, not implemented.

Nested canonical types (`Struct`/`List`/`Map`), `KindFixedBinary`, the
Kafka `raw`/Confluent-Avro decoders, transport-native metadata columns,
and `append-idempotent` write mode were all on this list at one point —
they've since shipped and moved into [Sources](sources),
[Sinks](sinks), and [Enrichment](enrichment).

### Registered for v2 (not v1 gaps)

Deliberate deferrals, recorded so they are not rediscovered as debt. None
is a correctness gap in v1.

- **Operator image/S3 planner**: the operator supports inline definitions
  only; an image or S3 pipeline definition is not implemented.
- **ADD COLUMN propagation**: a schema change requires declare-and-resume;
  live propagation is not automatic.
- **Dead-letter queue** (and multi-destination DLQ): a poison batch is
  terminal in v1 — see [Delivery guarantees](guarantees.md#poison-batch-terminal-no-dead-letter-queue).
- **`enrich_miss` as a materializable column**: the columnar join marks a
  left-join miss by leaving the reference columns NULL; it does not emit a
  dedicated wire column, so the `enrich_miss` metadata key cannot be
  materialized yet.

