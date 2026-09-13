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
