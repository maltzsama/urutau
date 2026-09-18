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

**Incremental and backfill-only sync modes** (`incremental`,
`backfill-only`) — declared as a future part of the contract, not
implemented.

Nested canonical types (`Struct`/`List`/`Map`), `KindFixedBinary`, the
Kafka `raw`/Confluent-Avro decoders, transport-native metadata columns,
and `append-idempotent` write mode were all on this list at one point —
they've since shipped and moved into [Sources](../sources/index.md),
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
- **Plugin-sink position store**: external plugin sinks have no transactional
  place to persist a position, so the out-of-sink store described in
  [Position](../architecture/state-position.md) is required for them. It is
  designed but not implemented — a first sketch (a bbolt package) was
  removed as dead code.
- **Contributor sign-off policy (DCO vs CLA)**: not decided. A per-commit
  DCO is simple; a one-time CLA is the rights grant a future Apache
  Foundation donation needs. See [CONTRIBUTING.md](https://github.com/maltzsama/urutau/blob/main/CONTRIBUTING.md#sign-off-dco-vs-cla--open).

