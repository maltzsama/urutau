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
- **The coordinator has no standby.** One coordinator per pipeline,
  `replicas: 1` — a second replica cannot exist: there is one binlog/WAL
  connection per pipeline, and go-mysql cannot run two replication
  connections to the same source (a second `canal.Run()` kills the first).
  A coordinator crash is recovered by restart (the StatefulSet's restart
  policy, plus the liveness probe on `/statusz`); there is no warm standby,
  so MTTR is one pod restart. True coordinator HA needs a fencing primitive
  that does not exist yet — a standby promoted only after proving the
  primary is actually gone.

If you hit any of these, open an issue rather than working around them
silently — they're tracked, not forgotten.

## Roadmap

**Kinesis as a source.** Unlike Kafka, Kinesis reshards — closing and
parent-child shard lineage is a position-contract problem Kafka doesn't
have, and it isn't modeled yet.

**Backfill-only sync mode** (`backfill-only`) — declared as a future part of
the contract, not implemented. (`incremental` shipped for Postgres; see
[Sources: Postgres](../sources/postgres.md#incremental-mode).)

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
- **CRD version conversion**: only `v1alpha1` exists, with no conversion
  webhook and no precedent for a version bump in four releases. When a
  `v1beta1`/`v1` is first needed, designate `v1alpha1` as the conversion
  **hub** (`api/v1alpha1/conversion.go`, implementing `conversion.Hub`) so
  versions convert through it rather than pairwise; add a `conversion:` stanza
  to the CRD reusing the existing webhook Service/Certificate (no new cert
  infrastructure); and write a migration note for CRs created under the old
  version. The fields most likely to force the bump are the deliberately
  unstructured `definition.inline` (`x-kubernetes-preserve-unknown-fields`)
  and the free-string resource quantities.

