---
sidebar_position: 6
---

# Pipeline specification (`pipeline.yaml`)

This page is the field-by-field reference for the pipeline YAML — the
single artifact that describes a replication job.
`urutau run -f`, `urutau-coordinator run -f`, and the `definition.inline`
field of a `CDCPipeline` all consume the **same** format, and it is
validated by the **same** server-side rules (`spec.Validate`) — there is no
second, looser path.

## Shape

```yaml
pipeline: orders            # required; names the run and its worker groups
source: { … }               # required; where rows come from
sink: { … }                 # required; where rows go
tables: [ … ]               # required; at least one
```

## `source`

| Field | Required | Notes |
| --- | --- | --- |
| `kind` | yes | Registered driver: `mysql`, `postgres`, `kafka` |
| `uri` | yes* | Connection string: a MySQL/Postgres DSN, or the Kafka broker list. On Kubernetes, filled from `URUTAU_SOURCE_URI`. *Mutually exclusive with `postgres` |
| `snapshotUri` | no | Read-only URI for the snapshot `SELECT`; lets a worker run as a SELECT-only user. Falls back to `uri` |
| `serverId` | mysql | Replication server id (string holding a `uint32`). Must be unique per MySQL instance |
| `slotName` | postgres | Logical replication slot; required for Postgres |
| `snapshotMode` | no | `none` disables the snapshot. Must be `none` for Kafka |
| `groupId` | kafka | Consumer group |
| `partitionedByPrimaryKey` | kafka | Assert the topics are key-partitioned; required for `upsert` |
| `format` | kafka | `debezium` (default), `raw`, `avro` |
| `schemaRegistry` | avro | Confluent-compatible registry base URL |
| `postgres` | postgres | Structured connection fields (alternative to `uri`). See below |

### `source.postgres`

Structured PostgreSQL connection config. Mutually exclusive with `source.uri`.
When present, `uri` is ignored for connection building.

| Field | Required | Notes |
| --- | --- | --- |
| `host` | yes | Database hostname |
| `port` | no | Port (default 5432) |
| `database` | yes | Database name |
| `username` | no | Connection user |
| `password` | no | Connection password |
| `params` | no | Extra DSN key-value pairs (e.g. `application_name`) |
| `ssl.mode` | no | `disable` (default), `require`, `verify-ca`, `verify-full` |
| `ssl.ca` | no | Server CA certificate PEM path |
| `ssl.cert` | no | Client certificate PEM path (mutual TLS) |
| `ssl.key` | no | Client private key PEM path (mutual TLS) |
| `ssh.host` | no | SSH bastion hostname |
| `ssh.port` | no | SSH port (default 22) |
| `ssh.username` | no | SSH user |
| `ssh.password` | no | SSH password |
| `ssh.privateKey` | no | SSH private key path |
| `ssh.passphrase` | no | Private key passphrase |
| `maxThreads` | no | Max concurrent snapshot connections (1..32, default 10) |
| `retryCount` | no | Transient-connection retries with backoff (default 0) |

See [Sources](../sources/index.md) for driver-specific behavior — e.g. the MySQL `uri`
accepts `timezone` and TLS (`tls`, `ssl-ca`, `ssl-cert`, `ssl-key`,
`ssl-server-name`) query parameters.

## `sink`

| Field | Required | Notes |
| --- | --- | --- |
| `type` | no | `iceberg+rest` (default), `clickhouse`, `couchbase` |
| `uri` | yes | Catalog/endpoint URI; on Kubernetes, filled from `URUTAU_SINK_URI` |
| `namespace` | yes | Default namespace for bare targets |
| `warehouse` | iceberg | Catalog warehouse name |
| `clientId` / `clientSecret` / `scope` | iceberg | OAuth2 credentials; filled from `URUTAU_SINK_*` on Kubernetes |
| `commitMode` | couchbase | `fast` (default) or `atomic` |
| `defaults.writeMode` | no | Pipeline-wide `writeMode` default |
| `defaults.targetFileSize` | no | Target data-file size |
| `maintenance.enabled` | no | Background Iceberg table maintenance (Iceberg sink only — rejected on any other sink type). `false` by default — omitting the whole `maintenance` block, or leaving `enabled` unset, is the same as `false` |
| `maintenance.compaction.interval` / `.targetFileSize` / `.minInputFiles` | no | Small-file compaction. Defaults `5m` / `512Mi` / `5` |
| `maintenance.snapshotExpiry.interval` / `.retainLast` / `.maxAge` | no | Snapshot history pruning. Defaults `10m` / `1` / `168h`. `maxAge` is a safety window, not just a retention count — see [Sinks](sinks.md#table-maintenance) |
| `maintenance.orphanCleanup.interval` / `.olderThan` | no | Unreferenced-file deletion. Defaults `1h` / `72h` |

See [Sinks](sinks.md) for each sink's semantics and limits.

## `tables[]`

| Field | Required | Notes |
| --- | --- | --- |
| `source` | yes | `schema.table` in the source |
| `target` | yes | `namespace.table` in the sink |
| `primaryKey` | upsert | Required for `writeMode: upsert` |
| `writeMode` | no | `upsert` (default), `append`, `append-idempotent` |
| `onDelete` | append | `record` (default) or `skip`; a `DELETE` on append needs `filterImmutable` |
| `filter` | no | Row filter expression |
| `filterImmutable` | no | Required for `append` + `filter` |
| `partitionBy` | no | Iceberg partition transform |
| `createIfNotExists` | no | Create the target table |
| `workers.number` | no | Partition the table across N workers by key range |
| `workers.cpu` / `workers.memory` | no | Per-table Kubernetes resources |
| `identity` | append-idempotent | Transport-metadata columns making the table idempotent |
| `metadata` | no | Pipeline metadata columns (`op`, `commit_ts`, …) |
| `cast` | no | Override a source column's canonical type |
| `columns` | kafka | Explicit schema (no introspection) |
| `bootstrap` | no | `snapshot` (default), `adopt`, `adopt-verify` |
| `enrich` | no | Broadcast reference joins |

### `writeMode`

- **`upsert`** — a change replaces the row at `primaryKey` (update/delete
  reflected as state). Requires `primaryKey`.
- **`append`** — every change is a new row; nothing is updated. Requires
  `filterImmutable` when a `filter` is set.
- **`append-idempotent`** — append, but a transport-metadata `identity`
  makes replay a no-op.

### `workers`

`workers: {number: N}` splits the table's primary-key range into `N`
contiguous ranges and derives the group names
`<pipeline>-<target>-<index>`. `N <= 1` or absent means one worker, no
partitioning. A sink that cannot handle concurrent writers rejects
`N > 1` at boot. See [Distributed mode](../guides/distributed.md).

### `enrich[]`

Joins each event against a small reference table held in memory
(broadcast hash join). Declared in order; an inner-join miss drops the
event. See [Enrichment](enrichment.md).

## Validation

The same rules run in three places, all server-side:

1. **At boot** — `urutau run` / `urutau-coordinator run` load and validate
   before opening any connection.
2. **At admission** — the Kubernetes validating webhook validates the
   `definition.inline` spec on `kubectl apply`, with the credential/URI
   fields exempted (`spec.WithoutCredentials`), because the webhook cannot
   read the Secrets the coordinator will mount. See
   [Deploy on Kubernetes](../guides/deploy-kubernetes.md#the-webhook).
3. **In tests** — the e2e suite runs every example spec through the same
   validator, so the docs cannot drift from the code.

A spec that fails validation is rejected with a list of problems, each
naming the exact field path:

```
spec: tables[0].primaryKey: required when writeMode is upsert
```

## Environment fallback

On the Kubernetes path the URI/credential fields are left empty and filled
from the environment at load time (`spec.LoadYAML`):

| Environment | Field |
| --- | --- |
| `URUTAU_SOURCE_URI` | `source.uri` |
| `URUTAU_SINK_URI` | `sink.uri` |
| `URUTAU_SINK_CLIENT_ID` | `sink.clientId` |
| `URUTAU_SINK_CLIENT_SECRET` | `sink.clientSecret` |
| `URUTAU_SINK_SCOPE` | `sink.scope` |

An inline value always wins; the environment only fills what is empty.

## Related

- [Sources](../sources/index.md) · [Sinks](sinks.md) · [Enrichment](enrichment.md) ·
  [Delivery guarantees](guarantees.md)
- [CLI reference](cli.md) — the flags each binary adds.
