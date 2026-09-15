---
sidebar_position: 1
---

# CDC upsert

This is the baseline pipeline: replicate a table's changes, keep the
destination row in sync with the source row, keyed by primary key. If
you haven't set `writeMode`, this is what you already have — this page
just makes it explicit before the [append-only](append-only.md) and
[enrichment](enrich-reference-table.md) guides build on it.

## MySQL → Iceberg

```yaml
pipeline: e2e-mysql
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
  serverId: "1101"
sink:
  uri: http://localhost:8181/api/catalog
  namespace: raw
  warehouse: quickstart_catalog
  clientId: root
  clientSecret: s3cr3t
  scope: PRINCIPAL_ROLE:ALL
tables:
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
```

`serverId` is MySQL-specific — a unique id this replicator presents to
the binlog, same idea as a normal MySQL replica. This exact spec is what
`test/e2e/mysql_test.go` runs against a real MySQL + Iceberg stack:
DBLog snapshot under concurrent load, live binlog streaming, resume
after a stop/restart. Full file: [`examples/mysql-iceberg.yaml`](https://github.com/maltzsama/urutau/blob/main/examples/mysql-iceberg.yaml).

## Postgres → Iceberg

```yaml
pipeline: e2e-postgres
source:
  kind: postgres
  uri: postgres://repl:replpass@127.0.0.1:5433/shop?sslmode=disable
  slotName: urutau_e2e
sink:
  uri: http://localhost:8181/api/catalog
  namespace: raw
  warehouse: quickstart_catalog
  clientId: root
  clientSecret: s3cr3t
  scope: PRINCIPAL_ROLE:ALL
tables:
  - source: public.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
```

`slotName` is Postgres-specific — required, names the logical replication
slot the source creates and reads from (`pgoutput`). Full file:
[`examples/postgres-iceberg.yaml`](https://github.com/maltzsama/urutau/blob/main/examples/postgres-iceberg.yaml).

## MySQL → ClickHouse

```yaml
pipeline: e2e-mysql-clickhouse
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
  serverId: "1101"
sink:
  type: clickhouse
  uri: clickhouse://localhost:9002?password=clickpass
  namespace: lakehouse
tables:
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
```

ClickHouse is a native-protocol DSN (clickhouse-go v2), not a REST
catalog — `sink.type: clickhouse` must be set explicitly, it isn't
auto-detected the way Iceberg is the default. `sink.namespace` here is the
ClickHouse **database**, used only as a fallback if the DSN itself doesn't
carry one. Full file: [`examples/mysql-clickhouse.yaml`](https://github.com/maltzsama/urutau/blob/main/examples/mysql-clickhouse.yaml).

## MySQL → Couchbase

```yaml
pipeline: e2e-mysql-couchbase
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
  serverId: "1101"
sink:
  type: couchbase
  uri: couchbase://localhost
  namespace: lakehouse
  clientId: urutau
  clientSecret: urutaupass
tables:
  - source: shop.orders
    target: cb_orders
    primaryKey: [id]
    createIfNotExists: true
```

`sink.namespace` maps to the Couchbase **bucket**, not a catalog
namespace. `clientId`/`clientSecret` are cluster credentials (Couchbase's
connection string never embeds them). A table target is
`scope.collection` (one dot) or a bare collection name, which falls back
to `sink.scope` (default `_default`) — `cb_orders` above lands in
`_default`. Full file: [`examples/mysql-couchbase.yaml`](https://github.com/maltzsama/urutau/blob/main/examples/mysql-couchbase.yaml).

## What makes this "upsert"

Neither spec above sets `writeMode` — it defaults to `upsert`. `primaryKey`
is what makes upsert possible: an UPDATE on `id: 7` replaces the row for
`id: 7` in the destination, a DELETE removes it. There's exactly one row
per primary key value at any point in time, same as the source.

Compare this to [append-only](append-only.md), where every change becomes
a new row and nothing is ever collapsed or removed.

## Source × sink matrix

`✅ tested` means an example above (or `examples/`) is exercised end to end
by this repo's own e2e suite against a real instance of both sides.
`untested` means the source and sink drivers are independent of each
other and there's no contract reason it wouldn't work, but nobody has
run this exact combination through e2e — treat it as unverified, not
broken.

| Source ↓ / Sink → | Iceberg | ClickHouse | Couchbase |
|---|---|---|---|
| **MySQL** | ✅ `examples/mysql-iceberg.yaml` | ✅ `examples/mysql-clickhouse.yaml` | ✅ `examples/mysql-couchbase.yaml` |
| **Postgres** | ✅ `examples/postgres-iceberg.yaml` | untested | untested |
| **Kafka** | ✅ `examples/kafka-avro-nested.yaml` (append-only, see below) | untested | untested |

`createIfNotExists: true` and `primaryKey` are required on every table in
upsert mode regardless of source/sink. Kafka is always append-only (raw/
Avro landing, no stable key to upsert on) — see
[append-only](append-only.md#ondelete-what-happens-to-a-delete) for why
`onDelete: skip` is required there.

See [Sources](../reference/sources.md) and [Sinks](../reference/sinks.md)
for what each driver actually supports (capabilities, nested-type
mapping, resume semantics).
