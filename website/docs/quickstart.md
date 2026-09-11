---
sidebar_position: 1
---

# Quickstart

Replicate one MySQL table into Apache Iceberg, on your machine, in one
process. No Kubernetes, no broker.

## 1. Prerequisites

- Go ≥ 1.26 (to build the binary)
- Docker + Docker Compose (for MySQL and Iceberg — you won't run these in
  production, they're here so you have something to point the pipeline at)

## 2. Build the binary

```sh
git clone https://github.com/maltzsama/urutau
cd urutau
make build
```

This produces `bin/urutau` (the collapsed CLI — coordinator and worker in
one process). `make build` also builds `bin/urutau-coordinator`,
`bin/urutau-worker`, and `bin/urutau-operator`, which you don't need yet.

## 3. Start a source and a destination

The repo's e2e stack gives you a real MySQL (binlog enabled) and a real
Iceberg catalog (Polaris REST catalog + Trino to read the result back) with
one command:

```sh
docker compose -f test/e2e/docker-compose.yml up -d --wait mysql polaris trino rustfs bucket-init polaris-setup
```

This starts only what this quickstart needs — the full compose file also
has Postgres, ClickHouse, and Couchbase for other pipelines, which you can
skip for now. Give it a minute; `--wait` blocks until the containers report
healthy.

MySQL comes up with a `shop` database and an `orders` table already
created (see `test/e2e/mysql/init/init.sql`), plus a replication user
(`repl`/`replpass`).

## 4. Write a pipeline spec

```yaml
# pipeline.yaml
pipeline: orders-demo
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
  serverId: "1101"

sink:
  type: iceberg+rest
  uri: http://localhost:8181/api/catalog
  warehouse: quickstart_catalog
  namespace: bronze
  clientId: root
  clientSecret: s3cr3t
  scope: PRINCIPAL_ROLE:ALL

tables:
  - source: shop.orders
    target: bronze.orders
    primaryKey: [id]
    writeMode: upsert
    createIfNotExists: true
```

`serverId` is a string on purpose — it flows into the spec as written, and
a UUID server id is legal. Each replicator connecting to the same MySQL
needs a distinct one.

## 5. Run it

```sh
./bin/urutau run -f pipeline.yaml
```

This is the **collapsed mode**: reader, worker, and sink writer in one
process, one binary, no Kubernetes. It:

1. Snapshots the existing rows in `shop.orders` (DBLog: chunked `SELECT`,
   safe under concurrent writes).
2. Switches to streaming the binlog live.
3. Writes both into `bronze.orders` in Iceberg, upsert by `id`.

Leave it running — it's a long-lived process, like any replicator.

## 6. Prove it worked — read it back

Don't trust the write; read it back through a real SQL engine:

```sh
docker exec -it $(docker compose -f test/e2e/docker-compose.yml ps -q trino) \
  trino --execute "SELECT * FROM iceberg.bronze.orders"
```

`init.sql` only creates the `orders` table's schema, no rows — this query
comes back empty, which is correct. What matters is that the table exists
in Iceberg with the right columns (`DESCRIBE iceberg.bronze.orders` if you
want to check).

## 7. Change some data, watch it replicate

In another terminal, connect to MySQL and mutate `shop.orders`:

```sh
docker exec -it $(docker compose -f test/e2e/docker-compose.yml ps -q mysql) \
  mysql -uroot -prootpass shop \
  -e "INSERT INTO orders (id, v, amount) VALUES (101, 'hello', 9.99);"
```

Query Trino again (step 6). The insert shows up in a couple seconds. An
`UPDATE`/`DELETE` on the same `id` takes a little longer to show up — the
worker batches changes on a commit interval (a few seconds, not instant) —
but applies as an upsert / equality-delete in Iceberg, never a new
appended row next to the old one:

```sh
docker exec -it $(docker compose -f test/e2e/docker-compose.yml ps -q mysql) \
  mysql -uroot -prootpass shop \
  -e "UPDATE orders SET v='updated' WHERE id=101;"
```

## 8. Stop and clean up

```sh
# Ctrl-C the running `urutau run` process, then:
docker compose -f test/e2e/docker-compose.yml down
```

## What you just proved

- **Upsert reflects state**: an `UPDATE`/`DELETE` in MySQL changes the row
  in Iceberg, it doesn't append a new version next to the old one.
- **No broker**: `urutau run` read the binlog and wrote Iceberg directly —
  nothing sat in between.
- **The position lives in the sink**: restart `urutau run` right now
  against the same spec — it resumes from the last committed position
  (stored in Iceberg's own table properties), it does not re-snapshot.

## Next steps

- **Postgres or Kafka as the source** instead of MySQL: see
  [Sources and sinks](reference/sinks-and-sources.md#sources)
  for what each source needs (`serverId` is MySQL-specific; Postgres needs
  `slotName`, Kafka needs `bootstrapServers`).
- **ClickHouse or Couchbase as the sink** instead of Iceberg: same spec
  shape, different `sink.type` and connection fields — see
  [Sources and sinks](reference/sinks-and-sources.md#sinks).
- **Distributed mode** (coordinator + worker, for when one process isn't
  enough): `urutau-coordinator` and `urutau-worker` instead of `urutau
  run`, plus the Kubernetes operator (`cmd/operator`) if you want a CRD
  instead of hand-run binaries.
- **Enrichment** (joining events against a small reference table before
  they land): see `enrich` in [Semantics](reference/semantics.md#enrich-columnar-broadcast-join).
- **Writing your own source/sink**: [Plugins](guides/plugins.md).
- **The full behavior contract** (delivery guarantees, ordering, what
  happens on a poison batch): [Semantics](reference/semantics.md).

If something in this page doesn't work as written, that's a doc bug — open
an issue rather than silently working around it.
