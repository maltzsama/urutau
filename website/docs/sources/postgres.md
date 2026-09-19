---
sidebar_position: 3
---

# Postgres

Logical replication over `pgoutput`, via `pgx`. One replication connection per
pipeline, on a logical decoding slot.

`slotName` is required — the slot is the consistency anchor.

## Connection

Two ways to configure the connection — **mutually exclusive**:

### URI (default)

```yaml
source:
  kind: postgres
  uri: postgres://user:password@host:port/database?sslmode=disable
  slotName: my_slot
```

### Structured fields

```yaml
source:
  kind: postgres
  slotName: my_slot
  postgres:
    host: db.example.com
    port: 5432
    database: mydb
    username: reader
    password: secret
    params:
      application_name: urutau
    maxThreads: 10
    retryCount: 3
    initialWaitTime: 300
    ssl:
      mode: verify-full
      ca: /etc/ssl/ca.pem
      cert: /etc/ssl/client-cert.pem
      key: /etc/ssl/client-key.pem
    ssh:
      host: bastion.example.com
      port: 22
      username: ubuntu
      privateKey: /home/user/.ssh/id_ed25519
```

When `source.postgres` is set, `source.uri` is ignored for connection
building. `slotName` and `snapshotUri` remain flat regardless.

### TLS (`ssl`)

| Mode | Behavior |
|------|----------|
| `disable` (default) | No TLS |
| `require` | TLS, skip certificate verification |
| `verify-ca` | TLS, verify chain against CA but not hostname |
| `verify-full` | TLS, verify chain and hostname |

`cert` + `key` enable mutual TLS (client certificate). Both must be set
together.

### SSH tunnel (`ssh`)

Tunnels the connection through an SSH bastion. Supports password and
private key authentication. The tunnel is established once per connection
and reused.

### Connection tuning

| Field | Default | Description |
|-------|---------|-------------|
| `maxThreads` | `runtime.NumCPU()` | Max concurrent connections for snapshot chunk SELECTs (1..32), and the size of the concurrent row-normalization pool |
| `retryCount` | 3 | Transient-error retries with exponential backoff: snapshot queries are retried, and a lost replication stream reconnects and resumes from the committed position. 0 means "use the default" |
| `initialWaitTime` | 300 | Seconds the CDC reader waits for the first WAL message before failing with a non-retryable error (minimum 30). Detects a misconfigured slot or publication that would otherwise hang forever; the timer is satisfied by the first WAL data message |

### Distributed mode

In [distributed mode](../guides/distributed.md) the worker opens the snapshot
chunk `SELECT` from a DSN rendered from this block. `ssl.ca`, `ssl.cert` and
`ssl.key` are sent as **paths**, so every worker must mount those files at the
same paths as the coordinator. `ssh` is not supported in distributed mode —
set `snapshotUri` to a directly reachable read-only URI instead.

## Requirements

- `wal_level=logical`.
- Enough `max_replication_slots` and `max_wal_senders` for the pipeline's
  slots.
- A user with `REPLICATION` (and rights to create the publication/slot).

On start the runner makes the server side ready, idempotently: it sets
`REPLICA IDENTITY FULL` on every replicated table (so updates and deletes
carry the full old row, matching MySQL's `row_image=FULL`), creates the
logical publication `<slotName>_pub` listing exactly the pipeline's tables,
and creates the `pgoutput` slot — **before** the snapshot begins, so no
transaction between slot creation and the stream start is lost.

## Behavior

- **Position** — an LSN, tracked through the slot. The reader reports the
  pipeline's minimum committed position back to the server as
  `confirmed_flush`, so the slot never discards WAL for events still in
  flight to the sink. A restart resumes from the slot.
- **Snapshot** — the worker runs the chunk `SELECT` with
  [`snapshotUri`](../reference/pipeline-spec.md#source) when set, so it can
  use a SELECT-only user (the replication credential stays coordinator-side).
- **Before image** — deletes and updates carry the old row (PK-only unless
  `REPLICA IDENTITY FULL`, which the runner sets).
- **Snapshot consistency** — each chunk runs in a `REPEATABLE READ READ ONLY`
  transaction, so the chunk sees one consistent snapshot even under
  concurrent writes.
- **Snapshot chunking** — the default is physical **CTID** block ranges: no
  primary key required, uniform chunks regardless of key skew, sized from
  `sink.defaults.targetFileSize` (default `512Mi`) divided by the server block
  size. Range-partitioned tables are split proportionally across their leaf
  partitions. Set [`chunkColumn`](../reference/pipeline-spec.md#chunkcolumn) to
  chunk by a key column instead (value-range for integer/float, cursor
  stepping otherwise).
- **Worker partitioning** — `workers: {number: N > 1}` splits a single-column
  primary key into `N` contiguous ranges that govern both the snapshot and the
  live stream, so a key never changes owner. CTID is not routable, so a
  partitioned table is always chunked by its key.
- **Column projection** — [`columnFilter`](../reference/pipeline-spec.md#columnfilter)
  narrows the snapshot `SELECT` list and the CDC projection; the excluded
  columns are absent from the target. It must include the primary key.
- **Row filter** — [`filter`](../reference/pipeline-spec.md#filter) is pushed
  into the snapshot `WHERE` and evaluated on each CDC row before the Arrow
  hot-path. A row that leaves the filter produces a delete, so an upsert
  target drops the stale row.

## Example

### URI-based

```yaml
pipeline: shop
source:
  kind: postgres
  uri: postgres://repl:replpass@postgres:5432/shop?sslmode=disable
  slotName: urutau_shop
sink:
  uri: http://polaris:8181/api/catalog
  namespace: raw
  warehouse: quickstart_catalog
tables:
  - source: public.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
```

### Structured config with TLS and SSH

```yaml
pipeline: shop
source:
  kind: postgres
  slotName: urutau_shop
  postgres:
    host: postgres.internal
    port: 5432
    database: shop
    username: repl
    password: replpass
    ssl:
      mode: verify-full
      ca: /etc/ssl/ca.pem
    ssh:
      host: bastion.example.com
      username: ubuntu
      privateKey: /home/user/.ssh/id_ed25519
    maxThreads: 10
    retryCount: 3
    initialWaitTime: 300
sink:
  uri: http://polaris:8181/api/catalog
  namespace: raw
  warehouse: quickstart_catalog
tables:
  - source: public.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
```
