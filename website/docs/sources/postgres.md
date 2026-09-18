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
| `maxThreads` | 10 | Max concurrent connections for snapshot chunk SELECTs (1..32) |
| `retryCount` | 0 | Transient-connection retries with exponential backoff |

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
