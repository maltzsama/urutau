---
sidebar_position: 3
---

# Postgres

Logical replication over `pgoutput`, via `pgx`. One replication connection per
pipeline, on a logical decoding slot.

URI: `postgres://user:password@host:port/database` (a standard `pgx` DSN).
`slotName` is required — the slot is the consistency anchor.

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
