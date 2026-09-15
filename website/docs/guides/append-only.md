---
sidebar_position: 2
---

# Append-only pipelines

The default write mode is `upsert`: a row reflects the source's current
state, keyed by `primaryKey`. Some tables shouldn't collapse like that —
an audit log, an event stream, a Kafka topic with no stable key. For
those, use `writeMode: append`: every change becomes a new row, and
deletes never remove anything.

## Minimal config

```yaml
pipeline: orders-audit
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
sink:
  uri: http://localhost:8181/api/catalog
  namespace: raw
tables:
  - source: shop.orders
    target: raw.orders_log
    writeMode: append
    onDelete: record
```

Every insert, update, and delete on `shop.orders` lands as its own row in
`raw.orders_log`. Nothing is collapsed by primary key; a row that was
updated 5 times produces 5 rows.

## `onDelete`: what happens to a delete

Append mode still has to decide what a delete becomes, since there's no
row to remove:

- **`onDelete: record`** — the delete is appended as a row, carrying the
  event's before-image (the row's last known state). Requires a source
  that actually has a before-image to give you: MySQL binlog (row format)
  and Postgres logical replication both carry one. Use this when you want
  a complete change log, deletes included.
- **`onDelete: skip`** — the delete is dropped before it reaches the sink.
  Nothing is written for it at all.

**Kafka sources can't use `record`.** A Kafka message carries no
before-image — there's nothing to append. If your source is Kafka (or
any source with `format: raw`, which is always append-only), `onDelete`
must be `skip`, or `spec.Validate()` rejects the pipeline at boot with
exactly that reason.

```yaml
tables:
  - source: shop.orders-avro
    target: kafka_avro.orders
    writeMode: append
    onDelete: skip
```

## What changes underneath

**Iceberg**: upsert mode stages an equality-delete (by primary key) before
every append, so the old version of a touched row disappears. Append mode
skips that step entirely — nothing is ever deleted from the table, only
appended.

**ClickHouse**: write mode picks the table engine at creation time. Upsert
creates a `ReplacingMergeTree(seq, is_deleted)` ordered by the primary
key, with a tombstone column. Append creates a plain `MergeTree` with no
sort key and no tombstone column — just an append-only log. This is
decided once, at `createIfNotExists` time; it isn't something you can
flip later without recreating the table.

## When you'd want this instead of upsert

- **No stable primary key** — Kafka topics, webhook payloads, anything
  where "the current state of row X" doesn't mean anything.
- **You need the full history**, not just the latest state — an audit
  trail, a change log a downstream job replays.
- **The source doesn't reliably give you deletes as deletes** — with
  `onDelete: skip` you accept losing delete events rather than emitting
  a row you can't populate correctly.

If you're unsure which mode fits, the tell is this warning urutau emits
on `upsert` + `metadata: {from: op}`: in upsert mode a delete removes the
row before `op` can ever be read back as `"delete"` — if you need to see
that a row was deleted, not just that it's gone, you want `append`.

See [Sinks](../reference/sinks.md) for how each destination represents
write mode physically, and [Semantics](../reference/semantics.md) for the
full `writeMode`/`onDelete` grammar.
