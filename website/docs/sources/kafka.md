---
sidebar_position: 4
---

# Kafka

Streams from the committed offset, via `franz-go` with manual partition
assignment. No snapshot: Kafka has no DBLog phase, so the runner streams from
the committed offset (or the beginning on a fresh table).

The broker list goes in `uri` (e.g. `kafka:9092`). The message format is
selected with `format`:

| `format` | Decoder | Notes |
| --- | --- | --- |
| `debezium` (default) | Debezium JSON | parses the Debezium envelope into typed rows |
| `raw` | raw | lands the payload verbatim, no interpretation — append-only |
| `avro` | Confluent-Avro | resolves the writer schema by id from `schemaRegistry`, decodes nested records/arrays/maps into the canonical type system |

`avro` requires `schemaRegistry` (a Confluent-compatible HTTP registry URL).
Kafka has nothing to introspect, so `columns` must be declared explicitly for
`avro` and `raw`.

## Requirements

- For `upsert`, the topics must be key-partitioned by the primary key —
  declare `partitionedByPrimaryKey: true`. The engine cannot verify it; it is
  a conscious operator assertion. Without it, an update can land on a
  different partition than the row it replaces, and the sink cannot collapse
  them.
- `raw` and `avro` are append-only (`writeMode: append`), since Kafka carries
  no before-image on deletes.

## Behavior

- **Position** — a `(topic, partition) → offset` set. Kafka has no single
  resume coordinate; offsets are committed directly (there is no slot to hold
  back).
- **`commit_ts`** — the `commit_ts` metadata column is populated from the
  Debezium envelope's commit time when present.

## Example

```yaml title="Kafka pipeline"
pipeline: shop
source:
  kind: kafka
  uri: kafka:9092
  format: debezium
  partitionedByPrimaryKey: true
sink:
  uri: http://polaris:8181/api/catalog
  namespace: raw
  warehouse: quickstart_catalog
tables:
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
```
