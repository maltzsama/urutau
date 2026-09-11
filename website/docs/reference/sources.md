---
sidebar_position: 2
---

# Sources

One replication reader per source, mapped through the canonical type
system (`core.Schema`).

| Source | Library | Position | Snapshot |
| --- | --- | --- | --- |
| MySQL | `go-mysql`/canal | GTID | DBLog chunk `SELECT` |
| Postgres | `pgx`, pgoutput | LSN slot | DBLog chunk `SELECT` |
| Kafka | franz-go, manual partition assignment | offset | none — streams from the committed offset |

## Kafka

No snapshot: `Capabilities{Stream: true}` with no snapshot capability, so
the runner skips DBLog entirely for a Kafka source table.

Message format is selected per pipeline via `source.format`:

| `source.format` | Decoder | Notes |
| --- | --- | --- |
| `debezium` (default) | `DebeziumJSON` | parses the Debezium envelope into typed rows |
| `raw` | `Raw` | lands the payload verbatim, no interpretation — append-only |
| `avro` | Confluent-Avro | resolves the writer schema by id from `source.schemaRegistry`, decodes nested records/arrays/maps into the canonical type system |

`avro` requires `source.schemaRegistry` (a Confluent-compatible HTTP
registry URL). See
[Writing a plugin](../guides/plugins) if you're building a source that
isn't one of these three.
