---
sidebar_position: 1
---

# Sources

One replication reader per source, mapped through the canonical type system
(`core.Schema`). Each source page covers its connection URI, the server-side
requirements, and how it handles position, schema, and deletes.

| Source | Library | Position | Snapshot |
| --- | --- | --- | --- |
| [MySQL](mysql.md) | `go-mysql`/canal | GTID | DBLog chunk `SELECT` |
| [Postgres](postgres.md) | `pgx`, pgoutput | LSN slot | DBLog chunk `SELECT` |
| [Kafka](kafka.md) | franz-go, manual partition assignment | offset | none — streams from the committed offset |

All three map into the same canonical schema and emit the same wire format, so
everything downstream (worker, sink) is source-agnostic. See
[Writing a plugin](../guides/plugins) if you're building a source that isn't
one of these three.
