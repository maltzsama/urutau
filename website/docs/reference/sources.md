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

## MySQL

URI: `mysql://user:password@host:port/database`. Query parameters tune the
connection and how temporal columns are interpreted:

| Parameter | Default | Notes |
| --- | --- | --- |
| `timezone` | `UTC` | IANA location (e.g. `America/Sao_Paulo`). Applied to both the snapshot `SELECT` and the binlog decode, so a row's `DATETIME`/`TIMESTAMP`/`DATE` value is identical whichever path reads it. A naive `DATETIME` is interpreted in this zone; a `TIMESTAMP` keeps its instant. |
| `tls` | `false` | `true`, `verify-ca`, `verify-full`, `skip-verify`, or `false`. `verify-ca` checks the chain but not the hostname; `verify-full` checks both. |
| `ssl-ca` | — | Path to a CA bundle (PEM) used to verify the server. |
| `ssl-cert` / `ssl-key` | — | Client certificate/key (PEM) for mutual TLS; set together. |
| `ssl-server-name` | the host | Expected server name for verification / SNI. |

`timezone` and the TLS parameters apply to both the replication connection and
the query connection (the worker's snapshot `SELECT` re-opens the same URI, or
`snapshotUri` when set).

Notes:

- **Charset**: a string column's bytes arrive in the column's own character
  set and are decoded to UTF-8 using its collation (`latin1`, `sjis`, `gbk`,
  ...), matching what the snapshot `SELECT` returns over a utf8mb4 connection.
  `ENUM`/`SET` are decoded to their member text.
- **`commit_ts`**: the `commit_ts` metadata column is populated from the
  transaction's commit time (microsecond precision on MySQL 8.0.1+).
- Requires `binlog_format=ROW`, `binlog_row_image=FULL`, and GTID
  (`gtid_mode=ON`); the reader resumes by GTID set.

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
