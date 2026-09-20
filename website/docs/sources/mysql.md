---
sidebar_position: 2
---

# MySQL

Replication over the binlog, via `go-mysql`/canal. One replication connection
per pipeline, identified to the server by `serverId`.

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
the query connection. The worker runs the snapshot `SELECT` by re-opening the
same URI (or [`snapshotUri`](../reference/pipeline-spec.md#source) when set),
so a SELECT-only user and the same TLS/timezone settings reach it too.

## Requirements

- `binlog_format=ROW` and `binlog_row_image=FULL` — the reader decodes row
  events, and a delete carries its before image.
- `gtid_mode=ON` — the position is a GTID set, so a restart resumes exactly
  where it stopped.
- A user with `REPLICATION SLAVE` and `REPLICATION CLIENT`.

## Behavior

- **Position** — a cumulative GTID set, written atomically with every commit.
- **Charset** — a string column's bytes arrive in the column's own character
  set and are decoded to UTF-8 using its collation (`latin1`, `sjis`, `gbk`,
  ...), matching what the snapshot `SELECT` returns over a utf8mb4 connection.
  `ENUM`/`SET` are decoded to their member text.
- **Temporal** — `DATETIME`/`TIMESTAMP`/`DATE` are normalized to the
  `timezone` above on both paths, so snapshot and CDC agree (see the
  [temporal timezone](https://github.com/maltzsama/urutau/issues/139) fix).
- **`commit_ts`** — the `commit_ts` metadata column is populated from the
  transaction's commit time (microsecond precision on MySQL 8.0.1+).
- **Unsigned / unmappable columns** — `BIGINT UNSIGNED` and other types with
  no lossless canonical form are carried as `unknown`; declare a
  [`cast`](../reference/pipeline-spec.md#tables) to land them. Before
  [#180](https://github.com/maltzsama/urutau/issues/180) the introspection
  path did not read a column's unsignedness, so an unsigned column was
  declared `int64` and wrapped negative above 2^63 instead of asking for the
  cast; `BINARY(n)` similarly kept its declared length only on the CDC path.
- **`FLOAT` keeps MySQL's stored precision** — a 4-byte `FLOAT` is read as
  `float32` by both the snapshot and the CDC path and widened to `float64`
  once, so the two agree on the value MySQL actually stores: `0.1` lands as
  `0.10000000149011612`. Use `DOUBLE` for the 8-byte value. Rounding the
  widened result back to the shortest representation would make the paths
  disagree with each other and with the stored bits, so the source does not
  do it ([#181](https://github.com/maltzsama/urutau/issues/181)).

## Example

```yaml
pipeline: shop
source:
  kind: mysql
  uri: mysql://repl:replpass@mysql:3306/shop?timezone=America/Sao_Paulo
  serverId: "1101"
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
