---
sidebar_position: 3
---

# Sinks

## Iceberg

Upsert via equality delete, **delete-then-append as two separate commits**
(not one transaction — see [Architecture: E2E spike](../architecture/overview#e2e-spike)
for why append-then-delete-in-one-commit silently deletes the row it just
inserted).

Position is committed twice: as a snapshot property (audit trail, every
commit) and as a table property (O(1) resume, survives compaction).

**Nested columns:** `Struct` and `List` map to native Iceberg types and
round-trip correctly (proven end to end against a real catalog + Trino).
`Map` does not — `iceberg-go` v0.6.0's `AppendTable` rejects a map column
at write time. Cast a `Map` column to `string` (JSON) until iceberg-go
fixes this upstream.

## ClickHouse

Upsert via `ReplacingMergeTree(seq, is_deleted)` (`ORDER BY` the declared
primary key); append via plain `MergeTree`. One `INSERT` per batch —
upserts land as rows, deletes as tombstones hidden from `FINAL` reads.
Resume reads the position from the data itself (`argMax(position, seq)`),
never a separate control table.

**Partitioned tables (`workers: N`) keep the position per partition.** A
collapsed pipeline (`workers: 1`) writes one position scalar into the data
rows, as above. When a table is split across N workers, each owner records
its own coordinate in a side table `<target>_urutau_position`
(`owner, position, seq`), and `Position()` returns the **minimum safe**
across owners — never one partition's `argMax`, which would resume past a
lagging partition. Both paths share the same data table; only the resume
read differs.

Nested columns map natively: `List` → `Array`, `Map` → `Map`, `Struct` →
`Tuple`.

**Atomicity is narrower than Iceberg's.** The default table has no
`PARTITION BY`, which is what makes a batch atomic — ClickHouse guarantees
atomicity per insert, per partition, not across a multi-partition write.
Partitioning is opt-in and weakens that guarantee: a batch crossing a
partition boundary commits as multiple parts, no longer all-or-nothing.
Tombstone physical cleanup is operator maintenance
(`OPTIMIZE ... FINAL CLEANUP`); reads stay correct under `FINAL` whether or
not cleanup has run.

**Partitioned append is at-least-once.** With `workers: 1` the position
travels in the data rows, so one `INSERT` carries both and a crash is atomic.
A partitioned table (`workers: N`) commits the data and its per-owner
position in two separate statements; a crash between them leaves the rows
durable with the position un-advanced, and the restart replays the batch.
Upsert collapses the replay onto the same row (`ReplacingMergeTree(seq)`);
append has no key to collapse on, so the rows land a second time. That is the
at-least-once contract (`semantics.md`: duplicates are possible, loss is not)
— a table that must not duplicate should use `writeMode: upsert`.

## Couchbase

Key-document writes where upsert-by-key *is* the native operation — one
collection per table, one control document per collection carrying the
committed position (a single O(1) `Get` on resume, never a scan or
aggregation). Data fields live at the document's top level; pipeline
metadata lives under a reserved `_urutau` sub-object, so a data column
named `op` never collides with the metadata `op`. Deletes are `Remove`,
immediate, not tombstones.

Nested columns (`Struct`/`List`/`Map`) land as native JSON — the one sink
where nesting needs no special case.

**Commit mode is a choice, not a caveat.**

| `commitMode` | Behavior |
| --- | --- |
| `fast` (default) | data first, control document last. A crash in between leaves the position un-advanced; the restart replays the batch, which is idempotent (every mutation is keyed by primary key). |
| `atomic` | data + control document in one distributed ACID transaction. Closes the crash window; costs transaction overhead per batch. |

Every write acknowledges at synchronous-durability `majority`; on a single
node that requires a 0-replica bucket (which is what the sink creates) —
`DurabilityImpossible` is a loud error, never a silent downgrade.

**Partitioned tables (`workers > 1`) require `commitMode: atomic`.** The
`fast` path reads and rewrites the control document outside any
transaction, so two workers of the same table lose each other's position
and snapshot properties. `atomic` runs that read-modify-write inside the
distributed transaction, which serializes across pods. The coordinator
refuses to boot a partitioned Couchbase table whose sink is not in atomic
mode. (Append and upsert share the same commit path here, so the rule is
identical for both.)

## Cross-cutting

- **Metadata columns** — closed catalog, CDC (`op`, `commit_ts`,
  `ingest_ts`, `position`, `source_table`, `phase`) and transport-native
  (`stream`, `shard`, `sequence`, `msg_ts`, `msg_key`, `headers`) — land as
  nullable columns at the end of the schema, renamed per-table via
  `metadata`.
- **Per-column cast** — a closed matrix: widening always allowed,
  to-string always allowed, narrowing/parsing never (except the explicit
  temporal reinterpretation `timestamptz(assume_utc)`). An unmappable
  source type bypasses the cast rather than silently coercing.
- **Write modes** — `upsert` (reflect state by primary key), `append`
  (every change is a new row), `append-idempotent` (physically append, but
  the transport's own monotonic coordinate — partition+offset for Kafka —
  makes duplicates provably absent and cheaply removable).
