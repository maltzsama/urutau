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

### Table maintenance

`sink.maintenance` is Iceberg-only: a maintenance block on any other sink
type is a validation error, not a silently-ignored setting, so a pipeline
cannot look like it is compacting when nothing will run. It enables
compaction, snapshot expiry, and orphan file cleanup, disabled by default:

```yaml
sink:
  maintenance:
    enabled: true
    compaction:
      interval: 5m
      targetFileSize: 512Mi
      minInputFiles: 5
    snapshotExpiry:
      interval: 10m
      retainLast: 1
      maxAge: 168h
    orphanCleanup:
      interval: 1h
      olderThan: 72h
```

Each sub-block is independently optional — a table declaring only
`snapshotExpiry` never runs compaction or orphan cleanup.

**Maintenance runs in its own ephemeral worker, not in the coordinator.** The
coordinator only *schedules*: it provisions one maintenance worker per table —
a Deployment, exactly like the data workers, cloned from that table's own
worker pod template — and pushes the operations that are due when the worker
connects. The worker runs the pass and exits; its Deployment restarts it for
the next turn. So a long compaction never competes with the coordinator's
routing and commit path, and never takes the coordinator down with it. (In
the collapsed single-process runner there is no worker to launch, so the pass
runs in-process on the same schedule.) Because the pass is one-shot, the
three operations run in order — compaction, then snapshot expiry, then orphan
cleanup — so a compaction never races the expiry that dereferences the files
it just wrote.

**Compaction** rewrites small files into `targetFileSize`-sized ones once a
partition group has at least `minInputFiles` candidates. It needs no
safety window: a concurrent CDC commit that deletes a row in a file being
rewritten is caught by `iceberg-go`'s own rewrite conflict validator, and
the same retry path an ordinary commit uses resolves it — this holds for
both `upsert` and `append` tables (an append-only table never produces
delete files, so there is nothing to conflict with in the first place).
Every compaction commit also re-attaches the table's current `cdc.position`
property, so resume stays on the O(1) fast path instead of falling back to
the snapshot-summary walk-back.

**Snapshot expiry**'s `maxAge` is the one setting in this feature that is a
genuine safety window, not just a retention knob. The committed position
lives in two places: the `cdc.position` **table property** (the fast-resume
path above) and each snapshot's **summary** (the walk-back fallback, read
only when the property is absent). Snapshot expiry cannot prune the table
property — it is in the table metadata, not in a snapshot — but it does
prune the summaries the fallback needs. Expiring those before a stopped
pipeline recovers can strand recovery, or resume further ahead than what was
actually committed. Set `maxAge` to cover the longest downtime you're willing
to tolerate before giving up on resuming from where the pipeline left off —
this applies identically to `upsert` and `append` tables.

**Orphan cleanup**'s `olderThan` is a different, narrower safety window: it
protects a file a concurrent read or in-flight commit might still
reference, using `iceberg-go`'s own default (`72h`) unless overridden.

Only one maintenance pass runs per table at a time: a worker is never handed
a second assignment while its previous pass is still running, so two passes
cannot race the catalog.

Full file: [`examples/iceberg-maintenance.yaml`](https://github.com/maltzsama/urutau/blob/main/examples/iceberg-maintenance.yaml).

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
at-least-once contract ([Delivery guarantees](guarantees.md): duplicates are possible, loss is not)
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
