---
sidebar_position: 6
---

# Commit boundaries and crash windows

This page traces one batch from the source reader to a durable `cdc.position`
and names every point where a process can die in between. It is the map the
crash-recovery matrix tests against: each boundary has a deterministic fault
point (`internal/faultinject`) that kills the process at exactly that step.

The contract being tested does not change here: **a crash may cause replay,
never loss**. Duplicate delivery is acceptable; a lost, stale or resurrected
row is not.

## What is durable, and where

- **The only durable position is the sink's `cdc.position`** (on Iceberg: a
  table property, walked back from snapshot summaries). It is written in the
  same commit as the data it covers.
- The coordinator's confirmed position (`confirmedPosition`, the minimum
  acked position across workers) is **in memory**. It only drives source
  retention (a PostgreSQL slot); MySQL and Kafka ignore it.
- On every boot the coordinator resumes from the **minimum `cdc.position`
  across tables** (`resumeFrom`). A table without one is snapshotted, unless
  its source cannot snapshot (Kafka), in which case it streams from the
  source's start. So is a table whose `cdc.snapshot.state` is `not_started` or
  `in_progress`, position or not (see [The snapshot phase](#the-snapshot-phase)).
- A worker skips any batch at or before its table's committed position
  (`skipCovered`) and acks it, so a replay re-applies only what is not
  durable.

## The two commit paths

Which path a batch takes is decided per batch by the coordinator
(`BatchMeta.staged`): a table with more than one owner on a staging sink
(Iceberg) is **staged**; everything else commits **directly**.

### Direct path (one owner, or a non-staging sink)

| # | Step | Process | Durable after this step | Fault point |
|---|------|---------|-------------------------|-------------|
| D1 | Batch received from the Flight stream, not yet applied | worker | nothing new | `worker.batch-received` |
| D2 | Batch collapsed and handed to the committer, not yet committed | worker | nothing new | `worker.commit-before` |
| D3 | Sink commit done (equality deletes + data + `cdc.position` in one snapshot), ack not sent | worker | deletes, data and position | `worker.committed-before-ack` |
| D4 | Ack received by the coordinator, not yet recorded | coordinator | deletes, data and position | `coordinator.ack-before-record` |

Expected recovery:

- **D1, D2, D3 (worker dies).** The coordinator keeps running and awaits the
  worker (`loseWorker`): a new epoch, and the batch kept on its sent list.
  When the Pod reconnects, the batch is redelivered. In D3 it is already
  durable, so the worker skips and acks it. The run ends only if the worker
  delivers nothing of what it owes for the delivery timeout, or crashes three
  times in a row without delivering it.
- **D4 (coordinator dies).** Nothing is lost: the ack's commit is already
  durable. The restarted coordinator resumes from `cdc.position`, and workers
  skip what they had committed.

### Staged path (partitioned table on a staging sink)

The coordinator splits one binlog batch into one sub-batch per partition that
has rows, and groups them by batch id into a **cycle** that commits as one
unit, in send order (`stagedCycles`).

| # | Step | Process | Durable after this step | Fault point |
|---|------|---------|-------------------------|-------------|
| S1 | Sub-batch received, not yet applied | worker | nothing new | `worker.batch-received` |
| S2 | Data files written (`WriteStaged`), descriptor not shipped | worker | orphan data files, not referenced by any snapshot | `worker.staged-before-ship` |
| S3 | Descriptor shipped, ack not sent | worker | orphan data files | `worker.staged-shipped-before-ack` |
| S4 | Cycle complete (every owner delivered), `CommitStaged` not called | coordinator | orphan data files | `coordinator.cycle-before-commit` |
| S5 | `CommitStaged` done (one RowDelta with every delivery's files and the cycle's minimum position), confirmed position not recorded | coordinator | data and position | `coordinator.cycle-committed-before-record` |

Expected recovery:

- **S1–S3 (worker dies).** The cycles that still need this worker's delivery
  stay open, and so does every later cycle of the table, since cycles commit
  in send order: nothing commits over the gap. When the worker reconnects, its
  sub-batches are redelivered, restaged and delivered, and the cycles
  complete. In S3 the delivery had arrived: the redelivered one is dropped
  (one delivery per owner), and the worker skips the sub-batch once the cycle
  is durable.
- **S4 (coordinator dies).** Nothing of the cycle is durable. The restarted
  coordinator replays it from `cdc.position`.
- **S5 (coordinator dies).** The cycle is durable. The restart resumes past
  it, so nothing is replayed.

Orphan files left by S2–S4 are unreferenced, so readers never see them. They
stay in storage until orphan-file maintenance removes them, which only
happens when the sink's `maintenance.orphanCleanup` is configured.

### ClickHouse: the position travels on the data

ClickHouse is not a staging sink — it does not implement the staged writer —
so it takes the direct path only, for every table: a single owner, or each
partition owner, commits its own sub-batches. There is no coordinator cycle
and no orphan-file step.

The sink commits one batch as a **single INSERT** — upserts as rows, deletes
as tombstone rows (`is_deleted=1`, key set, everything else zero) that
`ReplacingMergeTree(seq, is_deleted)` resolves, newest `seq` per key wins.
The batch's position travels on **every row** of that one insert, so it can
never separate from the data. In addition, when the worker has an owner, the
commit writes a per-partition control row (`<table>_urutau_position`, one row
per owner holding `owner, position, seq`); `Position()` reads the MinSafe
across the current owners from it (see [state and position](./state-position.md)).

That leaves one window the worker-side boundary points do not name — they fire
before and after the whole commit, not between its two writes:

| # | Step | Process | Durable after this step | Fault point |
|---|------|---------|-------------------------|-------------|
| C1 | Data INSERT done (the position is on every row), the per-partition control row not yet written | worker | the batch's rows and their position | `worker.clickhouse-data-before-position` |

Expected recovery: **C1 (worker dies).** The rows are durable. The control
table has no entry for this owner, so `Position()` finds no per-owner minimum
and falls back to `argMax(position, seq)` over the rows of the INSERT — the
position never separated from the data. The restarted worker skips the batch;
nothing is lost, and ReplacingMergeTree still picks the highest `seq` per key,
so nothing stale wins. A crash here is thus a replay boundary, never a loss
boundary, and the ClickHouse half of the matrix asserts exactly that: the
sink converges to the source after each direct-path fault, including this one.

### Couchbase: one document per row, position in a control document

Couchbase is not a staging sink either, so **both of its commit modes** take
the direct path: the worker commits each sub-batch itself, there is no
coordinator cycle, and the direct boundaries apply to both. The sink is
key-addressed — one document per row, keyed by the primary-key tuple, upsert
replaces in place, delete removes the document outright (no tombstone, no
versioning, no merge) — so replaying a batch rewrites exactly the same keys and
can never duplicate a row.

The two modes differ only in how the commit's two writes are sequenced:

- **fast** (default): the data documents are written first, the
  position-carrying **control document** last. A crash in between leaves the
  data durable and the position un-advanced; the restart replays the batch,
  rewriting the same documents, then advances the position. This is the one
  internal window the worker-side points do not name — they fire before and
  after the whole commit, not between its two writes.
- **atomic**: data documents and the control document are written inside one
  gocb distributed transaction, so a failure mid-batch leaves no trace at all.
  There is no data-before-control window to name; the direct boundaries D1–D4
  cover it, and a crash before the commit leaves nothing durable.

| # | Step | Process | Durable after this step | Fault point |
|---|------|---------|-------------------------|-------------|
| CB1 | Fast mode: data documents durably upserted/removed, the control document (position) not yet written | worker | the batch's documents | `worker.couchbase-data-before-control` |

Expected recovery: **CB1 (worker dies).** The documents are durable; the
control document still holds the *previous* position, so the restart resumes
where it left off and replays the batch — idempotent by key. The position then
advances exactly once. As with ClickHouse, this is a replay boundary, never a
loss boundary, and the Couchbase half of the matrix asserts exactly that: the
sink converges to the source after each direct-path fault, in both commit
modes.

### PostgreSQL source: the slot never passes the committed position

The PostgreSQL source does not change the commit paths — the sink is still
Iceberg, so a table commits directly (one owner) or staged (several owners).
It adds a **source-side retention boundary** instead. The logical replication
slot is the server's anchor: `confirmed_flush_lsn` is the point up to which the
server may recycle WAL, so it must never move past what the sink has durably
committed. Advancing it too far would let the server discard WAL for events the
sink never committed — the data would be unrecoverable on the next resume.

The reader advances the slot by sending a standby status update
(`sendStandby`), reporting the coordinator's confirmed position — the minimum
`cdc.position` across the tables it feeds, never its own decoded position. So
the slot lags the sink by construction; the crash window is between the two:

| # | Step | Process | Durable after this step | Fault point |
|---|------|---------|-------------------------|-------------|
| PG1 | The sink commit (data and `cdc.position`) is durable; the reader has not yet reported the committed position back to the server | coordinator | the batch's data and position; the slot's `confirmed_flush_lsn` is unchanged | `source.postgres-slot-confirm-before` |

Expected recovery: **PG1 (coordinator dies).** Nothing is lost — the sink
commit is already durable, and the slot is *behind* it, the safe direction. On
restart the coordinator resumes from the minimum `cdc.position` and reconciles
the stored resume with the slot (`ValidateSlotState`): a resume ahead of the
slot (the sink committed past the last confirmed point) is returned as the
effective start and the slot is advanced to exactly that point (`AdvanceSlot`).
The PostgreSQL half of the matrix asserts the invariant directly after every
recovery, reading `pg_replication_slots.confirmed_flush_lsn` and the sink's
committed `cdc.position`: the former never exceeds the latter.

## The snapshot phase

The live stream runs while the snapshot copies tables one at a time, so the
stream commits to a table, and gives it a `cdc.position`, before that table's
snapshot has run. A position therefore does not prove the snapshot finished;
`cdc.snapshot.state` does:

- at boot, before the stream starts, every table about to be snapshotted is
  marked `not_started`;
- after a table's last window, the coordinator sends a snapshot-done marker
  behind it (on a staged table, its own cycle of the send order). The table's
  writer commits `complete` after everything sent ahead of it, with no
  position of its own;
- a table found `not_started` or `in_progress` at boot is snapshotted again.
  A table with a position and no state predates the marking and is taken as
  done.

A snapshot records its progress before its first chunk: the chunk bounds,
the partition ranges, and every chunk pending. The chunk is the scheduling
unit (its bounds come from a keyset seek), but the reader cuts each
chunk into **byte-capped windows** on the worker: the window size is derived
from the worker's memory limit, never a flag. Each window's Closes marker
names the chunks still to do after it, and the worker commits them as
`cdc.snapshot.pending`, state `in_progress`, in the same commit as the
window's rows. A restarted coordinator resumes an `in_progress` table from
there, with the recorded bounds, when the partition ranges are unchanged; it
starts the table over otherwise, since chunk ids are relative to the ranges.

A window's rows commit **no position**. Its Closes marker is sent at the
reader's position, which can be past stream batches of the table still on
their way through the pump; committed as the table's `cdc.position`, it would
cover them, and a crash would skip them on replay. Only the
stream advances a table's position. After a crash the stream replays from it
over the window's rows, which converges: every event carries its row's full
image. The worker acks a Closes marker by its batch id once the window's rows
are committed, and the coordinator releases the marker on nothing else.

The **worker** owns the window id and the position. It announces each window
with `WindowOpen(attempt, seq, pos)` as it reads, capturing `pos` *after* the
page's read, so the caught-up proof gates any event the window's SELECT could
have seen as pre-image. The window id is a `uint64` in the worker's own
namespace (`attempt` epoch plus a monotonic `seq`), never the coordinator's
`chunk_id`. The coordinator closes each window with its own `Closes` after
proving the reader caught up to that window's position — not the chunk's — and
releases the window's gated live rows ahead of the marker.

A worker lost mid-snapshot takes its windows with it. The coordinator does not
end the run: once the worker is back, the partition redoes, under fresh
window ids, every chunk whose Closes marker the worker had not committed, and
it waits for its last windows to commit before it is done. A chunk the earlier
generation committed **partially** is resumed from the last committed window's
high key, so already-committed windows are not re-emitted (an append-only
table would otherwise duplicate them).

| # | Step | Process | Durable after this step | Fault point |
|---|------|---------|-------------------------|-------------|
| P1 | About to snapshot a table the stream may already have committed to | coordinator | stream commits (a `cdc.position`), state `not_started` | `coordinator.snapshot-table-start` |

Expected recovery: **P1 (coordinator dies).** The restarted coordinator finds
the table `not_started` and snapshots it, so its pre-existing rows are copied
even though it holds a position. Re-copied rows are upserts.

## How the crash-recovery matrix's failure windows map here

| Failure window | Boundaries |
|-----------------|------------|
| Batch delivered to worker, before commit | D1, D2, S1 |
| Sink write staged, before commit | S2, S4 |
| Sink commit completed, worker has not acked | D3, S3, S5 |
| Sink data written, per-partition control position row not yet (ClickHouse) | C1 |
| Sink data documents written, control/position document not yet (Couchbase fast mode) | CB1 |
| Sink commit durable, PostgreSQL slot's confirmed position not yet reported to the server | PG1 |
| Ack sent, coordinator has not recorded the next state | D4 |
| Worker session lost with delivered-but-unacked batches | D1, D2, S1 (killing the worker is the session loss; recovered by redelivery) |
| Coordinator killed during an active commit cycle | S4, S5 |
| Coordinator killed before a table's snapshot, after the stream committed to it | P1 |

## Using the fault points

The fault points are compiled only into the race-instrumented e2e image
(`build/Dockerfile.race` builds with `-tags faultinject`). In every other build
they are empty functions: no file is read and there is no branch in the hot
path.

In the e2e image, a fault point is armed by writing a file inside the Pod:

```
kubectl exec <pod> -- sh -c 'printf "point=worker.committed-before-ack\ntable=raw.orders\n" > /tmp/urutau-fault'
```

| Key | Meaning |
|-----|---------|
| `point` | Required. The boundary name from the tables above. |
| `table` | Optional. Fire only for this target table. |
| `skip` | Optional. Let this many matching hits pass first, then fire. |

When the boundary is reached, the process removes the file, writes one line
to stderr and ends itself without running any deferred cleanup: SIGKILL, or an
immediate exit when it runs as the container's PID 1 (the kernel ignores a
signal PID 1 sends to itself, so SIGKILL alone would leave it hung). The line
names the boundary, the table, the batch and the positions, for example:

```
urutau: FAULT INJECTED point=worker.committed-before-ack table=raw.orders seq=42 position=...:1-1817
```

The container restarts in the same Pod, which is what the recovery paths
above are tested against. Removing the file first makes the fault one-shot:
the restarted process is not armed.
