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
| D3 | Upsert: equality deletes committed, appends not yet | worker | the deletes (the batch's keys are **temporarily absent**); `cdc.position` **not** advanced | `iceberg.upsert-between-delete-and-append` |
| D4 | Sink commit done (data + `cdc.position` in the same snapshot), ack not sent | worker | data and position | `worker.committed-before-ack` |
| D5 | Ack received by the coordinator, not yet recorded | coordinator | data and position | `coordinator.ack-before-record` |

Expected recovery:

- **D1, D2, D4 (worker dies).** The worker's session ends owing work, so the
  coordinator terminates the run for a clean replay (`signalSessionEnd`, the
  supervisor's detached-owing check). The coordinator restarts, resumes from
  the durable `cdc.position`, and re-reads the batch. In D4 the batch is
  already durable, so the worker skips and acks it.
- **D3 (worker dies).** The same replay re-applies the batch: the equality
  deletes are idempotent and the appends rewrite the rows. The keys are
  missing from the sink only until the replay commits.
- **D5 (coordinator dies).** Nothing is lost: the ack's commit is already
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

- **S1–S3 (worker dies).** The cycles that still needed this worker's
  delivery can never complete. The coordinator discards exactly those cycles
  (`discardWorker`), marks the table gapped so no later cycle commits over the
  gap, and terminates the run for a clean replay. In S3 the delivery may have
  arrived, so the cycle may commit before the terminate lands. That is safe:
  the worker still owed an ack, and the replay skips what became durable.
- **S4 (coordinator dies).** Nothing of the cycle is durable. The restarted
  coordinator replays it from `cdc.position`.
- **S5 (coordinator dies).** The cycle is durable. The restart resumes past
  it, so nothing is replayed.

Orphan files left by S2–S4 are unreferenced, so readers never see them. They
stay in storage until orphan-file maintenance removes them, which only
happens when the sink's `maintenance.orphanCleanup` is configured.

## The snapshot phase

The live stream runs while the snapshot copies tables one at a time, so the
stream commits to a table, and gives it a `cdc.position`, before that table's
snapshot has run. A position therefore does not prove the snapshot finished;
`cdc.snapshot.state` does (issue #428):

- at boot, before the stream starts, every table about to be snapshotted is
  marked `not_started`;
- after a table's last window, the coordinator sends a snapshot-done marker
  behind it (on a staged table, its own cycle of the send order). The table's
  writer commits `complete` after everything sent ahead of it, with no
  position of its own;
- a table found `not_started` or `in_progress` at boot is snapshotted again.
  A table with a position and no state predates the marking and is taken as
  done.

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
| Sink commit completed, worker has not acked | D4, S3, S5 |
| Ack sent, coordinator has not recorded the next state | D5 |
| Worker session lost with delivered-but-unacked batches | D1, D2, S1 (killing the worker is the session loss) |
| Coordinator killed during an active commit cycle | S4, S5 |
| Upsert split across two snapshots (not in the base matrix) | D3 |
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
