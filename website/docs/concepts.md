---
sidebar_position: 2
---

# Concepts

The mental model behind every pipeline, in one page. Read this before the
guides — it is what the field names and the guarantees actually mean.

## The job: reflect source state

A CDC pipeline reads a database's change log and writes the *current state*
of a table into a destination. When a row is updated or deleted at the
source, the destination row is updated or deleted too — it does **not**
accumulate one version per change.

That is the difference between Urutau and a log-shipper: the destination is
a **table that reflects the source**, not an append-only event stream. You
query it like a table, because it is one.

If you *do* want the raw event history, that is a different `writeMode`
(`append`) — see [Write modes](#write-modes) below.

## The two halves

Urutau's engine has two roles, and you can run them together or apart:

- The **coordinator** connects to the source, runs the initial snapshot,
  splits each table's key range, and routes changes to workers.
- A **worker** owns one slice of a table and writes it to the destination.

`urutau run` is the **collapsed** mode: one process, both roles, no network
in between. It is what the [Quickstart](quickstart.md) uses.

`urutau-coordinator` + `urutau-worker` are **distributed** mode: the same
roles as separate processes, so one hot table can be split across many
workers. See [Distributed mode](guides/distributed.md).

Kubernetes does not add a third mode — the operator just schedules the same
coordinator and workers for you. See
[Deploy on Kubernetes](guides/deploy-kubernetes.md).

## The lifecycle: snapshot, then stream

A table with no recorded position is **backfilled first**: the coordinator
reads the existing rows in chunks (a "snapshot"). Only once the snapshot is
caught up does it switch to **streaming** the live change log. The handoff
is safe under concurrent writes — a row changed during the snapshot is
reconciled by key, not duplicated.

After that, the pipeline is a long-lived process streaming changes. It is
not a batch job you re-run.

## Where the position lives

The most important design decision: **the committed position lives in the
destination, written in the same commit as the data it describes.**

There is no separate checkpoint store to drift out of sync. If the data is
there, the position that produced it is there too. A restart reads the
position back out of the destination and resumes — it does not re-snapshot.

This is what makes recovery boring: nothing durable lives in the coordinator
or the workers. Kill them all, restart, and they pick up where the
destination says they left off.

The one exception is external plugin sinks that cannot store a position
themselves; see [Position](architecture/state-position.md).

## Delivery: at-least-once

After a crash, a batch that was not durably committed is replayed.
Duplicates are possible; **loss is not**. The destination must therefore be
idempotent by key — and every built-in sink is, which is why replay is
safe.

The full contract (ordering, cross-table atomicity, what happens to a
poison batch) is in [Delivery guarantees](reference/guarantees.md). Read it
before depending on a behavior you have not seen in an example.

## Write modes

Each table declares how changes map onto the destination:

| `writeMode` | What a change does | Use when |
| --- | --- | --- |
| `upsert` (default) | Replaces the row at `primaryKey` — updates and deletes are reflected | You want a mirror of the source table |
| `append` | Every change is a new row; nothing is updated | You want the change history, not the state |
| `append-idempotent` | Appends, but a transport coordinate makes replay provably a no-op | History, but the source has a stable offset (e.g. Kafka) |

`upsert` requires a `primaryKey`. See
[CDC upsert](guides/cdc-upsert.md) and
[Append-only pipelines](guides/append-only.md).

## The spec is the artifact

One YAML document describes the whole job: where rows come from (`source`),
where they go (`sink`), and which tables map to which (`tables`). The same
spec feeds `urutau run`, the coordinator, and a Kubernetes `CDCPipeline` —
and the same validator checks it in all three places.

Field-by-field: [Pipeline specification](reference/pipeline-spec.md).

## Next

- Get one running: [Quickstart](quickstart.md).
- The exact behavior: [Delivery guarantees](reference/guarantees.md).
- The pieces under the hood: [Architecture overview](architecture/overview.md).
