---
sidebar_position: 8
---

# Reliability

What survives a crash, how a pipeline heals itself, and how to reconstruct
what it did. This page is about the *durable* side of running a pipeline.

For the live signals, see [Monitoring](monitoring.md). For the exact
delivery contract, see [Delivery guarantees](../reference/guarantees.md).

## Recovery is the default

The committed position lives in the destination, written in the same commit
as the data (see [Position](../architecture/state-position.md)). So a
restart is always safe:

- Kill the process mid-batch → the batch is replayed from the last
  committed position. Duplicates are possible, loss is not.
- Kill the whole cluster → bring it back, and it resumes from the
  destination. It does not re-snapshot.

There is no checkpoint store to keep in sync and no recovery procedure
beyond "start it again".

## Supervision: the coordinator heals workers

The coordinator supervises its workers, not the other way around. A worker
that dies or stops acking is **recovered**, not dropped, and the job keeps
running:

- A worker that is **lost** — its Pod killed or OOM-killed, its stream cut —
  is awaited. The StatefulSet brings the Pod back under the same name; it
  reconnects, and the coordinator redelivers what it owed: every batch it
  was sent but had not acked, then its queue. The worker skips what its
  committed position already covers, so nothing is applied twice on an
  upsert table.
- A worker that stops acking for `--ack-timeout` (`30s`) while it owes work
  is **reset** and recovered the same way. A worker whose uploads keep
  flowing is busy on slow storage, not stalled, and is left alone.
- A lost worker in the middle of a snapshot takes its chunk windows with it.
  Once it is back, its partition redoes the chunks whose rows it had not
  committed; the other partitions and tables go on.
- A restarted **coordinator** resumes a table's snapshot from its recorded
  progress (`cdc.snapshot.pending`), not from its first chunk. Its workers
  retry every few seconds (at most 5s apart) and resolve its name on every
  attempt, so they find it again within seconds of its return, well inside
  the time it waits for them. At boot it waits until each worker has
  connected once; a worker lost again after that is recovered like any
  other lost worker, not awaited by the boot.

The job ends only for what does not heal by itself:

- an error the worker reports itself (schema drift, a failed commit);
- `--max-consecutive-crashes` (`3`): a crash loop. The same worker crashes
  that many times in a row without delivering what it owed when it came
  back — a batch that OOM-kills it every time. The error names the worker,
  its committed position, and the Pod's last termination reason;
- `--worker-delivery-timeout` (`5m`): a worker that owes work and delivers
  none of it for that long, connected or not — a Pod that cannot be
  scheduled, a network partition that never heals, a table's only worker
  killed again every time it comes back. It holds during the snapshot too.
  The error carries the reason Kubernetes gives;
- a stalled worker on an **append** table with unacked batches: redelivering
  a batch that was committed before its ack was lost would append it twice.

Whether a loss was a crash is read from the worker's Pod when it comes back:
an OOM kill, a panic or an error is a crash; a Pod replaced from outside (a
drain, a pod-kill) is not, and neither is a worker that exited because it
lost the coordinator, which it records as `network:` in its termination
message. The count starts over once the worker has delivered everything it
owed when it came back and stayed up for an ack timeout, so crashes spaced
out by healthy stretches — hours apart on a quiet table — never add up.
`--max-resets` and `--reset-window` are deprecated and ignored.

## Audit trail (`--eventlog`)

`--eventlog s3://bucket/prefix` writes a JSONL audit trail of the run's
decisions — assignments, commits, resets, snapshot boundaries — one JSON
object per line. It is append-only and independent of the sink, so it
survives a sink failure. Credentials and endpoint come from the standard
`AWS_*` environment.

The trail is laid out under a shared root so it is discoverable with plain
S3 listing — no database, no index:

```text title="Audit trail path convention"
s3://<bucket>/<prefix>/<pipeline>/run-<id>/events-NNNNNN.jsonl
```

Listing `<prefix>/` yields the pipeline names; listing
`<prefix>/<pipeline>/` yields the run ids. A pipeline's trail stays
discoverable after its `CDCPipeline` is deleted, because the pipeline name
is in the key. (A direct `urutau run --eventlog` with no named pipeline
omits the `<pipeline>/` segment.)

Use it for post-incident forensics: "why did this row land late?" is
usually answerable from the event log even after the metrics have rolled
over.

On Kubernetes, set it in the `CDCPipeline` — the operator renders the same
flag. Without it, an operator-managed pipeline writes **no** trail:

```yaml title="Eventlog configuration"
spec:
  coordinator:
    eventlog:
      bucket: my-trails
      rootPrefix: urutau     # → --eventlog s3://my-trails/urutau
```

S3 credentials come from the standard `AWS_*` environment (or the Pod's
workload identity). For an S3-compatible store (MinIO, RustFS), set
`eventlog.endpoint` (rendered as `--eventlog-endpoint`, path-style addressing)
and `eventlog.secret`, a Secret with `accessKeyId` and `secretAccessKey` that
the operator mounts into the coordinator only. The trail is best-effort, so a
missing credential warns rather than failing the pipeline.

The trail also carries every coordinator and worker log line (`kind=log`), so
a run's logs outlive its Pods: a replaced coordinator Pod starts a new run in
the same pipeline's trail, and nothing it logged before is lost with it.

## Checkpoints (`--checkpoint`)

`--checkpoint s3://bucket/prefix` additionally writes **async position
manifests** every `--checkpoint-interval` seconds (default 10). These are a
convenience for external monitoring and for auditing how far the pipeline
has progressed.

They are **not** the recovery source of truth — the destination is. The
checkpoint is a progress report, not a resume point.

## Next

- **The state model behind all of this**:
  [Position](../architecture/state-position.md).
- **The delivery contract**: [Delivery guarantees](../reference/guarantees.md).
- **Live signals**: [Monitoring](monitoring.md).
