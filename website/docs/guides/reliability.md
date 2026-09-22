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
that stops acking is **reset**, not silently dropped:

- `--ack-timeout` (`30s`) — a worker silent for this long is reset: its
  partition is re-routed and it must reconnect and re-sync from the last
  committed position.
- `--max-resets` (`5`) within `--reset-window` (`15m`) — after this many
  resets the coordinator **terminates** the job rather than loop forever.

Tune them together. A long `--ack-timeout` tolerates slow commits but
delays recovery; a small `--max-resets` fails fast but can kill a job over
a transient network blip. The defaults assume a healthy catalog and a
stable network.

## Audit trail (`--eventlog`)

`--eventlog s3://bucket/prefix` writes a JSONL audit trail of the run's
decisions — assignments, commits, resets, snapshot boundaries — one JSON
object per line. It is append-only and independent of the sink, so it
survives a sink failure. Credentials and endpoint come from the standard
`AWS_*` environment.

The trail is laid out under a shared root so it is discoverable with plain
S3 listing — no database, no index:

```
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

```yaml
spec:
  coordinator:
    eventlog:
      bucket: my-trails
      rootPrefix: urutau     # → --eventlog s3://my-trails/urutau
```

S3 credentials still come from the standard `AWS_*` environment (or the
Pod's workload identity); the trail is best-effort, so a missing credential
warns rather than failing the pipeline.

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
