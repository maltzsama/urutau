---
sidebar_position: 7
---

# Operations

What to watch, and what to turn on, once a pipeline is running for real.
Everything here applies to collapsed mode (`urutau run`) and distributed
mode alike; the coordinator owns most of it.

## Metrics

Pass `--metrics-addr :9090` (coordinator) or `--metrics-addr :9091`
(worker) to serve Prometheus metrics. Empty disables the endpoint.

Coordinator metrics:

| Metric | Type | Meaning |
| --- | --- | --- |
| `urutau_coordinator_lag_seconds` | gauge | Reader-to-worker lag |
| `urutau_coordinator_inflight_bytes` | gauge (worker) | Unacked batch bytes per worker |
| `urutau_coordinator_worker_resets_total` | counter (reason) | Worker resets by reason |
| `urutau_coordinator_commits_total` | counter (table) | Commits acked by the worker |
| `urutau_coordinator_events_decoded_total` | counter | Decoded source events |

Worker metrics:

| Metric | Type | Meaning |
| --- | --- | --- |
| `urutau_worker_rows_written_total` | counter (table, op) | Rows written |
| `urutau_worker_commit_duration_seconds` | histogram (table) | Iceberg commit latency |
| `urutau_worker_commit_failures_total` | counter (table) | Failed commits |
| `urutau_worker_equality_deletes_written_total` | counter (table) | Equality deletes written |
| `urutau_worker_snapshot_progress_ratio` | gauge (table) | Snapshot progress, 0..1 |
| `urutau_worker_dblog_dropped_by_window_total` | counter (table) | Snapshot rows discarded by DBLog windows |
| `urutau_worker_deletes_dropped_total` | counter (table) | Append-only deletes dropped |

Enrichment:

| Metric | Type | Meaning |
| --- | --- | --- |
| `urutau_enrich_misses_total` | counter | Left-join misses (event passed with NULLs) |
| `urutau_enrich_inner_dropped_total` | counter | Events dropped by an inner-join miss |
| `urutau_enrich_evicted_total` | counter | Events evicted from the cold-start buffer |

### What to alert on

- `urutau_coordinator_lag_seconds` climbing — the reader is falling behind
  the source. Usually the sink, not the reader.
- `urutau_coordinator_worker_resets_total` rising — workers are crashing or
  the network is flapping. Watch the `reason` label.
- `urutau_worker_commit_failures_total` non-zero and rising — the catalog
  is rejecting commits. This is a data-loss risk if it persists.
- `urutau_enrich_evicted_total` rising — the cold-start buffer is too small
  (`enrich.bufferLimits.maxEvents`) or the reference refresh is too slow.

## `/statusz`

The coordinator serves `/statusz` on the same address as `/metrics` when
`--metrics-addr` is set. It renders the live state — connected workers,
their phases, the current position, and per-table progress — as JSON. This
is the endpoint to hit when you want to know *what it is doing right now*,
as opposed to what the counters say.

```sh
curl -s http://coordinator:9090/statusz | jq
```

## Audit trail (`--eventlog`)

`--eventlog s3://bucket/prefix` writes a JSONL audit trail of the run's
decisions — assignments, commits, resets, snapshot boundaries — one JSON
object per line. It is append-only and independent of the sink, so it
survives a sink failure. AWS credentials and endpoint come from the
standard environment (`AWS_*`).

Use it for post-incident forensics: "why did this row land late?" is
usually answerable from the event log even when the metrics have rolled
over.

## Checkpoints (`--checkpoint`)

The durable position lives in the sink (see
[State & position](../architecture/state-position.md)), so a restart is
always safe. `--checkpoint s3://bucket/prefix` additionally writes
**async position manifests** every `--checkpoint-interval` seconds
(default 10). These are a convenience for external monitoring and for
auditing how far the pipeline has progressed — they are **not** the
recovery source of truth; the sink is.

## Supervision

The coordinator supervises workers, not the reverse:

- `--ack-timeout` (`30s`) — a worker that has not acked within this window
  is **reset**: its partition is re-routed and it must reconnect and
  re-sync from the last committed position.
- `--max-resets` (`5`) within `--reset-window` (`15m`) — after this many
  resets the coordinator **terminates** the job instead of looping forever.

Tune these together. A long `--ack-timeout` tolerates slow commits but
delays recovery; a small `--max-resets` fails fast but can kill a job over
a transient network blip. The defaults assume a healthy catalog and a
stable network.

## TLS

Without TLS flags, the coordinator's control plane is **plaintext**, and
it says so:

```
WARN coordinator: control plane is PLAINTEXT — the Assignment carries the source DSN; set TLS cert/key/CA
```

The assignment carries the source DSN — credentials included — so treat
the control plane as sensitive. Generate a CA and a server certificate for
the coordinator, then pass `--tls-cert`/`--tls-key`/`--tls-ca` to the
coordinator and the matching client flags to every worker. All three flags
must be set together or not at all.

## Logging

`--log-format json` makes logs machine-parseable for aggregation;
`--log-level debug` is very chatty and should not be left on in
production. The default `text` format is for humans.

## Next steps

- **The behavior contract** (delivery guarantees, poison batches):
  [Semantics](../reference/semantics.md).
- **The state model behind recovery**:
  [State & position](../architecture/state-position.md).
- **Deploying all of this**: [Deploy on Kubernetes](deploy-kubernetes.md).
