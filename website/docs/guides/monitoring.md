---
sidebar_position: 7
---

# Monitoring

The live signals a running pipeline emits: what to scrape, what to look at
right now, and what to alert on. This page is about *watching* a pipeline.

For the durable record of what a pipeline did — the audit trail and
checkpoints — see [Reliability](reliability.md).

## Metrics

Pass `--metrics-addr :9090` (coordinator) or `--metrics-addr :9091`
(worker) to serve Prometheus metrics on `/metrics`. Empty disables the
endpoint.

**Coordinator**

| Metric | Type | Meaning |
| --- | --- | --- |
| `urutau_coordinator_lag_seconds` | gauge | Reader-to-worker lag |
| `urutau_coordinator_inflight_bytes` | gauge (worker) | Unacked batch bytes per worker |
| `urutau_coordinator_worker_resets_total` | counter (reason) | Worker resets by reason |
| `urutau_coordinator_commits_total` | counter (table) | Commits acked by the worker |
| `urutau_coordinator_events_decoded_total` | counter | Decoded source events |

**Worker**

| Metric | Type | Meaning |
| --- | --- | --- |
| `urutau_worker_rows_written_total` | counter (table, op) | Rows written |
| `urutau_worker_commit_duration_seconds` | histogram (table) | Iceberg commit latency |
| `urutau_worker_commit_failures_total` | counter (table) | Failed commits |
| `urutau_worker_equality_deletes_written_total` | counter (table) | Equality deletes written |
| `urutau_worker_snapshot_progress_ratio` | gauge (table) | Snapshot progress, 0..1 |
| `urutau_worker_dblog_dropped_by_window_total` | counter (table) | Snapshot rows discarded by DBLog windows |
| `urutau_worker_deletes_dropped_total` | counter (table) | Append-only deletes dropped |

**Enrichment**

| Metric | Type | Meaning |
| --- | --- | --- |
| `urutau_enrich_misses_total` | counter | Left-join misses (event passed with NULLs) |
| `urutau_enrich_inner_dropped_total` | counter | Events dropped by an inner-join miss |
| `urutau_enrich_evicted_total` | counter | Events evicted from the cold-start buffer |

## Live state: `/statusz`

The coordinator serves `/statusz` on the same address as `/metrics` when
`--metrics-addr` is set. It renders the live state as JSON — connected
workers, their phases, the current position, per-table progress.

Reach for it when you want to know *what it is doing right now*, as opposed
to what the counters say:

```sh
curl -s http://coordinator:9090/statusz | jq
```

## Logs

- `--log-format json` makes logs machine-parseable for aggregation; the
  default `text` is for humans.
- `--log-level debug` is very chatty — do not leave it on in production.

## What to alert on

| Signal | Likely meaning |
| --- | --- |
| `urutau_coordinator_lag_seconds` climbing | The reader is falling behind the source. Usually the sink, not the reader. |
| `urutau_coordinator_worker_resets_total` rising | Workers are crashing or the network is flapping. Watch the `reason` label. |
| `urutau_worker_commit_failures_total` rising | The catalog is rejecting commits — a data-loss risk if it persists. |
| `urutau_enrich_evicted_total` rising | The cold-start buffer is too small or the reference refresh is too slow. |

## Next

- **What survives a crash, and how to prove what happened**:
  [Reliability](reliability.md).
- **The delivery contract**: [Delivery guarantees](../reference/guarantees.md).
