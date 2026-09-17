---
sidebar_position: 10
---

# What's new in v0.2.0

Released 2026-09-17. [Full changelog](https://github.com/maltzsama/urutau/releases/tag/v0.2.0).

## Iceberg table maintenance

Background compaction, snapshot expiry, and orphan file cleanup for Iceberg
tables — the [issue #96](https://github.com/maltzsama/urutau/issues/96)
feature. Enabled per-table via `sink.maintenance` in the pipeline spec:

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

Each sub-block is independently optional. Maintenance runs in its own
ephemeral worker Pod (distributed mode) or in-process (single-process
mode) — never in the coordinator itself.

See [Sinks: Table maintenance](reference/sinks.md#table-maintenance) for
the full reference, including safety windows and conflict handling.

**Key details:**

- Compaction uses `iceberg-go`'s bin-pack planner to merge small files
  into `targetFileSize`-sized outputs.
- Snapshot expiry's `maxAge` is a safety window, not just retention — it
  must cover your longest tolerable downtime.
- Orphan cleanup removes files no longer referenced by any snapshot, after
  a configurable `olderThan` delay.
- Each operation has its own Prometheus metrics (see
  [Monitoring](guides/monitoring.md#metrics)).

## Coordinator dashboard

An embedded monitoring UI served from the coordinator's HTTP server —
[issue #97](https://github.com/maltzsama/urutau/issues/97). No CDN, no
external dependencies: Alpine.js + Chart.js compiled into the binary.

```sh
urutau run -f pipeline.yaml --metrics-addr :9090
# Open http://localhost:9090
```

**Views:**

- **Overview** — pipeline status, worker count, uptime
- **Streams** — per-table position, lag, throughput charts, commit
  latency, maintenance stats
- **Workers** — phase, epoch, assigned tables, last ack, inflight bytes
- **Events** — color-coded operational log with filters
- **Logs** — coordinator structured log tail with level filter

**Actions:** Cancel pipeline, restart worker (with confirmation dialogs).

**Real-time updates** via Server-Sent Events — no polling.

See [Dashboard](guides/dashboard.md) for the full guide and
[Dashboard API](reference/dashboard-api.md) for the REST/SSE reference.

## Worker metrics reporting

Workers now report their per-table metrics (rows written, commit latency,
equality deletes, snapshot progress) back to the coordinator over gRPC.
The coordinator aggregates them for the dashboard and Prometheus endpoints.

This is transparent — no spec changes needed. Worker Prometheus endpoints
(`--metrics-addr` on the worker) continue to work independently.

## Commit mode validation

`sink.commitMode` is now **rejected** on non-Couchbase sinks at validation
time ([#99](https://github.com/maltzsama/urutau/issues/99)). Previously,
setting `commitMode: atomic` on an Iceberg or ClickHouse sink was silently
ignored, which could mislead operators into thinking they had stronger
atomicity guarantees than the sink provides.

## Bug fixes

- **Maintenance worker lifecycle** — maintenance workers now run as
  ephemeral Pods (`restartPolicy: Never`) instead of Deployments, and
  idle workers are dismissed after their pass completes.
- **Worker metrics in Kubernetes** — metrics reporting was disabled by
  default in Kubernetes; now enabled and scoped via RBAC.
- **Dashboard polish** — SSE stream keep-alive, chart window selector,
  light theme button visibility, copy-to-clipboard, and drawer lag chart
  all fixed post-launch.
