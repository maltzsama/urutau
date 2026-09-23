---
sidebar_position: 8
---

# Dashboard

The coordinator ships with an embedded monitoring dashboard — a single-page
application served from the same HTTP server as `/metrics` and `/statusz`.
No CDN, no external dependencies, no build step: the HTML, JavaScript
(Alpine.js + Chart.js), and CSS are compiled into the Go binary via
`//go:embed`. It works on-prem, in air-gapped networks, and behind VPNs.

## Enabling the dashboard

The dashboard is a **coordinator** feature: the collapsed `urutau run` CLI has
no Prometheus registry, so it does not serve one. Pass `--metrics-addr` to the
coordinator. The dashboard is served on the same address:

```sh command="urutau-coordinator run -f pipeline.yaml --metrics-addr :9090"
urutau-coordinator run -f pipeline.yaml --metrics-addr :9090
```

On Kubernetes, set `spec.coordinator.metricsAddr` in the `CDCPipeline` CR:

```yaml title="CDCPipeline CR with metrics"
apiVersion: urutau.maltzsama.github.io/v1alpha1
kind: CDCPipeline
spec:
  coordinator:
    metricsAddr: ":9090"
```

Open `http://localhost:9090` in a browser. The dashboard is the root page;
`/metrics` (Prometheus) and `/statusz` (JSON) remain available at their
 usual paths.

## Views

### Overview

The landing page shows the pipeline at a glance:

- **Pipeline name** and run ID
- **Status** indicator: healthy / degraded / failed
- **Source** kind and **sink** type
- **Workers**: online count, snapshot progress
- **Uptime** since start
- **Maintenance** enabled/disabled

### Streams (Tables)

One card per source → target stream. Each card shows:

| Field | Meaning |
| --- | --- |
| Source / Target | `schema.table` → `namespace.table` |
| Write mode | `upsert`, `append`, or `append-idempotent` |
| Position | Current CDC position (with copy button) |
| Lag | Time since last commit |
| Rows | Total written + rate (rows/s) |
| Commits | Total + failures |
| Commit latency | Average ms per commit |
| Equality deletes | Count (upsert mode) |
| Snapshot progress | 0..1 during initial backfill |

Clicking a stream card opens a drawer with **throughput** and **commit
latency** charts (1-hour client-side buffer, window selector for
5m/15m/1h/6h).

### Workers

One card per connected worker:

| Field | Meaning |
| --- | --- |
| Worker name | `<pipeline>-<target>-<index>` |
| Status | `starting`, `snapshotting`, `streaming` |
| Epoch | Reset count (bumped on supervisor reset) |
| Assigned tables | Tables this worker owns |
| Last ack | Seconds since last acknowledgement |
| Inflight bytes | Unacked batch bytes |
| Committed positions | Per-table last committed position |

### Events

A scrollable, color-coded log of operational events:

- **Commit failure** — catalog rejected a commit
- **Worker reset** — supervisor restarted a stale worker
- **Schema change** — DDL detected in source
- **Maintenance** — compaction/expiry/orphan pass completed
- **Pipeline** — start, stop, snapshot transitions

Filter by event type and worker. Events are kept in a 1000-entry ring
buffer in memory.

### Logs

The coordinator's structured log tail, with level filter (debug / info /
warn / error). Useful for debugging without SSH-ing into the pod.

## Actions

The dashboard exposes two actions, both requiring a confirmation dialog:

| Action | What it does |
| --- | --- |
| **Cancel pipeline** | Sends `Shutdown` to all workers; terminates the pipeline gracefully |
| **Restart worker** | Bumps the worker's epoch, cancels its session, and triggers a supervisor reset |

Actions are logged as events and visible in the Events view.

## Real-time updates

The dashboard uses **Server-Sent Events (SSE)** for live updates. On
connect, the server pushes a full snapshot (pipeline, tables, workers,
events, logs). After that, only deltas are pushed — no polling, no
WebSocket complexity. The browser's `EventSource` handles reconnection
automatically.

The SSE endpoint is `GET /api/v1/stream`. A periodic comment (`: ping`)
keeps idle proxies from timing out the connection.

## API endpoints

The dashboard's JSON API is also available for programmatic access. See
[Dashboard API](../reference/dashboard-api.md) for the full reference.

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/v1/pipeline` | GET | Pipeline summary |
| `/api/v1/tables` | GET | All streams |
| `/api/v1/tables/{name}` | GET | One stream (by source or target name) |
| `/api/v1/workers` | GET | All workers |
| `/api/v1/workers/{name}` | GET | One worker |
| `/api/v1/events` | GET | Event log (filter: `type`, `worker`, `limit`) |
| `/api/v1/logs` | GET | Coordinator log tail (filter: `level`, `limit`) |
| `/api/v1/stream` | GET | SSE stream |
| `/api/v1/actions/cancel` | POST | Cancel pipeline |
| `/api/v1/actions/restart/{worker}` | POST | Restart worker |
| `/healthz` | GET | Liveness probe |
| `/readyz` | GET | Readiness probe |

## Security

The dashboard assumes a trusted network (same trust model as the gRPC
control plane). There is no authentication in v0.2.0 — the endpoint is
intended for internal monitoring, not public exposure.

For untrusted networks, put the dashboard behind a reverse proxy with
authentication, or restrict access via network policies.

## Architecture

The dashboard is a separate package (`internal/dashboard`) that reads
coordinator state through the `State` interface — the same dependency
direction the rest of the orchestration keeps. The coordinator implements
`State`; the dashboard never imports the coordinator.

```text title="Dashboard architecture"
coordinator.go
  └── dashboard.Handler
        ├── /api/v1/*  (JSON API)
        ├── /api/v1/stream  (SSE)
        ├── /healthz, /readyz  (probes)
        └── /*  (embedded SPA)
```

The SPA assets (`index.html`, `app.js`, `style.css`, plus vendored
Alpine.js/Chart.js/Pico.css) are embedded at compile time via
`//go:embed static/*`. No runtime filesystem access.
