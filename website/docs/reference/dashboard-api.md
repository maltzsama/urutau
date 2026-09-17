---
sidebar_position: 4
---

# Dashboard API

The coordinator's embedded dashboard exposes a JSON API alongside the web
UI. All endpoints are served on the same address as `/metrics` (set via
`--metrics-addr`).

## Base URL

```
http://<coordinator>:9090
```

## Endpoints

### `GET /api/v1/pipeline`

Returns the pipeline summary.

**Response:**

```json
{
  "pipeline": "orders-demo",
  "run_id": "a1b2c3d4",
  "source_kind": "mysql",
  "sink_type": "iceberg+rest",
  "tables": 1,
  "workers": 2,
  "started_at": "2026-09-17T10:00:00Z",
  "uptime_s": 3600,
  "status": "healthy",
  "snapshot_active": false,
  "maintenance_enabled": true
}
```

| Field | Type | Description |
| --- | --- | --- |
| `pipeline` | string | Pipeline name from the spec |
| `run_id` | string | Unique run identifier |
| `source_kind` | string | Source driver (`mysql`, `postgres`, `kafka`) |
| `sink_type` | string | Sink type (`iceberg+rest`, `clickhouse`, `couchbase`) |
| `tables` | int | Number of configured tables |
| `workers` | int | Number of connected workers |
| `started_at` | string | RFC3339 timestamp of pipeline start |
| `uptime_s` | int64 | Seconds since start |
| `status` | string | `healthy`, `degraded`, or `failed` |
| `snapshot_active` | bool | True during initial snapshot phase |
| `maintenance_enabled` | bool | True if any table has maintenance enabled |

---

### `GET /api/v1/tables`

Returns all streams (source → target).

**Response:**

```json
[
  {
    "source": "shop.orders",
    "target": "bronze.orders",
    "write_mode": "upsert",
    "position": "mysql-bin.000003:12345",
    "lag_s": 2.5,
    "rows_total": 150000,
    "rows_rate": 1200.5,
    "commits": 450,
    "commit_failures": 0,
    "commit_latency_ms": 45.2,
    "equality_deletes": 1200,
    "deletes_dropped": 0,
    "snapshot_progress": 1.0,
    "maintenance": {
      "compaction": {
        "runs": 12,
        "last_run": "2026-09-17T11:00:00Z",
        "files_removed": 48,
        "files_added": 6,
        "bytes_before": 104857600,
        "bytes_after": 98566144
      },
      "expiry": {
        "runs": 6,
        "last_run": "2026-09-17T11:05:00Z",
        "snapshots_removed": 30
      },
      "orphan": {
        "runs": 1,
        "last_run": "2026-09-17T11:00:00Z",
        "files_deleted": 42,
        "bytes_freed": 10485760
      }
    }
  }
]
```

---

### `GET /api/v1/tables/{name}`

Returns one stream by source or target name.

**Parameters:**

| Name | In | Description |
| --- | --- | --- |
| `name` | path | Source or target table name (e.g. `shop.orders` or `bronze.orders`) |

**Response:** Same as a single element from `GET /api/v1/tables`.

**Status codes:**

- `200 OK` — stream found
- `404 Not Found` — no stream matches the name

---

### `GET /api/v1/workers`

Returns all connected workers.

**Response:**

```json
[
  {
    "name": "orders-demo-bronze.orders-0",
    "status": "streaming",
    "epoch": 0,
    "tables": ["bronze.orders"],
    "last_ack_s": 1.2,
    "inflight_bytes": 524288,
    "committed": {
      "bronze.orders": "mysql-bin.000003:12345"
    }
  }
]
```

| Field | Type | Description |
| --- | --- | --- |
| `name` | string | Worker group name (`<pipeline>-<target>-<index>`) |
| `status` | string | `starting`, `snapshotting`, or `streaming` |
| `epoch` | uint64 | Reset count (bumped on supervisor reset) |
| `tables` | []string | Tables this worker is assigned to |
| `last_ack_s` | float64 | Seconds since last acknowledgement |
| `inflight_bytes` | int64 | Unacked batch bytes |
| `committed` | map | Per-table last committed CDC position |

---

### `GET /api/v1/workers/{name}`

Returns one worker by name.

**Parameters:**

| Name | In | Description |
| --- | --- | --- |
| `name` | path | Worker group name |

**Response:** Same as a single element from `GET /api/v1/workers`.

**Status codes:**

- `200 OK` — worker found
- `404 Not Found` — no worker matches the name

---

### `GET /api/v1/events`

Returns the operational event log, newest first.

**Query parameters:**

| Name | Default | Description |
| --- | --- | --- |
| `type` | (all) | Filter by event type (e.g. `commit_failure`, `worker_reset`) |
| `worker` | (all) | Filter by worker name |
| `limit` | 200 | Max events to return (cap: 1000) |

**Response:**

```json
[
  {
    "ts": "2026-09-17T11:05:32.123456789Z",
    "type": "compaction",
    "worker": "orders-demo-bronze.orders-0",
    "table": "bronze.orders",
    "message": "compaction",
    "fields": {
      "files_removed": 48,
      "files_added": 6
    }
  }
]
```

---

### `GET /api/v1/logs`

Returns the coordinator's structured log tail, newest first.

**Query parameters:**

| Name | Default | Description |
| --- | --- | --- |
| `level` | `debug` | Minimum level: `debug`, `info`, `warn`, `error` |
| `limit` | 500 | Max log lines to return |

**Response:**

```json
[
  {
    "ts": "2026-09-17T11:05:32.123456789Z",
    "level": "INFO",
    "msg": "coordinator: worker acked",
    "attrs": {
      "worker": "orders-demo-bronze.orders-0",
      "position": "mysql-bin.000003:12345"
    }
  }
]
```

---

### `GET /api/v1/stream`

Server-Sent Events (SSE) endpoint for real-time updates.

On connect, the server pushes a full **snapshot** event containing the
complete state (pipeline, tables, workers, events, logs). After that, only
**delta** events are pushed:

| Event type | Payload |
| --- | --- |
| `snapshot` | Full state on connect |
| `state` | Updated pipeline/tables/workers |
| `event` | New operational event |
| `log` | New coordinator log line |

A periodic `: ping` comment keeps idle proxies from timing out.

**Example (browser):**

```javascript
const es = new EventSource('/api/v1/stream');
es.addEventListener('snapshot', (e) => {
  const state = JSON.parse(e.data);
  renderDashboard(state);
});
es.addEventListener('state', (e) => {
  const delta = JSON.parse(e.data);
  updateDashboard(delta);
});
```

**Example (curl):**

```sh
curl -N http://coordinator:9090/api/v1/stream
```

---

### `POST /api/v1/actions/cancel`

Terminates the pipeline gracefully — sends `Shutdown` to all workers.

**Response:**

```json
{"ok": true}
```

**Status codes:**

- `200 OK` — shutdown initiated
- `500 Internal Server Error` — shutdown failed

---

### `POST /api/v1/actions/restart/{worker}`

Resets one worker's session: bumps its epoch, cancels its gRPC session,
and triggers a supervisor reset. The worker reconnects with a fresh epoch.

**Parameters:**

| Name | In | Description |
| --- | --- | --- |
| `worker` | path | Worker group name |

**Response:**

```json
{"ok": true}
```

**Status codes:**

- `200 OK` — restart initiated
- `500 Internal Server Error` — restart failed

---

### `GET /healthz`

Kubernetes **liveness** probe. Returns `200 OK` with body `ok` when the
coordinator is running.

### `GET /readyz`

Kubernetes **readiness** probe. Returns `200 OK` with body `ok` when the
coordinator is ready to serve traffic.

---

## Error responses

All error responses return plain text in the body:

```
no such stream
```

HTTP status codes follow standard REST conventions:

| Code | Meaning |
| --- | --- |
| 200 | Success |
| 400 | Bad request (invalid query parameter) |
| 404 | Resource not found |
| 500 | Internal server error |
