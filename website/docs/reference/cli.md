---
sidebar_position: 7
---

# CLI reference

Urutau ships **four binaries** from one image (`build/Dockerfile`):

| Binary | Role | Typical use |
| --- | --- | --- |
| `urutau` | Collapsed mode: reader + worker + sink in one process | A laptop, a single VM, `make build` |
| `urutau-coordinator` | Reads the source, runs the DBLog snapshot, serves workers | Distributed mode |
| `urutau-worker` | Owns one partition's writes to the sink | Distributed mode |
| `urutau-operator` | Reconciles `CDCPipeline` CRs on Kubernetes | [Deploy on Kubernetes](../guides/deploy-kubernetes.md) |

All three pipeline binaries read the same YAML spec — see
[Pipeline specification](pipeline-spec.md) — and share the logging flags:

- `--log-level` — `debug` | `info` | `warn` | `error` (default `info`)
- `--log-format` — `text` | `json` (default `text`)

`--file`/`-f` defaults to `pipeline.yaml` in the working directory.

## `urutau`

The collapsed CLI. Two subcommands: `run` and `version`.

### `urutau run`

Runs the whole pipeline in one process: DBLog snapshot, then live
streaming, writing straight to the sink. This is what the
[Quickstart](../quickstart.md) uses.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-f`, `--file` | `pipeline.yaml` | Pipeline spec (inline YAML) |
| `--server-id` | `1101` | MySQL replication server id. The spec's `source.serverId` wins when set |
| `--chunk-size` | `10000` | DBLog snapshot chunk size (rows per `SELECT`) |
| `--max-parallel-chunks` | `0` | Concurrent chunk `SELECT`s during snapshot (`0` = serial) |
| `--window-timeout` | `5m` | DBLog window timeout (pathology detector) |
| `--eventlog` | _(off)_ | `s3://bucket/prefix` JSONL audit trail |
| `--plugin` | _(none)_ | Path to a Go plugin (`.so`); repeatable |
| `--source-plugin` | _(none)_ | External source plugin binary (Arrow Flight) |
| `--sink-plugin` | _(none)_ | External sink plugin binary (Arrow Flight) |

### `urutau version`

Prints the binary's version, commit, and build date.

## `urutau-coordinator`

The coordinator half of distributed mode. It owns the source connection,
runs the snapshot, routes rows to workers over Arrow Flight, commits
staged Iceberg cycles, and supervises worker health. It is a long-lived
server, not a one-shot.

### `urutau-coordinator run`

| Flag | Default | Meaning |
| --- | --- | --- |
| `-f`, `--file` | `pipeline.yaml` | Pipeline spec |
| `--listen` | `:50051` | gRPC + Flight listen address |
| `--metrics-addr` | _(off)_ | Serve `/metrics` and `/statusz` on this address |
| `--tls-cert` | _(off)_ | Server certificate for the control plane (mTLS) |
| `--tls-key` | _(off)_ | Server private key (mTLS) |
| `--tls-ca` | _(off)_ | CA that signs worker client certs (mTLS) |
| `--allow-insecure-control-plane` | `false` | Run the control plane plaintext (explicit opt-out of the fail-closed default) |
| `--server-id` | `1101` | MySQL replication server id (spec wins) |
| `--chunk-size` | `10000` | Snapshot chunk size |
| `--max-parallel-chunks` | `0` | Concurrent chunk `SELECT`s (`0` = serial) |
| `--window-timeout` | `5m` | DBLog window timeout |
| `--wait-worker` | `2m` | How long to wait for every expected worker session |
| `--ack-timeout` | `30s` | A worker is stale without an ack for this long |
| `--max-resets` | `5` | Resets within the window before the job terminates |
| `--reset-window` | `15m` | Sliding window for the reset count |
| `--eventlog` | _(off)_ | `s3://bucket/prefix` audit trail |
| `--checkpoint` | _(off)_ | `s3://bucket/prefix` async position manifests |
| `--checkpoint-interval` | `10` | Checkpoint write interval (seconds) |
| `--plugin` | _(none)_ | Go plugin path; repeatable |

**mTLS:** set all three of `--tls-cert`, `--tls-key`, `--tls-ca`, or none.
With none the control plane is **plaintext**, and the coordinator **refuses
to boot** — the Flight assignment carries the source DSN, so plaintext leaks
credentials on the wire. Pass `--allow-insecure-control-plane` to accept
plaintext explicitly (it then warns at startup instead of failing). Always use
mTLS outside a trusted network. See
[Distributed mode](../guides/distributed.md#secure-the-control-plane).

## `urutau-worker`

The worker half. It connects to a coordinator, receives an assignment
(which table, which partition, which source DSN), and owns the writes to
the sink for its partition.

### `urutau-worker run`

| Flag | Default | Meaning |
| --- | --- | --- |
| `--coordinator` | `127.0.0.1:50051` | Coordinator address (`host:port`) |
| `--name` | `$HOSTNAME` | Worker name (must match the coordinator's expectation) |
| `--catalog-uri` | `$URUTAU_SINK_URI` or `http://localhost:8181/api/catalog` | Iceberg REST catalog URI |
| `--warehouse` | `$URUTAU_SINK_WAREHOUSE` or `quickstart_catalog` | Catalog warehouse name |
| `--client-id` | `$URUTAU_SINK_CLIENT_ID` | Catalog OAuth2 client id |
| `--client-secret` | `$URUTAU_SINK_CLIENT_SECRET` | Catalog OAuth2 client secret |
| `--scope` | `$URUTAU_SINK_SCOPE` or `PRINCIPAL_ROLE:ALL` | Catalog OAuth2 scope |
| `--namespace` | `raw` | Fallback namespace for bare targets |
| `--max-rows` | `1000` | Flush the batch once this many rows are buffered |
| `--max-interval` | `2s` | Flush cadence |
| `--metrics-addr` | _(off)_ | Serve `/metrics` on this address |
| `--plugin` | _(none)_ | Go plugin path; repeatable |
| `--tls-cert` / `--tls-key` / `--tls-ca` | _(off)_ | Client cert for the control plane (mTLS) |

The catalog settings fall back to the `URUTAU_SINK_*` environment, which
is how the Kubernetes operator passes Secret-backed credentials to a
worker (it cannot inline a Secret's value into a Pod spec). A flag always
wins over the environment. A worker owns its own catalog access, so these
are not optional in practice — but an unauthenticated catalog is legal, so
the flags are not *required*; a bad catalog fails when the worker opens it.

## `urutau-operator`

The Kubernetes operator. Plain flags (no subcommands); see
[Deploy on Kubernetes](../guides/deploy-kubernetes.md).

| Flag | Default | Meaning |
| --- | --- | --- |
| `--coordinator-image` | _(required)_ | Image for coordinator + worker Pods (the operator's own, by convention) |
| `--metrics-bind-address` | `:8080` | Metrics endpoint |
| `--health-probe-bind-address` | `:8081` | `/healthz` + `/readyz` endpoint |
| `--enable-webhook` | `true` | Serve the validating admission webhook |
| `--field-manager` | `urutau-operator` | Server-Side Apply field manager — the name the operator applies objects as. Must be unique per controller managing the same objects. |

The operator also accepts the controller-runtime zap flags
(`--zap-log-level`, `--zap-encoder`, `--zap-devel`, …).
