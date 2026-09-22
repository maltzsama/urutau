---
sidebar_position: 5
---

# Distributed mode

The collapsed CLI (`urutau run`) puts the reader, the worker, and the sink
writer in one process. That is enough for a laptop, but it means one
process owns every table's throughput, and a crash stops everything.

Distributed mode splits the same engine in two:

- **`urutau-coordinator`** — connects to the source, runs the DBLog
  snapshot, partitions each table's key range, routes rows to workers over
  Arrow Flight, commits staged Iceberg cycles, and supervises worker
  health.
- **`urutau-worker`** — connects to the coordinator, receives an
  assignment (table, key range, source DSN), and owns the writes to the
  sink for that partition.

This is the engine Kubernetes automates; if you are on a cluster, read
[Deploy on Kubernetes](deploy-kubernetes.md) instead — the coordinator and
workers are the same, just scheduled for you.

## How workers are assigned

The coordinator decides the worker count and names; a worker never chooses
its own work. For a table with `workers: {number: N}`, the coordinator
splits the primary-key range into `N` contiguous ranges and derives the
group names `<pipeline>-<target>-<index>`. It waits for exactly those
`--name`s to connect (up to `--wait-worker`), then hands each its range.

A worker started with an unexpected `--name`, or one that never connects,
is a startup failure, not a silent degradation: the coordinator waits
`--wait-worker` and then fails.

## Running it by hand

Use the same spec you would give `urutau run`, with `workers: {number: N}`
on the tables you want to parallelize.

```sh title="Run coordinator and workers"
# Terminal 1 — the coordinator
./bin/urutau-coordinator run -f pipeline.yaml --listen :50051

# Terminal 2..N+1 — one worker per derived name
./bin/urutau-worker run \
  --coordinator localhost:50051 \
  --name orders-0 \
  --catalog-uri http://localhost:8181/api/catalog \
  --client-id root --client-secret s3cr3t
```

The worker names must be exactly the derived ones the coordinator expects.
For a table `raw.orders` in pipeline `orders` with `number: 3`, they are
`orders-raw.orders-0`, `orders-raw.orders-1`, `orders-raw.orders-2`.

The catalog settings (`--catalog-uri`, `--client-id`, …) can also come
from `URUTAU_SINK_URI`, `URUTAU_SINK_CLIENT_ID`, and friends — see the
[CLI reference](../reference/cli.md#urutau-worker).

## Secure the control plane

The Flight assignment carries the **source DSN**, credentials included. Run
the control plane under mTLS: give the coordinator
`--tls-cert`/`--tls-key`/`--tls-ca` and every worker the matching client
flags. With no TLS flags the coordinator **refuses to boot** — it will not
send the DSN in the clear by omission. To accept plaintext explicitly (e.g. a
trusted network), pass `--allow-insecure-control-plane`, which downgrades the
failure to a startup warning:

```
WARN coordinator: control plane is PLAINTEXT — the Assignment carries the source DSN; set TLS cert/key/CA (running because --allow-insecure-control-plane was set)
```

A minimal CA plus a server certificate for the coordinator and a client
certificate for each worker:

```sh title="Generate mTLS certificates"
# CA
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=urutau-ca"

# Coordinator server cert — the SAN must match how workers dial it
openssl req -newkey rsa:4096 -nodes -keyout server.key -out server.csr \
  -subj "/CN=urutau-coordinator"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -out server.crt \
  -extfile <(printf "subjectAltName=DNS:urutau-coordinator,DNS:localhost,IP:127.0.0.1")

# Worker client cert
openssl req -newkey rsa:4096 -nodes -keyout client.key -out client.csr \
  -subj "/CN=urutau-worker"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -out client.crt
```

```sh title="Run with mTLS"
urutau-coordinator run -f pipeline.yaml \
  --tls-cert server.crt --tls-key server.key --tls-ca ca.crt

urutau-worker run --coordinator urutau-coordinator:50051 \
  --tls-cert client.crt --tls-key client.key --tls-ca ca.crt
```

## Source credentials and files on workers

The worker owns the snapshot chunk `SELECT`, so the coordinator sends it a
**source DSN** — credentials included. Two consequences for a structured
`source.postgres` block:

- **TLS material is sent as paths, not contents.** The DSN carries
  `sslrootcert`/`sslcert`/`sslkey` (from `ssl.ca`/`ssl.cert`/`ssl.key`), so
  every worker must mount those files at the **same paths** as the
  coordinator. Otherwise the assignment validates but the snapshot
  connection fails. Mount them from the same Secret/volume on both.
- **SSH tunnels are not supported in distributed mode.** A DSN cannot carry
  the tunnel, and the worker would connect directly to the database. Set
  `source.snapshotUri` to a directly reachable read-only URI (or use the
  collapsed runner). The coordinator rejects the combination otherwise.

## Supervision and resets

A worker that stops acking is not silently dropped. If a worker goes
silent for `--ack-timeout` (`30s` default), the coordinator **resets** the
assignment: the partition is re-routed, and the worker must reconnect and
re-sync from the last committed position. This is the recovery path for a
worker crash or a network partition.

Too many resets in a short window means something is systematically wrong
(a bad sink, a flapping network), so the coordinator stops retrying: after
`--max-resets` (`5`) resets within `--reset-window` (`15m`), the job
**terminates** rather than loop forever. Both are configurable.

Resets are safe because the position lives in the **sink**, not in the
worker: a resumed partition reads its last committed position back from
the sink and continues. See
[State & position](../architecture/state-position.md).

## Concurrent writers

Parallelizing a table means several writers committing to it. Both
built-in sinks support this, with different mechanisms:

- **Iceberg**: workers stage data files without committing; the
  coordinator commits one cycle's staged files as a single unit — one
  committer, one position write, no race.
- **ClickHouse**: workers commit independently, but the durable position
  is kept per partition and read back as the minimum safe value.

A sink that does not support concurrent writers rejects
`workers: {number: N > 1}` at boot with an error naming the table.
[Operator](../architecture/operator.md#concurrent-writers-two-different-fixes-for-the-same-problem)
has the details.

## Runtime scaling

A running coordinator can re-slice a table across a different number of
workers — adding or removing owners and swapping the routing snapshot
atomically, with no coordinator restart. The coordinator's `ScaleTable` does
the re-slice; under Kubernetes, KEDA drives it from the per-table lag metric
(see [Autoscaling with KEDA](#autoscaling-with-keda)).

The sequence is **prepare → commit**, and every wait is bounded: a step that
cannot complete fails the scale and leaves the old layout in place, so a
scale is a retryable no-op rather than an incident — never an unbounded
pause, never data loss.

1. **Prepare** — register any new owner (so its Hello is accepted the moment
   its pod starts), then **pause the table's input** at the coordinator's
   pump. The flip requires the table to owe nothing (no in-flight batch, no
   open staged cycle), and a continuously loaded table never reaches that on
   its own; pausing the input lets the queue drain, which is what makes the
   barrier converge under load.
2. **Commit** — swap the routing snapshot atomically, then resume the input.
   A reader that loaded the old snapshot keeps routing a whole batch by it,
   so a batch is never split across two layouts; the pump's held batch, and
   every batch after it, is routed by the new layout.
3. **Retire** (scale-in only) — each removed owner drains, then is detached.

A prepare step that times out — the table never drains, a commit never lands
— resumes the input and returns with the old layout intact. The scaler
retries, and worker recovery stays the supervisor's job: a worker that
stalls owing work is terminated for a clean replay from the committed
position, not reset mid-flight (which would replay its batches).

The pause briefly holds the whole pipeline's reader — the pump is shared, so
other tables stall for the duration of the drain. That is the price of not
losing the events that arrive during the flip, and the drain is bounded, so
the stall is too.

The commit mode travels **per batch** (`BatchMeta.staged`), decided by the
coordinator at send time, not frozen in the worker's assignment. That is what
makes a table that *becomes* partitioned under a running worker safe: the
surviving owner, which attached when the table was unpartitioned, stages the
batches the coordinator now marks staged instead of committing them directly —
a direct commit would leave its staged cycle open and block every cycle
behind it in the table's send order (issue #312).

A table's `workers.max` caps how far it may scale **up**; it never blocks a
scale-down. A table that sets it ignores the coordinator's default (32).

### Autoscaling with KEDA

In Kubernetes, a table's workers run as **one StatefulSet** named
`<pipeline>-<target>`, with `replicas = workers.number` at boot. The name is
DNS-sanitized (a `.` in the target becomes `-`), because a StatefulSet pod's
hostname is `<statefulset>-<ordinal>` — a single DNS label — and that string is
exactly the derived worker group name. So a replica *is* its partition, with no
identity plumbing.

That single replica count is what KEDA scales. Start the operator with
`--keda-prometheus-address <url>` and it renders one `ScaledObject` per table
that sets `workers.max`:

- `minReplicaCount` = `workers.number`, `maxReplicaCount` = `workers.max`;
- a Prometheus trigger on `urutau_coordinator_lag_seconds{table="<target>"}`,
  with `--keda-lag-threshold` (default 30s) as the per-replica lag target.

The coordinator never writes the replica count — it **follows** it. A reconcile
loop reads each worker StatefulSet's `spec.replicas` and calls `ScaleTable` when
the routing owner count diverges, so KEDA (or a manual `kubectl scale`) is the
only writer and the two never fight. A cluster without the KEDA CRD is
unaffected: the operator logs and skips the ScaledObject.

Omit `workers.max` and the count is fixed — no ScaledObject, no autoscaling.

## Next steps

- **Automate the lifecycle**: [Deploy on Kubernetes](deploy-kubernetes.md).
- **Live signals**: [Monitoring](monitoring.md).
- **Audit log, checkpoints, supervision**: [Reliability](reliability.md).
- **All flags**: [CLI reference](../reference/cli.md).
