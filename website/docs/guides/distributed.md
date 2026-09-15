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

```sh
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

The Flight assignment carries the **source DSN**, credentials included. On
a network you do not fully trust, run the control plane under mTLS: give
the coordinator `--tls-cert`/`--tls-key`/`--tls-ca` and every worker the
matching client flags. With no TLS flags the coordinator logs a warning
and speaks plaintext:

```
WARN coordinator: control plane is PLAINTEXT — the Assignment carries the source DSN; set TLS cert/key/CA
```

See [Operations](operations.md#tls) for generating the certificates.

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

## Next steps

- **Automate the lifecycle**: [Deploy on Kubernetes](deploy-kubernetes.md).
- **Metrics, audit log, checkpoints**: [Operations](operations.md).
- **All flags**: [CLI reference](../reference/cli.md).
