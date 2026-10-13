---
sidebar_position: 9
---

# Benchmark harness

`TestBenchmark` runs one configured CDC workload from MySQL to Iceberg and
measures it. It is the [production-readiness workload](production-readiness-workload.md)
— the same generator, oracle and final comparison — with the load set by a
configuration instead of a fixed profile, and with the measurements a
benchmark needs.

The harness owns the source tables, the load and every measurement. The
engine under test only has to carry `shop.<table>` to `raw.<table>`, so the
same configuration and seed give any engine the same workload.

The code lives in `test/e2e/pods`: `bench_config_test.go` (the
configuration), `bench_probe_test.go` (latency and resources),
`bench_test.go` (the run and the results file), `bench_unit_test.go`
(cluster-free tests).

## Running it

It runs as a Job inside the pod e2e cluster, like every pod e2e scenario:

```bash
make e2e-pods-up
BENCH_CONFIG=$PWD/test/e2e/pods/bench/smoke.json \
URUTAU_E2E_PODS_IMAGE=urutau:dev \
RUN='^TestBenchmark$' TIMEOUT=60m make e2e-pods-test
```

`URUTAU_E2E_PODS_IMAGE=urutau:dev` selects the production image; without it
the pipeline runs the race-instrumented one, whose numbers are not the
product's. The results are copied out with the run's artifacts as
`benchmark-<name>-<seed>.json`.

## Configuration

A JSON document. An unknown field is an error, so a misspelt knob never
silently keeps its default.

| Field | Meaning |
|-------|---------|
| `name` | Names the run and, for `urutau`, the pipeline (`bench-<name>`). |
| `seed` | Replays every random draw. Omitted: drawn from the clock and recorded. |
| `engine` | `urutau` applies a CDCPipeline. `external` prints the tables and waits for another engine to carry them. |
| `image` | The Urutau image, when not the e2e default. |
| `podPrefix`, `podNamespace` | The Pods whose resources are measured. Required for `external`; the pipeline's own Pods for `urutau`. |
| `initialRows` | Rows seeded per table before the engine starts. |
| `duration`, `settle` | The live window, and how long the sink may take to equal the source after it. |
| `warmup` | The start of the live window left out of the latency and rate figures: the snapshot and the first catch-up. |
| `meanRate`, `maxTxnRows`, `maxPayloadBytes` | The run-wide load: mean mutations/s per table, largest transaction, payload bound. |
| `tables[]` | The tables of the run, each with its own `meanRate`, `maxTxnRows`, `maxPayloadBytes`, `mix` (`insert`/`update`/`delete` weights) and `workers`. An omitted value is the run-wide one. |

The three table kinds are the production-readiness tables: `accounts`
(unsigned 64-bit key), `items` (string key) and `events` (composite key, large
payloads, backlog episodes). Each has its own random stream, so their
distributions are independent. Within a stream the rate, the transaction
size, the mix and the payload size change every few seconds around the
configured values; transactions and the CDC batches they become are never of
one fixed size.

The `accounts` and `items` payload columns are sized by their DDL (2,048 and
4,096 bytes): `maxPayloadBytes` can lower their bound, not raise it.

## What is measured

Nothing is asked of the engine under test.

- **End-to-end latency.** Every insert and update writes a `rev` that only
  grows within its table. The harness polls `max(rev)` of each Iceberg table
  through Trino every 2 s; a transaction is visible once the highest `rev`
  the sink has shown reaches the highest `rev` it wrote, and its latency is
  that poll's time minus its commit time in MySQL. It is an upper bound by at
  most one poll. A transaction of deletes alone writes no `rev` and is not
  measured. Reported per table as p50, p90, p99, with the number of measured
  transactions that never became visible.
- **Throughput.** What each stream actually committed: operations by kind,
  mutations/s over the measured window, rows and payload bytes per
  transaction. And what the sink committed: rows and bytes per Iceberg
  snapshot, and bytes written.
- **Resources.** CPU and memory working set of every Pod under test, sampled
  from the kubelet every 10 s, per Pod and summed. Network bytes received and
  sent come from the kubelet too where it reports them; on a single node
  whose runtime does not (minikube on docker), a helper Pod in the host's PID
  namespace reads them from `/proc`. `networkMeasured` says whether any
  sample carried them.
- **Recovery.** `settleSeconds`: how long after the live window the sink took
  to equal the source.
- **Correctness.** The final Iceberg state is compared with MySQL row by row,
  as in the production-readiness run. `valid` is true only when they are
  equal and the oracle agreed with MySQL; the numbers of an invalid run
  measure a pipeline that lost or invented data.

The file also records the configuration, the seed, the source's final GTID
position, the image, the Kubernetes, MySQL and Trino versions, each node's
CPU, memory, OS, kernel and architecture, and, for `urutau`, the pipeline
manifest (`engine-manifest.yaml` in the run's artifacts).

The seed reproduces every random draw, not the timing: regimes end on the
wall clock, so a replay can cut them at different transactions.
