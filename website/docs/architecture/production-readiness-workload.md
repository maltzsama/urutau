---
sidebar_position: 8
---

# Production-readiness workload

The production-readiness matrix runs every fault, scaling and maintenance
dimension against one live **MySQL → Iceberg CDC pipeline**. This page covers
the pipeline's input and its truth: a stochastic workload over three MySQL
tables, and the oracle that says what Iceberg must end up holding. Chaos
scheduling, KEDA re-slicing and maintenance build on top of it.

The code lives in `test/e2e/pods`:

| File | Contents |
|------|----------|
| `workload_gen_test.go` | Profiles, the three tables, random regimes, the transaction builder and the oracle. Pure: no database. |
| `workload_run_test.go` | Runs the streams against MySQL, reconciles the oracle, reads and compares MySQL and Iceberg state. |
| `workload_stats_test.go` | Observed distributions and the diagnostics file. |
| `validation_test.go` | Position and progress gates (see [Position and progress gates](#position-and-progress-gates)). |
| `artifacts_test.go` | The log collector and the failure dump (see [Artifacts](#artifacts)). |
| `workload_unit_test.go` | Cluster-free tests of the generator, run by plain `go test`. |
| `chaos_controller_test.go` | The chaos controller: planner, executor and record (see [Chaos](#chaos)). |
| `chaos_controller_unit_test.go` | Cluster-free tests of the planner and the rendered Chaos Mesh resources. |
| `production_readiness_test.go` | `TestProductionReadinessWorkload` and `TestProductionReadinessChaos`, end to end in the pod e2e cluster, and the shared runner. |
| `production_readiness_matrix_test.go` | `TestProductionReadinessMatrix`: KEDA, re-slicing, maintenance and chaos (see [Matrix](#matrix-keda-re-slicing-maintenance-and-chaos)). |

## Running it

```bash
make e2e-pods-up        # once; needs make k8s-load-race first
URUTAU_E2E_PODS=1 go test ./test/e2e/pods/ -run '^TestProductionReadinessWorkload$' -v -timeout 60m
# full profile:
URUTAU_E2E_PODS=1 URUTAU_E2E_PROFILE=full go test ./test/e2e/pods/ -run '^TestProductionReadinessWorkload$' -v -timeout 120m
```

| Variable | Effect |
|----------|--------|
| `URUTAU_E2E_PROFILE` | `smoke` (default) or `full`. |
| `URUTAU_E2E_SEED` | Replays a run's random choices. The seed is logged at the start of every run. |
| `URUTAU_E2E_ARTIFACTS` | Directory for the diagnostics file (default: the system temp dir, under `urutau-e2e/`). |
| `URUTAU_E2E_TABLES` | Comma-separated table kinds (`accounts`, `items`, `events`) to narrow a run while debugging one table. The coverage checks still expect all three, so a narrowed run always fails coverage, naming the omitted tables: it is never a pass of the matrix. |

The test's own budget is the live window plus the settle timeout plus 15
minutes for boot, seeding and teardown (48 minutes for smoke, 105 for full),
so the `-timeout` values above leave it room to report its own failure.

The seed reproduces every random draw, not the timing: regimes end on the
wall clock, so a replay can cut them at different transactions.

## The three tables

Each run creates fresh source tables and sink targets with a unique suffix,
so no run resumes from another's position or replays its binlog.

| Table | Primary key | Workers | What it exercises |
|-------|-------------|---------|-------------------|
| `pr_accounts_*` | `id BIGINT UNSIGNED`, cast to `uint64` | 2 | Unsigned keys above 2^63 end to end. Keys are drawn from `[2^63 − 2^40, 2^63 + 3·2^40)`, so a quarter sit below 2^63 and the two-worker partition boundary (the middle of the seeded range) lands above 2^63. |
| `pr_items_*` | `sku VARCHAR(40)` | 2 | String-key range partitioning. Keys are `SKU-` plus ten characters from `[0-9A-Z]`, inside the MySQL chunker's charset and free of case-insensitive collisions. |
| `pr_events_*` | `(tenant_id INT, event_id BIGINT)` | 1 | A composite key, `MEDIUMTEXT` payloads up to the profile's maximum, a nullable decimal, and backlog episodes. |

Every table has a `rev BIGINT` column that takes a new, run-wide increasing
value on every insert and update. It guarantees an UPDATE always changes the
row (MySQL writes no binlog event for an UPDATE that changes nothing), and it
lets the comparison tell a stale row from a wrong one.

## Profiles

Smoke and full share every line of the generator; only the sizes differ.

| | smoke | full |
|---|---|---|
| Initial rows per table | 2,000 | 1,000,000 |
| Mean mutations/s per table | 30 | 1,000 |
| Live window | 3 min | 30 min |
| Largest transaction | 150 rows | 2,000 rows |
| Largest `events` payload | 64 KiB | 256 KiB |
| Settle timeout | 30 min | 60 min |

## How the workload stays random

Each table has its own stream, seeded from the run seed and the table's
position. A stream draws a **regime** every few seconds and runs
transactions under it until it expires:

- mutation rate: the profile mean times a log-normal factor (0.05× to 8×);
- transaction size: exponential around a log-uniform mean, plus an
  occasional jumbo transaction at the profile maximum (and one guaranteed
  jumbo early in every stream);
- insert/update/delete mix: log-normal weights around 45/35/20, pulled back
  when the live row count drifts below half or above twice the seeded size;
- payload size: log-uniform between 16 bytes and a regime cap;
- bursts: some transactions start a run of back-to-back transactions with
  no pause.

Keys: inserts draw fresh keys, and about one in twenty reinserts a key the
run deleted, so delete-then-reinsert is exercised. Updates and deletes pick a
uniformly random live key.

Consecutive inserts in a transaction go out as one multi-row INSERT (one
binlog rows event with many rows), so the pipeline sees the batching a bulk
writer produces. The workload never builds CDC batches itself.

**Backlog episodes** (folded in from #355): the `events` stream overloads
itself periodically, at 8× the mean rate with larger transactions and
full-size payloads, for 10–40 s. The first episode starts in the first third
of the live window, later ones at random intervals. The other two tables keep
their own pace throughout.

The live streams start right after the pipeline is applied, before the
coordinator is up, so mutations overlap the snapshot as well as the stream.

## The oracle

The oracle is a primary key → row-image map per table, updated only from
transactions that **committed**. A transaction is built against the live key
set, executed in one MySQL transaction, and then:

- **committed**: applied to the oracle;
- **failed before COMMIT**: rolled back, and the key set is restored;
- **failed at COMMIT** (ambiguous): every key it touched is read back from
  MySQL, and the oracle takes MySQL's answer.

An UPDATE or DELETE that touches anything but exactly one row fails the
transaction: the oracle said the key was live, so MySQL and the oracle
disagree.

The row image is the canonical text of every value column, the same text
both sides produce: `CAST(x AS CHAR)` in MySQL and `CAST(x AS VARCHAR)` in
Trino, with payloads compared as MD5 (`MD5(x)` against
`lower(to_hex(md5(to_utf8(x))))`). Decimals are generated as exact units and
rendered with every fractional digit, as both engines print a `DECIMAL`.

## The comparison

After the live window, the test waits until every Iceberg table equals its
MySQL table (or the settle timeout runs out) and then checks:

1. **oracle vs MySQL**: must be empty. A difference is a workload bug, not a
   pipeline bug.
2. **MySQL vs Iceberg**: must be empty.

Each difference is classified:

| Category | Meaning |
|----------|---------|
| missing | a live key absent from the observed state |
| extra | a key this run never wrote |
| resurrected delete | a key this run deleted, present again |
| stale | the right key with an older `rev` |
| incorrect | the right key with the current (or a newer) `rev` and different content |
| duplicated | one key, several rows |

The test also fails if the run did not cover what the matrix needs: every
operation on every table, every operation on each side of 2^63 and of the
`pr_accounts` partition boundary, at least one backlog episode, a
single-row and a maximum-size transaction, and `events` payloads from under
1 KiB to at least half the profile maximum.

## Position and progress gates

Row state is one gate; the committed position is checked **independently**
of it (`validation_test.go`).

After the settle, each table's `cdc.position` (the table property, read
through Trino's `$properties`):

- is contained in MySQL's `gtid_executed`: never beyond what the source
  executed;
- strictly contains the `gtid_executed` read right before the table's last
  generated transaction: the table's last change was the last one pending,
  and it is covered.

While the run lasts, a sampler reads every table's committed position and
committed mutation count every 15 s:

- **no regression**: a later committed position must contain the earlier
  one;
- **no starvation** (from #355): a table whose position has not moved for
  5 minutes while its source kept changing fails the run, if another table's
  position moved in that stretch. A restart pauses every table at once,
  which is not starvation.

The whole history lands in the diagnostics file under `progress`.

When the streams stop, the workload also records `@@GLOBAL.gtid_executed`:
the expected position once the last generated mutation committed. Each
stream records `gtid_executed` when it stops as well, but that is only an
upper bound on its own last position: other streams may commit in between,
and the driver cannot report one transaction's own GTID.

Every table must also read back through Trino once the run converges: its
rows, `$snapshots` and `$properties`.

## Chaos

`TestProductionReadinessChaos` runs the same workload with the chaos
controller (`chaos_controller_test.go`) injecting faults into the live
coordinator and worker Pods for the whole live window. The pass condition is
the workload's own: once the faults stop, every table converges to MySQL
exactly. The run also fails if no experiment was injected, if one failed to
inject or to be removed, or if one is still present after the stop.

```bash
URUTAU_E2E_PODS=1 go test ./test/e2e/pods/ -run '^TestProductionReadinessChaos$' -v -timeout 60m
```

A seeded **planner** draws each fault; it is pure and unit-tested without a
cluster:

| Draw | Values |
|------|--------|
| Kind | worker or coordinator `pod-kill`, worker or coordinator `pod-failure`, worker ↔ coordinator network `partition`, `loss` (10–90 %) or `delay` (50–1000 ms), `cpu` stress (1–2 workers), `memory` stress (128–512 MB) |
| Target | one Pod (workers, or for stress sometimes the coordinator), one table's workers, or every worker (network) |
| Start | exponential gap around the profile mean (20 s smoke, 30 s full), capped at 4× the mean |
| Duration | uniform between the profile bounds (5–30 s smoke, 10 s–2 min full) |
| Overlap | whether it may start while others run, up to 2 (smoke) or 3 (full) at once |

The **executor** resolves the concrete target when it injects, from the Pods
running at that moment (scaling and re-slicing change them), creates the
Chaos Mesh resource, waits for its `AllInjected` condition, holds it for its
duration, and deletes it. A `pod-kill` is deleted as soon as the kill lands,
because Chaos Mesh reapplies it to every new Pod the selector matches. The
controller never deletes a Pod or signals a process itself.

Every experiment is recorded in the diagnostics file under `chaos`: kind,
Chaos Mesh resource and name, target, parameters, planned duration, request,
injection and removal times, and the pipeline's state when it started (Pods
with phase and restarts, each worker StatefulSet's replicas, maintenance
Pods, whether the workload was in a backlog episode, and the experiments
already active). The seed replays the draws, never the recorded times: the
record is for diagnosis, not a schedule.

## Matrix: KEDA, re-slicing, maintenance and chaos

`TestProductionReadinessMatrix` composes everything over the same workload:

```bash
URUTAU_E2E_PODS=1 go test ./test/e2e/pods/ -run '^TestProductionReadinessMatrix$' -v -timeout 130m
```

- **KEDA**: the two partitioned tables declare `workers.max: 4` over a
  baseline of 2, so the operator renders a ScaledObject and KEDA scales the
  worker StatefulSets on the coordinator's backlog. Scaling is never done by
  the test.
- **Re-slicing under CDC**: every replica change re-slices the table while
  the workload keeps writing.
- **Maintenance**: every operation runs through the coordinator's ephemeral
  maintenance Pods on short intervals (compaction every 20 s, expiry every
  30 s with `maxAge: 1m`, orphan cleanup every 30 s with the minimum
  `olderThan: 1h`).
- **Chaos**: the controller runs throughout; in addition, whenever a worker
  StatefulSet's replica count changes (a re-slice starting), it injects a
  worker pod-kill and a worker ↔ coordinator network partition at once, so
  faults overlap partition transitions by construction.
- A longer live window (8 minutes) and settle (60 minutes): with four workers
  per table and staged tables committing one cycle at a time (#414), the
  backlog of a run took about 36 minutes to drain before converging exactly.

Besides the workload's exact convergence, the run proves each item from
durable evidence, not from the coordinator's metrics (a restart resets them):

| Criterion | Evidence |
|-----------|----------|
| KEDA scale-out during the live window | worker StatefulSet replicas, sampled every 5 s, above the baseline before the window ends |
| KEDA scale-in | replicas back to the baseline after the backlog drains (up to 12 minutes, the HPA stabilization window) |
| Faults during partition transitions | chaos events with a `re-slice …` trigger, injected |
| Compaction, preserving `cdc.position` | a `replace` snapshot in every table, carrying `cdc.position` |
| Snapshot expiry | every snapshot seen 90 s into the window is gone by the end |
| Orphan cleanup never removes live files | the tables equal MySQL; a file planted in the accounts table's data directory, younger than `olderThan`, survives |

That orphan cleanup removes a real orphan is proven where a file can be
backdated, `TestOrphanCleanupRemovesAKnownOrphanOnly`
(`internal/sink/iceberg`); an S3 object cannot be. The matrix found that a
cleanup outlasting its window deleted concurrent commits (#423).

## Settling

After the live window the test polls MySQL and Iceberg every 10 s until
every table matches, up to the profile's settle timeout. A failed read (Trino
restarting, a port-forward dropped over a long run) re-establishes the Trino
port-forward before the next attempt, rather than failing the same way until
the deadline.

Partitioned tables are the slow ones to drain: with the race image a
`workers: 2` table commits about one Iceberg snapshot per coordinator cycle,
a median of 1–2 rows (#414), and took about 10 minutes to converge after a
3-minute smoke window. The smoke settle is 30 minutes for that reason.

A failed run keeps its MySQL source tables (and, like every pod e2e run, its
Iceberg targets), so the keys the diff names can be read back.

## Artifacts

Each run writes into `production-readiness-<profile>-<seed>/` under
`URUTAU_E2E_ARTIFACTS`:

- `logs/`: every container of the pipeline, followed from the moment it
  runs, one file per Pod and restart (`<pod>-r<N>.log`), so a log survives
  its container's replacement. The data-race gate scans all of them, not just
  the containers alive at the end.
- `diagnostics/` (on failure, dumped before teardown): the CDCPipeline, the
  worker and coordinator StatefulSets, Pods and `describe`, events, Services,
  ScaledObjects and HPAs, Chaos Mesh resources, the data services, each
  table's `cdc.position`, properties and snapshots, and the oracle, MySQL and
  Iceberg state of every table as sorted `key rev image` files.

## Diagnostics

Every run writes `production-readiness-<profile>-<seed>.json`, pass or fail:

- the seed, the profile and the live window;
- the expected source position;
- per table: operation counts, per-side counts for `pr_accounts`, the
  expected partition boundary, backlog episodes, failed and ambiguous
  transactions;
- distributions (min, max, mean, p50/p90/p99 and power-of-two buckets) of
  mutations per second, rows and bytes per MySQL transaction, and payload
  size;
- rows and bytes per Iceberg commit, read from the target's `$snapshots`
  summaries (one snapshot per worker commit);
- both final diffs.
