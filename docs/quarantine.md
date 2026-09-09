# QUARANTINE closure plan

> The quarantine label marks transitional code: a row-oriented path that
> dies when the columnar milestone it names lands. Marking is cheap,
> closing is not — without a public plan the count only goes up. This
> document is that plan.
>
> **Metric:** `grep -rn QUARANTINE --include="*.go" internal/ cmd/ test/ | wc -l`
> **Baseline:** 40 (this commit). **Rule (from the previous audit):** the
> count must FALL cycle over cycle — if it rises again, the milestone is
> not being worked and the milestone definition gets re-evaluated.
>
> Enforcement is partly mechanical: `internal/architecture/architecture_test.go`
> keeps quarantine types out of the public contract surface.

## Current distribution (audit @ f84a1f7, count 40)

| Group | Files | Points |
|---|---|---|
| G2 — worker dual-regime (M4) | `internal/worker/worker.go` | 11 |
| G4 — bridge seam | `internal/dataplane/bridge.go`, `internal/enrich/batch.go` | 6 |
| G3 — sink unpack bridges | `sink/clickhouse`, `sink/couchbase`, `sink/iceberg`, `plugin/sink.go` | 11 |
| G1 — row-based sources | `sourcepull`, `source/mysql`, `source/kafka`, `runner` | 4 |
| G5 — test-side unpacks | `runner_test`, `worker_audit_test`, `e2e/couchbase_test`, `test/plugin/standalone` | 5 |
| — | misc single points (architecture guard, coordinator) | 3 |

## Groups and what closes them

### G1 — sources decode rows (M4: "sources produce Arrow directly")
MySQL, Kafka and sourcepull still emit `rowchange.Change`; `runner`
bridges rows into batches. Closed when sources build
`*dataplane.Batch` natively. **Closes: 4 points + unlocks G1-consumers.**

### G2 — worker dual-regime (M4: "worker consumes Batch directly")
`worker.go` concentrates a third of all debt: `decodeToChanges`,
the delete-rewrite single-row enrich bridge, the row-based window map,
the per-change coordinator pump, buffer drops on bridge error. These two
regimes (row and columnar) coexisting in one file is the biggest single
source of quarantine mass. **Closes: 11 points. Feeding groups: G1.**

### G3 — sink unpack bridges ("sink consumes RecordBatch directly")
ClickHouse, Couchbase and the plugin sink decode RecordBatch back to rows
(`unpackBatch`) and coerce per-value; Iceberg's writer carries the same
value-level `Convert` path (W-1 left it standing on purpose). Closed when
sinks consume pre-cast columnar data. **Closes: 11 points. Order:
Iceberg first (reference implementation), then ClickHouse, Couchbase,
plugin sink.**

### G4 — bridge seam
`bridge.go`'s schema inference and the enrich seam exist only because
producers still hand over rows (G1/G2) and consumers still want rows
(G3). **Closes as a BYPRODUCT of G2 — no independent work.**

### G5 — test-side unpacks
Fakes and e2e helpers unpack RecordBatch to assert row-oriented state.
They update last, with each production group they shadow. **Closes: 5
points; not counted in the trend until their production group lands.**

## Closure order

1. **G2** (worker M4) — biggest mass, kills G4 as a side effect.
2. **G1** (sources columnar) — G2's feed; either can start, G2 defines.
3. **G3** sinks, Iceberg first.
4. **G5** test helpers, alongside each group.

M4 is realistic as defined — the worker file proves the columnar path
already exists; the quarantine is the row path waiting to die, not a
feature to build.
