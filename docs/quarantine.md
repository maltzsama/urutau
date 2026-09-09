# QUARANTINE closure plan — v2 (emended)

> The quarantine label marks transitional code: a row-oriented path that
> dies when the columnar milestone it names lands. Marking is cheap,
> closing is not — without a public plan the count only goes up. This
> document is that plan.
>
> **Sequencing is by RISK × VALUE, not biggest-mass-first.** The metric is
> a constraint (it must fall cycle over cycle) — never the objective. The
> biggest mass (the worker batcher, G2) is also the most correctness-
> critical: it is done last, behind an oracle.

## Metrics — pinned commands (commit-anchored)

Two named commands, ONE number each. A number that cannot be reproduced by
a pinned command is not a metric.

| # | Command | @ `bbb1c34` | Role |
|---|---|---|---|
| 1 — **TOTAL** | `grep -rn "QUARANTINE" --include="*.go" internal/ cmd/ test/ \| wc -l` | **29** | the whole tree |
| 2 — **TREND** | `grep -rn "QUARANTINE" --include="*.go" internal/ \| grep -vE "_test\.go:" \| wc -l` | **23** | production-only — must FALL cycle over cycle |

G5 (test-side "shadows": `worker_audit_test` 2, `runner_test` 1,
`architecture_test` 1, `test/e2e/couchbase_test` 1, `test/plugin/standalone`
1) is NOT counted in the TREND: those points die alongside their
production group, and counting them in the trend would let test renames
fake a drop. TOTAL includes them so nothing is ever silently orphaned.

**Deadline discipline:** every group has a date. A group without a date
does not enter the plan. If a date slips, the group is re-scoped down or
dropped — never silently extended.

## Sequence

### G0 — coordinator live-stream pump at batch granularity — DONE (2026-09-09)
Coordinator points (2 of the TREND), closed in the G0 commit. The pump
decoded each source batch back to rows (`changesFromReader`) and re-encoded
**one row per Flight batch**. Batch-native: `sourceBatches` forwards the
reader's batches whole; `enqueueBatch` serializes each once via
`transport.EncodeRecord`; the snapshot gate holds raw batches with explicit
Release discipline. #9 propagation preserved on the batch path.
Granularity-insensitivity test added (worker writes identical rows fed as
one / two / per-change batches). **TREND 23 → 21, TOTAL 29 → 27.**

### G0.5 — runner snapshot relay — RE-HOMED into G2.3 + G1 (query source)
`runner.go` (1 of TREND). DISCOVERED DURING G0: the relay's rows→batch
bridge feeds `worker.AddWindowRows`, which decodes the batch back to rows
for the window map — a DOUBLE round-trip whose two ends (relay encode,
worker window decode) are each coupled to the query source (`scanChunk`,
`src.Scan(row-map callback)` — a G1 producer) and to columnar window
storage (G2.3). There is no independent small step: removing the relay's
bridge without making windows columnar merely moves the decode. Re-homed:
the point closes when G2.3 lands (windows as stored batch) with the query
source producing the chunk batch (G1). Recorded here so the runner point is
not silently claimed by a token rename.

### G2 — worker batcher M4 — the big one — DATE 2026-09-26
`worker.go` (11) + `bridge.go` (5) + `enrich/batch.go` seam (1). The
batcher's per-row work is NOT mechanical decoding — it is four coupled,
batch-spanning row invariants. They are merged here as the design that
must be preserved, in sub-PRs, each green:

- **G2.0 — ORACLE FIRST (gate for all of G2):** freeze the row batcher as
  a reference. The dataplane equivalence suite covers Collapse/Filter/
  predicates — it does NOT cover the batcher (window dedup, bootstrap
  guard, flush partition, append op-rewrite). Build batcher-level golden
  tests first: same logical stream, row-batcher output == columnar-batcher
  output, byte-identical at the sink boundary. No G2.x lands without G2.0
  green. (This is why G2 is blocked regardless of order.)
- **G2.1** partition by mask (`Snapshot && touched`) — replaces the
  snapshot/bootstrap split loop;
- **G2.2** columnar `__op` rewrite for append-mode delete-record — the
  `Before`-image semantics, columnar;
- **G2.3** windows as stored batch + touched-key filter + `__pos` re-stamp
  at Closes — AddWindowRows, dedup and Closes together (they are coupled);
- **G2.4** enrich seam: the `enrich/batch.go` join is row-based. Either G2
  makes it columnar (broadcast join over BatchReader — CR-069 §3.4 scope)
  or it is **deferred EXPLICITLY to CR-069** with the point re-homed there.
  NOT silently "closed as a byproduct".

**Kills:** worker row path + bridge.go + (G2.4) enrich seam.

### G1 — sources produce Arrow directly — DATE 2026-10-03
`sourcepull/pull.go` (1), `source/mysql/adapter.go` (1),
`source/kafka/kafka.go` (1). The source decoders still emit rowchange; the
pull bridge encodes them (schema-inferred, empty `core.Schema{}`). Batch
builders move into the sources against the introspected schema. Kills
`BatchFromChangeBatch` production use → `bridge.go` dies here for real.

### G3 — sinks consume RecordBatch directly — DONE (2026-09-XX)
Iceberg, ClickHouse, Couchbase, plugin — all four converted to
`transport.BatchReader` in the sinks PR. 11 points closed. (Order note:
sinks were done first in practice because they were the lowest-risk mass;
the risk×value rule is satisfied — see the plan preamble.)

### G5 — test-side shadows — alongside each group
`worker_audit_test` (2, genuinely M4-gated: nested drift is undetectable
through the known-schema bridge), `runner_test` (1), `architecture_test`
(1, the deliberate rowchange NOT-listed note), `test/e2e/couchbase` (1),
`test/plugin/standalone` (1). Update last, with the group they shadow.

## Current distribution

| File | TREND points |
|---|---|
| `internal/worker/worker.go` | 11 |
| `internal/dataplane/bridge.go` | 5 |
| `internal/coordinator/coordinator.go` | 2 |
| `internal/sourcepull/pull.go`, `source/mysql`, `source/kafka`, `runner`, `enrich/batch` | 1 each |

## Rule

TREND **23 @ `bbb1c34`** must FALL. Every PR cites its group and the two
pinned numbers before/after. If TREND rises, the milestone is not being
worked — the group definition gets re-evaluated, not the count relabeled.
