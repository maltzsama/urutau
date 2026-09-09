# QUARANTINE closure — COMPLETE

> The quarantine label marked transitional code that would die when the
> columnar milestone it named landed. **Every marker is now gone: TREND 0,
> TOTAL 0 (pinned commands below).** This page records the architecture
> that the migration converged on and the decisions that closed the last
> markers — so a future audit does not rediscover them as "debt".

## Metrics — pinned commands

| # | Command | @ main (merge #49) | @ `fix/source-native-cdc` |
|---|---|---|---|
| 1 — **TOTAL** | `grep -rn "QUARANTINE" --include="*.go" internal/ cmd/ test/ \| wc -l` | 8 | **0** |
| 2 — **TREND** | `grep -rn "QUARANTINE" --include="*.go" internal/ \| grep -vE "_test\.go:" \| wc -l` | 2 | **0** |

## The architecture the migration converged on

**The row universe ends at the CDC decoder; everything downstream is
columnar.**

```
CDC decoder (binlog/JSON events, row-shaped)
   → row events (rowchange.Change)
   → SOURCE BOUNDARY: drift gate (native shape vs canonical schema) + encode
   → columnar wire batch (canonical schema; stable per table)
   → coordinator (batch-native pump, G0) / runner
   → worker (fully columnar batcher, G2) → sinks (consume RecordBatch directly)
```

### Why the row universe legitimately ends at the decoder

Binlog and JSON change events are row events. Decoding them directly into
Arrow arrays is the theoretical "source-native" ideal, but it is
contradicted by the drift requirement the migration surfaced:

- Nested schema drift (a field added inside a struct column) is only
  detectable where the **native row shape** exists, before any encode
  against the canonical schema (which would silently drop the unknown
  field — a real silent-loss gap).
- That place is the source boundary, where the canonical schema is also
  known (introspected at Open).
- A source-native decoder sending its own live shape downstream would move
  drift detection away from the schema owner, or require the resolved
  schema to ride the reader contract into every source.

So the boundary owns both the drift gate and the encode. This is a
documented architecture decision (the plan's "re-evaluate the milestone
definition"), backed by the drift gate in `sourcepull.makeBatch`
(`sourcepull/drift_test.go`) — not a relabel of unfinished work.

## What was closed, wave by wave

- **Waves 0-4 of DP-AUDIT v4** (fix/audit-code, PR #48): codec type
  system, schema round-trip, transform preservation, enrich/delete
  contracts, EncodeKey design.
- **G3 sinks** (fix/audit-structural): all four sinks consume the
  RecordBatch directly via `transport.BatchReader`; composite codec
  symmetric (encode + decode).
- **G0 coordinator** (fix/audit-structural): batch-native pump, one
  serialize per source batch, granularity-insensitivity proof.
- **G2 worker** (fix/audit-structural): fully columnar batcher — windows
  as stored batch + touched set, mask partition, columnar collapse,
  whole-batch enrich, schema-based drift. `decodeToChanges` and
  `IngestFromChanges` deleted.
- **G1 source boundary** (fix/source-native-cdc): snapshot windows encode
  against the introspected schema; mysql introspects at Open and installs
  canonical schemas on the puller; **drift gate at the boundary** closes
  the nested-drift silent-loss gap (new, tested).
- **G5 test shadows**: test helpers' comments re-scoped where their
  "dies when..." premise referred to production work that landed; the two
  nested-drift worker skips were relocated to real sourcepull drift tests
  (the behavior lives at the source boundary, not the worker).

## Remaining honest gaps (not quarantine — tracked elsewhere)

- **Enrich join is row-based** by design until the columnar broadcast join
  (CR-069 §3.4) — the seam is permanent, not transitional.
- **Nested drift for schema-less producers** (kafka/plugin without
  SetSchemas) is still unguarded at the boundary — their resolved schema
  is owned upstream and should ride the reader contract when it lands.
