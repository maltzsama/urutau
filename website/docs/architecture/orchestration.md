---
sidebar_position: 6
---

# One orchestration for collapsed and distributed mode (design)

Status: **accepted** (issue #404; part of the #397 restructure). This is a
design record — the code steps are tracked as follow-ups below.

## The problem

`urutau run` (`internal/runner`, collapsed) and `urutau-coordinator`
(`internal/coordinator`, distributed) each implement boot, introspection,
resume, gating and the snapshot loop. `internal/runner` does **not** import
`internal/coordinator`, so every feature is built twice, and when it is built
once the two modes diverge silently.

## Divergence snapshot (as of 7d6c2af7) and disposition

| Concern | Collapsed runner | Distributed coordinator | Verdict |
|---|---|---|---|
| Incremental mode | yes (`runIncremental`) | rejected at boot | **intended** — a coordinator-mode incremental pass is future work; the boot error is explicit |
| `tables[].bootstrap` (adopt / adopt-verify / `startAt: explicit`) | yes | silently ignored (#405) | **gap** — fixed by the shared resume/bootstrap step |
| Resumable snapshot progress (`snapshot.ReadSnapshotProgress`/`Persist`) | yes | not read/written by the coordinator (staged cycles cover it) | **intended** — staged cycles are the distributed equivalent; document it |
| Partitioning (`workers > 1`) | rejected (`rejectCollapsedPartitioning`) | yes | **intended** — collapsed mode is single-worker by construction; the boot error is explicit |
| DBLog gate | one global gate (`relay`), unbounded buffer | keyed by `(target, partition)`, bounded (`gateDrain`) | **gap** — runner must use the shared gate |
| Enrich wildcard columns at boot | `enrich.New` + `Stage.LoadWildcards` | `enrich.LoadWildcardColumns` | **gap** — one algorithm |
| Maintenance | in-process `maintenance.RunLoop` | ephemeral maintenance workers | **intended** — a mode-specific execution of the same maintenance plan |
| Resume | `resumeFrom` | `Coordinator.resumeFrom` (+`OwnerCount`, `caps.Snapshot`) | **gap** — one resolver |
| External plugin adapters | `NewRunnerWithAdapters` | via the driver registry only | **intended** — the collapsed runner is the in-process plugin host |

## Decision

**Option B — shared orchestration core, two transports** — is accepted. Extract
what both modes do into shared packages, keeping the two drivers thin; then
re-evaluate **Option A** (collapse `urutau run` onto the real coordinator over
an in-memory `bufconn`) with a benchmark of the in-memory Arrow transport.

Rationale: each step of B removes a class of divergence on its own, is
low-risk and independently mergeable, and is a prerequisite for A. Collapsing
onto the coordinator (A) before the shared steps would port incremental,
bootstrap and snapshot-progress twice.

### Steps

1. `internal/gate` — one gate implementation with the coordinator's semantics
   (keyed windows, bounded buffer). The runner's `relay` becomes a user with a
   single key. *(follow-up)*
2. `internal/plan` — introspection → `{refs, wire, resolved, casts, bySource}`,
   including enrich wildcard expansion (one algorithm, not two). *(follow-up)*
3. Resume + bootstrap resolution as one function returning `{start position,
   tables to snapshot, tables to adopt}`. *(follow-up)*
4. The snapshot/adopt loop as one driver over a small interface
   (`OpenWindow`, `AddWindowRows`, `Release`, `Persist`), implemented
   in-process by the runner and over the wire by the coordinator. *(follow-up)*
5. Re-evaluate Option A with an in-memory-transport benchmark. *(follow-up)*

## Non-goals

- Changing the wire contract (`proto/`, Arrow wire schema, the Flight plugin
  contract).
- Making partitioning, maintenance or incremental uniform across modes: those
  stay **intended** mode-specific behaviors, each with an explicit boot error
  when a spec asks for it in the wrong mode.
