---
sidebar_position: 6
---

# One orchestration for collapsed and distributed mode (design)

Status: **accepted and implemented** (issue #404; part of the #397 restructure).
Steps 1–4 (the shared core) are merged; step 5 (the Option A re-evaluation) is
recorded below. This is a design record.

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
| `tables[].bootstrap` (adopt / adopt-verify / `startAt: explicit`) | yes | silently ignored (#405) | **resolved** — shared `internal/resume` (step 3) |
| Resumable snapshot progress (`snapshot.ReadSnapshotProgress`/`Persist`) | yes | not read/written by the coordinator (staged cycles cover it) | **intended** — staged cycles are the distributed equivalent; document it |
| Partitioning (`workers > 1`) | rejected (`rejectCollapsedPartitioning`) | yes | **intended** — collapsed mode is single-worker by construction; the boot error is explicit |
| DBLog gate | one global gate (`relay`), unbounded buffer | keyed by `(target, partition)`, bounded (`gateDrain`) | **resolved** — one shared `internal/gate` (step 1) |
| Enrich wildcard columns at boot | `enrich.New` + `Stage.LoadWildcards` | `enrich.LoadWildcardColumns` | **resolved** — one algorithm in `internal/plan` (step 2) |
| Maintenance | in-process `maintenance.RunLoop` | ephemeral maintenance workers | **intended** — a mode-specific execution of the same maintenance plan |
| Resume | `resumeFrom` | `Coordinator.resumeFrom` (+`OwnerCount`, `caps.Snapshot`) | **resolved** — one resolver, `internal/resume` (step 3) |
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
   single key. **Done** (#736).
2. `internal/plan` — introspection → `{refs, wire, resolved, casts, bySource}`,
   including enrich wildcard expansion (one algorithm, not two). **Done** (#738).
3. Resume + bootstrap resolution as one function returning `{start position,
   tables to snapshot, tables to adopt}`. **Done** — `internal/resume` (#739).
4. The snapshot/adopt loop as one driver over a small interface
   (`OpenWindow`, `AddWindowRows`, `Release`, `Persist`). **Partially done.**
   `snapshot.SnapshotTable` already IS the shared per-chunk driver: the
   collapsed runner and the coordinator's rendezvous fan-out both run it. The
   still-duplicated *prelude* (progress read, resume-cursor decision, adopt /
   complete) is now shared too — `snapshot.Resumable`/`ReadProgress`/
   `MarkComplete` (#741). The coordinator's **single-owner range loop** is not
   fused into the driver: it is structurally inverted (N byte-capped windows
   per chunk with per-window proof/markers, asynchronous `WindowOpen`/
   `ChunkReady`, worker loss/redo, and a packed `partition<<20|index` durable
   cursor). Driving it with `SnapshotTable` would change observable window
   granularity and the persisted cursor format — a **behavior change**, so it
   is a separate future issue, not this refactor.
5. Re-evaluate Option A with an in-memory-transport benchmark. **Done** — see
   below.

## Step 5 — re-evaluation of Option A (collapse `urutau run` onto the coordinator)

**Decision: Option A is NOT taken.** The collapsed runner stays a distinct
in-process driver over the shared core.

Option A would run `urutau run` as a real coordinator + one worker, wired over
an in-memory `bufconn`. `bufconn` removes the socket, but it does not remove
the **Arrow wire codec** at each hop of the single-process fast path. The wire
format is Arrow IPC, so every batch is encoded once and decoded once on the way
through the (in-process) transport. The repo's own transport benchmark measures
that cost (`internal/transport/wire_bench_test.go`; 2000-row
id/name/amount batch, ~252 KB record, on the author's 16-core machine):

| Step | Time | Alloc | Note |
|---|---|---|---|
| `EncodeRecord` (coordinator side) | ~130 µs/batch | 703 KB, 79 allocs | IPC body = 1.00× the record |
| `DecodeRecord` (worker side) | ~56 µs/batch | 263 KB, 123 allocs | |

So Option A would add ~186 µs + ~966 KB of transient allocation per 2000-row
batch (~93 ns/row) to the collapsed path, purely to move data between two
objects living in the same process. That is the opposite of what a "collapsed"
mode exists for. (The same evidence retired the zero-copy transport frontier
in #455: Arrow Flight's wire format IS Arrow IPC, so there is no second copy to
remove.)

The other reasons A is unattractive hold regardless of the benchmark:

- The collapsed runner is the **in-process plugin host**
  (`NewRunnerWithAdapters`) and carries mode-specific boot semantics
  (single-worker by construction, incremental pass, no partitioning) that a
  coordinator+worker would have to re-learn and re-expose.
- The **problem was divergence, not the process split**, and Option B's shared
  core (steps 1–4) already removed it: the gate, introspection, resume and the
  snapshot prelude are now single implementations. Collapsing further buys no
  additional de-duplication.

A future Option A would only be justified if a zero-copy in-process transport
(or sharing the record batch by pointer instead of IPC) could be built without
changing the wire contract — a non-goal below. Absent that, the two modes stay:
one shared core, two transports.

## Non-goals

- Changing the wire contract (`proto/`, Arrow wire schema, the Flight plugin
  contract).
- Making partitioning, maintenance or incremental uniform across modes: those
  stay **intended** mode-specific behaviors, each with an explicit boot error
  when a spec asks for it in the wrong mode.
