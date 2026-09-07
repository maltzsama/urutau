# Urutau

*Tupi–Guaraní for the potoo — a nightjar that stands motionless through the night, watching. A fitting name for a process that spends its life quietly watching a binlog.*

Go ≥ 1.26 · pre-0.1.0, hardening in progress · license: **Apache-2.0**

Urutau replicates MySQL, Postgres, and Kafka into Apache Iceberg, ClickHouse, and Couchbase **reflecting source state** — upsert by primary key, first-class UPDATE/DELETE — with the CDC position committed **alongside the data it describes**, never in a store that could drift from it.

> This repository holds the **Go engine** (coordinator, workers, CLI,
> operator). The Python SDK/planner lives in its own repository.

## Why

For the common case — one sink, no multi-consumer replay — Debezium+Kafka puts a broker in the data path to solve a problem you don't have. Urutau reads the replication log once and writes the destination directly, with no broker between the two.

It also writes natively. Some CDC-to-lakehouse tools hand the actual write off to a JVM sidecar process — a JAR, a gRPC hop, a second runtime to keep alive. Urutau writes in the same Go binary that reads the log: one process, one failure domain, no second runtime in the build.

Recovery follows from the same idea. Nothing durable lives in what can die — the coordinator and workers are replaceable; state lives only in the source's own log, the destination table, and the pipeline definition, all of which survive a total restart. Recovering from a dead cluster means `kubectl apply` and reading the committed position back out of the sink, not replaying a separate checkpoint log.

**The engine is closed; the driver seam is open.** Sources and sinks are public Go contracts at the module root (`source`, `sink`, `driver`, `core`, `change`, `position`, `spec`) — a source or sink is a package that implements a handful of small interfaces and registers itself, never touching an `internal/` path. See [`docs/plugins.md`](docs/plugins.md) and the reference implementation in [`test/plugin`](test/plugin/fake.go), a source and sink written against nothing but those contracts.

## What it looks like

```yaml
# pipeline.yaml
pipeline: orders-demo
source:
  kind: mysql
  uri: mysql://user:pass@localhost:3306/shop
  serverId: "1101"

sink:
  type: iceberg+rest
  uri: http://localhost:8181/api/catalog
  warehouse: quickstart_catalog
  namespace: bronze

tables:
  - source: shop.orders
    target: bronze.orders
    primaryKey: [id]
    partitionBy: [day(created_at)]
    writeMode: upsert
```

```sh
urutau run -f pipeline.yaml
```

`run` is the collapsed mode: coordinator and worker in one process against
the sink — no Kubernetes required to try it. (`serverId` is a string on
purpose: it flows into the spec as written, and a UUID server id is legal.)

## Status

**Pre-0.1.0.** The engine runs end to end — MySQL/Postgres/Kafka into Iceberg, ClickHouse, or Couchbase, single-process or distributed, with the k8s operator — and the commit path has been verified by reading back through Trino rather than trusting a successful write. Correctness-critical paths are still being actively hardened; see **Known limitations** before relying on this for anything you can't afford to lose.

- **Sources:** MySQL (`go-mysql`/canal, GTID, heartbeat), Postgres (`pgx`, pgoutput, LSN slot), Kafka (franz-go, manual partition assignment, debezium-json/raw/avro decoders) — one replication reader per source, mapped through the canonical type system. Kafka registers `Capabilities{Stream: true}` (no snapshot capability); the runner skips DBLog and streams directly from the committed offset.
- **Iceberg sink:** upsert via equality delete, delete-then-append as two separate commits (see **E2E spike** below for why), position committed as both a snapshot property (audit trail) and a table property (O(1) resume, survives compaction).
- **ClickHouse sink:** upsert via `ReplacingMergeTree(seq, is_deleted)` (`ORDER BY` the declared primary key), append via plain `MergeTree`. One `INSERT` per batch — upserts as rows, deletes as tombstones hidden from `FINAL` reads. Resume reads the position from the data itself — `argMax(position, seq)` — never a separate control table. **The atomicity guarantee is narrower than Iceberg's**: the default table has no `PARTITION BY`, which is what makes a batch atomic (ClickHouse guarantees atomicity per insert, per partition — not across a multi-partition write). Partitioning is opt-in and weakens that guarantee: a batch crossing a partition boundary commits as multiple parts, no longer all-or-nothing. Tombstone physical cleanup is operator maintenance (`OPTIMIZE ... FINAL CLEANUP`); reads are correct under `FINAL` regardless of whether cleanup has run.
- **Couchbase sink:** key-document writes where upsert-by-key IS the native operation — one collection per table, one control document per collection carrying the committed position (a single O(1) `Get` on resume, never a scan or aggregation). Documents hold data fields at the top level and pipeline metadata under a reserved `_urutau` sub-object, so a data field named `op` never collides with the metadata `op`. Nested canonical types (`Struct`/`List`/`Map`) land as native JSON — the one sink where nesting is not a special case. Deletes are `Remove`, immediate, not tombstones. **The atomicity trade is a mode, not a caveat**: `commitMode: fast` (default) writes data first and the control document last — a crash in between leaves the position un-advanced and the restart replays the batch, which is idempotent because every mutation is keyed by the row's primary key. `commitMode: atomic` wraps data and control document in a distributed ACID transaction, closing the window at the cost of transaction overhead per batch. Every write acknowledges at synchronous-durability `majority`; on a single node that requires a 0-replica bucket (which is what the sink creates) — `DurabilityImpossible` is a loud error, not a silent downgrade.
- **Metadata columns:** closed catalog — CDC (`op`, `commit_ts`, `ingest_ts`, `position`, `source_table`, `phase`) and transport-native (`stream`, `shard`, `sequence`, `msg_ts`, `msg_key`, `headers`) — landed as nullable columns at the end of the canonical schema, renamed per-table via `metadata`.
- **Per-column cast:** explicit type overrides with a closed matrix — widening always, to-string always, narrowing/parsing never except explicit temporal reinterpretation (`timestamptz(assume_utc)`). Unmappable source types bypass the cast rather than silently coercing.
- **DBLog snapshot:** generic in `internal/snapshot` — chunk by primary key, low/high watermarks, and a caught-up **proof** that closes each window (never a timer; `windowTimeout` is a pathology detector, not a trigger). Skipped for sources without snapshot capability (Kafka).
- **Worker:** per-key collapse in upsert mode, pass-through in append mode, strictly serialized commits per table, over a sink-agnostic contract.
- **Enrichment:** broadcast hash join against small reference tables (`internal/enrich`) — the reference is read whole into worker RAM, every event matches in O(1) against the map, and a periodic full re-read swaps the map atomically (in-flight events finish on the old image). No lookup per event, no shuffle, no windowed state. **Projection is explicit**: `select` is required and lists the reference columns the event receives — nothing unselected ever lands in the sink. **Column namespacing follows Spark DataFrame semantics**: unrenamed columns are automatically prefixed with `{table}.{column}` (e.g., `users.name`, `products.id`) to prevent silent collisions when multiple references inject columns with the same name. `select: ["*"]` injects all reference columns with prefixes; source columns remain unprefixed. `as` renames at load time and overrides the prefix: `select: [name, tier]` with `as: {"users.name": "user_name"}` yields `user_name` (no prefix) and `users.tier` (prefixed). The `as` map is keyed by the prefixed column name for consistency. A projected name that collides with an event column **overwrites** it (the reference is the point of the join) — rename when coexistence matters. `joinType: left|inner` per reference (inner drops on a miss; left passes with NULL reference columns and `enrich_miss` when the table declares that metadata column); `onColdStart: buffer|pass|drop` governs events that arrive before the first reference load, with the buffer bounded by both events (`maxEvents`) and time (`maxWait`) — an evacuated event follows the join type, so there is never a third miss policy. **Enrichment is point-in-time, and pipelines with it say so**: `statusz` reports `enrichment: point-in-time` — the enriched columns are not reproducible by replay (the reference is a snapshot, not CDC), while source columns and position stay deterministic either way. A pipeline without `enrich` runs the identical code path it always did.
- **Distributed split:** coordinator (control plane + Flight data plane, flow budget, supervisor) and workers, with the two streams lifecycle-coupled on one connection — a dead channel is detected by both sides, never a silent zombie.
- **Supervision:** a worker that stops acking is reset (epoch bump, session cancel); resets beyond the cap within the window terminate the job — crashloops stay dead until a human clears them.
- **Driver registry:** self-registration from `init()`, blank-imported by `internal/builtin`. A missing registration now fails loudly: the error lists every registered kind and points at the blank-import, and `TestAllDriversRegistered` fails in CI if a built-in driver silently stops registering.
- **Operator:** CDCPipeline CRD, coordinator StatefulSet reconciler,
  validating webhook, envtest suite.

### Known limitations

This list is expected to shrink, not grow, before 0.1.0:

- A terminated pipeline (`status.terminated` set) is terminal by design — the operator parts ways and nothing recreates the job; a new run means a new CR.
- Composite (nested) columns are not yet mappable into ClickHouse: `KindList`/`KindMap`/`KindStruct` hit the escape valve and require an explicit cast today (Iceberg and Couchbase take them natively). Native `Array`/`Map`/`Tuple` mapping is a planned follow-up.
- Iceberg tables with a `map` column cannot be written by `iceberg-go` v0.6.0 (`AppendTable` rejects the composite record); tracked upstream, worked around by an explicit cast to string.
- The ClickHouse sink's atomicity is per-insert-per-partition, not per-commit like Iceberg's; see **Status** above and don't opt into `PARTITION BY` without reading that paragraph.

If you hit any of these, please open an issue rather than working around them silently — they're tracked, not forgotten.

### Roadmap

In: nested canonical types (`Struct`/`List`/`Map`) and `KindFixedBinary`; Kafka `raw` and Confluent-Avro decoders (the Avro decoder is the first real consumer of the nested types); transport-native metadata columns (`stream`, `shard`, `sequence`, `msg_ts`, `msg_key`, `headers`); the append-idempotent write mode keyed on the transport's own monotonic coordinate. Ahead: Kinesis as a source (which, unlike Kafka, reshards — closing/parent-child shard lineage the position contract will need to model), native ClickHouse mapping for the nested types (`Array`/`Map`/`Tuple`), and the post-0.1.0 sync modes declared in the contract (`incremental`, `backfill-only`).

## Architecture

Sources and sinks are decoupled behind public contracts at the module root — `source`, `sink`, `driver`, `core`, `change`, `position`, `spec` — not under `internal/`.
A canonical type system (`core`) crosses the source↔sink boundary, so N sources × M sinks cost N+M type mappings instead of N×M.
The DBLog snapshot orchestrator is source-agnostic (`internal/snapshot`); each concrete driver is self-contained and registers itself with the driver registry (`driver`) from `init()` — the orchestration (`runner`/`coordinator`/`worker`) consumes only the contracts, never a concrete implementation. `internal/builtin` blank-imports the built-in drivers; a third-party driver registers the same way from its own module.

```mermaid
flowchart TB
    CORE["core — canonical types + cast + metadata"]
    SRC["source — contract"]
    SNK["sink — contract"]
    SNAP["snapshot — generic DBLog"]
    WRK["worker"]
    STD["spec / position / change"]

    subgraph impls ["implementations — never import each other"]
        MYSQL["source/mysql"]
        PG["source/postgres"]
        KAFKA["source/kafka"]
        ICE["sink/iceberg"]
        CH["sink/clickhouse"]
        CB["sink/couchbase"]
    end

    DRV["driver — registry (self-registration via init)"]
    RUN["runner / coordinator — consume interfaces only"]

    CORE --- SRC & SNK & SNAP & WRK & STD
    SRC --> MYSQL & PG & KAFKA
    SNK --> ICE & CH & CB
    MYSQL & PG & KAFKA & ICE & CH & CB --> DRV
    DRV --> RUN
```

The dependency walls are enforced by a test (`internal/architecture`) that checks direct imports via `go list` — a leak fails CI, not a future driver. The same test locks in that `test/plugin` imports only the public contracts (`TestPluginPackageImportsOnlyContracts`), so the plugin seam can't quietly grow an `internal/` dependency either.

## Repository map

| Path | Role |
| --- | --- |
| `cmd/urutau` | CLI (`run -f pipeline.yaml`, …) |
| `cmd/coordinator` | coordinator binary (reader, router, supervisor, Flight) |
| `cmd/worker` | worker binary (sink writer, Flight consumer) |
| `cmd/operator` | Kubernetes operator (CRD reconciler + webhook) |
| `core` | **public.** canonical type system (`Kind`, `Schema`, `TableRef`), cast policy, metadata catalog |
| `source` | **public.** source contract (`Source`, `Reader`, `ChunkSource`, `Capabilities`, `Runtime`, `Chunk`) |
| `sink` | **public.** sink contract (`Sink`, `TableWriter` with commit invariants, `Config`) |
| `driver` | **public.** the driver registry — `RegisterSource`/`RegisterSink`, resolved by kind/type |
| `change` | **public.** row change event, per-key collapse, batch, write mode |
| `position` | **public.** position contract (GTID/LSN/Kafka offsets, `Compare`/`Contains`) |
| `spec` | **public.** resolvedSpec + single server-side validation |
| `test/plugin` | reference external driver — a source + sink written against only the public contracts |
| `internal/builtin` | blank-imports the built-in drivers so their `init()` registers them |
| `internal/snapshot` | generic DBLog orchestrator (chunk + caught-up proof) |
| `internal/source/mysql` | MySQL source (`go-mysql`/canal, GTID) |
| `internal/source/postgres` | Postgres source (`pgx`, pgoutput, LSN slot) |
| `internal/source/kafka` | Kafka source (franz-go, manual partition assignment, debezium-json) |
| `internal/source/kafka/decoder` | Kafka message decoders |
| `internal/sink/iceberg` | Iceberg writes (upsert/equality delete, `FromCanonical`, cast projection) |
| `internal/sink/clickhouse` | ClickHouse sink (`ReplacingMergeTree` upsert, tombstone deletes, position-as-column resume) |
| `internal/sink/couchbase` | Couchbase sink (key-document upsert, `_urutau` metadata sub-object, control-document position, fast/atomic commit modes) |
| `internal/coordinator` | reader/router loops, flow budget, supervisor, control plane |
| `internal/worker` | per-table batcher + serialized committer |
| `internal/enrich` | broadcast hash join enrichment (reference maps, cold-start buffer, point-in-time) |
| `internal/transport` | gRPC control + Arrow Flight; generated code in `internal/transport/pb` |
| `internal/eventlog` | per-run-id JSONL audit trail in S3 |
| `internal/observability` | lean Prometheus metrics + live `/statusz` |
| `internal/architecture` | import-boundary tests — enforces the walls in the diagram above |
| `api/v1alpha1` | CDCPipeline CR types |
| `config/` | CRD + RBAC manifests |
| `proto/` | coordinator↔worker wire contract |
| `docs/plugins.md` | how to write a third-party source or sink |

## Development

```sh
make bootstrap        # buf, golangci-lint, setup-envtest pinned into ./bin
make envtest-setup    # install the operator envtest control plane
make build            # bin/urutau, bin/urutau-coordinator, bin/urutau-worker, bin/urutau-operator
make test             # go test -race ./... (operator envtest skipped without assets)
make lint             # golangci-lint
make proto            # buf lint + generate (generated code is committed)
```

No `protoc` needed — generation uses `buf` with the `protoc-gen-go`/
`protoc-gen-go-grpc` plugins pinned as `go tool`.

## E2E spike

The suite proves the write path by **reading it back** — Iceberg through Trino, ClickHouse through its own `FINAL` reads, Couchbase through independent SDK reads — rather than trusting a successful commit. Stack: MySQL + Postgres (sources), RustFS (S3) + Polaris (REST catalog) + Trino, a ClickHouse container, and a Couchbase container (single-node, 0-replica bucket — the configuration where synchronous durability works).

```sh
make e2e-test   # compose up --wait, then URUTAU_E2E=1 go test ./test/e2e
make e2e-down   # tear the stack down
```

It exercises append, equality delete, the `cdc.position` snapshot/table properties, and the full MySQL pipeline — binlog → DBLog snapshot → stream → Iceberg, with resume after downtime.

**Key finding:** in `iceberg-go` v0.6.0, an append and an equality delete staged in **one** transaction produce two snapshots, and the delete gets the higher sequence number — it also deletes the freshly appended file. A correct Iceberg upsert is therefore delete-then-append in **separate** commits, never append-then-delete in one. The underlying principle is what the `sink.TableWriter` contract states as its invariant — *the position must never advance past durably written data* — and each sink encodes it with its own mechanism: Iceberg via delete-then-append with the position on the last commit, ClickHouse via the position traveling on every row of a single INSERT.

## Writing a driver

The engine is closed; the driver seam is open. A source or sink is a Go package that implements the contracts in `source`/`sink`/`core`/`change`/ `position`, registers itself from `init()` via `driver.RegisterSource`/ `RegisterSink`, and never imports anything under `internal/`. Full guide, including the registration pattern and the capability negotiation (`Capabilities`, `MaxConnections`, `Modes`), in [`docs/plugins.md`](docs/plugins.md). The reference implementation — [`test/plugin`](test/plugin/fake.go) — is a working source and sink written against nothing but the public contracts, exercised end-to-end by its own test.

## Companion repository

The Python authoring SDK and planner (the `.py` pipeline definitions this engine's operator resolves) live in a separate repository. This repo never imports Python and never executes user code directly — the planner runs in an init container, ahead of the coordinator.

## Contributing

Issues and PRs are welcome. All code, comments, commit messages, and documentation in this repository are **English**. `CONTRIBUTING.md` (DCO/ CLA decision, code of conduct) is not written yet — treat that as an open item, not an oversight to work around.

## License

This project is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for details.
