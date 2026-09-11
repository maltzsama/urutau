# Urutau

*Tupi–Guaraní for the potoo — a nightjar that stands motionless through the night, watching. A fitting name for a process that spends its life quietly watching a binlog.*

Go ≥ 1.26 · pre-0.1.0, hardening in progress · license: **Apache-2.0**

Urutau replicates MySQL, Postgres, and Kafka into Apache Iceberg,
ClickHouse, and Couchbase **reflecting source state** — upsert by primary
key, first-class UPDATE/DELETE — with the CDC position committed
**alongside the data it describes**, never in a store that could drift
from it.

> This repository holds the **Go engine** (coordinator, workers, CLI,
> operator). The Python SDK/planner lives in its own repository.

## Why

For the common case — one sink, no multi-consumer replay — Urutau reads
from MySQL, Postgres, or an existing Kafka/Redpanda topic and writes the
destination directly, without standing up any new broker or relay in
between.

It also writes natively. Some CDC-to-lakehouse tools hand the actual write
off to a JVM sidecar process — a JAR, a gRPC hop, a second runtime to keep
alive. Urutau writes in the same Go binary that reads the log: one
process, one failure domain, no second runtime in the build.

Recovery follows from the same idea. Nothing durable lives in what can
die — the coordinator and workers are replaceable; state lives only in the
source's own log, the destination table, and the pipeline definition, all
of which survive a total restart. Recovering from a dead cluster means
`kubectl apply` and reading the committed position back out of the sink,
not replaying a separate checkpoint log.

**The engine is closed; the driver seam is open.** Sources and sinks are
public Go contracts at the module root — a source or sink is a package
that implements a handful of small interfaces and registers itself, never
touching an `internal/` path. See [Writing a driver](https://maltzsama.github.io/urutau/docs/guides/plugins).

## Get started

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
the sink — no Kubernetes required to try it.

**→ [Quickstart](https://maltzsama.github.io/urutau/docs/quickstart)** walks through this end to
end on your machine — build the binary, stand up a real MySQL + Iceberg
locally with Docker Compose, run the pipeline, read the result back
through Trino, and watch a live change replicate.

## Status

**Pre-0.1.0.** The engine runs end to end — MySQL/Postgres/Kafka into
Iceberg, ClickHouse, or Couchbase, single-process or distributed, with the
k8s operator — and the commit path has been verified by reading back
through Trino rather than trusting a successful write. Correctness-critical
paths are still being actively hardened; read
[Known limitations and roadmap](https://maltzsama.github.io/urutau/docs/reference/roadmap)
before relying on this for anything you can't afford to lose.

## Documentation

Documentation is hosted at **[maltzsama.github.io/urutau](https://maltzsama.github.io/urutau/)**.

| Page | What's in it |
| --- | --- |
| [Quickstart](https://maltzsama.github.io/urutau/docs/quickstart) | Run a real pipeline on your machine, step by step |
| [Semantics](https://maltzsama.github.io/urutau/docs/reference/semantics) | **The behavior contract** — delivery guarantees, ordering, delete-image handling, enrich join grammar, poison-batch policy. Read this before depending on any behavior not shown in an example. |
| [Sources](https://maltzsama.github.io/urutau/docs/reference/sources) | MySQL, Postgres, Kafka — what each needs, Kafka's decoder formats |
| [Sinks](https://maltzsama.github.io/urutau/docs/reference/sinks) | Iceberg, ClickHouse, Couchbase — commit mechanics, nested-column support, atomicity trade-offs |
| [Enrichment](https://maltzsama.github.io/urutau/docs/reference/enrichment) | Broadcast reference join — grammar, cold start, examples |
| [Known limitations and roadmap](https://maltzsama.github.io/urutau/docs/reference/roadmap) | What's genuinely missing today, kept current |
| [Plugin contract](https://maltzsama.github.io/urutau/docs/reference/plugin-contract) | The normative Arrow Flight subprocess plugin contract |
| [Writing a driver](https://maltzsama.github.io/urutau/docs/guides/plugins) | How to write a source or sink — both mechanisms (Go `.so` plugin and Arrow Flight subprocess) |
| [Architecture](https://maltzsama.github.io/urutau/docs/architecture/overview) | Package boundaries, the dependency diagram, repository map, E2E spike findings |
| [EncodeKey](https://maltzsama.github.io/urutau/docs/architecture/encode-key) | Design note: the collapse-stage key encoding |
| [State position](https://maltzsama.github.io/urutau/docs/architecture/state-position) | Design note: where a committed position lives, and the sink-vs-store arbitration rule |

## Companion repository

The Python authoring SDK and planner (the `.py` pipeline definitions this
engine's operator resolves) live in a separate repository. This repo never
imports Python and never executes user code directly — the planner runs in
an init container, ahead of the coordinator.

## Contributing

Issues and PRs are welcome. All code, comments, commit messages, and
documentation in this repository are **English**. `CONTRIBUTING.md` (DCO/
CLA decision, code of conduct) is not written yet — treat that as an open
item, not an oversight to work around.

## Development

```sh
make bootstrap        # buf, golangci-lint, setup-envtest pinned into ./bin
make envtest-setup    # install the operator envtest control plane
make build            # bin/urutau, bin/urutau-coordinator, bin/urutau-worker, bin/urutau-operator
make test             # go test -race ./... (operator envtest skipped without assets)
make lint             # golangci-lint
make proto            # buf lint + generate (generated code is committed)
make docs-site        # install + serve docs at localhost:3000
```

No `protoc` needed — generation uses `buf` with the `protoc-gen-go`/
`protoc-gen-go-grpc` plugins pinned as `go tool`.

## License

This project is licensed under the **Apache License 2.0**. See the
[LICENSE](LICENSE) file for details.
