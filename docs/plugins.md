# Writing a driver (source or sink)

Urutau's engine is closed, but its **driver seam is open**. A source or sink
is a Go package that implements a handful of small public interfaces and
registers itself with the driver registry. The orchestration (runner,
coordinator, worker) consumes only those interfaces — it never imports a
concrete driver.

Everything a driver needs lives in the public packages at the module root:

| Package | Contents |
| --- | --- |
| `source` | source contract: `Source`, `Reader`, `SourceReader`, `ChunkSource`, `Capabilities`, `Runtime`, `Chunk` |
| `sink` | sink contract: `Sink`, `TableWriter`, `Config` |
| `driver` | the registry: `RegisterSource`, `RegisterSink`, `OpenSource`, `OpenSink` |
| `core` | canonical type system (`Schema`, `Column`, `ColumnType`, `Kind`, `TableRef`) |
| `change` | the row-change event and batch (`Change`, `Batch`, `Op`) |
| `position` | replication position (`Position`, `GTID`, `LSN`, `Offsets`) |
| `spec` | the resolved pipeline spec |

A driver **must not** import anything under `internal/`. The reference
implementation is [`test/plugin`](../test/plugin/fake.go): a source and a sink
written entirely against the public contracts, registered from `init()`, and
driven end-to-end by `test/plugin/fake_test.go`. `internal/architecture`
locks this in CI (`TestPluginPackageImportsOnlyContracts`).

## The registration pattern

A driver registers itself from `init()`, exactly like `database/sql`:

```go
func init() {
    driver.RegisterSource("kinesis", kinesisCaps, func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
        return openKinesis(s, rt)
    })
    driver.RegisterSink("delta", func(ctx context.Context, cfg sink.Config) (sink.Sink, error) {
        return openDelta(ctx, cfg)
    })
}
```

The engine resolves a driver by its spec keys: `source.kind` and
`sink.type` (default `iceberg+rest`). A binary must blank-import the driver
package (or a bundle like `internal/builtin`) so its `init()` runs; a
third-party driver registers the same way from its own module.

## Writing a source

A source implements `source.Source` — three composed interfaces:

```go
type Source interface {
    Streamer      // Open(ctx, refs) (Reader, error)
    Positioner    // InitialPosition(ctx); ParsePosition(s)
    Introspector  // Introspect(ctx, t) (ref, schema, warns, error)
}
```

- `Introspect` resolves one spec table into a `core.TableRef` + canonical
  `core.Schema` (cast + metadata columns applied). It returns the **canonical**
  schema only; the sink maps it to its own storage schema.
- `Open` returns a `source.Reader`, the stream handle. The source owns its
  own connections (no `*sql.DB` crosses the contract).
- `InitialPosition` is the first-boot start; `ParsePosition` decodes a stored
  `cdc.position`.

The `Reader` streams changes and reports the watermarks the DBLog snapshot
orchestrator needs:

```go
type Reader interface {
    SourceReader  // Synced(), Master(ctx), OpenWindow(ctx, chunkID), ClearWindow()
    Stream(ctx, from) (<-chan change.Change, <-chan error)
    Close()
    SetConfirmed(func() position.Position)
}
```

- `Stream` emits `change.Change` on the returned channel and delivers the
  terminal error (nil for a clean, ctx-driven end) on the error channel.
- `Synced`/`Master` are the read/high watermarks. The caught-up proof is
  **not** a method on the reader — it lives in the shared `position`
  comparison (`position.Position.Contains`), so a driver never reimplements
  "am I caught up".

A relational source additionally implements `source.QuerySource`
(`NewChunker` + `CloseQuery`) for the DBLog snapshot chunk SELECTs; a stream
source (Kafka, Kinesis, SQS) does not, and reports
`Capabilities{Stream: true}` only.

`Capabilities` is registration data, not a method: `Snapshot`, `ChunkQuery`,
`Stream`, `MaxConnections`, `BeforeImage`, `MonotonicSequence`.

## Writing a sink

A sink implements `sink.Sink` — six composed capabilities:

```go
type Sink interface {
    Ensurer        // EnsureTable(ctx, ref, schema, partitionBy, cast)
    Writer         // Writer(ctx, ref, cast, meta) (TableWriter, error)
    Positioner     // Position(ctx, ref) (string, error)
    PropertySetter // SetProperties(ctx, ref, props)
    PropertyGetter // Properties(ctx, ref) (map[string]string, error)
    Closer         // Close()
}
```

The per-table `TableWriter` carries two correctness invariants that every
sink inherits:

```go
type TableWriter interface {
    Commit(ctx, b change.Batch) error
    Close() error
}
```

1. **Delete-then-append.** Equality deletes and data rows must never be
   staged in one transaction (in `iceberg-go` v0.6.0 that produces two
   snapshots with the delete holding the higher sequence number, deleting the
   fresh rows too). Deletes commit first, data second.
2. **Position last.** The `cdc.position` travels inside `change.Batch.Position`
   and is written only on the **last** commit of a batch — never on a delete
   commit while an append is still pending.

`Position` returns the committed position (empty string = never written).
`SetProperties`/`Properties` back the snapshot progress and adoption
bookkeeping.

## Checklist

1. Import only `source`, `sink`, `driver`, `core`, `change`, `position`,
   `spec` — no `internal/`.
2. Register from `init()` with `driver.RegisterSource` / `driver.RegisterSink`.
3. Compile-time-check the interface: `var _ source.Source = mySource{}`.
4. Blank-import the package (or its bundle) from every binary that boots it.
5. `go test ./internal/architecture` stays green.
