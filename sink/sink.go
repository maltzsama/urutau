// Package sink defines the destination catalog contract. A sink consumes
// core.Schema and commits change.Batch; it knows nothing about any source.
// The contract is composed of small capability interfaces; the driver
// registry resolves a spec's sink into a concrete Sink.
package sink

import (
	"context"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
)

// Config is everything a sink needs, in neutral terms. Driver-specific
// knobs (warehouse, credentials, file size, codec) live in Options.
type Config struct {
	Type      string // "iceberg+rest" (empty defaults to it)
	URI       string // REST catalog endpoint / connection string
	Namespace string
	Options   map[string]string // warehouse, client_id, client_secret, scope, …
}

// TableWriter commits one table's batches. The CDC position travels inside
// change.Batch.Position (a serialized position string), and implementations
// MUST honour the invariant below — it is correctness, not style:
//
// The position must never advance past durably written data. How each sink
// achieves that is its own mechanism:
//
//   - Iceberg (transactional commits): delete-then-append in SEPARATE
//     commits, position written only on the LAST one. Staging equality
//     deletes and data rows in a single transaction produces two snapshots
//     in iceberg-go v0.6.0 with the delete holding the HIGHER sequence
//     number — it also deletes the freshly appended rows.
//   - ClickHouse (no multi-statement transaction): the position travels on
//     every row of ONE insert, so it can never separate from the data.
type TableWriter interface {
	// Commit writes the collapsed batch and the position. A batch that
	// fails must leave the table untouched — a partially applied batch is
	// indistinguishable from data loss on resume.
	Commit(ctx context.Context, b change.Batch) error

	Close() error
}

// Ensurer creates the target table from the canonical schema if absent, and
// validates compatibility if present. The sink derives its own target
// identifier and storage schema (e.g. Iceberg field mapping) internally.
// Mode is the write shape the table serves — upsert (versioned, dedup
// capable) or append (plain log) — so sinks whose storage engine is chosen
// at DDL time (ClickHouse MergeTree family) build a table that tells the
// truth about what it is. Sinks with mode-agnostic tables (Iceberg) may
// ignore it.
type Ensurer interface {
	EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, partitionBy []string, cast core.CastPolicy, mode change.WriteMode) error
}

// Writer opens the per-table committer. The primary key, cast plan and
// metadata columns come from the pipeline plan (the ref carries the PK and
// source table identity).
type Writer interface {
	Writer(ctx context.Context, ref core.TableRef, cast core.CastPolicy, meta []core.MetadataColumn) (TableWriter, error)
}

// Positioner reads the committed CDC position of a target table. An empty
// string means the table has never been written and needs the snapshot.
type Positioner interface {
	Position(ctx context.Context, ref core.TableRef) (string, error)
}

// PropertySetter writes arbitrary properties to a target table (snapshot
// progress, operator bookkeeping).
type PropertySetter interface {
	SetProperties(ctx context.Context, ref core.TableRef, props map[string]string) error
}

// PropertyGetter reads a target table's properties (snapshot progress
// resume). A missing table yields an empty map with no error.
type PropertyGetter interface {
	Properties(ctx context.Context, ref core.TableRef) (map[string]string, error)
}

// Closer releases the sink's catalog connection.
type Closer interface {
	Close() error
}

// Sink is a destination catalog. It is the composition of the small
// capability interfaces above; a sink must satisfy all of them.
type Sink interface {
	Ensurer
	Writer
	Positioner
	PropertySetter
	PropertyGetter
	Closer
}
