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

// TableWriter commits one table's batches. Implementations MUST honour the
// two invariants below; they are correctness, not style. The CDC position
// travels inside change.Batch.Position (a serialized position string).
type TableWriter interface {
	// Commit writes the collapsed batch and the position.
	//
	// INVARIANT 1 (delete-then-append): equality deletes and the data rows
	// MUST NOT be staged in a single transaction. In iceberg-go v0.6.0 that
	// produces two snapshots with the delete holding the HIGHER sequence
	// number, so it also deletes the freshly appended rows. Deletes commit
	// first, data second.
	//
	// INVARIANT 2 (position last): the CDC position is written only on the
	// LAST commit of the batch. Writing it on the delete commit while an
	// append is still pending would advance the position past data that was
	// never written — permanent loss on crash.
	Commit(ctx context.Context, b change.Batch) error

	Close() error
}

// Ensurer creates the target table from the canonical schema if absent, and
// validates compatibility if present. The sink derives its own target
// identifier and storage schema (e.g. Iceberg field mapping) internally.
type Ensurer interface {
	EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, partitionBy []string, cast core.CastPolicy) error
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
