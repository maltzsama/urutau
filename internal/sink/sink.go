// Package sink holds the destination catalog contracts. Sinks
// consume core.Schema and commit change.Batch; they know nothing about any
// source. Drivers open sinks of one scheme; the driver registry lives in
// internal/drivers.
package sink

import (
	"context"
	"errors"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	"github.com/maltzsama/urutau/internal/position"
)

// ErrNoPosition is returned by Position when a table has never been
// written, meaning it needs the snapshot.
var ErrNoPosition = errors.New("sink: no committed position")

// Config is what every sink needs, in neutral terms.
type Config struct {
	URI       string // "iceberg+rest://host/catalog"
	Namespace string
	Options   map[string]string // warehouse, credentials, file size, codec
}

// Sink is a destination catalog. It knows nothing about any source.
type Sink interface {
	// EnsureTable creates the target table from the canonical schema if
	// absent, and validates compatibility if present.
	EnsureTable(ctx context.Context, ref core.TableRef, s core.Schema) error

	// Writer returns the per-table committer.
	Writer(ctx context.Context, ref core.TableRef, s core.Schema) (TableWriter, error)

	// Position reads the committed CDC position of a target table.
	// Returns ErrNoPosition when the table has never been written.
	Position(ctx context.Context, ref core.TableRef) (position.Position, error)

	Close() error
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

// Driver opens sinks of one scheme.
type Driver interface {
	Scheme() string
	Open(ctx context.Context, cfg Config) (Sink, error)
}
