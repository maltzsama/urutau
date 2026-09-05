// Package source holds the replication source contracts. Sources
// map their native types into core.Schema and stream row changes into
// change.Change; they know nothing about any sink. Drivers open sources of
// one scheme; the driver registry lives in internal/drivers.
package source

import (
	"context"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/position"
)

// Config is what every source needs, in neutral terms. Driver-specific
// knobs (server_id, slot name, heartbeat) live in Options.
type Config struct {
	URI     string // "mysql://user:pass@host:3306/db"
	Tables  []core.TableRef
	Options map[string]string // driver-specific, validated by the driver
}

// Source is the replication reader: ONE connection per source, streaming
// row changes from a position. It knows nothing about any sink.
type Source interface {
	// Open establishes the single replication connection.
	Open(ctx context.Context) error

	// Stream emits changes from `from` (nil = current master position).
	// The channel closes when ctx is done or the stream ends.
	Stream(ctx context.Context, from position.Position) (<-chan change.Change, error)

	// Current is the source's own current position — the master coordinate,
	// not what has been read. Used as the DBLog watermark.
	Current(ctx context.Context) (position.Position, error)

	// CaughtUp reports whether the reader has provably consumed everything
	// up to `high`. This is the DBLog proof and MUST NOT be time-based.
	CaughtUp(high position.Position) bool

	Close() error
}

// Introspector derives the canonical schema of a source table. Separate
// from Source because it uses a query connection, not the replication one.
type Introspector interface {
	Schema(ctx context.Context, sourceTable string) (core.Schema, error)
}

// Chunk is a half-open primary-key range [Low, High); the last chunk of a
// table sets InclusiveHigh.
type Chunk struct {
	ID            uint32
	Low, High     []any
	InclusiveHigh bool
}

// Chunker splits a table by primary key for the DBLog snapshot and reads
// each chunk. Uses query connections, bounded by maxParallel.
type Chunker interface {
	Chunks(ctx context.Context, ref core.TableRef, chunkSize int) ([]Chunk, error)
	ReadChunk(ctx context.Context, ref core.TableRef, c Chunk) ([]core.Row, error)
}

// Driver opens sources of one scheme ("mysql", "postgres").
type Driver interface {
	Scheme() string
	Open(ctx context.Context, cfg Config) (Source, Introspector, Chunker, error)
}
