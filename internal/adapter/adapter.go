// Package adapter defines the contracts the pipeline drivers consume: the
// per-source surfaces (query access, introspection, chunking, the replication
// reader, and the position codec) as interfaces plus the shared type aliases.
// It deliberately imports NO concrete source or sink — the assembly that
// resolves a spec's source kind into an implementation lives in internal/
// drivers, which is the only package (besides cmd/) that knows the
// implementations.
package adapter

import (
	"context"
	"database/sql"

	"github.com/apache/iceberg-go"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Runtime carries the replication knobs a driver passes through to the
// source reader.
type Runtime = source.Runtime

// Capabilities declares what a source supports.
type Capabilities = source.Capabilities

// StreamSource is the replication reader surface a driver drives.
type StreamSource = source.StreamSource

// Source is the per-source surface every source provides. It deliberately
// carries NO SQL shape: introspection, reading and position are the common
// pipeline jobs (the db parameter is the source's optional query connection,
// nil for sources without one). The SQL-only surface — opening the query
// connection and chunked snapshot SELECTs — lives on QuerySource, which only
// relational sources implement, so a stream source never stubs methods it
// cannot do.
type Source interface {
	// Caps returns the source's capabilities. The runner uses this to
	// decide which pipeline phases to run.
	Caps() Capabilities
	// Introspect resolves one spec table into its ref, the canonical schema
	// with the declared cast and metadata columns applied, and the final
	// Iceberg schema built from it. The warnings carry the advisory cast
	// outcomes.
	Introspect(ctx context.Context, db *sql.DB, t spec.Table) (core.TableRef, core.Schema, *iceberg.Schema, []core.Warning, error)
	// NewReader builds the replication reader over the pipeline's tables.
	NewReader(ctx context.Context, db *sql.DB, refs []source.TableRef, out chan<- change.Change) (StreamSource, error)
	// InitialPosition is the stream start for a first boot (no resume).
	InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error)
	// ParsePosition decodes a stored cdc.position string.
	ParsePosition(s string) (position.Position, error)
}

// QuerySource is the SQL-backed surface of a relational source: opening the
// query connection used by chunk SELECTs, schema introspection and position
// queries, and building the chunk SELECT source for one table. MySQL and
// Postgres implement it; a stream source (kafka) does not, and therefore has
// no SQL methods to stub.
type QuerySource interface {
	// OpenQuery opens the SQL connection for chunk SELECTs, schema
	// introspection, and position queries.
	OpenQuery(ctx context.Context) (*sql.DB, error)
	// NewChunker builds the chunk SELECT source for one table.
	NewChunker(db *sql.DB, source, pk string, chunkSize int) (source.ChunkSource, error)
}
