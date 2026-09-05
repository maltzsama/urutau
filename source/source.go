// Package source defines the replication source contract. A source maps its
// native types into core.Schema and streams row changes into change.Change;
// it knows nothing about any sink. The contract is deliberately small: a
// source implements a handful of focused interfaces, and the driver registry
// resolves a spec's source kind into a concrete Source. Orchestration
// composes the interfaces it needs.
package source

import (
	"context"
	"log/slog"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/spec"
)

// TableRef maps one source table to its target and primary key. It is an
// alias of the canonical core.TableRef — the pipeline-wide table identity.
type TableRef = core.TableRef

// Mode declares the synchronization mode a source supports.
type Mode uint8

const (
	// ModeCDC is log-based change data capture — the only mode implemented.
	ModeCDC Mode = iota
	// ModeIncremental is cursor-column incremental (e.g. updated_at). It
	// exists in the contract now so adding it later does not change
	// signatures. Not implemented: it misses deletes and needs a cursor
	// column; it serves sources without replication access (RDS without
	// grants, replicas without binlog). Post-0.1.0.
	ModeIncremental
	// ModeBackfillOnly is a single snapshot with no stream. Declared in the
	// contract; not implemented.
	ModeBackfillOnly
)

// Capabilities declares what a source supports: snapshot via DBLog, chunk
// SELECT introspection, and/or replication streaming. It is registration
// data — static per kind, not per instance — so the driver registry holds it
// and the orchestrator reads it without opening a source.
type Capabilities struct {
	Snapshot   bool
	ChunkQuery bool
	Stream     bool
	// MaxConnections is the upper bound on concurrent query connections
	// the source tolerates. 0 means no opinion (unlimited).
	MaxConnections int
	// Modes lists the synchronization modes this source supports.
	Modes []Mode
	// BeforeImage reports whether the source carries the row image that was
	// deleted on a DELETE. MySQL (binlog row) always does; Postgres only
	// per replica identity; Kafka does not (tombstones are null). Append
	// tables with onDelete: record depend on this.
	BeforeImage bool
	// MonotonicSequence reports whether the transport coordinate of a
	// message never reappears (Kafka offset, Kinesis sequence) — the
	// property append-idempotent relies on.
	MonotonicSequence bool
}

// Runtime carries the replication knobs a driver passes through to the
// source reader.
type Runtime struct {
	ServerID  uint32
	Heartbeat time.Duration
	Logger    *slog.Logger
}

// Chunk is a half-open primary-key range [Low, High). Low/High are tuples in
// PK column order.
type Chunk struct {
	Low  []any
	High []any
}

// ChunkSource is the chunk SELECT surface the snapshot orchestrator
// consumes. The SQL-backed chunkers implement it.
type ChunkSource interface {
	// PK returns the primary key columns, in key order.
	PK() []string
	// Bounds returns the ordered chunk boundary keys.
	Bounds(ctx context.Context) ([][]any, error)
	// Scan reads one chunk, calling fn per row keyed by column name.
	Scan(ctx context.Context, ch Chunk, fn func(row map[string]any) error) error
}

// SourceReader is the reader surface the snapshot orchestrator needs:
// positions for the watermarks, and the window that tags events InWindow at
// decode time — so no event can escape the window by racing a channel pull.
type SourceReader interface {
	Synced() position.Position
	Master(ctx context.Context) (position.Position, error)
	// OpenWindow starts the DBLog window for chunkID: the reader captures
	// its own source watermark and, from now on, tags decoded events whose
	// position is strictly past that watermark InWindow for the chunk.
	// Events at or before the watermark are already reflected in the chunk
	// SELECT and must not be tagged. ClearWindow ends the window.
	OpenWindow(ctx context.Context, chunkID uint32)
	ClearWindow()
}

// Reader is the replication reader surface a driver drives: the DBLog
// watermark surface plus the stream. A Reader is a handle obtained from
// Source.Open; the stream itself flows through a channel.
type Reader interface {
	SourceReader
	// Stream begins streaming from `from`, emitting changes on the returned
	// channel. The terminal-error channel receives the stream's final error
	// exactly once (nil for a clean, ctx-driven end) and then the stream is
	// over. Call in a goroutine.
	Stream(ctx context.Context, from position.Position) (<-chan change.Change, <-chan error)
	Close()
	// SetConfirmed installs a callback that returns the minimum position
	// committed to the sink across all tables. The Postgres reader uses
	// this to advance the slot's confirmed_flush_lsn only to the point
	// that has been durably written — never past it.
	SetConfirmed(f func() position.Position)
}

// Streamer opens the replication reader over the pipeline's tables. The
// source owns its own connections; the reader carries no SQL shape.
type Streamer interface {
	Open(ctx context.Context, refs []TableRef) (Reader, error)
}

// Positioner resolves the stream start position: the first-boot start and
// the codec for a stored cdc.position.
type Positioner interface {
	// InitialPosition is the stream start for a first boot (no resume).
	InitialPosition(ctx context.Context) (position.Position, error)
	// ParsePosition decodes a stored cdc.position string.
	ParsePosition(s string) (position.Position, error)
}

// Introspector resolves one spec table into its ref, the canonical schema
// with the declared cast and metadata columns applied, and the advisory
// cast warnings. The final target (Iceberg/Delta) schema is the sink's
// concern, not the source's.
type Introspector interface {
	Introspect(ctx context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error)
}

// Source is the minimal surface every source provides: it streams changes,
// resolves positions, and introspects schemas. It deliberately carries NO
// SQL shape; the SQL-only surface (the chunked snapshot SELECT) lives on
// QuerySource, which only relational sources implement, so a stream source
// never stubs methods it cannot do.
type Source interface {
	Streamer
	Positioner
	Introspector
}

// QuerySource is the SQL-backed surface of a relational source: the chunk
// SELECT source for one table and the query-connection lifecycle. MySQL and
// Postgres implement it; a stream source (kafka) does not. The source owns
// its query connection — opened at driver construction, released on
// CloseQuery — so the orchestration never touches *sql.DB.
type QuerySource interface {
	// NewChunker builds the chunk SELECT source for one table.
	NewChunker(source, pk string, chunkSize int) (ChunkSource, error)
	// CloseQuery releases the source's query connection.
	CloseQuery() error
}
