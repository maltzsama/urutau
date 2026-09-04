// Package types holds shared types for source adapters: capabilities and
// runtime configuration. These live in a separate package to avoid import
// cycles between the adapter and individual source implementations.
package types

import (
	"context"
	"log/slog"
	"time"

	"github.com/maltzsama/urutau/internal/position"
	"github.com/maltzsama/urutau/internal/snapshot"
)

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
// SELECT introspection, and/or replication streaming.
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

// StreamSource is the replication reader surface a driver drives: the DBLog
// watermark surface plus start/stop of the stream.
type StreamSource interface {
	snapshot.SourceReader
	// Start begins streaming at the given position, blocking until the
	// stream ends or ctx is cancelled. Call in a goroutine.
	Start(ctx context.Context, at position.Position) error
	Close()
	// SetConfirmed installs a callback that returns the minimum position
	// committed to the sink across all tables. The Postgres reader uses
	// this to advance the slot's confirmed_flush_lsn only to the point
	// that has been durably written — never past it.
	SetConfirmed(f func() position.Position)
}
