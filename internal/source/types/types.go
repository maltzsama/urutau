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

// Capabilities declares what a source supports: snapshot via DBLog, chunk
// SELECT introspection, and/or replication streaming.
type Capabilities struct {
	Snapshot   bool
	ChunkQuery bool
	Stream     bool
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
	// that has been durably written — never past it. See CR-019.
	SetConfirmed(f func() position.Position)
}
