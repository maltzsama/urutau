// Package dblog holds the source-agnostic DBLog snapshot orchestrator:
// chunking by primary key, low/high watermarks, and the caught-up proof
// that closes each window — never a timer. Sources implement the three
// interfaces (ChunkSource, SourceReader, Relay) on top of their own
// replication protocol; the proof logic lives here, once.
package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/position"
)

// TableRef maps one source table to its target and primary key. It is an
// alias of the canonical core.TableRef — the pipeline-wide table identity.
type TableRef = core.TableRef

// Chunk is a half-open primary-key range [Low, High). The last chunk of a
// table is emitted with the marker in the orchestrator; Low/High are tuples
// in PK column order.
type Chunk struct {
	Low  []any
	High []any
}

// Chunks materializes the half-open ranges from the bounds. Each chunk is
// [bounds[i], bounds[i+1]); the last is [bounds[n-1], nil) (open high).
func Chunks(bounds [][]any) []Chunk {
	if len(bounds) == 0 {
		return nil
	}
	out := make([]Chunk, 0, len(bounds))
	for i := 0; i < len(bounds)-1; i++ {
		out = append(out, Chunk{Low: bounds[i], High: bounds[i+1]})
	}
	out = append(out, Chunk{Low: bounds[len(bounds)-1], High: nil})
	return out
}

// Relay is the boundary the DBLog orchestrator pushes through: it feeds the
// chunk rows into the worker's window and releases the chunk's Closes marker
// (which flushes what the window still holds). The collapsed runner
// implements it over its ingest channel; unit tests use a fake.
type Relay interface {
	// Release sends the Closes marker for the chunk at the given position.
	// Must be called after caught-up; the marker's position is the safe
	// resume point.
	Release(table string, chunkID uint32, at position.Position)
	// AddWindowRows feeds the chunk SELECT result into the worker's window.
	AddWindowRows(target string, chunkID uint32, rows []change.Change) error
	// GateOn starts buffering the table's live events while its chunk
	// SELECT is in flight; GateFlush releases them InWindow-tagged, only
	// after AddWindowRows has populated the window. This is the ordering the
	// window proof needs — a live event must never be deduplicated against
	// an empty window. The distributed coordinator implements the same gate
	// over its pump; the collapsed runner over its relay.
	GateOn(table string, chunkID uint32)
	GateFlush()
}

// ChunkSource is the chunk SELECT surface the orchestrator consumes. The
// SQL-backed chunkers implement it; unit tests drive SnapshotTable with a
// fake.
type ChunkSource interface {
	// PK returns the primary key columns, in key order.
	PK() []string
	// Bounds returns the ordered chunk boundary keys.
	Bounds(ctx context.Context) ([][]any, error)
	// Scan reads one chunk, calling fn per row keyed by column name.
	Scan(ctx context.Context, ch Chunk, fn func(row map[string]any) error) error
}

// SourceReader is the reader surface the orchestrator needs: positions for
// the watermarks, and the window that tags events InWindow at decode time —
// so no event can escape the window by racing a channel pull.
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

// SnapshotConfig tunes the DBLog snapshot phase.
type SnapshotConfig struct {
	WindowTimeout time.Duration
	CaughtUpPoll  time.Duration
	// Existing progress from a previous run. When non-nil with persisted
	// bounds, bounds are read from Progress.Bounds (not recalculated) and
	// only chunks in Progress.Pending are processed.
	Progress *SnapshotProgress
	// Persist durably stores the snapshot state at cold start: bounds are
	// calculated ONCE and written before the first chunk, so a restart
	// resumes from the persisted bounds instead of recalculating them over
	// a table that has since received rows — recalculated bounds would not
	// correspond to the saved progress. The per-chunk pending updates
	// travel with the data commits; this hook covers only the initial
	// state.
	Persist func(SnapshotProgress) error
}

// SnapshotCallback is called when a chunk completes. The caller persists
// the updated pending list atomically with the commit that advances
// position — same crash-safety invariant as cdc.position itself.
type SnapshotCallback func(table string, completedChunkID uint32, remaining []uint32)

// SnapshotTable runs DBLog for a table with no committed position: chunks by
// PK, low watermark before each SELECT, high watermark after, then waits for
// the reader to provably catch up past high before releasing the window. The
// window never closes on a timer — only on the caught-up proof; exceeding
// WindowTimeout is a pathology surfaced as an error.
//
// When cfg.Progress is provided (resume path), bounds are read from the
// persisted state and only pending chunks are processed. The callback is
// called after each chunk completes so the caller can persist progress.
func SnapshotTable(
	ctx context.Context,
	chunker ChunkSource,
	reader SourceReader,
	relay Relay,
	target string,
	cfg SnapshotConfig,
	cb SnapshotCallback,
) error {
	// Determine bounds: either from persisted state (resume) or fresh
	// calculation (cold start).
	var bounds [][]any
	var pending []uint32

	if cfg.Progress != nil && cfg.Progress.State == StateInProgress && len(cfg.Progress.Bounds) > 0 {
		// Resume: use persisted bounds and pending list.
		bounds = cfg.Progress.Bounds
		pending = cfg.Progress.Pending
	} else {
		// Cold start: calculate bounds once and persist them before any
		// chunk runs — the persisted bounds are what a restart resumes
		// from.
		var err error
		bounds, err = chunker.Bounds(ctx)
		if err != nil {
			return err
		}
		// All chunks are pending initially.
		pending = make([]uint32, len(bounds))
		for i := range pending {
			pending[i] = uint32(i)
		}
		if cfg.Persist != nil {
			err := cfg.Persist(SnapshotProgress{
				State:   StateInProgress,
				Bounds:  bounds,
				Pending: pending,
				Started: time.Now().UTC().Format(time.RFC3339),
			})
			if err != nil {
				return fmt.Errorf("dblog: persist snapshot bounds: %w", err)
			}
		}
	}

	chunks := Chunks(bounds)

	for _, chunkID := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if int(chunkID) >= len(chunks) {
			continue // safety: bounds may have shrunk if table was compacted
		}
		ch := chunks[chunkID]

		// Gate the table's live events ahead of the SELECT so none can be
		// deduplicated against an empty window, and open the reader window:
		// events decoded from now on are tagged InWindow (past the reader's
		// source watermark), applied synchronously at decode.
		relay.GateOn(target, chunkID)
		reader.OpenWindow(ctx, chunkID)
		low := reader.Synced()

		rows, err := scanChunk(ctx, chunker, ch, target, low)
		if err != nil {
			reader.ClearWindow()
			return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
		}
		if err := relay.AddWindowRows(target, chunkID, rows); err != nil {
			reader.ClearWindow()
			return err
		}
		// The window now holds the chunk rows: release the gated live events
		// InWindow-tagged so they deduplicate against them.
		relay.GateFlush()

		// The caught-up proof: the reader must have provably consumed
		// everything the source had committed by the end of the SELECT. High
		// is a FIXED source position — never a live master — so a busy
		// source cannot keep the window open forever.
		high, err := reader.Master(ctx)
		if err != nil {
			reader.ClearWindow()
			return fmt.Errorf("dblog: chunk %d: master: %w", chunkID, err)
		}
		if err := WaitCaughtUp(ctx, reader, high, cfg); err != nil {
			reader.ClearWindow()
			return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
		}
		at := reader.Synced()
		reader.ClearWindow()
		relay.Release(target, chunkID, at)

		// Notify caller of completed chunk so progress is persisted.
		if cb != nil {
			remaining := RemoveFromPending(pending, chunkID)
			cb(target, chunkID, remaining)
		}
	}
	return nil
}

// scanChunk runs the chunk SELECT and wraps each row as an insert carrying
// the low watermark position. Keys come from the source PK columns.
func scanChunk(ctx context.Context, src ChunkSource, ch Chunk, target string, low position.Position) ([]change.Change, error) {
	pk := src.PK()
	var rows []change.Change
	err := src.Scan(ctx, ch, func(row map[string]any) error {
		key := make([]any, 0, len(pk))
		for _, col := range pk {
			key = append(key, row[col])
		}
		rows = append(rows, change.Change{
			Op:       change.OpInsert,
			Table:    target,
			Key:      key,
			After:    row,
			Position: low.String(),
			Snapshot: true,
			IngestTS: time.Now(),
		})
		return nil
	})
	return rows, err
}

// WaitCaughtUp polls the reader's synced position until it contains high —
// the proof that everything the source had committed by the end of the chunk
// SELECT has been read. High is a fixed source watermark; the window never
// closes on a timer and never chases a moving master. Exported so the
// distributed coordinator's snapshot flow (worker-side SELECT) reuses the
// same proof.
func WaitCaughtUp(ctx context.Context, reader SourceReader, high position.Position, cfg SnapshotConfig) error {
	poll := cfg.CaughtUpPoll
	if poll <= 0 {
		poll = time.Second
	}
	timeout := cfg.WindowTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}

	deadline := time.Now().Add(timeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		synced := reader.Synced()
		if synced != nil && synced.Contains(high) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("window stuck open past %s: readPos %s does not contain high %s",
				timeout, synced, high)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}
