// Package snapshot holds the source-agnostic DBLog snapshot orchestrator:
// chunking by primary key, low/high watermarks, and the caught-up proof
// that closes each window — never a timer. Sources implement the three
// interfaces (ChunkSource, SourceReader, Relay) on top of their own
// replication protocol; the proof logic lives here, once.
package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// Chunks materializes the half-open ranges from the bounds. Each chunk is
// [bounds[i], bounds[i+1]); the last is [bounds[n-1], nil) (open high).
func Chunks(bounds [][]any) []source.Chunk {
	if len(bounds) == 0 {
		return nil
	}
	out := make([]source.Chunk, 0, len(bounds))
	for i := 0; i < len(bounds)-1; i++ {
		out = append(out, source.Chunk{Low: bounds[i], High: bounds[i+1]})
	}
	out = append(out, source.Chunk{Low: bounds[len(bounds)-1], High: nil})
	return out
}

// Relay is the boundary the DBLog orchestrator pushes through: it feeds the
// chunk rows into the worker's window and releases the chunk's Closes marker
// (which flushes what the window still holds). The collapsed runner
// implements it over its ingest channel; unit tests use a fake.
type Relay interface {
	// Release sends the Closes marker for the chunk at the given position.
	// Must be called after caught-up; the marker's position is the safe
	// resume point. It observes ctx: if the relay or the worker has already
	// died, the blocking handshake must unblock with ctx.Err() rather than
	// wedge the snapshot forever (issue #551).
	Release(ctx context.Context, table string, chunkID uint32, at position.Position) error
	// AddWindowRows feeds the chunk SELECT result into the worker's window.
	// The batch is already Arrow (built straight from the chunk SELECT), so no
	// []rowchange.Change or map[string]any is materialized (#584).
	AddWindowRows(target string, chunkID uint32, batch *dataplane.Batch) error
	// GateOn starts buffering the table's live events while its chunk
	// SELECT is in flight; GateFlush releases them InWindow-tagged, only
	// after AddWindowRows has populated the window. This is the ordering the
	// window proof needs — a live event must never be deduplicated against
	// an empty window. The distributed coordinator implements the same gate
	// over its pump; the collapsed runner over its relay. GateFlush observes
	// ctx for the same reason Release does (#551).
	GateOn(table string, chunkID uint32)
	GateFlush(ctx context.Context) error
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
	// Schema is the table's canonical (projected) schema, the shape the
	// chunk SELECT is encoded into (#584). Required when the source has rows.
	Schema core.Schema
	// ChunkSize is the source's configured chunk size, passed to ScanArrow as
	// the expected row count so the encoder sizes its buffers once instead of
	// growing by doubling across a large chunk.
	ChunkSize int
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
	chunker source.ChunkSource,
	reader source.SourceReader,
	relay Relay,
	target string,
	cfg SnapshotConfig,
	cb SnapshotCallback,
) error {
	// Determine bounds: either from persisted state (resume) or fresh
	// calculation (cold start).
	var bounds [][]any
	var pending []uint32

	if Resumable(cfg.Progress) {
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

		rows, err := scanChunk(ctx, chunker, ch, target, low, cfg.Schema, cfg.ChunkSize)
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
		if err := relay.GateFlush(ctx); err != nil {
			reader.ClearWindow()
			return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
		}

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
		if err := relay.Release(ctx, target, chunkID, at); err != nil {
			return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
		}

		// Notify caller of completed chunk so progress is persisted.
		if cb != nil {
			remaining := RemoveFromPending(pending, chunkID)
			cb(target, chunkID, remaining)
		}
	}
	return nil
}

// arrowChunkScanner is a chunk source that reads a chunk straight into an
// Arrow builder, bypassing the map[string]any row path (#448/#454).
type arrowChunkScanner interface {
	ScanArrow(ctx context.Context, ch source.Chunk, enc *transport.RowEncoder, expected int) (int, error)
}

// scanChunk runs the chunk SELECT and returns it as one Arrow batch of
// snapshot inserts carrying the low watermark position. A source that reads
// straight into Arrow (ScanArrow) is preferred; any other source falls back to
// Scan, whose rows are appended cell by cell. Either way the chunk is held as
// Arrow, never as []rowchange.Change + map[string]any (#584).
func scanChunk(ctx context.Context, src source.ChunkSource, ch source.Chunk, target string, low position.Position, cs core.Schema, expected int) (*dataplane.Batch, error) {
	enc, err := transport.NewRowEncoder(cs, nil)
	if err != nil {
		return nil, err
	}
	defer enc.Release()
	pos := ""
	if low != nil {
		pos = low.String()
	}

	if s, ok := src.(arrowChunkScanner); ok {
		if _, err := s.ScanArrow(ctx, ch, enc, expected); err != nil {
			return nil, err
		}
		rec := enc.NewRecord()
		if pos != "" {
			// Every snapshot row carries the low watermark; ScanArrow does
			// not know it, so stamp the whole record once.
			rec, err = transport.WithPosition(rec, pos)
			if err != nil {
				return nil, err
			}
		}
		return &dataplane.Batch{Table: target, Record: rec, Mode: dataplane.AppendMode}, nil
	}

	n := 0
	err = src.Scan(ctx, ch, func(row map[string]any) error {
		for i, col := range cs.Columns {
			if err := enc.AppendValue(i, row[col.Name]); err != nil {
				return err
			}
		}
		enc.EndRow(transport.RowMeta{
			Op:       rowchange.OpInsert,
			Position: pos,
			Snapshot: true,
			Phase:    core.PhaseSnapshot,
			IngestTS: time.Now(),
		})
		n++
		if n == reserveAfterRows {
			enc.Reserve(expected)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &dataplane.Batch{Table: target, Record: enc.NewRecord(), Mode: dataplane.AppendMode}, nil
}

// reserveAfterRows is how many rows the Scan fallback reads before sizing the
// encoder's buffers for the whole chunk (mirrors ScanArrow).
const reserveAfterRows = 32

// WaitCaughtUp polls the reader's synced position until it contains high —
// the proof that everything the source had committed by the end of the chunk
// SELECT has been read. High is a fixed source watermark; the window never
// closes on a timer and never chases a moving master. Exported so the
// distributed coordinator's snapshot flow (worker-side SELECT) reuses the
// same proof.
func WaitCaughtUp(ctx context.Context, reader source.SourceReader, high position.Position, cfg SnapshotConfig) error {
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
