package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/position"
)

// Relay is the boundary the DBLog orchestrator pushes through: it feeds the
// chunk rows into the worker's window and releases the chunk's Closes marker
// (which flushes what the window still holds). The collapsed runner
// implements it over its ingest channel; unit tests use a fake.
type Relay interface {
	// Release sends the Closes marker for the chunk at the given position.
	// Must be called after caught-up; the marker's position is the safe
	// resume point.
	Release(table string, chunkID uint32, at *position.GTID)
	// AddWindowRows feeds the chunk SELECT result into the worker's window.
	AddWindowRows(target string, chunkID uint32, rows []change.Change) error
}

// ChunkSource is the chunk SELECT surface the orchestrator consumes. The
// SQL-backed Chunker implements it; unit tests drive SnapshotTable with a
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
// the watermarks, and the window mark that tags events InWindow at decode
// time — so no event can escape the window by racing a channel pull.
type SourceReader interface {
	Synced() *position.GTID
	Master() (*position.GTID, error)
	MarkWindow(chunkID uint32)
	ClearWindow()
}

// SnapshotConfig tunes the DBLog snapshot phase.
type SnapshotConfig struct {
	WindowTimeout time.Duration
	CaughtUpPoll  time.Duration
}

// SnapshotTable runs DBLog for a table with no committed position: chunks by
// PK, low watermark before each SELECT, high watermark after, then waits for
// the reader to provably catch up past high before releasing the window. The
// window never closes on a timer — only on the caught-up proof; exceeding
// WindowTimeout is a pathology surfaced as an error.
func SnapshotTable(
	ctx context.Context,
	chunker ChunkSource,
	reader SourceReader,
	relay Relay,
	target string,
	cfg SnapshotConfig,
) error {
	bounds, err := chunker.Bounds(ctx)
	if err != nil {
		return err
	}
	chunks := Chunks(bounds)

	for i, ch := range chunks {
		chunkID := uint32(i)
		if err := ctx.Err(); err != nil {
			return err
		}

		// Open the window first: events decoded from now on carry the
		// InWindow tag for this chunk, applied synchronously at decode.
		reader.MarkWindow(chunkID)
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

		high := reader.Synced()
		if err := waitCaughtUp(ctx, reader, high, cfg); err != nil {
			reader.ClearWindow()
			return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
		}
		at := reader.Synced()
		reader.ClearWindow()
		relay.Release(target, chunkID, at)
	}
	return nil
}

// scanChunk runs the chunk SELECT and wraps each row as an insert carrying
// the low watermark position. Keys come from the source PK columns.
func scanChunk(ctx context.Context, src ChunkSource, ch Chunk, target string, low *position.GTID) ([]change.Change, error) {
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
		})
		return nil
	})
	return rows, err
}

// waitCaughtUp polls the master position until the reader's synced set
// contains it — the proof that everything up to high (and the current master
// state) has been read.
func waitCaughtUp(ctx context.Context, reader SourceReader, high *position.GTID, cfg SnapshotConfig) error {
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
		synced := reader.Synced()
		if synced.Contains(high) {
			if master, err := reader.Master(); err == nil && synced.Contains(master) {
				return nil
			}
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
