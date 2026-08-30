package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/position"
)

// Relay is the boundary the DBLog orchestrator pushes through: it buffers a
// table's live events during a chunk SELECT, feeds the chunk rows into the
// worker's window, and releases tagged events + the Closes marker into the
// ingest channel. The collapsed runner implements it with its routing loop;
// unit tests use a fake.
type Relay interface {
	// Buffer suspends the relay of one table: subsequent events for it are
	// accumulated (not dispatched) until Release.
	Buffer(table string)
	// Release sends every buffered event tagged InWindow, then the Closes
	// marker for the chunk at the given position. Must be called after
	// caught-up; the marker's position is the safe resume point.
	Release(table string, chunkID uint32, at *position.GTID)
	// Ingest returns the channel the orchestrator writes into.
	Ingest() chan<- change.Change
	// AddWindowRows feeds the chunk SELECT result into the worker's window.
	AddWindowRows(target string, chunkID uint32, rows []change.Change) error
}

// SourceReader is the minimal reader surface the orchestrator needs.
type SourceReader interface {
	Synced() *position.GTID
	Master() (*position.GTID, error)
}

// SnapshotConfig tunes the DBLog snapshot phase.
type SnapshotConfig struct {
	ChunkSize     int
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
	chunker *Chunker,
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

		relay.Buffer(target)
		low := reader.Synced()

		rows, err := scanChunk(ctx, chunker, ch, target, low)
		if err != nil {
			return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
		}
		if err := relay.AddWindowRows(target, chunkID, rows); err != nil {
			return err
		}

		high := reader.Synced()
		if err := waitCaughtUp(ctx, reader, high, cfg); err != nil {
			return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
		}
		at := reader.Synced()
		relay.Release(target, chunkID, at)
	}
	return nil
}

// scanChunk runs the chunk SELECT and wraps each row as an insert carrying
// the low watermark position. Keys come from the chunker PK columns.
func scanChunk(ctx context.Context, chunker *Chunker, ch Chunk, target string, low *position.GTID) ([]change.Change, error) {
	var rows []change.Change
	err := chunker.Scan(ctx, ch, func(row map[string]any) error {
		key := make([]any, 0, len(chunker.pk))
		for _, col := range chunker.pk {
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
