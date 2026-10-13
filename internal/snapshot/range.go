package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// A chunk's rows come from the worker's own SELECT and never pass through
// the flow budget, and the next chunk used to be requested as soon as the
// previous Closes marker was queued. Once chunk SELECTs were fast (#447) the
// events worker was handed a chunk every ~1.5 s against far slower commits:
// its heap held ~6 chunks in windows and ~6 more awaiting commit, and it was
// OOM-killed in every snapshot (#452).
//
// A chunk's Closes marker leaves the worker's in-flight window once the
// worker has committed it and everything queued before it, so the markers
// still held are the chunks not yet committed. A new chunk waits while
// MaxUncommittedChunks of them are: one commits while the next is read.

// MaxUncommittedChunks bounds a worker's snapshot chunks whose Closes marker
// is not yet committed.
const MaxUncommittedChunks = 2

// DefaultChunkTimeout is the snapshot watchdog's default: how long one
// chunk's round-trip may take.
const DefaultChunkTimeout = 10 * time.Minute

// chunkCommitPoll is the FALLBACK re-check cadence: the pacer wakes on the
// ack notification (issue #587), and this bounds how long a missed signal
// could stall it.
const chunkCommitPoll = 20 * time.Millisecond

// AwaitBelow returns once fewer than below of worker's markers are
// uncommitted (holds says which are still in its in-flight window),
// ErrWorkerLost when lost closes first (its windows died), or when ctx ends
// (the chunk watchdog). wake is the ack notification: an ack that drops a
// marker sends a coalesced signal, so the pacer wakes on the event instead of
// polling; the slow ticker is only a safety net.
func (m *Markers) AwaitBelow(ctx context.Context, worker string, below int, holds func(id uint64) bool, wake, lost <-chan struct{}) error {
	poll := time.NewTicker(chunkCommitPoll)
	defer poll.Stop()
	for {
		if len(m.Held(worker, holds)) < below {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-lost:
			return ErrWorkerLost
		case <-wake:
		case <-poll.C:
		}
	}
}

// RangePartition names one partition of a table's range snapshot, read by
// its single owner's own SELECTs.
type RangePartition struct {
	Source    string // the source table, as the worker knows it
	Partition int
	Worker    string
	// ChunkTimeout is the watchdog on one chunk's round-trip. It must outlast
	// WaitCaughtUp's own window timeout, or a slow-but-healthy catch-up would
	// look wedged.
	ChunkTimeout time.Duration
	// Todo is the table's chunks still to do, nil for every chunk.
	Todo map[uint32]bool
}

// RangeHost is the coordinator as one partition's range snapshot sees it:
// its worker, its gate, and the worker's replies.
type RangeHost interface {
	// AwaitBack returns once the worker is attached and not awaited after a
	// loss.
	AwaitBack(ctx context.Context) error
	// Lost is closed when the worker's current session is lost.
	Lost() <-chan struct{}
	// Epoch is the worker's current generation.
	Epoch() uint64
	// Redo is called when the worker was lost (and is back) since the last
	// round-trip: it returns the chunk to redo from, having set that chunk's
	// low bound to its resume cursor and reset the gate's ready mark.
	Redo(chunks []source.Chunk, windows map[uint64]int, next int) int
	// OpenGate opens the partition's window gate, after sending what the
	// table had accumulated before it.
	OpenGate(ctx context.Context) error
	// CloseGate seals the gate and releases what it collected after the last
	// chunk.
	CloseGate(ctx context.Context) error
	// AwaitCommits returns once fewer than below of the worker's chunks are
	// uncommitted.
	AwaitCommits(ctx context.Context, below int, lost <-chan struct{}) error
	// Out is the worker's control stream.
	Out() chan<- *pb.CoordinatorMessage
	// WindowOpens and ChunkReadies carry the worker's replies.
	WindowOpens() <-chan *pb.WindowOpen
	ChunkReadies() <-chan *pb.ChunkReady
	// CloseWindow proves the reader caught up to one announced window, then
	// releases its gated live rows and sends its Closes marker.
	CloseWindow(ctx context.Context, wo *pb.WindowOpen, lost <-chan struct{}, pending []uint32, chunkID uint32, highKey []any) error
	Log() *slog.Logger
}

// RunRangePartition runs the DBLog snapshot of one partition over its
// chunks: only they are sent to its worker, and the gate lifecycle is scoped
// to this partition alone.
//
// A worker lost mid-snapshot takes its windows with it (issue #461): the rows
// of every chunk whose Closes marker it had not committed lived only in its
// memory. Once it is back, the partition redoes from the first such chunk.
// Window ids are the worker's own (attempt, seq), announced in WindowOpen, so
// a lost window's Closes marker, redelivered to the reconnected worker,
// closes nothing it has open.
func RunRangePartition(ctx context.Context, h RangeHost, p RangePartition, chunks []source.Chunk) error {
	if len(chunks) == 0 {
		return nil // this partition's range contains no rows right now
	}
	opened := false
	windows := map[uint64]int{} // window seq → chunk index, this partition
	var epoch uint64
	haveEpoch := false
	for i := 0; i < len(chunks); {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.Todo != nil && !p.Todo[ChunkRef(p.Partition, i)] {
			i++ // committed before a coordinator restart: resumed past
			continue
		}
		if err := h.AwaitBack(ctx); err != nil {
			return err
		}
		lost := h.Lost()
		cur := h.Epoch()
		if haveEpoch && cur != epoch {
			// Lost (and back) since the previous round-trip.
			i = h.Redo(chunks, windows, i)
			h.Log().Warn("coordinator: snapshot worker was lost; redoing its uncommitted chunks",
				"table", p.Source, "partition", p.Partition, "worker", p.Worker, "from_chunk", i)
		}
		// The epoch the ChunkRequests are sent under; the worker echoes it
		// on ChunkReady and WindowOpen so a reply from a superseded
		// generation is ignored.
		epoch, haveEpoch = cur, true
		if !opened {
			if err := h.OpenGate(ctx); err != nil {
				return err
			}
			opened = true
		}
		// The snapshot watchdog: a worker that attaches but stops draining
		// would otherwise block the chunk round-trip (send, wait, flush)
		// forever — the supervisor does not run during the snapshot (issue
		// #206).
		timeout := p.ChunkTimeout
		if timeout <= 0 {
			timeout = DefaultChunkTimeout
		}
		chunkCtx, cancel := context.WithTimeout(ctx, timeout)
		err := h.AwaitCommits(chunkCtx, MaxUncommittedChunks, lost)
		if err == nil {
			err = RangeChunk(chunkCtx, h, p, chunks[i], ChunkRef(p.Partition, i), i, epoch, lost, PendingAfter(p.Todo, p.Partition, i), windows)
		}
		if err == nil && i == len(chunks)-1 {
			// The partition's last chunk: its windows must all be committed
			// before the partition is done, or a loss right after would
			// leave rows only a lost window held.
			err = h.AwaitCommits(chunkCtx, 1, lost)
		}
		cancel()
		if errors.Is(err, ErrWorkerLost) {
			continue // the next pass finds the new epoch and redoes
		}
		if err != nil {
			// A parent deadline/cancel (the run's own) surfaces here as
			// DeadlineExceeded too; report it as-is rather than blaming a
			// wedged worker. Only a chunk deadline with a live parent is the
			// watchdog firing.
			if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("coordinator: snapshot %s: chunk %d did not complete within %s (the worker may be wedged): %w",
					p.Source, i, timeout, err)
			}
			return err
		}
		i++
	}
	// Seal this partition's gate and release anything collected after its
	// last chunk. Other partitions' gates (if any) are untouched.
	return h.CloseGate(ctx)
}

// RangeChunk runs one chunk's round-trip: send the ChunkRequest, wait for
// the worker's WindowOpen announcements and ChunkReady, closing each window
// as it opens. ctx carries the watchdog deadline, so a worker that never acks
// the chunk fails the run instead of wedging it. windows records each
// window's chunk index, for a redo.
func RangeChunk(ctx context.Context, h RangeHost, p RangePartition, ch source.Chunk, chunkID uint32, index int, epoch uint64, lost <-chan struct{}, pending []uint32, windows map[uint64]int) error {
	boundsB, err := transport.EncodeBounds(ch.Low, ch.High)
	if err != nil {
		return fmt.Errorf("coordinator: chunk %d bounds: %w", chunkID, err)
	}
	req := &pb.ChunkRequest{Table: p.Source, ChunkId: chunkID, Bounds: boundsB}

	select {
	case h.Out() <- &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Chunk{Chunk: req}}:
	case <-lost:
		return ErrWorkerLost
	case <-ctx.Done():
		return ctx.Err()
	}

	// handleWindowOpen closes one announced window: prove caught up to its
	// position, release its gated live rows, send its Closes.
	handleWindowOpen := func(wo *pb.WindowOpen) error {
		if wo.Table != p.Source || wo.ChunkId != chunkID || wo.Attempt != epoch {
			h.Log().Warn("coordinator: ignoring stale/unexpected WindowOpen",
				"table", wo.Table, "chunk", wo.ChunkId, "attempt", wo.Attempt, "want_epoch", epoch)
			return nil
		}
		windows[wo.Seq] = index
		highKey, err := DecodeHighKey(wo.HighKey)
		if err != nil {
			return fmt.Errorf("coordinator: window %d high key: %w", wo.Seq, err)
		}
		return h.CloseWindow(ctx, wo, lost, pending, chunkID, highKey)
	}

	// The worker streams WindowOpen as it reads the chunk (byte-cap), then
	// ChunkReady. Close each window as it opens, in order.
	for {
		select {
		case wo := <-h.WindowOpens():
			if err := handleWindowOpen(wo); err != nil {
				return err
			}
		case cr := <-h.ChunkReadies():
			if cr.Table == p.Source && cr.ChunkId == chunkID && cr.Epoch == epoch {
				h.Log().Info("chunk ready", "table", p.Source, "partition", p.Partition, "chunk", chunkID, "rows", cr.Rows)
				// WindowOpen and ChunkReady arrive on SEPARATE channels, so
				// the select above can receive ChunkReady while earlier
				// WindowOpen messages are still buffered. The worker sends
				// every WindowOpen before ChunkReady, so drain and close the
				// buffered ones now — otherwise their gated live rows are
				// never released and their windows never commit.
				for {
					select {
					case wo := <-h.WindowOpens():
						if err := handleWindowOpen(wo); err != nil {
							return err
						}
					default:
						return nil
					}
				}
			}
			h.Log().Warn("coordinator: ignoring stale/unexpected ChunkReady",
				"table", cr.Table, "chunk", cr.ChunkId, "epoch", cr.Epoch, "want_epoch", epoch)
		case <-lost:
			return ErrWorkerLost
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
