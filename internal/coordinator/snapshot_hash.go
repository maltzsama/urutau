package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	route "github.com/maltzsama/urutau/internal/routing"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// snapshotFanoutTable runs the DBLog snapshot for a partitioned table with the
// READ and the OWNERSHIP decoupled (#574).
//
// The coordinator reads each chunk itself, by the source's index (the
// collation range scan, #588 intact), and fans each row out to the owner the
// SAME rendezvous hash assigns on the live stream. The chunk's collation
// range only decides what the SELECT returns; it never decides who owns the
// row. So a string key whose collation order and byte order disagree lands on
// the same owner in the snapshot and in the stream, and no key is ever written
// by two workers.
//
// A range cannot be the owner function here: bounds sampled under a non-binary
// collation are not contiguous in byte order, so clipping chunks to them drops
// rows. The owner function is the rendezvous hash (internal/routing), shared
// byte-for-byte with enqueueBatch.
//
// The rows travel over the existing WindowTag.Snapshot path: the owner stores
// them in its window (AddWindowRows), the gate releases the chunk's live
// events InWindow-tagged afterwards, and the Closes marker flushes the window.
//
// A window's rows are not redelivered (recordSent): a worker lost
// mid-snapshot comes back without the windows it was sent, and reports each
// as lost when its Closes marker is redelivered. The chunks from the first
// lost one on are read again once the pass over the table has drained
// (snapshot.FanoutPasses, #829).
func (c *Coordinator) snapshotFanoutTable(ctx context.Context, rdr source.SourceReader, chunker source.ChunkSource, ref source.TableRef, cfg snapshot.SnapshotConfig) error {
	rt := c.loadRouting()
	owners, ok := rt.OwnersOf(ref.Target)
	if !ok || len(owners) == 0 {
		return fmt.Errorf("coordinator: snapshot: no worker owns %s", ref.Target)
	}
	cs, ok := c.canonical[ref.Source]
	if !ok {
		return fmt.Errorf("coordinator: snapshot %s: no canonical schema for %s", ref.Target, ref.Source)
	}
	cfg.Schema = cs
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = c.cfg.ChunkSize
	}

	tref := core.TableRef{Source: ref.Source, Target: ref.Target}
	relay := &distributedRelay{c: c, ctx: ctx, ref: ref, owners: owners}
	// Resume: reuse recorded bounds and pending when the previous run was
	// interrupted mid-snapshot. Only progress THIS path recorded is safe: the
	// range path also stamped propSnapshotPartitions and packed its pending
	// ids (partition<<20 | index), which this path would misread as plain
	// chunk indices, skip every chunk and mark an empty snapshot complete.
	// resumableProgress rejects that stale shape so the snapshot runs afresh
	// (idempotent upserts).
	if c.snk != nil {
		if props, err := c.snk.Properties(ctx, tref); err == nil {
			cfg.Progress = resumableProgress(props)
		}
		// Persist is called once, at cold start, with the full chunk set. The
		// list is captured here so Release can name the chunks still to do
		// after each window; the worker commits that list atomically with the
		// window's rows (SnapshotPending on the Closes marker), so a crash
		// between a chunk's commit and the next never skips an uncommitted
		// chunk. The coordinator must NOT write a reduced pending list itself:
		// it would not be atomic with the rows (the range path's contract).
		cfg.Persist = func(sp snapshot.SnapshotProgress) error {
			if sp.Pending != nil {
				relay.plan(sp.Bounds, sp.Pending)
			}
			props, err := snapshot.EncodeSnapshotProgress(&sp)
			if err != nil {
				return err
			}
			return c.snk.SetProperties(ctx, tref, props)
		}
		if cfg.Progress != nil {
			relay.plan(cfg.Progress.Bounds, cfg.Progress.Pending)
		}
	}

	c.fanout.Begin(ref.Target)
	defer c.fanout.End(ref.Target)
	for {
		if err := snapshot.SnapshotTable(ctx, chunker, rdr, relay, ref.Target, cfg, nil); err != nil {
			return err
		}
		if relay.err != nil {
			return relay.err
		}
		// Seal the table's gate and release the trailing live events
		// (ordinary changes, no window tag).
		if err := c.closeWindow(ctx, ref.Target, 0); err != nil {
			return err
		}
		// The snapshot is done only once every window is committed: an
		// owner that lost one leaves its chunk, and those after it, to do.
		from, lost, err := c.fanout.Await(ctx, ref.Target)
		if err != nil {
			return err
		}
		if !lost {
			return nil
		}
		redo := snapshot.PendingFrom(relay.passChunks, from)
		if len(redo) == 0 || len(relay.bounds) == 0 {
			return fmt.Errorf("coordinator: snapshot %s: chunk %d was lost with its worker and is not in the pass to read again", ref.Target, from)
		}
		c.log.Warn("coordinator: snapshot chunks lost with a worker; reading them again",
			"table", ref.Target, "from_chunk", from, "chunks", len(redo))
		cfg.Progress = &snapshot.SnapshotProgress{State: snapshot.StateInProgress, Bounds: relay.bounds, Pending: redo}
		relay.plan(relay.bounds, redo)
	}
}

// resumableProgress returns the snapshot progress this fan-out path may
// resume, or nil when there is none or it was written by the range path.
//
// The range path persists pending chunk ids packed as partition<<20 | index
// (chunkRef) and stamps propSnapshotPartitions; the fan-out uses plain chunk
// indices, so resuming a pack-format list would make every id out of range,
// skip every chunk, and complete a snapshot that copied nothing. Presence of
// propSnapshotPartitions is the discriminator: it is only ever written by the
// range path, so its absence means the progress is the fan-out's own.
func resumableProgress(props map[string]string) *snapshot.SnapshotProgress {
	if props[propSnapshotPartitions] != "" {
		return nil
	}
	sp, err := snapshot.ReadSnapshotProgress(props)
	if err != nil || !snapshot.Resumable(sp) {
		return nil
	}
	return sp
}

// distributedRelay adapts snapshot.SnapshotTable to the distributed
// coordinator. GateOn/GateFlush drive the coordinator's existing gate (one
// window for the whole table, partition 0); AddWindowRows fans the chunk's
// rows out to their hash owners; Release closes the chunk's window on every
// owner. The Relay interface cannot return errors from every method, so the
// first failure is latched and surfaced by AddWindowRows (which can) and by
// the caller after SnapshotTable returns.
type distributedRelay struct {
	c        *Coordinator
	ctx      context.Context
	ref      source.TableRef
	owners   []*workerState
	windowID uint64
	// pending is the table's chunk set still to do, from the cold-start
	// Persist or the resumed progress. Release derives each window's
	// SnapshotPending from it, which the worker commits with the rows.
	pending []uint32
	// passChunks and bounds are the chunks of the pass under way, in the
	// order it takes them, and the bounds that number them: what a pass
	// that lost a chunk reads again from.
	passChunks []uint32
	bounds     [][]any
	err        error
}

// plan sets the chunks the next pass does.
func (r *distributedRelay) plan(bounds [][]any, pending []uint32) {
	r.bounds = bounds
	r.passChunks = append([]uint32(nil), pending...)
	r.pending = append([]uint32(nil), pending...)
}

func (r *distributedRelay) fail(err error) {
	if r.err == nil && err != nil {
		r.err = err
	}
}

// GateOn opens the table's gate (partition 0) and flushes accumulated live
// batches that predate the window, so a live event is never deduplicated
// against an empty window.
func (r *distributedRelay) GateOn(table string, chunkID uint32) {
	if r.err != nil {
		return
	}
	r.windowID = uint64(chunkID)
	if err := r.c.openWindowFlushed(r.ctx, table, 0); err != nil {
		r.fail(err)
	}
}

// GateFlush releases the events held since GateOn, InWindow-tagged for the
// window, after the window's rows were enqueued by AddWindowRows.
func (r *distributedRelay) GateFlush(ctx context.Context) error {
	if r.err != nil {
		return r.err
	}
	if err := r.c.flushWindow(ctx, r.ref.Target, 0, r.windowID); err != nil {
		r.fail(err)
		return err
	}
	// The chunk's rows are still in the owners' window, and the catch-up to
	// its high watermark comes next. The flush forgot the window was ready,
	// so a gate the catch-up filled blocked the pump, the reader stopped and
	// the catch-up never came (#820): it stays ready until Release.
	r.c.markWindowReady(r.ref.Target, 0, r.windowID)
	return nil
}

// endChunkDrain stops the early drain of the chunk's window before its
// Closes marker goes out: a batch drained after the marker would be tagged
// for a window the owners have closed. Taken under the gate's flush lock, so
// a drain under way has delivered before the marker is queued.
func (r *distributedRelay) endChunkDrain() {
	r.c.gate.WithFlush(func() {
		r.c.gate.ClearReady(r.ref.Target, 0)
	})
}

// AddWindowRows splits the chunk's rows by owner and queues each owner's
// share as a snapshot window batch. It takes ownership of batch.
func (r *distributedRelay) AddWindowRows(target string, chunkID uint32, batch *dataplane.Batch) error {
	if r.err != nil {
		batch.Release()
		return r.err
	}
	if err := r.fanOut(target, chunkID, batch); err != nil {
		r.fail(err)
		return err
	}
	return nil
}

func (r *distributedRelay) fanOut(target string, chunkID uint32, batch *dataplane.Batch) error {
	reader, err := transport.NewBatchReader(batch.Record, r.ref.PrimaryKey)
	if err != nil {
		batch.Release()
		return fmt.Errorf("coordinator: snapshot %s: %w", target, err)
	}
	names := ownerNames(r.owners)
	nrows := reader.NumRows()
	owner := make([]int, nrows)
	for i := 0; i < nrows; i++ {
		p, err := route.OwnerOfKey(reader.Key(i), names)
		if err != nil {
			batch.Release()
			return fmt.Errorf("coordinator: snapshot %s: row %d: %w", target, i, err)
		}
		if p < 0 {
			batch.Release()
			return fmt.Errorf("coordinator: snapshot %s: row %d's key %v has no owner", target, i, reader.Key(i))
		}
		owner[i] = p
	}
	subs, err := splitByOwner(r.ctx, batch.Record, owner, len(r.owners))
	if err != nil {
		batch.Release()
		return fmt.Errorf("coordinator: snapshot %s: split by owner: %w", target, err)
	}
	for p, sub := range subs {
		if sub == nil {
			// An owner with no row in the chunk still gets its window:
			// without one, its Closes marker finds a window it does not
			// hold, which is what a lost window looks like (#829).
			sub = batch.Record.NewSlice(0, 0)
		}
		meta := &pb.BatchMeta{
			Table:  target,
			Window: &pb.WindowTag{Snapshot: true, WindowId: uint64(chunkID)},
		}
		subBatch := &dataplane.Batch{Table: target, Record: sub, Mode: dataplane.AppendMode}
		if err := r.c.enqueueTo(r.ctx, r.owners[p], subBatch, meta); err != nil {
			releaseRecords(subs[p+1:])
			batch.Release()
			return err
		}
	}
	batch.Release()
	// The window now holds its rows: wake a pump blocked on a full gate.
	r.c.markWindowReady(target, 0, uint64(chunkID))
	return nil
}

// Release closes the chunk's window on every owner. The window's rows commit
// as one staged cycle spanning the owners (a table with no rows for an owner
// still delivers an empty cycle member), so the coordinator commits them in
// the table's send order.
func (r *distributedRelay) Release(ctx context.Context, table string, chunkID uint32, at position.Position) error {
	if r.err != nil {
		return r.err
	}
	// The chunks still to do AFTER this one: the worker commits this list
	// with the window's rows, so a crash cannot resume past a chunk whose
	// rows were never committed.
	remaining := snapshot.RemoveFromPending(r.pending, chunkID)
	r.pending = remaining
	r.endChunkDrain()
	if err := r.c.closeFanoutChunk(ctx, r.ref, r.owners, at, uint64(chunkID), remaining); err != nil {
		r.fail(err)
		return err
	}
	return nil
}

// closeFanoutChunk queues a Closes marker for one chunk on every owner. The
// marker carries the chunks still to do, which the worker commits atomically
// with the window's rows (#461) — the coordinator never persists that list
// itself, which would not be atomic with the rows.
func (c *Coordinator) closeFanoutChunk(ctx context.Context, ref source.TableRef, owners []*workerState, at position.Position, windowID uint64, pending []uint32) error {
	meta := &pb.BatchMeta{
		Table:  ref.Target,
		LowPos: at.String(),
		Window: &pb.WindowTag{Closes: true, WindowId: windowID, SnapshotPending: pending},
	}
	if c.stagesCycles() && c.isStagedTable(ref.Target) {
		meta.BatchId = c.batchSeq.Add(1)
		names := make([]string, len(owners))
		for i, w := range owners {
			names[i] = w.name
		}
		c.staged.ExpectWindow(core.TableRef{Target: ref.Target}, meta.BatchId, names)
		c.fanout.Note(ref.Target, meta.BatchId, uint32(windowID))
	}
	for _, w := range owners {
		if err := c.enqueueTo(ctx, w, nil, cloneBatchMeta(meta)); err != nil {
			return err
		}
	}
	c.noteWindow(ref.Target, owners[len(owners)-1])
	return nil
}
