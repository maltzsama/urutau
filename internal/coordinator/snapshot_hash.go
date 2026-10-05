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
// Because the coordinator pushes through each owner's queue, a worker lost
// mid-snapshot needs no special recovery: its queued window rows and Closes
// are redelivered when it reconnects.
func (c *Coordinator) snapshotFanoutTable(ctx context.Context, rdr source.SourceReader, chunker source.ChunkSource, ref source.TableRef, cfg snapshot.SnapshotConfig) error {
	rt := c.loadRouting()
	owners, ok := rt.ownersOf(ref.Target)
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
	// Resume: reuse recorded bounds and pending when the previous run was
	// interrupted mid-snapshot. The partition-layout guard of the range path
	// is irrelevant here — chunks are not clipped per worker.
	if c.snk != nil {
		if props, err := c.snk.Properties(ctx, tref); err == nil {
			if sp, perr := snapshot.ReadSnapshotProgress(props); perr == nil &&
				sp.State == snapshot.StateInProgress && len(sp.Bounds) > 0 {
				cfg.Progress = sp
			}
		}
		cfg.Persist = func(sp snapshot.SnapshotProgress) error {
			return c.snk.SetProperties(ctx, tref, snapshot.EncodeSnapshotProgress(&sp))
		}
	}
	cb := func(_ string, _ uint32, remaining []uint32) {
		if c.snk == nil {
			return
		}
		props := snapshot.EncodeSnapshotProgress(&snapshot.SnapshotProgress{
			State:   snapshot.StateInProgress,
			Pending: remaining,
		})
		if err := c.snk.SetProperties(ctx, tref, props); err != nil {
			c.log.Warn("coordinator: snapshot progress", "table", ref.Target, "err", err)
		}
	}

	relay := &distributedRelay{c: c, ctx: ctx, ref: ref, owners: owners}
	if err := snapshot.SnapshotTable(ctx, chunker, rdr, relay, ref.Target, cfg, cb); err != nil {
		return err
	}
	if relay.err != nil {
		return relay.err
	}
	// Seal the table's gate and release the trailing live events (ordinary
	// changes, no window tag).
	return c.closeWindow(ctx, ref.Target, 0)
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
	err      error
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
func (r *distributedRelay) GateFlush() {
	if r.err != nil {
		return
	}
	if err := r.c.flushWindow(r.ctx, r.ref.Target, 0, r.windowID); err != nil {
		r.fail(err)
	}
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
			continue
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
func (r *distributedRelay) Release(table string, chunkID uint32, at position.Position) {
	if r.err != nil {
		return
	}
	if err := r.c.closeFanoutChunk(r.ctx, r.ref, r.owners, at, uint64(chunkID)); err != nil {
		r.fail(err)
	}
}

// closeFanoutChunk queues a Closes marker for one chunk on every owner.
func (c *Coordinator) closeFanoutChunk(ctx context.Context, ref source.TableRef, owners []*workerState, at position.Position, windowID uint64) error {
	meta := &pb.BatchMeta{
		Table:  ref.Target,
		LowPos: at.String(),
		Window: &pb.WindowTag{Closes: true, WindowId: windowID},
	}
	if c.stagesCycles() && c.isStagedTable(ref.Target) {
		meta.BatchId = c.batchSeq.Add(1)
		names := make([]string, len(owners))
		for i, w := range owners {
			names[i] = w.name
		}
		c.staged.expectWindow(core.TableRef{Target: ref.Target}, meta.BatchId, names)
	}
	for _, w := range owners {
		if err := c.enqueueTo(ctx, w, nil, cloneBatchMeta(meta)); err != nil {
			return err
		}
	}
	c.noteWindow(ref.Target, owners[len(owners)-1])
	return nil
}
