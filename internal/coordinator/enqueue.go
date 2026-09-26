package coordinator

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
)

// enqueueBatch queues ONE source batch on a worker's Flight stream and
// charges its share of the global flow budget. A full budget blocks here —
// the backpressure that stalls the pump and, through it, the reader. The
// charge is released when the worker's Ack covers the batch's position
// (onAck).
//
// b must be non-nil: it is a source batch, serialized ONCE as-is (no per-row
// re-encode). meta may be nil (plain live) or carry a window tag; the table
// and commit position are derived from the batch when the meta lacks them.
// OWNERSHIP: enqueueBatch always releases b on every exit.
//
// Window-lifecycle markers (the empty Closes record) are NOT this function's
// job — they go to one explicit partition owner via enqueueTo.
func (c *Coordinator) enqueueBatch(ctx context.Context, b *dataplane.Batch, meta *pb.BatchMeta) error {
	if b != nil {
		defer b.Release()
	}
	if b == nil && (meta == nil || meta.Table == "") {
		return fmt.Errorf("coordinator: marker batch requires a table in meta")
	}
	if meta == nil {
		meta = &pb.BatchMeta{}
	}
	// ONE snapshot load for this whole batch: a concurrent re-slice may
	// publish a new layout mid-batch, and routing half the rows by each
	// would split a key range across two owners (issue #312).
	rt := c.loadRouting()
	owners, ok := rt.ownersOf(meta.Table)
	if !ok {
		if b != nil {
			meta.Table = b.Table
		}
		owners, ok = rt.ownersOf(meta.Table)
		if !ok {
			return fmt.Errorf("coordinator: no worker owns table %s", meta.Table)
		}
	}

	if b == nil {
		// Window-lifecycle markers are enqueued to one explicit partition
		// owner via enqueueTo (snapshotTable does this per partition);
		// enqueueBatch carries source batches only (issue #214).
		return fmt.Errorf("coordinator: enqueueBatch requires a batch; window markers go through enqueueTo")
	}

	// A partition owner whose Pod died (its session attached, then detached)
	// and has not been retired by a re-slice yet can never drain a batch
	// routed to it: the batch strands, and other partitions' cycles advance
	// the durable position over the gap. Fail for a clean replay instead of
	// routing into the void (issue #372). A supervisor reset is excluded: it
	// is detached only between the reset and the reconnect, and its queue
	// survives, so the batch is delivered on reconnect rather than stranded.
	for _, w := range owners {
		if c.ownerDetached(w) && !c.supervisor.isPending(w.name) {
			return fmt.Errorf("coordinator: owner %s of %s is detached; terminating for a clean replay", w.name, meta.Table)
		}
	}

	if len(owners) == 1 {
		return c.enqueueTo(ctx, owners[0], b, meta)
	}

	// Partitioned table: one cycle id for the whole binlog batch. Every
	// sub-batch sent below shares it, so the staged deliveries group back
	// into the one cycle that must commit atomically (WK-001 C5). The
	// cycle expects the partitions that actually have rows and are not
	// covered at boot — a nil sub-batch is never sent, and a covered one
	// is skipped by its worker, so neither is expected (see cycleOwners).
	//
	// The id is assigned INTO meta: a caller must pass a fresh meta per
	// batch, or two batches share one cycle and the second one's
	// deliveries are dropped as not committable.
	if meta.BatchId == 0 {
		meta.BatchId = c.batchSeq.Add(1)
	}
	// Partitioned table: split b's rows by primary-key range, one
	// sub-batch per owning partition — the SAME ranges the DBLog
	// snapshot uses (c.partitionRanges), so a key is always routed to
	// the one worker that also owns it during bootstrap.
	pk := c.primaryKeyFor(meta.Table)
	if len(pk) == 0 {
		return fmt.Errorf("coordinator: table %s has %d partition owners but no primary key to route by", meta.Table, len(owners))
	}
	ranges := rt.rangesOf(meta.Table)
	if len(ranges) != len(owners) {
		return fmt.Errorf("coordinator: table %s: %d partition ranges for %d owners", meta.Table, len(ranges), len(owners))
	}
	reader, err := transport.NewBatchReader(b.Record, pk)
	if err != nil {
		return fmt.Errorf("coordinator: table %s: partition routing: %w", meta.Table, err)
	}
	nrows := reader.NumRows()
	owner := make([]int, nrows)
	for i := 0; i < nrows; i++ {
		p, err := partitionOwner(ranges, reader.Key(i))
		if err != nil {
			return fmt.Errorf("coordinator: table %s: row %d: %w", meta.Table, i, err)
		}
		if p < 0 {
			return fmt.Errorf("coordinator: table %s: row %d's key %v matches no partition range", meta.Table, i, reader.Key(i))
		}
		owner[i] = p
	}
	subBatches, err := splitByOwner(ctx, b.Record, owner, len(owners))
	if err != nil {
		return fmt.Errorf("coordinator: table %s: split by partition: %w", meta.Table, err)
	}
	total := 0
	for _, sub := range subBatches {
		if sub != nil {
			total++
		}
	}
	// Each sub-batch's high position (its last row's __pos) is computed
	// here, once, and sent as its HighPos: the worker decides "covered" on
	// exactly this value, and the cycle must not expect a delivery the
	// worker will skip.
	highs := make([]string, len(subBatches))
	cycleOwners := make([]string, 0, total)
	for p, sub := range subBatches {
		if sub == nil {
			continue
		}
		reader, rerr := transport.NewBatchReader(sub, nil)
		if rerr != nil {
			releaseRecords(subBatches)
			return fmt.Errorf("coordinator: table %s: partition %d: %w", meta.Table, p, rerr)
		}
		if reader.NumRows() > 0 {
			highs[p] = reader.Position(reader.NumRows() - 1)
		}
		if c.coveredAtBoot(meta.Table, highs[p]) {
			continue
		}
		cycleOwners = append(cycleOwners, owners[p].name)
	}
	// Only a staging sink commits per cycle; for any other concurrent sink
	// (ClickHouse, Couchbase) the workers commit their own sub-batches, so
	// no cycle is tracked and none can leak.
	if c.stagesCycles() && len(cycleOwners) > 0 {
		c.staged.expect(core.TableRef{Target: meta.Table}, meta.BatchId, cycleOwners)
		c.log.Debug("coordinator: cycle expected", "table", meta.Table, "seq", meta.BatchId, "nrows", nrows, "owners", cycleOwners)
	}
	for p, sub := range subBatches {
		if sub == nil {
			continue // no rows for this partition in this batch
		}
		subMeta := cloneBatchMeta(meta)
		subMeta.HighPos = highs[p]
		subBatch := &dataplane.Batch{Table: b.Table, Record: sub, Watermark: b.Watermark, Mode: b.Mode}
		if err := c.enqueueTo(ctx, owners[p], subBatch, subMeta); err != nil {
			// enqueueTo already released subBatch (and its Record — the same
			// pointer as sub), so do NOT release sub again; release only the
			// sub-batches not yet sent (issue #217).
			releaseRecords(subBatches[p+1:])
			return err
		}
	}
	return nil
}

// coveredAtBoot mirrors the worker's covered check (batchReceiver.covered):
// a sub-batch whose high position is at or before the table's committed
// position at boot is skipped by its worker, acked but never staged. An
// Incomparable pair is never covered, matching the worker.
func (c *Coordinator) coveredAtBoot(table, high string) bool {
	cp, ok := c.bootCommitted[table]
	if !ok || high == "" {
		return false
	}
	p, err := c.src.ParsePosition(high)
	if err != nil {
		return false
	}
	cmp := p.Compare(cp)
	return cmp != position.Incomparable && cmp <= 0
}

// enqueueTo serializes and queues ONE batch (or a nil-record marker) on
// ONE worker's Flight stream, charging its share of the global flow
// budget. A full budget blocks here — the backpressure that stalls the
// pump and, through it, the reader. The charge is released when the
// worker's Ack covers the batch's position (onAck).
//
// OWNERSHIP: releases b (if non-nil) on every exit — the caller must not
// use b again after this returns, matching enqueueBatch's existing
// single-owner contract.
func (c *Coordinator) enqueueTo(ctx context.Context, w *workerState, b *dataplane.Batch, meta *pb.BatchMeta) error {
	if b != nil {
		defer b.Release()
	}
	// A partitioned table's sub-batches all share one cycle id, assigned
	// once by enqueueBatch (WK-001 C5): the staged cycle key. Every other
	// caller gets its own id here.
	if meta.BatchId == 0 {
		meta.BatchId = c.batchSeq.Add(1)
	}
	// The coordinator owns the table's commit mode and states it per batch:
	// a staging sink with >1 owner stages (the coordinator commits the
	// cycle), everything else commits directly. Setting it here, at send
	// time, keeps every owner on the same mode even when the table changes
	// mode under it (1 owner → N on a re-slice, issue #312).
	meta.Staged = c.isStagedTable(meta.Table)

	var body []byte
	var metaBytes []byte
	var err error
	if b == nil {
		// Resolve the canonical schema for typed wire encoding of the
		// zero-row marker record. A marker whose table is not in refs would
		// otherwise encode with a zero-value schema (0 data columns) and be
		// rejected downstream with an error pointing at the wrong place.
		var cs core.Schema
		found := false
		for _, ref := range c.refs {
			if ref.Target == meta.Table {
				cs = c.canonical[ref.Source]
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("coordinator: marker batch: table %q has no canonical schema (not in refs)", meta.Table)
		}
		body, metaBytes, err = transport.EncodeBatch(nil, cs, meta, nil)
		if err != nil {
			return err
		}
	} else {
		// The batch already carries the wire schema (data + metadata
		// columns); serialize it whole. The commit position is the last
		// row's __pos — ack truncation frees the batch once the worker
		// commits at or past it.
		if meta.HighPos == "" {
			reader, rerr := transport.NewBatchReader(b.Record, nil)
			if rerr != nil {
				return rerr
			}
			if reader.NumRows() > 0 {
				meta.HighPos = reader.Position(reader.NumRows() - 1)
			}
		}
		body, err = transport.EncodeRecord(b.Record)
		if err != nil {
			return err
		}
		metaBytes, err = proto.Marshal(meta)
		if err != nil {
			return fmt.Errorf("coordinator: marshal batch meta: %w", err)
		}
	}
	n := int64(len(body) + len(metaBytes))
	if err := c.budget.acquire(ctx, w.name, n); err != nil {
		return err
	}
	c.log.Debug("coordinator: enqueue sub-batch", "owner", w.name, "table", meta.Table, "seq", meta.BatchId, "highPos", meta.HighPos, "staged", meta.Staged)
	// Marker batches (window closes) carry their position in LowPos.
	posStr := meta.HighPos
	if posStr == "" {
		posStr = meta.LowPos
	}
	var high position.Position
	if posStr != "" {
		high, err = c.src.ParsePosition(posStr)
		if err != nil {
			c.budget.release(w.name, n)
			return fmt.Errorf("coordinator: batch %s position %q: %w", meta.Table, posStr, err)
		}
	}
	select {
	case w.queue <- queuedBatch{id: meta.BatchId, body: body, meta: metaBytes}:
		if idx := c.indexOf(w.name); idx != nil {
			idx.add(inflightBatch{id: meta.BatchId, table: meta.Table, high: high, bytes: n, oversized: c.budget.isOversized(n)})
		}
		c.noteSent(meta.Table, posStr)
		return nil
	case <-ctx.Done():
		c.budget.release(w.name, n)
		return ctx.Err()
	}
}

// primaryKeyFor returns the primary key columns for the table targeted
// by target (a TARGET table name, matching c.refs' shape).
func (c *Coordinator) primaryKeyFor(target string) []string {
	for _, ref := range c.refs {
		if ref.Target == target {
			return ref.PrimaryKey
		}
	}
	return nil
}

// cloneBatchMeta returns a shallow copy of meta — enqueueTo mutates
// BatchId/HighPos on the instance it's given, and a marker sent to every
// partition owner (or a sub-batch's per-partition meta) must not share
// one struct across concurrent-ish sends.
func cloneBatchMeta(meta *pb.BatchMeta) *pb.BatchMeta {
	return proto.Clone(meta).(*pb.BatchMeta)
}

// splitByOwner filters rec into len(nOwners) sub-records, one per
// partition index in owner (owner[i] is the partition row i belongs to).
// A partition with zero matching rows gets a nil entry (skipped by the
// caller) rather than an empty-but-non-nil record — enqueueTo's
// zero-row marker path is for markers only, not empty data batches.
func splitByOwner(ctx context.Context, rec arrow.RecordBatch, owner []int, nOwners int) ([]arrow.RecordBatch, error) {
	out := make([]arrow.RecordBatch, nOwners)
	for p := 0; p < nOwners; p++ {
		idxBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		for i, o := range owner {
			if o == p {
				idxBuilder.Append(int64(i))
			}
		}
		if idxBuilder.Len() == 0 {
			idxBuilder.Release()
			continue
		}
		idxArr := idxBuilder.NewInt64Array()
		idxBuilder.Release()
		datum, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.RecordDatum{Value: rec}, &compute.ArrayDatum{Value: idxArr.Data()})
		idxArr.Release()
		if err != nil {
			releaseRecords(out)
			return nil, fmt.Errorf("partition %d: %w", p, err)
		}
		// Take returns a *RecordDatum for a record input; guard the assertion
		// so a future kernel change cannot panic the coordinator (issue #211).
		rd, ok := datum.(*compute.RecordDatum)
		if !ok {
			datum.Release()
			releaseRecords(out)
			return nil, fmt.Errorf("partition %d: unexpected Take datum %T", p, datum)
		}
		out[p] = rd.Value
	}
	return out, nil
}

// releaseRecords releases every non-nil record in a splitByOwner result.
func releaseRecords(recs []arrow.RecordBatch) {
	for _, r := range recs {
		if r != nil {
			r.Release()
		}
	}
}
