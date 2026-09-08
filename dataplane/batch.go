// Package dataplane defines the columnar zero-copy data plane seam type
// for the CDC pipeline. One Batch per table, one RecordBatch in the
// wire schema, plus checkpoint bookkeeping.
//
// This package is PUBLIC — source, sink, and plugin contracts reference
// dataplane.Batch without violating the internal/ boundary (architecture
// test TestContractsArePluginSafe).
//
// OWNERSHIP: whoever receives a Batch owns it and must Release() it (or
// hand it off). The Go GC does not free Arrow buffers — Release does.
// Every dataplane test runs under a checked allocator; a leaked buffer
// fails the test.
//
// Wire mapping (frozen in M2a):
//
//	§8.1 (plugin contract)        internal (CR-021)
//	─────────────────────────────  ──────────────────────────────
//	op       Utf8 "c"/"u"/"d"      __op       uint8 0/1/2
//	after    Struct                 flat columns (field-array + validity)
//	before   Struct                 d → before→flat; u → dropped
//	                                 (evolves: CR-XXX adds __before_*)
//	offset   Binary ≤256            __pos      String (opaque, zero-copy)
//	ts_source                       __commit_ts Nanoseconds
//
// Physical sinks truncate __commit_ts as needed (Iceberg µs).
package dataplane

import (
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/change"
)

// Batch is the unit of work on the data plane: one table, one
// RecordBatch in the wire schema, plus checkpoint bookkeeping.
//
// Single-table is a precondition: PK semantics are table-scoped;
// collapse over a mixed-table batch is a bug. The wire schema is
// per-table, so this is structural.
type Batch struct {
	Table string
	// Record is the wire schema: data columns in schema order,
	// followed by __op, __pos, __commit_ts. System columns
	// __ingest_ts, __snapshot, and __phase are injected by
	// AddMetadata at the last stage (§3.6, four-readers rule) —
	// they are NOT on the wire.
	Record arrow.RecordBatch
	// Watermark is __pos of the LAST row as received — the commit
	// point. Captured at RECEIVE time, before any transform (§3.3).
	// Transforms reorder and drop rows; they may never move the
	// watermark. Opaque to the dataplane; the checkpoint layer
	// decides encoding.
	//
	// Snapshot-path batches carry NO watermark: chunk rows are
	// SELECT results with no __pos. Their checkpoint is
	// snapshot-completion, stored outside the data plane (state
	// store / properties).
	Watermark []byte

	// Mode is the write shape this batch serves: upsert (equality-delete
	// capable) or append (plain log, deletes already rewritten/dropped by
	// the worker). The sink must know it to apply the right write path.
	Mode change.WriteMode

	// SnapshotState is the resumable-backfill state machine value
	// ("not_started", "in_progress", "complete"). Traveled with the
	// batch so the sink can persist it atomically with the position.
	// Empty when the table is not in snapshot phase.
	SnapshotState string
	// SnapshotPending lists chunk IDs still to process. Persisted
	// atomically with position so a crash resumes from the right
	// chunk. Nil when not in snapshot phase.
	SnapshotPending []uint32
}

// Release frees the Arrow buffers held by the Record. Safe to call on a
// nil Record (no-op). The caller must not use the Batch after Release.
func (b *Batch) Release() {
	if b.Record != nil {
		b.Record.Release()
		b.Record = nil
	}
}
