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
// The wire record carries a fixed metadata tail (transport.WireMetadataFields):
// __op, __pos, __commit_ts, __ingest_ts, __snapshot, __phase — all six born
// at the source, none injected downstream. Physical sinks truncate
// __commit_ts as needed (Iceberg µs).
package dataplane

import "github.com/apache/arrow-go/v18/arrow"

// WriteMode selects the write shape a batch serves: upsert
// (equality-delete capable) or append (plain log, deletes already
// rewritten/dropped by the worker). The sink must know it to apply the
// right write path.
type WriteMode uint8

const (
	// ModeUnset is the zero value — invalid. A batch must carry an explicit
	// mode; a zero value silently defaulting to upsert would corrupt write
	// semantics. Sinks reject it.
	ModeUnset WriteMode = iota
	// UpsertMode collapses batches per-key and applies equality deletes.
	UpsertMode
	// AppendMode passes every change through without collapse; deletes
	// arrive as rows carrying the operation.
	AppendMode
)

// Batch is the unit of work on the data plane: one table, one
// RecordBatch in the wire schema, plus checkpoint bookkeeping.
//
// DO NOT COPY: a Batch holds a reference-counted arrow.RecordBatch.
// Copying the struct (b2 := *b) shares the Record; calling Release on both
// double-decrements the refcount and frees memory in use. Batches must be
// passed and returned as pointers, and ownership transferred once.
//
// Single-table is a precondition: PK semantics are table-scoped;
// collapse over a mixed-table batch is a bug. The wire schema is
// per-table, so this is structural.
type Batch struct {
	// Table is the SINK-side target name (e.g. "raw.orders"), the identity
	// the worker routes on and the sink writes to. core.TableRef splits the
	// source/target pair; this is the target half.
	Table string
	// Record is the wire schema: data columns in schema order, followed by
	// the six metadata columns (transport.WireMetadataFields): __op, __pos,
	// __commit_ts, __ingest_ts, __snapshot, __phase. All six are on the wire,
	// written by the source encoder.
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
	Mode WriteMode

	// SnapshotState is the resumable-backfill state machine value.
	// CLOSED SET (R5): "" (not in snapshot), "not_started", "in_progress",
	// "complete". It is a string rather than an enum type because the
	// canonical enum lives in internal/snapshot, which this public package
	// must not import; producers set it from snapshot.State, and no other
	// value is valid. Traveled with the batch so the sink can persist it
	// atomically with the position.
	SnapshotState string
	// SnapshotPending lists chunk IDs still to process. Persisted
	// atomically with position so a crash resumes from the right
	// chunk. Nil when not in snapshot phase.
	SnapshotPending []uint32
}

// Release frees the Arrow buffers held by the Record. Safe to call on a
// nil Batch or nil Record (no-op). The caller must not use the Batch after
// Release.
func (b *Batch) Release() {
	if b == nil || b.Record == nil {
		return
	}
	b.Record.Release()
	b.Record = nil
}
