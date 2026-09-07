// Package dataplane defines the columnar zero-copy data plane type for
// the CDC pipeline. One Batch per table, one arrow.Record in the CR-021
// wire schema, plus checkpoint bookkeeping.
//
// OWNERSHIP: whoever receives a Batch owns it and must Release() it (or
// hand it off). The Go GC does not free Arrow buffers — Release does.
// Every dataplane test runs under memory.NewCheckedAllocator; a leaked
// buffer fails the test.
//
// The Batch is the seam type: built-in sources, plugin sources, built-in
// sinks, and plugin sinks all consume *dataplane.Batch. This is the line
// where "the plugin system" and "the core" become the same data plane.
package dataplane

import "github.com/apache/arrow-go/v18/arrow"

// Batch is the unit of work on the data plane: one table, one
// arrow.Record in the CR-021 wire schema, plus checkpoint bookkeeping.
//
// Single-table is a precondition: PK semantics are table-scoped;
// collapse over a mixed-table batch is a bug. The wire schema is
// per-table (CR-021), so this is structural.
type Batch struct {
	Table string
	// Record is the CR-021 wire schema: data columns in schema order,
	// followed by __op, __pos, __commit_ts, __ingest_ts, __snapshot.
	Record arrow.Record
	// Watermark is __pos of the LAST row as received — the commit point.
	// Captured at RECEIVE time, before any transform (CR-069 §3.3).
	// Transforms reorder and drop rows; they may never move the watermark.
	// The value's type follows __pos's wire type; it is opaque to the
	// dataplane.
	Watermark any
}

// Release frees the Arrow buffers held by the Record. Safe to call on a
// nil Record (no-op). The caller must not use the Batch after Release.
func (b *Batch) Release() {
	if b.Record != nil {
		b.Record.Release()
		b.Record = nil
	}
}
