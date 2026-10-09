package dataplane

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// This file is the ONE place batches are concatenated or merged (issue #508):
// four drifted per-column helpers used to live in internal/worker with
// different schema checks and allocators. Callers pass an allocator (nil →
// DefaultAllocator) and get a schema check.

// ConcatBatches concatenates batches that must share a schema, skipping empty
// ones. It returns nil when nothing carries rows. The returned batch owns its
// record (a single input is Retained, never aliased to the caller's pointer).
func ConcatBatches(alloc memory.Allocator, bs []*Batch) (*Batch, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	parts := make([]*Batch, 0, len(bs))
	for _, b := range bs {
		if b != nil && b.Record != nil && b.Record.NumRows() > 0 {
			parts = append(parts, b)
		}
	}
	if len(parts) == 0 {
		return nil, nil
	}
	first := parts[0]
	for _, b := range parts[1:] {
		if !sameSchema(b.Record.Schema(), first.Record.Schema()) {
			return nil, fmt.Errorf("dataplane: concat: schema mismatch: %s vs %s (source batches must share a stable schema)",
				colsOf(b.Record.Schema()), colsOf(first.Record.Schema()))
		}
	}
	staged := false
	for _, p := range parts {
		staged = staged || p.Staged
	}
	out := &Batch{Table: first.Table, Watermark: first.Watermark, Mode: first.Mode,
		SnapshotState: first.SnapshotState, SnapshotPending: first.SnapshotPending, Seq: first.Seq, Staged: staged}
	if len(parts) == 1 {
		first.Record.Retain()
		out.Record = first.Record
		return out, nil
	}
	schema := first.Record.Schema()
	var rows int64
	for _, p := range parts {
		rows += p.Record.NumRows()
	}
	cols := make([]arrow.Array, schema.NumFields())
	arrs := make([]arrow.Array, len(parts))
	for i := range cols {
		for j, p := range parts {
			arrs[j] = p.Record.Column(i)
		}
		col, err := array.Concatenate(arrs, alloc)
		if err != nil {
			releaseAll(cols[:i])
			return nil, err
		}
		cols[i] = col
	}
	out.Record = array.NewRecordBatch(schema, cols, rows)
	releaseAll(cols)
	return out, nil
}

// MergeBatches concatenates two batches with the same schema, preserving a,
// b's metadata (the result is append mode, like the collapse's upserts +
// deletes merge). Either side may be nil or empty. The returned batch owns its
// record; the caller still owns and releases a and b.
func MergeBatches(alloc memory.Allocator, a, b *Batch) (*Batch, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	empty := func(x *Batch) bool { return x == nil || x.Record == nil || x.Record.NumRows() == 0 }
	switch {
	case empty(a) && empty(b):
		return nil, nil
	case empty(a):
		return retainBatch(b), nil
	case empty(b):
		return retainBatch(a), nil
	}
	schema := a.Record.Schema()
	ncols := int(schema.NumFields())
	nrows := a.Record.NumRows() + b.Record.NumRows()
	cols := make([]arrow.Array, ncols)
	for i := range ncols {
		cat, err := array.Concatenate([]arrow.Array{a.Record.Column(i), b.Record.Column(i)}, alloc)
		if err != nil {
			releaseAll(cols[:i])
			return nil, err
		}
		cols[i] = cat
	}
	rec := array.NewRecordBatch(schema, cols, nrows)
	releaseAll(cols)
	return &Batch{
		Table:           a.Table,
		Record:          rec,
		Watermark:       a.Watermark,
		Mode:            a.Mode,
		SnapshotState:   a.SnapshotState,
		SnapshotPending: a.SnapshotPending,
		Seq:             a.Seq,
		Staged:          a.Staged || b.Staged,
	}, nil
}

// ConcatRecords concatenates Arrow records that share a schema (the chunk
// encoder's per-column concatenation). The caller owns the result.
func ConcatRecords(alloc memory.Allocator, parts []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	schema := parts[0].Schema()
	var rows int64
	for _, p := range parts {
		rows += p.NumRows()
	}
	cols := make([]arrow.Array, schema.NumFields())
	for i := range cols {
		arrs := make([]arrow.Array, len(parts))
		for j, p := range parts {
			arrs[j] = p.Column(i)
		}
		col, err := array.Concatenate(arrs, alloc)
		if err != nil {
			releaseAll(cols[:i])
			return nil, err
		}
		cols[i] = col
	}
	rec := array.NewRecordBatch(schema, cols, rows)
	releaseAll(cols)
	return rec, nil
}

func retainBatch(b *Batch) *Batch {
	b.Record.Retain()
	return &Batch{Table: b.Table, Record: b.Record, Watermark: b.Watermark, Mode: b.Mode,
		SnapshotState: b.SnapshotState, SnapshotPending: b.SnapshotPending, Seq: b.Seq, Staged: b.Staged}
}

func releaseAll(arrs []arrow.Array) {
	for _, a := range arrs {
		a.Release()
	}
}

// sameSchema reports field-for-field equality (names, types, order).
func sameSchema(a, b *arrow.Schema) bool {
	if a.NumFields() != b.NumFields() {
		return false
	}
	for i := 0; i < a.NumFields(); i++ {
		af, bf := a.Field(i), b.Field(i)
		if af.Name != bf.Name || !arrow.TypeEqual(af.Type, bf.Type) {
			return false
		}
	}
	return true
}

// colsOf renders a schema's column names, for a mismatch error message.
func colsOf(s *arrow.Schema) string {
	names := make([]string, s.NumFields())
	for i := range names {
		names[i] = s.Field(i).Name
	}
	return strings.Join(names, ",")
}
