// Package dataplane — bridge from change.Batch to *dataplane.Batch.
//
// QUARANTINE: this file is a TRANSITION adapter. It converts the old
// row-oriented change.Batch into a columnar dataplane.Batch using the
// transport's EncodeBatch (typed, tested). The round-trip through IPC
// serialization is intentional: it reuses existing tested code instead
// of hand-rolling a new builder (lesson from ghost commit e273866c).
//
// This bridge DIES when the worker switches to columnar (commit 3/4).
// Do not add logic here — it is temporary by design.
package dataplane

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// BatchFromChangeBatch converts a row-oriented change.Batch into a
// columnar dataplane.Batch via the transport's EncodeBatch. The RecordBatch
// carries the wire schema (data + __op + __pos + __commit_ts + __ingest_ts
// + __snapshot). The caller owns the returned Batch.
//
// QUARANTINE: dies in commit 3/4 when the worker produces Batch directly.
func BatchFromChangeBatch(b change.Batch, cs core.Schema) (*Batch, error) {
	// Merge upserts and deletes into a single ordered slice.
	all := make([]change.Change, 0, len(b.Upserts)+len(b.Deletes))
	all = append(all, b.Upserts...)
	all = append(all, b.Deletes...)
	if len(all) == 0 {
		return &Batch{Table: b.Table, Watermark: []byte(b.Position)}, nil
	}

	// If no schema provided, infer from the changes.
	if len(cs.Columns) == 0 {
		cs = schemaFromChanges(all)
	}

	meta := &pb.BatchMeta{
		Table:   b.Table,
		HighPos: b.Position,
	}

	body, _, err := transport.EncodeBatch(all, cs, meta)
	if err != nil {
		return nil, fmt.Errorf("bridge: encode: %w", err)
	}

	reader, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("bridge: ipc reader: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return nil, fmt.Errorf("bridge: empty record")
	}
	rec := reader.RecordBatch()
	if rec == nil {
		return nil, fmt.Errorf("bridge: nil record")
	}
	rec.Retain()

	return &Batch{
		Table:           b.Table,
		Record:          rec,
		Watermark:       []byte(b.Position),
		SnapshotState:   b.SnapshotState,
		SnapshotPending: b.SnapshotPending,
	}, nil
}

// schemaFromChanges infers a core.Schema from the changes' After/Before maps.
// QUARANTINE: dies in commit 3/4.
func schemaFromChanges(changes []change.Change) core.Schema {
	seen := make(map[string]core.ColumnType)
	for _, c := range changes {
		src := c.After
		if src == nil {
			src = c.Before
		}
		for k, v := range src {
			if _, exists := seen[k]; !exists {
				seen[k] = goTypeToCore(v)
			}
		}
	}
	cols := make([]core.Column, 0, len(seen))
	for k, ct := range seen {
		cols = append(cols, core.Column{Name: k, Type: ct})
	}
	return core.Schema{Columns: cols}
}

// goTypeToCore maps a Go value to a core.ColumnType.
// QUARANTINE: dies in commit 3/4.
func goTypeToCore(v any) core.ColumnType {
	switch v.(type) {
	case bool:
		return core.ColumnType{Kind: core.KindBool}
	case int, int32:
		return core.ColumnType{Kind: core.KindInt32}
	case int64:
		return core.ColumnType{Kind: core.KindInt64}
	case uint64:
		return core.ColumnType{Kind: core.KindUInt64}
	case float32:
		return core.ColumnType{Kind: core.KindFloat32}
	case float64:
		return core.ColumnType{Kind: core.KindFloat64}
	case string:
		return core.ColumnType{Kind: core.KindString}
	case []byte:
		return core.ColumnType{Kind: core.KindBinary}
	default:
		return core.ColumnType{Kind: core.KindString}
	}
}
