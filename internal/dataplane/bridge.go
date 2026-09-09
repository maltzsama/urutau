// Package dataplane — the row-to-wire encoder at the CDC decoder boundary.
//
// BatchFromChangeBatch converts a rowchange.Batch (the CDC decoders' output
// — binlog/JSON events are row-shaped) into a columnar wire batch via the
// transport's EncodeBatch (typed, tested). The IPC round-trip reuses tested
// code instead of a hand-rolled builder. The worker is fully columnar since
// commit a6cd459 (it no longer calls this); the encoder survives here
// because the row universe ends at the source decoders, not in the worker.
//
// Callers encode against the introspected schema when it is reachable (the
// snapshot builders use the worker's known schema; the enrich seam carries
// its own). schemaFromChanges is the FALLBACK for schema-less row producers
// (the live CDC puller), which upstream owns the resolved schema for —
// sources gain it when the reader contract carries it (quarantine plan G1).
package dataplane

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// BatchFromChangeBatch converts a rowchange.Batch into a columnar
// dataplane.Batch via the transport's EncodeBatch. The RecordBatch carries
// the wire schema (data + __op + __pos + __commit_ts + __ingest_ts +
// __snapshot). The caller owns the returned Batch.
//
// cs is the canonical schema to encode against; an empty cs falls back to
// per-batch inference (schemaFromChanges) for schema-less row producers.
func BatchFromChangeBatch(b rowchange.Batch, cs core.Schema) (*Batch, error) {
	// D-6: the batch's single arrival-ordered slice IS the wire order.
	all := b.Changes
	if len(all) == 0 {
		return &Batch{Table: b.Table, Watermark: []byte(b.Position), Mode: rowchange.ToDataplaneMode(b.Mode)}, nil
	}

	// Schema: known schema, plus any columns the changes carry that the
	// known schema lacks (enriched columns, e.g. join output). Enrichment
	// adds columns after registration, so the known schema alone would
	// drop them on the wire. When cs is empty (cold path — first bridge
	// call before schema registration), schemaFromChanges infers from
	// the row values.
	cs = mergeSchema(all, cs)

	// C-8 fail-fast: deletes without a PK produce orphaned NULL tuples in
	// the sink. The enrich path cannot reconstitute a key from After (it
	// may be nil or empty). When PK is empty the pump must not send deletes.
	if len(cs.PrimaryKey) == 0 {
		nDeletes := 0
		for _, c := range all {
			if c.Op == rowchange.OpDelete {
				nDeletes++
			}
		}
		if nDeletes > 0 {
			return nil, fmt.Errorf("dataplane: batch %q has %d delete(s) but schema has no PrimaryKey — deletes would become orphaned NULLs in the sink", b.Table, nDeletes)
		}
	}

	meta := &pb.BatchMeta{
		Table:   b.Table,
		HighPos: b.Position,
	}

	body, _, err := transport.EncodeBatch(all, cs, meta, nil)
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
		Mode:            rowchange.ToDataplaneMode(b.Mode),
		SnapshotState:   b.SnapshotState,
		SnapshotPending: b.SnapshotPending,
	}, nil
}

// schemaFromChanges infers a core.Schema from the changes' After/Before maps.
// Columns are sorted by name for deterministic output. Fallback for
// schema-less row producers (the live CDC puller); the resolved schema is
// owned upstream and should ride the reader contract (quarantine plan G1).
func schemaFromChanges(changes []rowchange.Change) core.Schema {
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
		// Nullable: an enriched (left-join miss) column legitimately holds
		// NULL, and any inferred column may be absent from some changes —
		// re-encoding against a NOT NULL assumption would fail.
		ct.Nullable = true
		cols = append(cols, core.Column{Name: k, Type: ct})
	}
	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })
	return core.Schema{Columns: cols}
}

// goTypeToCore maps a Go value to a core.ColumnType for schema inference.
func goTypeToCore(v any) core.ColumnType {
	switch v.(type) {
	case bool:
		return core.ColumnType{Kind: core.KindBool}
	case int:
		return core.ColumnType{Kind: core.KindInt64} // int is 64-bit on 64-bit platforms
	case int32:
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
	case time.Time:
		return core.ColumnType{Kind: core.KindTimestampTZ}
	default:
		return core.ColumnType{Kind: core.KindString} // inference fallback: unknown value type
	}
}

// mergeSchema returns the known schema extended with any columns present in
// the changes but missing from it (enriched columns, e.g. join output).
// Does NOT mutate the incoming cs — returns a new Schema.
func mergeSchema(changes []rowchange.Change, cs core.Schema) core.Schema {
	if len(cs.Columns) == 0 {
		return schemaFromChanges(changes)
	}
	has := make(map[string]bool, len(cs.Columns))
	for _, c := range cs.Columns {
		has[c.Name] = true
	}
	merged := make([]core.Column, len(cs.Columns))
	copy(merged, cs.Columns)
	inferred := schemaFromChanges(changes)
	for _, col := range inferred.Columns {
		if !has[col.Name] {
			merged = append(merged, col)
			has[col.Name] = true
		}
	}
	return core.Schema{Columns: merged, PrimaryKey: cs.PrimaryKey}
}
