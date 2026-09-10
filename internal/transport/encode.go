package transport

// RecordFromChanges builds a wire-schema Arrow RecordBatch from row-oriented
// changes WITHOUT the IPC round-trip. EncodeBatch wraps it for the Flight
// wire; callers that already hold an arrow.RecordBatch pipeline (the enrich
// seam, the snapshot relay, the chunk executor, the live puller) use it
// directly so a row producer's output re-enters the columnar world in one
// step instead of encode-to-bytes then decode-back.
//
// MergeSchema / InferSchemaFromChanges are the schema helpers a row producer
// needs when the canonical schema is absent (live CDC) or grows after
// registration (enrichment adds join-output columns). Producers that hold a
// resolved schema and emit inserts only (snapshot, chunk scan) pass it
// straight through.

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// RecordFromChanges renders rows into a wire-schema RecordBatch: data columns
// typed per the canonical schema, followed by the fixed metadata columns
// (__op, __pos, __commit_ts, __ingest_ts, __snapshot). A nil alloc falls
// back to the default allocator (M-5).
//
// Deletes: a delete may carry only a key (the MySQL binlog decoder) — the
// key is projected onto the PK columns so the equality delete matches at
// read time. A partial before-image is backfilled from the key tuple. A
// delete with neither image nor key is an error (H-6).
//
// OWNERSHIP: the returned record is the caller's; Release it.
func RecordFromChanges(rows []rowchange.Change, cs core.Schema, alloc memory.Allocator) (arrow.RecordBatch, error) {
	schema, err := CoreSchemaToArrow(cs)
	if err != nil {
		return nil, err
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	bld := array.NewRecordBuilder(alloc, schema)
	defer bld.Release()

	numDataCols := len(cs.Columns)
	for i := range rows {
		r := &rows[i]
		// Data columns: look up in After (or Before for deletes when After is nil).
		src := r.After
		if src == nil {
			src = r.Before
		}
		// A delete may carry no row image at all — only the key. The equality
		// delete needs the key values on the wire to match at read time, so
		// project the key onto the PK columns; without it the delete file
		// holds NULL tuples and silently deletes nothing.
		if src == nil && len(r.Key) > 0 {
			src = make(map[string]any, len(r.Key))
			for k, name := range cs.PrimaryKey {
				if k < len(r.Key) {
					src[name] = r.Key[k]
				}
			}
		}
		// Same guarantee when a row image exists but lacks a key column
		// (partial before-images): backfill from the key tuple. The row map
		// is copied, never mutated — it belongs to the caller.
		if src != nil && len(cs.PrimaryKey) > 0 && len(r.Key) > 0 {
			missing := false
			for i, name := range cs.PrimaryKey {
				if i < len(r.Key) && src[name] == nil {
					missing = true
					break
				}
			}
			if missing {
				cp := make(map[string]any, len(src)+len(cs.PrimaryKey))
				for k, v := range src {
					cp[k] = v
				}
				for i, name := range cs.PrimaryKey {
					if i < len(r.Key) && cp[name] == nil {
						cp[name] = r.Key[i]
					}
				}
				src = cp
			}
		}
		// H-6: a delete with no image and no key produces a NULL tuple — the
		// equality delete would match nothing. Fail explicitly.
		if r.Op == rowchange.OpDelete && src == nil && len(r.Key) == 0 {
			return nil, fmt.Errorf("transport: delete sem key e sem imagem — o equality delete casaria nada")
		}
		for j, col := range cs.Columns {
			var v any
			if src != nil {
				v = src[col.Name]
			}
			if err := appendTypedValue(bld.Field(j), col.Type, v); err != nil {
				return nil, fmt.Errorf("transport: column %q row %d: %w", col.Name, i, err)
			}
		}
		// Metadata columns.
		bld.Field(numDataCols).(*array.Uint8Builder).Append(uint8(r.Op))
		bld.Field(numDataCols + 1).(*array.StringBuilder).Append(r.Position)
		if r.CommitTS.IsZero() {
			bld.Field(numDataCols + 2).AppendNull()
		} else {
			bld.Field(numDataCols + 2).(*array.TimestampBuilder).AppendTime(r.CommitTS)
		}
		if r.IngestTS.IsZero() {
			// Parity with CommitTS (M-4): a zero timestamp means "not set" —
			// it must not masquerade as a real instant on the wire.
			bld.Field(numDataCols + 3).AppendNull()
		} else {
			bld.Field(numDataCols + 3).(*array.TimestampBuilder).AppendTime(r.IngestTS)
		}
		bld.Field(numDataCols + 4).(*array.BooleanBuilder).Append(r.Snapshot)
		// __phase: the producer's value when set, otherwise derived from the
		// Snapshot boolean so producers that predate the Phase field still
		// land a phase on the wire. Empty and non-snapshot → null.
		switch {
		case r.Phase != "":
			bld.Field(numDataCols + 5).(*array.StringBuilder).Append(r.Phase)
		case r.Snapshot:
			bld.Field(numDataCols + 5).(*array.StringBuilder).Append(core.PhaseSnapshot)
		default:
			bld.Field(numDataCols + 5).(*array.StringBuilder).Append(core.PhaseStream)
		}
	}

	return bld.NewRecordBatch(), nil
}

// recordToIPC serializes one record as a complete Arrow IPC stream.
func recordToIPC(rec arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		return nil, fmt.Errorf("transport: ipc write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("transport: ipc close: %w", err)
	}
	return buf.Bytes(), nil
}

// InferSchemaFromChanges infers a core.Schema from the changes' After/Before
// maps. Columns are sorted by name for deterministic output. The fallback
// for schema-less row producers (the live CDC puller) — the resolved schema
// is owned upstream and should ride the reader contract.
func InferSchemaFromChanges(changes []rowchange.Change) core.Schema {
	seen := make(map[string]core.ColumnType)
	for _, c := range changes {
		src := c.After
		if src == nil {
			src = c.Before
		}
		for k, v := range src {
			if _, exists := seen[k]; !exists {
				seen[k] = goValueToCore(v)
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

// goValueToCore maps a Go value to a core.ColumnType for schema inference.
func goValueToCore(v any) core.ColumnType {
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

// MergeSchema returns the known schema extended with any columns present in
// the changes but missing from it (enriched columns, e.g. join output).
// Does NOT mutate the incoming cs — returns a new Schema. When cs is empty
// the whole schema is inferred from the row values.
func MergeSchema(changes []rowchange.Change, cs core.Schema) core.Schema {
	if len(cs.Columns) == 0 {
		return InferSchemaFromChanges(changes)
	}
	has := make(map[string]bool, len(cs.Columns))
	for _, c := range cs.Columns {
		has[c.Name] = true
	}
	merged := make([]core.Column, len(cs.Columns))
	copy(merged, cs.Columns)
	inferred := InferSchemaFromChanges(changes)
	for _, col := range inferred.Columns {
		if !has[col.Name] {
			merged = append(merged, col)
			has[col.Name] = true
		}
	}
	return core.Schema{Columns: merged, PrimaryKey: cs.PrimaryKey}
}
