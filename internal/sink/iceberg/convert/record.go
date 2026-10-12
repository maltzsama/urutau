package convert

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// ReleaseCols releases the columns built so far (the first upTo entries).
func ReleaseCols(cols []arrow.Array, upTo int) {
	for i := range upTo {
		if cols[i] != nil {
			cols[i].Release()
		}
	}
}

// colIndexByName returns the index of the field named name, or -1.
func colIndexByName(s *arrow.Schema, name string) int {
	for i := range s.NumFields() {
		if s.Field(i).Name == name {
			return i
		}
	}
	return -1
}

// Uint8Column returns the named column as a Uint8 array, or an error when it
// is missing or the wrong type.
func Uint8Column(src arrow.RecordBatch, name string) (*array.Uint8, error) {
	idx := colIndexByName(src.Schema(), name)
	if idx < 0 {
		return nil, fmt.Errorf("column %q not on wire", name)
	}
	col, ok := src.Column(idx).(*array.Uint8)
	if !ok {
		return nil, fmt.Errorf("column %q type %T, want *array.Uint8", name, src.Column(idx))
	}
	return col, nil
}

// StringColumn returns the named column as a String array, or an error when it
// is missing or the wrong type.
func StringColumn(src arrow.RecordBatch, name string) (*array.String, error) {
	idx := colIndexByName(src.Schema(), name)
	if idx < 0 {
		return nil, fmt.Errorf("column %q not on wire", name)
	}
	col, ok := src.Column(idx).(*array.String)
	if !ok {
		return nil, fmt.Errorf("column %q type %T, want *array.String", name, src.Column(idx))
	}
	return col, nil
}

// TimestampColumn builds the named wire timestamp column in the target field's
// unit and zone (dt), preserving the instant; a missing or non-timestamp
// column becomes a typed null.
func TimestampColumn(src arrow.RecordBatch, name string, dt arrow.DataType) (arrow.Array, error) {
	target, ok := dt.(*arrow.TimestampType)
	if !ok {
		return nil, fmt.Errorf("iceberg: metadata %q: target field is %s, not a timestamp", name, dt)
	}
	idx := colIndexByName(src.Schema(), name)
	if idx < 0 {
		return NullColumn(target, src.NumRows()), nil
	}
	col := src.Column(idx)
	if col.DataType().ID() != arrow.TIMESTAMP {
		return NullColumn(target, src.NumRows()), nil
	}
	tsCol := col.(*array.Timestamp)
	unit := tsCol.DataType().(*arrow.TimestampType).Unit
	// Build in the TARGET field's unit and zone: the wire carries __commit_ts
	// as ns and __ingest_ts as us, while an Iceberg Timestamptz field is
	// timestamp[us, UTC]. A hardcoded unit mismatches the schema, which makes
	// array.NewRecordBatch panic.
	bb := array.NewTimestampBuilder(memory.DefaultAllocator, target)
	defer bb.Release()
	for i := range tsCol.Len() {
		if tsCol.IsNull(i) {
			bb.AppendNull()
			continue
		}
		// ToTime interprets the raw value with the SOURCE unit; AppendTime
		// stores it in the target's — the absolute instant is preserved.
		bb.AppendTime(tsCol.Value(i).ToTime(unit))
	}
	return bb.NewTimestampArray(), nil
}

// ChangeOpString renders a rowchange op as its wire string. It switches on the
// rowchange.Op* constants (not literals) and rejects an unknown op instead of
// absorbing it as a delete — a future op must not silently become one.
func ChangeOpString(op uint8) (string, error) {
	switch rowchange.Op(op) {
	case rowchange.OpInsert:
		return "insert", nil
	case rowchange.OpUpdate:
		return "update", nil
	case rowchange.OpDelete:
		return "delete", nil
	default:
		return "", fmt.Errorf("iceberg: unknown __op %d", op)
	}
}

// NullColumn builds an n-row null array of type dt.
func NullColumn(dt arrow.DataType, n int64) arrow.Array {
	bb := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "x", Type: dt, Nullable: true}}, nil))
	defer bb.Release()
	for range n {
		bb.Field(0).AppendNull()
	}
	return bb.Field(0).NewArray()
}

// ConstString builds an n-row string array holding v.
func ConstString(n int64, v string) arrow.Array {
	bb := array.NewStringBuilder(memory.DefaultAllocator)
	defer bb.Release()
	for range n {
		bb.Append(v)
	}
	return bb.NewStringArray()
}

// SplitByOp splits a batch into upsert rows (op != delete) and delete rows
// (op == delete) using columnar masks.
func SplitByOp(ctx context.Context, b *dataplane.Batch) (upserts, deletes *dataplane.Batch, err error) {
	if b.Record == nil || b.Record.NumRows() == 0 {
		return nil, nil, nil
	}
	opIdx := colIndexByName(b.Record.Schema(), "__op")
	if opIdx < 0 {
		return nil, nil, fmt.Errorf("iceberg: __op column not found")
	}
	opCol, ok := b.Record.Column(opIdx).(*array.Uint8)
	if !ok {
		return nil, nil, fmt.Errorf("iceberg: __op type %T", b.Record.Column(opIdx))
	}
	up := array.NewBooleanBuilder(memory.DefaultAllocator)
	del := array.NewBooleanBuilder(memory.DefaultAllocator)
	for i := range opCol.Len() {
		if opCol.IsNull(i) {
			return nil, nil, fmt.Errorf("iceberg: __op is NULL at row %d", i)
		}
		isDel := opCol.Value(i) == uint8(rowchange.OpDelete)
		up.Append(!isDel)
		del.Append(isDel)
	}
	upArr := up.NewBooleanArray()
	delArr := del.NewBooleanArray()
	defer upArr.Release()
	defer delArr.Release()
	opts := compute.DefaultFilterOptions()
	fUp, err := compute.FilterRecordBatch(ctx, b.Record, upArr, opts)
	if err != nil {
		return nil, nil, fmt.Errorf("iceberg: filter upserts: %w", err)
	}
	fDel, err := compute.FilterRecordBatch(ctx, b.Record, delArr, opts)
	if err != nil {
		fUp.Release()
		return nil, nil, fmt.Errorf("iceberg: filter deletes: %w", err)
	}
	if fUp.NumRows() > 0 {
		// Copy the whole batch and replace only the Record, so every other
		// field (Mode, SnapshotState, SnapshotPending, …) travels with the
		// split instead of being silently dropped. The copy does not share
		// b.Record (it is replaced), so there is no double-Release.
		up := *b
		up.Record = fUp
		upserts = &up
	} else {
		fUp.Release()
	}
	if fDel.NumRows() > 0 {
		del := *b
		del.Record = fDel
		deletes = &del
	} else {
		fDel.Release()
	}
	return upserts, deletes, nil
}

// ExtractKeys extracts the primary-key values of every row across the given
// batches as [][]any — the equality-delete key boundary (CR-069 §4.1):
// iceberg-go takes delete keys in its own form, so conversion happens on the
// already-filtered key batch, never on the data.
func ExtractKeys(batches []*dataplane.Batch, pkCols []string) ([][]any, error) {
	var keys [][]any
	for _, b := range batches {
		if b == nil || b.Record == nil {
			continue
		}
		idxs := make([]int, len(pkCols))
		for i, c := range pkCols {
			idx := colIndexByName(b.Record.Schema(), c)
			if idx < 0 {
				return nil, fmt.Errorf("iceberg: PK column %q not in batch", c)
			}
			idxs[i] = idx
		}
		for r := 0; r < int(b.Record.NumRows()); r++ {
			key := make([]any, len(pkCols))
			for i, idx := range idxs {
				v, err := scalarValue(b.Record.Column(idx), r)
				if err != nil {
					return nil, fmt.Errorf("iceberg: PK column %q: %w", pkCols[i], err)
				}
				key[i] = v
			}
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// scalarValue extracts one scalar from an Arrow array at row. Used only for
// the equality-delete key boundary (§4.1) — never on the data path.
//
// The value it returns must be one that AppendColumn accepts for the PK
// column's Iceberg type — deleteRecord feeds it straight back through
// AppendColumn. Iceberg has no unsigned integer, so a KindUInt64 column is
// Decimal(20,0) in the schema (typemap) and on the data path; the delete key
// must carry the SAME canonical decimal form, not the raw uint64, or the
// delete path diverges from the data path (one value, one wire
// representation — the RV-11 family). Decimal, date and time keys travel as
// their canonical text, which is what AppendColumn's decimal/date/time
// builders parse. An unhandled array type is an ERROR: returning nil here
// would write a NULL key that matches no row, so the delete would silently
// never apply.
func scalarValue(col arrow.Array, row int) (any, error) {
	// A NULL key value cannot be an equality-delete key: the delete would
	// match no row. Falling through to c.Value(row) is worse — the array's
	// zero value (0, "", false) matches the row whose PK is exactly that, so
	// the delete would erase the WRONG row. Fail loud instead.
	if col.IsNull(row) {
		return nil, fmt.Errorf("iceberg: NULL primary-key value cannot be an equality-delete key")
	}
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(row), nil
	case *array.Int32:
		return c.Value(row), nil
	case *array.Uint8:
		return c.Value(row), nil
	case *array.Uint64:
		return strconv.FormatUint(c.Value(row), 10), nil // canonical decimal(20,0) text
	case *array.Float64:
		return c.Value(row), nil
	case *array.Float32:
		return c.Value(row), nil
	case *array.String:
		return c.Value(row), nil
	case *array.Binary:
		return c.Value(row), nil
	case *array.FixedSizeBinary:
		return c.Value(row), nil
	case *array.Boolean:
		return c.Value(row), nil
	case *array.Timestamp:
		return c.Value(row).ToTime(c.DataType().(*arrow.TimestampType).Unit), nil
	case *array.Decimal128:
		return c.Value(row).ToString(c.DataType().(*arrow.Decimal128Type).Scale), nil
	case *array.Date32:
		return time.Unix(int64(c.Value(row))*86400, 0).UTC().Format("2006-01-02"), nil
	case *array.Time64:
		// formatTimeOfDay is microsecond-based; the wire carries Time64us, but
		// normalize defensively so a different unit cannot shift the key.
		v := int64(c.Value(row))
		switch c.DataType().(*arrow.Time64Type).Unit {
		case arrow.Microsecond:
		case arrow.Nanosecond:
			v /= 1_000
		default:
			return nil, fmt.Errorf("unsupported Time64 unit %s", c.DataType())
		}
		return formatTimeOfDay(v), nil
	default:
		return nil, fmt.Errorf("unsupported primary-key column type %s", col.DataType())
	}
}

// formatTimeOfDay renders microseconds since midnight as the
// "15:04:05.ffffff" text timeToMicros parses back.
func formatTimeOfDay(micros int64) string {
	h := micros / 3_600_000_000
	micros %= 3_600_000_000
	m := micros / 60_000_000
	micros %= 60_000_000
	s := micros / 1_000_000
	micros %= 1_000_000
	return fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, s, micros)
}
