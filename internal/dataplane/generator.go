package dataplane

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/internal/transport"
)

// GeneratorOpts configures batch generation.
type GeneratorOpts struct {
	NumRows          int
	PKDomain         int              // distinct PKs; 0 = max(1, NumRows/3) — duplicates by default
	DeletesOnlyAtEnd bool             // true = deletes only at last row; false (default) = deletes at any position
	IncludeNullPK    bool             // true = one row with null PK
	Allocator        memory.Allocator // if nil, GoAllocator is used
}

// baseSchema returns the fixed part of the generator schema.
// Uses WireMetadataFields() for the metadata columns (H-12).
func baseSchema() *arrow.Schema {
	data := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__before_val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}
	fields := append(data, transport.WireMetadataFields()...)
	return arrow.NewSchema(fields, nil)
}

// GenerateBatch creates a batch with controlled data for property tests.
func GenerateBatch(seed int64, opts GeneratorOpts) *Batch {
	if opts.NumRows <= 0 {
		opts.NumRows = 10
	}
	pkDomain := opts.PKDomain
	if pkDomain <= 0 {
		pkDomain = max(1, opts.NumRows/3)
	}

	alloc := opts.Allocator
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}

	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed+1)))
	schema := baseSchema()
	bb := array.NewRecordBuilder(alloc, schema)

	// Track last value per PK for __before_val (real before-image).
	lastVal := make(map[int64]string)

	for i := range opts.NumRows {
		// id: modulo PKDomain for duplicates, or null if IncludeNullPK and first row
		var id int64
		if opts.IncludeNullPK && i == 0 {
			bb.Field(0).(*array.Int64Builder).AppendNull()
			id = -1 // sentinel for null PK
		} else {
			id = int64((i % pkDomain) + 1)
			bb.Field(0).(*array.Int64Builder).Append(id)
		}

		// val: random string
		val := fmt.Sprintf("val-%d", rng.IntN(100))
		bb.Field(1).(*array.StringBuilder).Append(val)

		// __op: mix of inserts, updates, and deletes
		op := uint8(OpInsert)
		if opts.DeletesOnlyAtEnd {
			if i == opts.NumRows-1 && rng.IntN(3) == 0 {
				op = OpDelete
			} else if i > 0 && rng.IntN(4) == 0 {
				op = OpUpdate
			}
		} else {
			if rng.IntN(5) == 0 {
				op = OpDelete
			} else if i > 0 && rng.IntN(3) == 0 {
				op = OpUpdate
			}
		}
		bb.Field(5).(*array.Uint8Builder).Append(op)

		// __before_val: last known val for this PK (real before-image for
		// updates/deletes), null for inserts.
		if id >= 0 {
			if before, ok := lastVal[id]; ok && op != OpInsert {
				bb.Field(2).(*array.StringBuilder).Append(before)
			} else {
				bb.Field(2).(*array.StringBuilder).AppendNull()
			}
			lastVal[id] = val
		} else {
			bb.Field(2).(*array.StringBuilder).AppendNull()
		}

		// amount: random float
		bb.Field(3).(*array.Float64Builder).Append(rng.Float64() * 1000)

		// active: random bool
		bb.Field(4).(*array.BooleanBuilder).Append(rng.IntN(2) == 0)

		// __pos: monotonically increasing within batch (§3.3 precondition)
		pos := fmt.Sprintf("pos-%04d", i)
		bb.Field(6).(*array.StringBuilder).Append(pos)

		// __commit_ts (nanoseconds per M2a decision)
		ts := timeFromNano(int64(i))
		bb.Field(7).(*array.TimestampBuilder).AppendTime(ts)

		// __ingest_ts (microseconds — same as commit for test purposes)
		bb.Field(8).(*array.TimestampBuilder).AppendTime(ts)

		// __snapshot (boolean — false for live data)
		bb.Field(9).(*array.BooleanBuilder).Append(false)
	}

	rec := bb.NewRecordBatch()
	bb.Release()

	// Watermark = __pos of last row
	lastIdx := opts.NumRows - 1
	posArr := rec.Column(6).(*array.String)
	watermark := []byte(posArr.Value(lastIdx))

	return &Batch{
		Table:     "test_table",
		Record:    rec,
		Watermark: watermark,
	}
}

// timeFromNano creates a UTC timestamp at the given nanosecond offset from epoch.
func timeFromNano(ns int64) time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, int(ns), time.UTC)
}

// --- Named adversarial scenarios ---

// AdversarialCompositeKey produces a batch where naive key concatenation
// would collide: ("ab","c") vs ("a","bc").
func AdversarialCompositeKey(seed int64, alloc memory.Allocator) *Batch {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk1", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pk2", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.StringBuilder).Append("ab")
	bb.Field(1).(*array.StringBuilder).Append("c")
	bb.Field(2).(*array.StringBuilder).Append("row1")
	bb.Field(3).(*array.Uint8Builder).Append(0)
	bb.Field(4).(*array.StringBuilder).Append("pos-0001")
	bb.Field(5).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(6).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(7).(*array.BooleanBuilder).Append(false)

	bb.Field(0).(*array.StringBuilder).Append("a")
	bb.Field(1).(*array.StringBuilder).Append("bc")
	bb.Field(2).(*array.StringBuilder).Append("row2")
	bb.Field(3).(*array.Uint8Builder).Append(0)
	bb.Field(4).(*array.StringBuilder).Append("pos-0002")
	bb.Field(5).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(6).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(7).(*array.BooleanBuilder).Append(false)

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_composite_key", Record: rec, Watermark: []byte("pos-0002")}
}

// AdversarialDeleteLast produces a batch whose last row is a DELETE.
func AdversarialDeleteLast(seed int64, alloc memory.Allocator) *Batch {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(0)
	bb.Field(2).(*array.StringBuilder).Append("pos-0001")
	bb.Field(3).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(4).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(5).(*array.BooleanBuilder).Append(false)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(2)
	bb.Field(2).(*array.StringBuilder).Append("pos-0002")
	bb.Field(3).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(4).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(5).(*array.BooleanBuilder).Append(false)

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_delete_last", Record: rec, Watermark: []byte("pos-0002")}
}

// AdversarialInsertAfterDelete produces a batch where a row is inserted
// after being deleted in the same batch.
func AdversarialInsertAfterDelete(seed int64, alloc memory.Allocator) *Batch {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(2)
	bb.Field(2).(*array.StringBuilder).Append("pos-0001")
	bb.Field(3).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(4).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(5).(*array.BooleanBuilder).Append(false)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(0)
	bb.Field(2).(*array.StringBuilder).Append("pos-0002")
	bb.Field(3).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(4).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(5).(*array.BooleanBuilder).Append(false)

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_insert_after_delete", Record: rec, Watermark: []byte("pos-0002")}
}

// AdversarialInt64Overflow produces a batch with int64 values > 2^53
// that would be corrupted by JSON float64 (CR-021 regression test).
func AdversarialInt64Overflow(seed int64, alloc memory.Allocator) *Batch {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "big", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	big := int64(1) << 53
	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Int64Builder).Append(big + 1)
	bb.Field(2).(*array.Uint8Builder).Append(0)
	bb.Field(3).(*array.StringBuilder).Append("pos-0001")
	bb.Field(4).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(5).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(6).(*array.BooleanBuilder).Append(false)

	bb.Field(0).(*array.Int64Builder).Append(2)
	bb.Field(1).(*array.Int64Builder).Append(math.MaxInt64)
	bb.Field(2).(*array.Uint8Builder).Append(0)
	bb.Field(3).(*array.StringBuilder).Append("pos-0002")
	bb.Field(4).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(5).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(6).(*array.BooleanBuilder).Append(false)

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_int64_overflow", Record: rec, Watermark: []byte("pos-0002")}
}

// AdversarialNullBefore produces a batch with null __before_val payload
// column (Kleene/null semantics test).
func AdversarialNullBefore(seed int64, alloc memory.Allocator) *Batch {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__before_val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.StringBuilder).AppendNull()
	bb.Field(2).(*array.StringBuilder).AppendNull()
	bb.Field(3).(*array.Uint8Builder).Append(1)
	bb.Field(4).(*array.StringBuilder).Append("pos-0001")
	bb.Field(5).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(6).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(7).(*array.BooleanBuilder).Append(false)

	bb.Field(0).(*array.Int64Builder).Append(2)
	bb.Field(1).(*array.StringBuilder).Append("hello")
	bb.Field(2).(*array.StringBuilder).AppendNull()
	bb.Field(3).(*array.Uint8Builder).Append(0)
	bb.Field(4).(*array.StringBuilder).Append("pos-0002")
	bb.Field(5).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(6).(*array.TimestampBuilder).AppendTime(time.Unix(0, 0).UTC())
	bb.Field(7).(*array.BooleanBuilder).Append(false)

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_null_before", Record: rec, Watermark: []byte("pos-0002")}
}

// EncodeKey encodes a composite key as length-prefixed binary concat
// of the string representations — used by collapse to avoid hash
// collisions (CR-069 §3.2).
//
// v4: type-prefixed binary encoding. Each field is [type-byte][payload…].
// Different types cannot collide because they carry distinct type tags.
// No fmt.Sprintf — direct binary encoding for determinism and speed.
func EncodeKey(record arrow.RecordBatch, row int, pkCols []string) ([]byte, error) {
	const (
		typeInt32   byte = 0x01
		typeInt64   byte = 0x02
		typeUInt64  byte = 0x03
		typeFloat32 byte = 0x04
		typeFloat64 byte = 0x05
		typeBool    byte = 0x06
		typeString  byte = 0x07
		typeDate32  byte = 0x09
		typeTime64  byte = 0x0A
		typeTSNZ    byte = 0x0B
	)
	var buf []byte
	var lenBuf [4]byte
	for _, col := range pkCols {
		idx := -1
		for i := 0; i < int(record.NumCols()); i++ {
			if record.Schema().Field(i).Name == col {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("dataplane: encode key: column %q not found", col)
		}
		arr := record.Column(idx)
		switch a := arr.(type) {
		case *array.Int32:
			buf = append(buf, typeInt32)
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(a.Value(row)))
			buf = append(buf, lenBuf[:]...)
		case *array.Int64:
			buf = append(buf, typeInt64)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], uint64(a.Value(row)))
			buf = append(buf, v[:]...)
		case *array.Uint64:
			buf = append(buf, typeUInt64)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], a.Value(row))
			buf = append(buf, v[:]...)
		case *array.Float32:
			buf = append(buf, typeFloat32)
			binary.LittleEndian.PutUint32(lenBuf[:], math.Float32bits(a.Value(row)))
			buf = append(buf, lenBuf[:]...)
		case *array.Float64:
			buf = append(buf, typeFloat64)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], math.Float64bits(a.Value(row)))
			buf = append(buf, v[:]...)
		case *array.Boolean:
			buf = append(buf, typeBool)
			if a.Value(row) {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case *array.String:
			buf = append(buf, typeString)
			s := a.Value(row)
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(s)))
			buf = append(buf, lenBuf[:]...)
			buf = append(buf, s...)
		case *array.Date32:
			buf = append(buf, typeDate32)
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(a.Value(row)))
			buf = append(buf, lenBuf[:]...)
		case *array.Time64:
			buf = append(buf, typeTime64)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], uint64(a.Value(row)))
			buf = append(buf, v[:]...)
		case *array.Timestamp:
			buf = append(buf, typeTSNZ)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], uint64(a.Value(row)))
			buf = append(buf, v[:]...)
		default:
			return nil, fmt.Errorf("dataplane: encode key: unsupported column type %T for PK column %q", arr, col)
		}
	}
	return buf, nil
}
