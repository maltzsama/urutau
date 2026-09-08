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
// Columns: id, val, __before_val, amount, active, __op, __pos, __commit_ts.
func baseSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__before_val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
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

		// __commit_ts
		ts := timeFromMicro(int64(i))
		bb.Field(7).(*array.TimestampBuilder).AppendTime(ts)
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

// timeFromMicro creates a UTC timestamp at the given microsecond offset from epoch.
func timeFromMicro(us int64) time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, int(us)*1000, time.UTC)
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
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.StringBuilder).Append("ab")
	bb.Field(1).(*array.StringBuilder).Append("c")
	bb.Field(2).(*array.StringBuilder).Append("row1")
	bb.Field(3).(*array.Uint8Builder).Append(0)
	bb.Field(4).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.StringBuilder).Append("a")
	bb.Field(1).(*array.StringBuilder).Append("bc")
	bb.Field(2).(*array.StringBuilder).Append("row2")
	bb.Field(3).(*array.Uint8Builder).Append(0)
	bb.Field(4).(*array.StringBuilder).Append("pos-0002")

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
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(0)
	bb.Field(2).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(2)
	bb.Field(2).(*array.StringBuilder).Append("pos-0002")

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
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(2)
	bb.Field(2).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(0)
	bb.Field(2).(*array.StringBuilder).Append("pos-0002")

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
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	big := int64(1) << 53
	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Int64Builder).Append(big + 1)
	bb.Field(2).(*array.Uint8Builder).Append(0)
	bb.Field(3).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.Int64Builder).Append(2)
	bb.Field(1).(*array.Int64Builder).Append(math.MaxInt64)
	bb.Field(2).(*array.Uint8Builder).Append(0)
	bb.Field(3).(*array.StringBuilder).Append("pos-0002")

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
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.StringBuilder).AppendNull()
	bb.Field(2).(*array.StringBuilder).AppendNull()
	bb.Field(3).(*array.Uint8Builder).Append(1)
	bb.Field(4).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.Int64Builder).Append(2)
	bb.Field(1).(*array.StringBuilder).Append("hello")
	bb.Field(2).(*array.StringBuilder).AppendNull()
	bb.Field(3).(*array.Uint8Builder).Append(0)
	bb.Field(4).(*array.StringBuilder).Append("pos-0002")

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_null_before", Record: rec, Watermark: []byte("pos-0002")}
}

// EncodeKey encodes a composite key as length-prefixed binary concat
// of the string representations — used by collapse to avoid hash
// collisions (CR-069 §3.2).
func EncodeKey(record arrow.RecordBatch, row int, pkCols []string) ([]byte, error) {
	var buf []byte
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
		var val string
		switch a := arr.(type) {
		case *array.Int64:
			val = fmt.Sprintf("i:%d", a.Value(row))
		case *array.String:
			val = "s:" + a.Value(row)
		case *array.Boolean:
			val = fmt.Sprintf("b:%t", a.Value(row))
		case *array.Float64:
			val = fmt.Sprintf("f:%v", a.Value(row))
		default:
			return nil, fmt.Errorf("dataplane: encode key: unsupported column type %T for PK column %q", arr, col)
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(val)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, val...)
	}
	return buf, nil
}
