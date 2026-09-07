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
	Seed           int64
	NumRows        int
	PKColumns      []string // column names to use as PK; nil = ["id"]
	CompositeTypes bool     // include Struct/List/Map columns in schema
	IncludeNullPK  bool     // true = one row with null PK (PR 41 error test)
}

// baseSchema returns the fixed part of the generator schema:
// id (int64, PK), op (uint8), pos (string), commit_ts (timestamp).
func baseSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
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
	if len(opts.PKColumns) == 0 {
		opts.PKColumns = []string{"id"}
	}

	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed+1)))
	schema := baseSchema()
	alloc := memory.NewGoAllocator()
	bb := array.NewRecordBuilder(alloc, schema)

	for i := range opts.NumRows {
		// id: sequential, or null if IncludeNullPK and first row
		if opts.IncludeNullPK && i == 0 {
			bb.Field(0).(*array.Int64Builder).AppendNull()
		} else {
			bb.Field(0).(*array.Int64Builder).Append(int64(i + 1))
		}

		// val: random string
		bb.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("val-%d", rng.IntN(100)))

		// amount: random float
		bb.Field(2).(*array.Float64Builder).Append(rng.Float64() * 1000)

		// active: random bool
		bb.Field(3).(*array.BooleanBuilder).Append(rng.IntN(2) == 0)

		// __op: mix of inserts and updates
		if i == opts.NumRows-1 && rng.IntN(3) == 0 {
			bb.Field(4).(*array.Uint8Builder).Append(2) // delete
		} else if i > 0 && rng.IntN(4) == 0 {
			bb.Field(4).(*array.Uint8Builder).Append(1) // update
		} else {
			bb.Field(4).(*array.Uint8Builder).Append(0) // insert
		}

		// __pos: monotonically increasing within batch (§3.3 precondition)
		pos := fmt.Sprintf("pos-%04d", i)
		bb.Field(5).(*array.StringBuilder).Append(pos)

		// __commit_ts
		ts := time.Date(2026, 1, 1, 0, 0, 0, i*1000, time.UTC)
		bb.Field(6).(*array.TimestampBuilder).AppendTime(ts)
	}

	rec := bb.NewRecordBatch()
	bb.Release()

	// Watermark = __pos of last row
	lastIdx := opts.NumRows - 1
	posArr := rec.Column(5).(*array.String)
	watermark := []byte(posArr.Value(lastIdx))

	return &Batch{
		Table:     "test_table",
		Record:    rec,
		Watermark: watermark,
	}
}

// --- Named adversarial scenarios ---

// AdversarialCompositeKey produces a batch where naive key concatenation
// would collide: ("ab","c") vs ("a","bc").
func AdversarialCompositeKey(seed int64) *Batch {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk1", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pk2", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	// Two rows whose naive concat would be identical: "ab"+"c" = "a"+"bc"
	bb.Field(0).(*array.StringBuilder).Append("ab")
	bb.Field(1).(*array.StringBuilder).Append("c")
	bb.Field(2).(*array.StringBuilder).Append("row1")
	bb.Field(3).(*array.Uint8Builder).Append(0) // insert
	bb.Field(4).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.StringBuilder).Append("a")
	bb.Field(1).(*array.StringBuilder).Append("bc")
	bb.Field(2).(*array.StringBuilder).Append("row2")
	bb.Field(3).(*array.Uint8Builder).Append(0) // insert
	bb.Field(4).(*array.StringBuilder).Append("pos-0002")

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_composite_key", Record: rec, Watermark: []byte("pos-0002")}
}

// AdversarialDeleteLast produces a batch whose last row is a DELETE.
func AdversarialDeleteLast(seed int64) *Batch {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(0) // insert
	bb.Field(2).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(2) // delete
	bb.Field(2).(*array.StringBuilder).Append("pos-0002")

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_delete_last", Record: rec, Watermark: []byte("pos-0002")}
}

// AdversarialInsertAfterDelete produces a batch where a row is inserted
// after being deleted in the same batch.
func AdversarialInsertAfterDelete(seed int64) *Batch {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(2) // delete
	bb.Field(2).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.Uint8Builder).Append(0) // insert (reinsert after delete)
	bb.Field(2).(*array.StringBuilder).Append("pos-0002")

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_insert_after_delete", Record: rec, Watermark: []byte("pos-0002")}
}

// AdversarialInt64Overflow produces a batch with int64 values > 2^53
// that would be corrupted by JSON float64 (CR-021 regression test).
func AdversarialInt64Overflow(seed int64) *Batch {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "big", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	big := int64(1) << 53 // 2^53 — exact boundary
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

// AdversarialNullBefore produces a batch with null __before_* payload
// columns (Kleene/null semantics test).
func AdversarialNullBefore(seed int64) *Batch {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.StringBuilder).AppendNull() // null val
	bb.Field(2).(*array.Uint8Builder).Append(1)     // update
	bb.Field(3).(*array.StringBuilder).Append("pos-0001")

	bb.Field(0).(*array.Int64Builder).Append(2)
	bb.Field(1).(*array.StringBuilder).Append("hello")
	bb.Field(2).(*array.Uint8Builder).Append(0) // insert
	bb.Field(3).(*array.StringBuilder).Append("pos-0002")

	rec := bb.NewRecordBatch()
	bb.Release()
	return &Batch{Table: "adv_null_before", Record: rec, Watermark: []byte("pos-0002")}
}

// EncodeKey encodes a composite key as length-prefixed binary concat
// of the string representations — used by collapse to avoid hash
// collisions (CR-069 §3.2).
func EncodeKey(record arrow.RecordBatch, row int, pkCols []string) []byte {
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
			continue
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
			val = fmt.Sprintf("x:%d", row)
		}
		// Length-prefix to prevent ("ab","c") == ("a","bc") collision
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(val)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, val...)
	}
	return buf
}
