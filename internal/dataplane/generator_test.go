package dataplane_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/internal/dataplane"
)

// opCol extracts the __op column as []uint8 from a batch.
func opCol(b *dataplane.Batch) []uint8 {
	if b == nil || b.Record == nil {
		return nil
	}
	for i := range b.Record.Schema().NumFields() {
		if b.Record.Schema().Field(i).Name == "__op" {
			col := b.Record.Column(i).(*array.Uint8)
			vals := make([]uint8, col.Len())
			for j := range col.Len() {
				vals[j] = col.Value(j)
			}
			return vals
		}
	}
	return nil
}

// fieldIndex finds a column index by name. Panics if not found (test only).
func fieldIndex(t *testing.T, b *dataplane.Batch, name string) int {
	t.Helper()
	for i := range b.Record.Schema().NumFields() {
		if b.Record.Schema().Field(i).Name == name {
			return i
		}
	}
	t.Fatalf("column %q not found", name)
	return -1
}

func TestGeneratorWatermarkIsLastRowPos(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 20, Allocator: alloc})
	defer b.Release()

	posIdx := fieldIndex(t, b, "__pos")
	posArr := b.Record.Column(posIdx).(*array.String)
	lastPos := posArr.Value(int(b.Record.NumRows() - 1))
	if string(b.Watermark) != lastPos {
		t.Errorf("Watermark = %q, want __pos of last row %q", b.Watermark, lastPos)
	}
}

func TestGeneratorSchemaCoherent(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 5, Allocator: alloc})
	defer b.Release()

	schema := b.Record.Schema()
	if schema == nil {
		t.Fatal("nil schema")
	}

	required := []string{"id", "val", "__before_val", "amount", "active", "__op", "__pos", "__commit_ts"}
	for _, name := range required {
		found := false
		for i := range schema.NumFields() {
			if schema.Field(i).Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing required column %q", name)
		}
	}
}

func TestGeneratorPKNotNull(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{
		NumRows:       10,
		IncludeNullPK: false,
		Allocator:     alloc,
	})
	defer b.Release()

	idCol := b.Record.Column(0).(*array.Int64)
	for i := range int(idCol.Len()) {
		if idCol.IsNull(i) {
			t.Errorf("row %d: PK is null (IncludeNullPK=false)", i)
		}
	}
}

func TestGeneratorPosMonotonic(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(99, dataplane.GeneratorOpts{NumRows: 30, Allocator: alloc})
	defer b.Release()

	posIdx := fieldIndex(t, b, "__pos")
	posArr := b.Record.Column(posIdx).(*array.String)
	for i := 1; i < int(posArr.Len()); i++ {
		if posArr.Value(i) <= posArr.Value(i-1) {
			t.Errorf("__pos not monotonic: row %d=%q > row %d=%q",
				i-1, posArr.Value(i-1), i, posArr.Value(i))
		}
	}
}

func TestGeneratorInt64Overflow(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialInt64Overflow(0, alloc)
	defer b.Release()

	idCol := b.Record.Column(0).(*array.Int64)
	bigCol := b.Record.Column(1).(*array.Int64)

	if idCol.Value(0) != 1 || idCol.Value(1) != 2 {
		t.Errorf("unexpected ids: %d, %d", idCol.Value(0), idCol.Value(1))
	}
	if bigCol.Value(0) != (int64(1)<<53)+1 {
		t.Errorf("big[0] = %d, want %d", bigCol.Value(0), (int64(1)<<53)+1)
	}
}

func TestGeneratorDeleteLast(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialDeleteLast(0, alloc)
	defer b.Release()

	opIdx := fieldIndex(t, b, "__op")
	opCol := b.Record.Column(opIdx).(*array.Uint8)
	if opCol.Value(int(b.Record.NumRows()-1)) != 2 {
		t.Error("last row should be DELETE (op=2)")
	}
}

func TestGeneratorInsertAfterDelete(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialInsertAfterDelete(0, alloc)
	defer b.Release()

	opIdx := fieldIndex(t, b, "__op")
	opCol := b.Record.Column(opIdx).(*array.Uint8)
	if opCol.Value(0) != 2 {
		t.Error("row 0 should be DELETE")
	}
	if opCol.Value(1) != 0 {
		t.Error("row 1 should be INSERT (reinsert after delete)")
	}
}

func TestGeneratorCompositeKeyDistinct(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialCompositeKey(0, alloc)
	defer b.Release()

	pk1 := b.Record.Column(0).(*array.String)
	pk2 := b.Record.Column(1).(*array.String)

	naive0 := pk1.Value(0) + pk2.Value(0)
	naive1 := pk1.Value(1) + pk2.Value(1)
	if naive0 != naive1 {
		t.Skip("naive concat doesn't collide — adversarial case not triggered")
	}

	if pk1.Value(0) == pk1.Value(1) && pk2.Value(0) == pk2.Value(1) {
		t.Error("composite key tuples must be distinct")
	}
}

func TestGeneratorNullBefore(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialNullBefore(0, alloc)
	defer b.Release()

	valCol := b.Record.Column(1).(*array.String)
	if !valCol.IsNull(0) {
		t.Error("row 0 val should be null")
	}
	if valCol.Value(1) != "hello" {
		t.Errorf("row 1 val = %q, want hello", valCol.Value(1))
	}

	// __before_val should be present in the schema
	beforeIdx := fieldIndex(t, b, "__before_val")
	if beforeIdx < 0 {
		t.Fatal("__before_val column not found in schema")
	}
}

func TestEncodeKeyDistinct(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialCompositeKey(0, alloc)
	defer b.Release()

	key0, err := dataplane.EncodeKey(b.Record, 0, []string{"pk1", "pk2"})
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}
	key1, err := dataplane.EncodeKey(b.Record, 1, []string{"pk1", "pk2"})
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	if string(key0) == string(key1) {
		t.Errorf("encoded keys must differ: %x vs %x", key0, key1)
	}
}

func TestGeneratorNullPK(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{
		NumRows:       3,
		IncludeNullPK: true,
		Allocator:     alloc,
	})
	defer b.Release()

	idCol := b.Record.Column(0).(*array.Int64)
	if !idCol.IsNull(0) {
		t.Error("row 0 should have null PK")
	}
	if idCol.IsNull(1) {
		t.Error("row 1 should not have null PK")
	}
}

func TestGeneratorDeletesMidBatch(t *testing.T) {
	found := false
	for seed := range 50 {
		alloc := checkedAlloc(t)
		b := dataplane.GenerateBatch(int64(seed), dataplane.GeneratorOpts{NumRows: 30, Allocator: alloc})
		ops := opCol(b)
		for i := 0; i < len(ops)-1; i++ { // exclude last row
			if ops[i] == 2 {
				found = true
			}
		}
		b.Release()
	}
	if !found {
		t.Error("no mid-batch delete across 50 seeds — hardest collapse case never generated")
	}
}

func TestGeneratorDuplicatePKDomain(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{
		NumRows:   20,
		PKDomain:  5,
		Allocator: alloc,
	})
	defer b.Release()

	idCol := b.Record.Column(0).(*array.Int64)
	seen := make(map[int64]bool)
	for i := range int(idCol.Len()) {
		v := idCol.Value(i)
		if v < 1 || v > 5 {
			t.Errorf("row %d: id=%d outside PKDomain [1,5]", i, v)
		}
		seen[v] = true
	}
	if len(seen) != 5 {
		t.Errorf("expected 5 distinct PKs, got %d", len(seen))
	}
}

func TestGeneratorDefaultPKDomain(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 9, Allocator: alloc})
	defer b.Release()

	// Default PKDomain = max(1, 9/3) = 3 → IDs 1..3 repeating
	idCol := b.Record.Column(0).(*array.Int64)
	seen := make(map[int64]bool)
	for i := range int(idCol.Len()) {
		seen[idCol.Value(i)] = true
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 distinct PKs with default domain, got %d", len(seen))
	}
}

func TestGeneratorBeforeValReal(t *testing.T) {
	alloc := checkedAlloc(t)
	// Use DeletesOnlyAtEnd=true so we get predictable inserts/updates
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{
		NumRows:          10,
		DeletesOnlyAtEnd: true,
		PKDomain:         3, // 3 distinct PKs → updates after first occurrence
		Allocator:        alloc,
	})
	defer b.Release()

	beforeIdx := fieldIndex(t, b, "__before_val")
	idIdx := fieldIndex(t, b, "id")

	idCol := b.Record.Column(idIdx).(*array.Int64)
	beforeCol := b.Record.Column(beforeIdx).(*array.String)
	opArr := opCol(b)

	// Track seen PKs — first occurrence of each PK must have null __before_val
	seenPKs := make(map[int64]bool)
	nrows := int(b.Record.NumRows())
	for i := 0; i < nrows; i++ {
		id := idCol.Value(i)
		op := opArr[i]
		if !seenPKs[id] {
			// First occurrence: __before_val must be null (insert)
			if !beforeCol.IsNull(i) {
				t.Errorf("row %d: first occurrence of PK %d has non-null __before_val %q", i, id, beforeCol.Value(i))
			}
			seenPKs[id] = true
		} else if op != 0 {
			// Subsequent non-insert: __before_val must be non-null
			if beforeCol.IsNull(i) {
				t.Errorf("row %d: update/delete of PK %d has null __before_val", i, id)
			}
		}
	}
}

// T-14: EncodeKey v4 — type-prefixed binary encoding for all Arrow types.
func TestEncodeKeyV4AllTypes(t *testing.T) {
	alloc := checkedAlloc(t)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk_int32", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "pk_int64", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "pk_uint64", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
		{Name: "pk_float64", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "pk_string", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pk_bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)

	bld := array.NewRecordBuilder(alloc, schema)
	defer bld.Release()
	bld.Field(0).(*array.Int32Builder).Append(42)
	bld.Field(1).(*array.Int64Builder).Append(9007199254740993)
	bld.Field(2).(*array.Uint64Builder).Append(18446744073709551615)
	bld.Field(3).(*array.Float64Builder).Append(3.14)
	bld.Field(4).(*array.StringBuilder).Append("hello")
	bld.Field(5).(*array.BooleanBuilder).Append(true)
	rec := bld.NewRecordBatch()
	defer rec.Release()

	key1, err := dataplane.EncodeKey(rec, 0, []string{"pk_int32", "pk_int64", "pk_uint64", "pk_float64", "pk_string", "pk_bool"})
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	bld2 := array.NewRecordBuilder(alloc, schema)
	defer bld2.Release()
	bld2.Field(0).(*array.Int32Builder).Append(43)
	bld2.Field(1).(*array.Int64Builder).Append(9007199254740994)
	bld2.Field(2).(*array.Uint64Builder).Append(18446744073709551614)
	bld2.Field(3).(*array.Float64Builder).Append(2.71)
	bld2.Field(4).(*array.StringBuilder).Append("world")
	bld2.Field(5).(*array.BooleanBuilder).Append(false)
	rec2 := bld2.NewRecordBatch()
	defer rec2.Release()

	key2, err := dataplane.EncodeKey(rec2, 0, []string{"pk_int32", "pk_int64", "pk_uint64", "pk_float64", "pk_string", "pk_bool"})
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}

	if bytes.Equal(key1, key2) {
		t.Error("different values must produce different keys")
	}

	if key1[0] != 0x01 { // typeInt32
		t.Errorf("first key byte = 0x%02X, want 0x01 (Int32 prefix)", key1[0])
	}
}
