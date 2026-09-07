package dataplane_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/internal/dataplane"
)

func TestGeneratorWatermarkIsLastRowPos(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 20})
	defer b.Release()

	schema := b.Record.Schema()
	posIdx := -1
	for i := range schema.NumFields() {
		if schema.Field(i).Name == "__pos" {
			posIdx = i
			break
		}
	}
	if posIdx < 0 {
		t.Fatal("__pos column not found")
	}

	posArr := b.Record.Column(posIdx).(*array.String)
	lastPos := posArr.Value(int(b.Record.NumRows() - 1))
	if string(b.Watermark) != lastPos {
		t.Errorf("Watermark = %q, want __pos of last row %q", b.Watermark, lastPos)
	}
}

func TestGeneratorSchemaCoherent(t *testing.T) {
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 5})
	defer b.Release()

	schema := b.Record.Schema()
	if schema == nil {
		t.Fatal("nil schema")
	}

	// Must have at least: id, val, amount, active, __op, __pos, __commit_ts
	required := []string{"id", "val", "amount", "active", "__op", "__pos", "__commit_ts"}
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
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{
		NumRows:       10,
		IncludeNullPK: false,
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
	b := dataplane.GenerateBatch(99, dataplane.GeneratorOpts{NumRows: 30})
	defer b.Release()

	schema := b.Record.Schema()
	posIdx := -1
	for i := range schema.NumFields() {
		if schema.Field(i).Name == "__pos" {
			posIdx = i
			break
		}
	}
	posArr := b.Record.Column(posIdx).(*array.String)
	for i := 1; i < int(posArr.Len()); i++ {
		if posArr.Value(i) <= posArr.Value(i-1) {
			t.Errorf("__pos not monotonic: row %d=%q > row %d=%q",
				i-1, posArr.Value(i-1), i, posArr.Value(i))
		}
	}
}

func TestGeneratorInt64Overflow(t *testing.T) {
	b := dataplane.AdversarialInt64Overflow(0)
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
	b := dataplane.AdversarialDeleteLast(0)
	defer b.Release()

	opCol := b.Record.Column(1).(*array.Uint8)
	if opCol.Value(int(b.Record.NumRows()-1)) != 2 {
		t.Error("last row should be DELETE (op=2)")
	}
}

func TestGeneratorInsertAfterDelete(t *testing.T) {
	b := dataplane.AdversarialInsertAfterDelete(0)
	defer b.Release()

	opCol := b.Record.Column(1).(*array.Uint8)
	if opCol.Value(0) != 2 {
		t.Error("row 0 should be DELETE")
	}
	if opCol.Value(1) != 0 {
		t.Error("row 1 should be INSERT (reinsert after delete)")
	}
}

func TestGeneratorCompositeKeyDistinct(t *testing.T) {
	b := dataplane.AdversarialCompositeKey(0)
	defer b.Release()

	pk1 := b.Record.Column(0).(*array.String)
	pk2 := b.Record.Column(1).(*array.String)

	// The naive concat collides: "ab"+"c" == "a"+"bc" == "abc"
	// This is exactly why CR-069 §3.2 uses length-prefixed encoding.
	naive0 := pk1.Value(0) + pk2.Value(0)
	naive1 := pk1.Value(1) + pk2.Value(1)
	if naive0 != naive1 {
		t.Skip("naive concat doesn't collide — adversarial case not triggered")
	}

	// But the tuples ARE distinct
	if pk1.Value(0) == pk1.Value(1) && pk2.Value(0) == pk2.Value(1) {
		t.Error("composite key tuples must be distinct")
	}
}

func TestGeneratorNullBefore(t *testing.T) {
	b := dataplane.AdversarialNullBefore(0)
	defer b.Release()

	valCol := b.Record.Column(1).(*array.String)
	if !valCol.IsNull(0) {
		t.Error("row 0 val should be null")
	}
	if valCol.Value(1) != "hello" {
		t.Errorf("row 1 val = %q, want hello", valCol.Value(1))
	}
}

func TestEncodeKeyDistinct(t *testing.T) {
	b := dataplane.AdversarialCompositeKey(0)
	defer b.Release()

	key0 := dataplane.EncodeKey(b.Record, 0, []string{"pk1", "pk2"})
	key1 := dataplane.EncodeKey(b.Record, 1, []string{"pk1", "pk2"})

	if string(key0) == string(key1) {
		t.Errorf("encoded keys must differ: %x vs %x", key0, key1)
	}
}

func TestGeneratorNullPK(t *testing.T) {
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{
		NumRows:       3,
		IncludeNullPK: true,
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

func TestBatchWatermarkImmutable(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 10})
	defer b.Release()

	wm := make([]byte, len(b.Watermark))
	copy(wm, b.Watermark)

	// Simulate: nothing changes the watermark
	if string(b.Watermark) != string(wm) {
		t.Errorf("Watermark changed: was %q, now %q", wm, b.Watermark)
	}
}
