package transport

import (
	"bytes"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// Past its reserved size a text buffer grows by a quarter, not to the next
// power of two as Arrow's own Append does (review of #454).
func TestAppendBytesGrowsByAQuarterPastTheReservation(t *testing.T) {
	enc, err := NewRowEncoder(core.Schema{Columns: []core.Column{
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Release()
	enc.ReservePerRow(100, []int{1300}) // ~136,500 bytes: doubling would reach 262,144
	bb := bytesBuilder(enc.bld.Field(0))
	reserved := bb.DataCap()
	cell := bytes.Repeat([]byte("x"), 1300)
	for bb.DataLen()+len(cell) <= reserved {
		enc.AppendBytes(0, cell)
		enc.EndRow(RowMeta{Op: rowchange.OpInsert})
	}
	enc.AppendBytes(0, cell) // past the reservation
	enc.EndRow(RowMeta{Op: rowchange.OpInsert})
	grown := bb.DataCap()
	t.Logf("reserved %d, grew to %d (%.2fx)", reserved, grown, float64(grown)/float64(reserved))
	if grown < bb.DataLen() || grown > reserved*13/10 {
		t.Fatalf("data capacity %d -> %d for %d bytes; want a quarter's growth", reserved, grown, bb.DataLen())
	}
}
