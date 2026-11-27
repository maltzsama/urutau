package columnar

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Op values matching the wire schema __op column (CR-021).
const (
	OpInsert = 0
	OpUpdate = 1
	OpDelete = 2
)

// colIndex resolves a column index by name, or -1 when the column is absent.
func colIndex(schema *arrow.Schema, name string) int {
	for i := range schema.NumFields() {
		if schema.Field(i).Name == name {
			return i
		}
	}
	return -1
}

// ValidateOpNulls rejects any null __op value in rec. __op is NOT NULL by
// wire contract (transport.WireMetadataFields), but Arrow records can still
// carry a null in the column: array.Uint8.Value reads it as 0 (OpInsert), so
// an op consumer that only looks at values silently mis-classifies it.
//
// Call it at every boundary that consumes __op before its own kernels run:
// the worker's commit switch (all write modes — append never calls Collapse)
// and its enrichment seam (whose keep mask starts as is_not_null(__op)).
//
// A record with no __op column, or with a non-Uint8 __op column, is left to
// the transport schema reader, which reports that violation with its own
// error; this function is a no-op for it.
func ValidateOpNulls(rec arrow.RecordBatch) error {
	if rec == nil || rec.NumRows() == 0 {
		return nil
	}
	idx := colIndex(rec.Schema(), "__op")
	if idx < 0 {
		return nil
	}
	opCol, ok := rec.Column(idx).(*array.Uint8)
	if !ok {
		return nil
	}
	for i := range opCol.Len() {
		if opCol.IsNull(i) {
			return errNullOp(i)
		}
	}
	return nil
}

// errNullOp is the single rejection message for a null __op value, shared by
// every boundary that validates the column.
func errNullOp(row int) error {
	return fmt.Errorf("dataplane: null __op at row %d", row)
}

// validateOpColumn checks that every value in the __op column is a known
// operation (Insert=0, Update=1, Delete=2) and that it is not null. Both are
// rejected before the split, but they are not equivalent downstream: an
// unknown non-null value would be selected by the "not delete" mask and
// routed as an upsert, while a null value makes both masks null (the
// comparison propagates the null), so FilterRecordBatch drops the row from
// both the upsert and the delete output.
func validateOpColumn(opCol *array.Uint8) error {
	for i := range opCol.Len() {
		if opCol.IsNull(i) {
			return errNullOp(i)
		}
		switch opCol.Value(i) {
		case OpInsert, OpUpdate, OpDelete:
		default:
			return fmt.Errorf("dataplane: invalid __op %d at row %d", opCol.Value(i), i)
		}
	}
	return nil
}
