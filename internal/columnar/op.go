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

// validateOpColumn checks that every value in the __op column is a known
// operation (Insert=0, Update=1, Delete=2) and that it is not null. Both are
// rejected before the split: an unknown non-null value would be routed as an
// upsert by the delete mask, and a null value — read by Value as 0 — makes
// both masks null, so the row would vanish from both outputs.
func validateOpColumn(opCol *array.Uint8) error {
	for i := range opCol.Len() {
		if opCol.IsNull(i) {
			return fmt.Errorf("dataplane: null __op at row %d", i)
		}
		switch opCol.Value(i) {
		case OpInsert, OpUpdate, OpDelete:
		default:
			return fmt.Errorf("dataplane: invalid __op %d at row %d", opCol.Value(i), i)
		}
	}
	return nil
}
