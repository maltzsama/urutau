package dataplane

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
// operation (Insert=0, Update=1, Delete=2). An unknown value is an immediate
// error: it would silently vanish from the split otherwise.
func validateOpColumn(opCol *array.Uint8) error {
	for i := range opCol.Len() {
		switch opCol.Value(i) {
		case OpInsert, OpUpdate, OpDelete:
		default:
			return fmt.Errorf("dataplane: invalid __op %d at row %d", opCol.Value(i), i)
		}
	}
	return nil
}
