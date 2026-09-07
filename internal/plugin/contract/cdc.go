package contract

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// Op values for the CDC change record.
const (
	OpInsert = "c"
	OpUpdate = "u"
	OpDelete = "d"
)

// CDCRecordSchema returns the fixed schema of a CDC change record (CONTRACT §8.1).
// Columns: op(0), before(1), after(2), offset(3), ts_source(4).
// tableFields defines the columns of the source table for before/after structs.
func CDCRecordSchema(tableFields []arrow.Field) *arrow.Schema {
	beforeAfter := arrow.StructOf(tableFields...)
	return arrow.NewSchema([]arrow.Field{
		{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "before", Type: beforeAfter, Nullable: true},
		{Name: "after", Type: beforeAfter, Nullable: true},
		{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
		{Name: "ts_source", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
}

// ValidateOp checks that op is one of the three allowed values.
func ValidateOp(op string) error {
	switch op {
	case OpInsert, OpUpdate, OpDelete:
		return nil
	default:
		return fmt.Errorf("invalid op %q: must be %q, %q, or %q", op, OpInsert, OpUpdate, OpDelete)
	}
}
