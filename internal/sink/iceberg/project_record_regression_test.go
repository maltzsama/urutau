package iceberg

// Regression tests for two defects fixed in project_record.go:
//
//	2. declaring a metadata column panicked projectRecord on an Arrow type mismatch
//	3. scalarValue returned nil (a NULL equality-delete key) for decimal/date/time/float32 PKs

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// msg_ts (timestamp) used to fall through buildMetaColumn to a utf8 null,
// which panics array.NewRecordBatch. It must now project a column whose type
// equals the field's.
func TestMetadataColumnsMatchDeclaredFieldType(t *testing.T) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "msg_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}
	w := &TableWriter{
		dataSchema: arrow.NewSchema(fields, nil),
		metaByName: map[string]core.MetadataColumn{
			"msg_ts": {From: core.MetaMsgTS, As: "msg_ts"},
		},
		cast: core.CastPolicy{},
	}
	b := wireBatch(t, [3]any{int64(1), "x", rowchange.OpUpdate})
	defer b.Release()

	out, err := w.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord: %v", err)
	}
	defer out.Release()
	for i, f := range fields {
		if got := out.Column(i).DataType(); !arrow.TypeEqual(got, f.Type) {
			t.Errorf("column %q type = %s, want %s", f.Name, got, f.Type)
		}
	}
}
