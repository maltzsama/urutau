package flightserver

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// buildRecord materializes an in-memory record for the given fields/values so
// pluginRecordToWire can be exercised without a Flight transport.
func buildRecord(t *testing.T, schema *arrow.Schema, values map[string][]any) arrow.RecordBatch {
	t.Helper()
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	t.Cleanup(bld.Release)
	for i, f := range schema.Fields() {
		vals := values[f.Name]
		switch b := bld.Field(i).(type) {
		case *array.StringBuilder:
			for _, v := range vals {
				b.Append(v.(string))
			}
		case *array.BinaryBuilder:
			for _, v := range vals {
				b.Append(v.([]byte))
			}
		case *array.Int32Builder:
			for _, v := range vals {
				b.Append(v.(int32))
			}
		default:
			t.Fatalf("unsupported builder for %q", f.Name)
		}
	}
	rec := bld.NewRecordBatch()
	t.Cleanup(rec.Release)
	return rec
}

func wireSchema(dataFields ...arrow.Field) *arrow.Schema {
	fields := []arrow.Field{
		{Name: "op", Type: arrow.BinaryTypes.String},
		{Name: "offset", Type: arrow.BinaryTypes.Binary},
	}
	fields = append(fields, dataFields...)
	return arrow.NewSchema(fields, nil)
}

// A contract-conforming record (op utf8, offset binary, data utf8) converts
// without error; a skewed record (data column not utf8) must fail with a
// typed error instead of panicking the Flight server goroutine.
func TestPluginRecordToWireValidatesColumnTypes(t *testing.T) {
	okSchema := wireSchema(arrow.Field{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true})
	okRec := buildRecord(t, okSchema, map[string][]any{
		"op":     {"u"},
		"offset": {[]byte("1-1")},
		"v":      {"a"},
	})
	if _, err := pluginRecordToWire(okRec, memory.DefaultAllocator); err != nil {
		t.Fatalf("pluginRecordToWire(conforming) = %v, want nil", err)
	}

	badSchema := wireSchema(arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	badRec := buildRecord(t, badSchema, map[string][]any{
		"op":     {"u"},
		"offset": {[]byte("1-1")},
		"v":      {int32(7)},
	})
	_, err := pluginRecordToWire(badRec, memory.DefaultAllocator)
	if err == nil {
		t.Fatal("pluginRecordToWire(skewed) = nil, want typed error")
	}
	if !strings.Contains(err.Error(), `"v"`) || !strings.Contains(err.Error(), "utf8") {
		t.Fatalf("error = %v, want it to name the column and the wanted type", err)
	}
}
