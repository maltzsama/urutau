package flightserver

import (
	"strings"
	"testing"
	"time"

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
		case *array.TimestampBuilder:
			for _, v := range vals {
				b.Append(v.(arrow.Timestamp))
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

// ts_source is decoded with the unit the record's own schema declares, not a
// fixed one. Arrow's Type.Name() is "timestamp" for every unit, so a
// name-only check passes a millisecond or nanosecond column straight through
// to a microsecond decode — which does not fail, it silently lands the event
// decades or millennia away from its real instant.
func TestPluginRecordToWireHonorsTimestampUnit(t *testing.T) {
	instant := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name  string
		unit  arrow.TimeUnit
		ticks arrow.Timestamp
	}{
		{"microseconds (what internal/plugin/sink.go writes)", arrow.Microsecond, arrow.Timestamp(instant.UnixMicro())},
		{"milliseconds", arrow.Millisecond, arrow.Timestamp(instant.UnixMilli())},
		{"nanoseconds", arrow.Nanosecond, arrow.Timestamp(instant.UnixNano())},
		{"seconds", arrow.Second, arrow.Timestamp(instant.Unix())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := wireSchema(
				arrow.Field{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "ts_source", Type: &arrow.TimestampType{Unit: tc.unit, TimeZone: "UTC"}, Nullable: true},
			)
			rec := buildRecord(t, schema, map[string][]any{
				"op":        {"u"},
				"offset":    {[]byte("1-1")},
				"v":         {"a"},
				"ts_source": {tc.ticks},
			})

			wire, err := pluginRecordToWire(rec, memory.DefaultAllocator)
			if err != nil {
				t.Fatalf("pluginRecordToWire = %v, want nil", err)
			}
			if wire == nil || wire.NumRows() != 1 {
				t.Fatalf("wire record = %v rows, want 1", wire)
			}
			defer wire.Release()

			// commit_ts is the first timestamp column on the wire (encode.go
			// lays out data columns, then op, position, commit_ts, ingest_ts).
			got, ok := firstTimestamp(t, wire)
			if !ok {
				t.Fatal("wire record carries no timestamp column")
			}
			if !got.Equal(instant) {
				t.Errorf("CommitTS = %v, want %v — the column's declared unit (%v) must drive the decode",
					got.Format(time.RFC3339Nano), instant.Format(time.RFC3339Nano), tc.unit)
			}
		})
	}
}

// firstTimestamp returns the first non-null value of the wire record's first
// timestamp column — commit_ts, per internal/transport/encode.go's layout.
func firstTimestamp(t *testing.T, rec arrow.RecordBatch) (time.Time, bool) {
	t.Helper()
	for i, f := range rec.Schema().Fields() {
		tst, ok := f.Type.(*arrow.TimestampType)
		if !ok {
			continue
		}
		col, ok := rec.Column(i).(*array.Timestamp)
		if !ok || col.Len() == 0 || col.IsNull(0) {
			continue
		}

		return col.Value(0).ToTime(tst.Unit).UTC(), true
	}

	return time.Time{}, false
}
