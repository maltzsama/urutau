package convert

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// appendColumn must refuse a value the column type cannot hold.
func TestAppendColumnTypeErrors(t *testing.T) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	defer b.Release()
	intField := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64}
	if err := AppendColumn(b.Field(0), intField, []any{"not-a-number"}); err == nil {
		t.Fatal("string into int64 column must be rejected")
	}
	if err := AppendColumn(b.Field(0), intField, []any{float64(1.5)}); err == nil {
		t.Fatal("fractional float into int64 column must be rejected")
	}
	if err := AppendColumn(b.Field(0), intField, []any{nil, int64(1), int32(2)}); err != nil {
		t.Fatalf("valid ints rejected: %v", err)
	}
}

func TestDateToDays(t *testing.T) {
	tests := []struct {
		input string
		want  int32
		err   bool
	}{
		{"1970-01-01", 0, false},
		{"2024-01-02", 19724, false},
		{"not-a-date", 0, true},
	}
	for _, tt := range tests {
		got, err := dateToDays(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("dateToDays(%q) error = %v, wantErr %v", tt.input, err, tt.err)
		}
		if !tt.err && got != tt.want {
			t.Errorf("dateToDays(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestTimeToMicros(t *testing.T) {
	tests := []struct {
		input string
		want  int64
		err   bool
	}{
		{"00:00:00", 0, false},
		{"12:00:00", 12 * 3600_000_000, false},
		{"15:04:05", 15*3600_000_000 + 4*60_000_000 + 5*1_000_000, false},
		{"15:04:05.123456", 15*3600_000_000 + 4*60_000_000 + 5*1_000_000 + 123456, false},
		{"not-a-time", 0, true},
	}
	for _, tt := range tests {
		got, err := timeToMicros(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("timeToMicros(%q) error = %v, wantErr %v", tt.input, err, tt.err)
		}
		if !tt.err && got != tt.want {
			t.Errorf("timeToMicros(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
