package enrich

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestAppendTypedRoutesDriverValues — Row 2 of THE TWO ROWS. Every Go type
// database/sql hands back must land in its typed builder with the exact
// value; nil is a NULL cell; a []byte from a TEXT column becomes a string.
func TestAppendTypedRoutesDriverValues(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		a := buildTyped(t, arrow.PrimitiveTypes.Int64, int64(7), int32(9), nil, []byte("11")).(*array.Int64)
		defer a.Release()
		if a.Value(0) != 7 || a.Value(1) != 9 || !a.IsNull(2) || a.Value(3) != 11 {
			t.Fatalf("int64 = %v", a)
		}
	})
	t.Run("string", func(t *testing.T) {
		a := buildTyped(t, arrow.BinaryTypes.String, "ana", []byte("beto"), nil).(*array.String)
		defer a.Release()
		if a.Value(0) != "ana" || a.Value(1) != "beto" || !a.IsNull(2) {
			t.Fatalf("string = %v", a)
		}
	})
	t.Run("float64", func(t *testing.T) {
		a := buildTyped(t, arrow.PrimitiveTypes.Float64, float64(1.5), nil, []byte("2.25")).(*array.Float64)
		defer a.Release()
		if a.Value(0) != 1.5 || !a.IsNull(1) || a.Value(2) != 2.25 {
			t.Fatalf("float64 = %v", a)
		}
	})
	t.Run("bool", func(t *testing.T) {
		a := buildTyped(t, arrow.FixedWidthTypes.Boolean, true, int64(0), nil).(*array.Boolean)
		defer a.Release()
		if !a.Value(0) || a.Value(1) || !a.IsNull(2) {
			t.Fatalf("bool = %v", a)
		}
	})
	t.Run("binary", func(t *testing.T) {
		a := buildTyped(t, arrow.BinaryTypes.Binary, []byte{0xde, 0xad}, nil).(*array.Binary)
		defer a.Release()
		if string(a.Value(0)) != "\xde\xad" || !a.IsNull(1) {
			t.Fatalf("binary = %v", a)
		}
	})
}

func buildTyped(t *testing.T, dt arrow.DataType, in ...any) arrow.Array {
	t.Helper()
	b := array.NewBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	for _, v := range in {
		if err := appendTyped(b, v); err != nil {
			t.Fatalf("appendTyped(%T): %v", v, err)
		}
	}
	return b.NewArray()
}

func TestAppendTypedTimestamp(t *testing.T) {
	b := array.NewBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	defer b.Release()
	tm := time.Date(2024, 3, 5, 12, 0, 0, 0, time.UTC)
	if err := appendTyped(b, tm); err != nil {
		t.Fatalf("appendTyped(time.Time): %v", err)
	}
	if err := appendTyped(b, nil); err != nil {
		t.Fatalf("appendTyped(nil): %v", err)
	}
	arr := b.NewArray().(*array.Timestamp)
	defer arr.Release()
	if arr.Value(0).ToTime(arrow.Microsecond).UTC() != tm {
		t.Fatalf("timestamp = %v, want %v", arr.Value(0).ToTime(arrow.Microsecond).UTC(), tm)
	}
	if !arr.IsNull(1) {
		t.Fatal("nil must be a NULL cell")
	}
}

func TestBuilderType(t *testing.T) {
	// builderType only reads DatabaseTypeName; a nil *sql.ColumnType would
	// panic, so exercise the mapping via a tiny stub is out of scope — the
	// switch is covered structurally by TestAppendTypedRoutesDriverValues
	// (every branch's builder is exercised). This test pins the string map.
	pairs := map[string]arrow.DataType{
		"BIGINT":      arrow.PrimitiveTypes.Int64,
		"INT":         arrow.PrimitiveTypes.Int64,
		"DOUBLE":      arrow.PrimitiveTypes.Float64,
		"DECIMAL":     arrow.PrimitiveTypes.Float64,
		"BOOL":        arrow.FixedWidthTypes.Boolean,
		"DATETIME":    &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
		"BYTEA":       arrow.BinaryTypes.Binary,
		"VARCHAR":     arrow.BinaryTypes.String,
		"TEXT":        arrow.BinaryTypes.String,
		"UUID":        arrow.BinaryTypes.String,
		"WEIRDVENDOR": arrow.BinaryTypes.String,
	}
	for name, want := range pairs {
		got := builderTypeByName(name)
		if !arrow.TypeEqual(got, want) {
			t.Errorf("builderTypeByName(%q) = %s, want %s", name, got, want)
		}
	}
}

func TestHasOrderBy(t *testing.T) {
	yes := []string{
		"SELECT id, name FROM users ORDER BY id",
		"select * from t order by created_at desc",
		"SELECT id FROM t ORDER BY id ;  ",
	}
	no := []string{
		"SELECT id, name FROM users",
		"SELECT id FROM (SELECT id FROM t ORDER BY id) x",
		"SELECT id FROM t GROUP BY id",
	}
	for _, q := range yes {
		if !hasOrderBy(q) {
			t.Errorf("hasOrderBy(%q) = false, want true", q)
		}
	}
	for _, q := range no {
		if hasOrderBy(q) {
			t.Errorf("hasOrderBy(%q) = true, want false", q)
		}
	}
}

func TestAppendOrderBy(t *testing.T) {
	// plain query → suffix
	got := appendOrderBy("SELECT id, name FROM users", "id")
	if !strings.Contains(got, "ORDER BY") || !strings.Contains(got, `"id"`) {
		t.Fatalf("plain: %q", got)
	}
	if strings.Contains(got, "SELECT * FROM (") {
		t.Fatalf("plain query should not be wrapped: %q", got)
	}
	// query ending in a paren or with LIMIT → wrapped subquery
	for _, q := range []string{
		"SELECT id FROM t LIMIT 10",
		"SELECT id FROM (SELECT id FROM base)",
		"SELECT id, count(*) FROM t GROUP BY id",
	} {
		w := appendOrderBy(q, "id")
		if !strings.HasPrefix(w, "SELECT * FROM (") || !strings.HasSuffix(w, `ORDER BY "id"`) {
			t.Fatalf("wrap(%q) = %q", q, w)
		}
	}
}
