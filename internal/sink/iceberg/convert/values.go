package convert

import (
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/core"
)

// maxExactFloat64 is the largest integer magnitude a float64 represents
// exactly (2^53). Beyond it, an int64→float64 conversion rounds silently.
const maxExactFloat64 = int64(1) << 53

// AppendColumn appends values to an Arrow builder, converting each to the
// field's target type and rejecting a value that does not fit.
func AppendColumn(builder array.Builder, field arrow.Field, values []any) error {
	switch builder.(type) {
	case *array.StructBuilder, *array.ListBuilder, *array.MapBuilder:
		for _, v := range values {
			if err := AppendOneComposite(builder, field, v); err != nil {
				return err
			}
		}
		return nil
	}
	name := field.Name
	switch b := builder.(type) {
	case *array.Int64Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case int64:
				b.Append(t)
			case int:
				b.Append(int64(t))
			case int32:
				b.Append(int64(t))
			case float64:
				if t != float64(int64(t)) {
					return fmt.Errorf("iceberg: column %q: cannot append %v as int64", name, v)
				}
				b.Append(int64(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as int64", name, v)
			}
		}
	case *array.Int32Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case int32:
				b.Append(t)
			case int:
				if t < math.MinInt32 || t > math.MaxInt32 {
					return fmt.Errorf("iceberg: column %q: %v overflows int32", name, t)
				}
				b.Append(int32(t))
			case int64:
				if t < math.MinInt32 || t > math.MaxInt32 {
					return fmt.Errorf("iceberg: column %q: %v overflows int32", name, t)
				}
				b.Append(int32(t))
			case float64:
				if t != float64(int64(t)) || t < math.MinInt32 || t > math.MaxInt32 {
					return fmt.Errorf("iceberg: column %q: cannot append %v as int32", name, v)
				}
				b.Append(int32(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as int32", name, v)
			}
		}
	case *array.Float32Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case float32:
				b.Append(t)
			case float64:
				b.Append(float32(t))
			case int64:
				b.Append(float32(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as float32", name, v)
			}
		}
	case *array.StringBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				b.Append(t)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as string", name, v)
			}
		}
	case *array.Float64Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case float64:
				b.Append(t)
			case float32:
				b.Append(float64(t))
			case int64:
				if t > maxExactFloat64 || t < -maxExactFloat64 {
					return fmt.Errorf("iceberg: column %q: %v loses precision as float64", name, t)
				}
				b.Append(float64(t))
			case int:
				if int64(t) > maxExactFloat64 || int64(t) < -maxExactFloat64 {
					return fmt.Errorf("iceberg: column %q: %v loses precision as float64", name, t)
				}
				b.Append(float64(t))
			case int32:
				b.Append(float64(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as double", name, v)
			}
		}
	case *array.BooleanBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case bool:
				b.Append(t)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as boolean", name, v)
			}
		}
	case *array.Decimal128Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				if err := b.AppendValueFromString(t); err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as decimal", name, v)
			}
		}
	case *array.Date32Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				days, err := dateToDays(t)
				if err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
				b.Append(arrow.Date32(days))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as date", name, v)
			}
		}
	case *array.Time64Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				micros, err := timeToMicros(t)
				if err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
				b.Append(arrow.Time64(micros))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as time", name, v)
			}
		}
	case *array.TimestampBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case time.Time:
				b.AppendTime(t)
			case string:
				tm, err := core.ParseTimestampText(t)
				if err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
				b.AppendTime(tm)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as timestamp", name, v)
			}
		}
	case *array.FixedSizeBinaryBuilder:
		want := b.Type().(*arrow.FixedSizeBinaryType).ByteWidth
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case []byte:
				// Fixed-size binary column (iceberg fixed(L)); uuid also
				// travels as raw bytes here. Append panics on a wrong length,
				// so check it and fail with a real error.
				if len(t) != want {
					return fmt.Errorf("iceberg: column %q: fixed-size binary value has %d bytes, want %d", name, len(t), want)
				}
				b.Append(t)
			case string:
				raw, err := core.ParseUUIDBytes(t)
				if err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
				b.Append(raw)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as fixed-size binary", name, v)
			}
		}
	case *array.BinaryBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case []byte:
				b.Append(t)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as binary", name, v)
			}
		}
	default:
		return fmt.Errorf("iceberg: column %q: unsupported builder %T", name, builder)
	}
	return nil
}

// dateToDays parses a "2006-01-02" date into days since the epoch.
func dateToDays(s string) (int32, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return 0, fmt.Errorf("date %q: %w", s, err)
	}
	return int32(t.Unix() / 86400), nil
}

var timeLayouts = []string{"15:04:05.999999999", "15:04:05.999999", "15:04:05"}

// timeToMicros parses a time-of-day text into microseconds since midnight.
func timeToMicros(s string) (int64, error) {
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return int64(t.Hour())*3600_000_000 + int64(t.Minute())*60_000_000 +
				int64(t.Second())*1_000_000 + int64(t.Nanosecond())/1000, nil
		}
	}
	return 0, fmt.Errorf("time %q: not a valid time of day", s)
}
