package couchbase

import (
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/sink/rowmeta"
	"github.com/maltzsama/urutau/internal/transport"
)

// reservedField is the document field the sink owns for its metadata
// sub-object. A data column (or metadata destination) with this name would
// collide with every pipeline's metadata, so it is rejected at ensure time —
// the ClickHouse reserved-columns rule, one name instead of three.
const reservedField = "_urutau"

// buildDoc renders one upserted change as the document body: data fields at
// the top level, pipeline metadata under the reserved "_urutau" sub-object.
// Couchbase is schemaless, but the resolved schema is still the walk order
// and the type oracle — a KindUUID lands as a hyphenated string rather than
// raw base64, a KindDecimal keeps its canonical text form, and everything
// else serializes to its natural JSON.
// buildDoc renders one upserted row as the document body: data fields at
// the top level, pipeline metadata under the reserved "_urutau" sub-object.
// The row is read column-oriented from the wire record (BatchReader) — no
// rowchange intermediate.
func (p *tablePlan) buildDoc(r *transport.BatchReader, i int) (map[string]any, map[string]any, error) {
	data := make(map[string]any, len(p.schema.Columns))
	meta := make(map[string]any, len(p.meta))
	row := rowmeta.Of(r, i)
	for _, col := range p.schema.Columns {
		if m, ok := p.meta[col.Name]; ok {
			v, err := rowmeta.Value(m.From, row, p.sourceTable)
			if err != nil {
				return nil, nil, fmt.Errorf("metadata %q: %w", col.Name, err)
			}
			jv, err := jsonValue(v)
			if err != nil {
				return nil, nil, fmt.Errorf("metadata %q: %w", col.Name, err)
			}
			meta[col.Name] = jv
			continue
		}
		ct, hasCast := p.cast.Target(col.Name)
		var from core.Kind
		if hasCast {
			// A cast column must be present on the wire: a missing column
			// would silently bypass the matrix and write the raw value.
			var kindOK bool
			from, kindOK = r.ColumnKind(col.Name)
			if !kindOK {
				return nil, nil, fmt.Errorf("couchbase: column %q: kind not found in wire schema — cast cannot be applied", col.Name)
			}
		}
		v, present := r.Value(col.Name, i)
		if !present {
			if hasCast {
				// ColumnKind said the column is on the wire; a divergence
				// here is a bug, not a missing optional column.
				return nil, nil, fmt.Errorf("couchbase: column %q: kind present but value absent from the wire", col.Name)
			}
			continue
		}
		if hasCast {
			cv, err := ct.Convert(from, v)
			if err != nil {
				return nil, nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
			v = cv
		}
		jv, err := jsonValueKind(col.Type.Kind, v)
		if err != nil {
			return nil, nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		data[col.Name] = jv
	}
	return data, meta, nil
}

// jsonValue converts a canonical Go value into its JSON document form.
// Nested composites recurse; leaves use their natural JSON encoding.
// KindUUID columns arrive as string after the cast converts them; raw
// []byte values are left for encoding/json to base64-encode.
func jsonValue(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, time.Time:
		return v, nil
	case []byte:
		// Binary data serializes to base64 via encoding/json. The cast
		// system already converts KindUUID to hyphenated string; raw
		// []byte of length 16 is NOT assumed to be a UUID.
		return v, nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			ev, err := jsonValue(e)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", k, err)
			}
			out[k] = ev
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			ev, err := jsonValue(e)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			out[i] = ev
		}
		return out, nil
	default:
		return nil, fmt.Errorf("couchbase: unsupported value type %T", v)
	}
}

// couchbaseDateLayout is the canonical date text form.
const couchbaseDateLayout = "2006-01-02"

// jsonValueKind converts a canonical value into its JSON document form using
// the column's kind for the values whose wire form is ambiguous on its own: a
// Date arrives as int32 days, a Time as int64 micros and a UUID as 16 raw
// bytes — encoding those as a bare number/base64 corrupts the document
// (issue #484). Everything else falls back to jsonValue's natural encoding.
func jsonValueKind(kind core.Kind, v any) (any, error) {
	switch kind {
	case core.KindDate:
		if days, ok := core.AsInt64(v); ok {
			return time.Unix(days*86400, 0).UTC().Format(couchbaseDateLayout), nil
		}
	case core.KindTime:
		if micros, ok := core.AsInt64(v); ok {
			return microsOfDayText(micros)
		}
	case core.KindUUID:
		switch t := v.(type) {
		case string:
			return t, nil
		case []byte:
			return uuidText(t)
		}
	}
	return jsonValue(v)
}

// microsOfDayText renders micros-since-midnight as canonical time text,
// matching core.castToString's KindTime output.
func microsOfDayText(micros int64) (string, error) {
	if micros < 0 || micros >= int64(24*time.Hour/time.Microsecond) {
		return "", fmt.Errorf("time-of-day %d micros out of range", micros)
	}
	ns := micros * 1000
	h := ns / int64(time.Hour)
	ns -= h * int64(time.Hour)
	m := ns / int64(time.Minute)
	ns -= m * int64(time.Minute)
	s := ns / int64(time.Second)
	ns -= s * int64(time.Second)
	if ns == 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s), nil
	}
	return fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, s, ns/1000), nil
}

// uuidText renders 16 raw bytes as the canonical hyphenated UUID text.
func uuidText(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("uuid bytes must be 16 long, got %d", len(b))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
