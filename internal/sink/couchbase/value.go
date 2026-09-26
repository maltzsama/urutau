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
		jv, err := jsonValue(v)
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
	case bool, string, int32, int64, float32, float64, time.Time:
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
