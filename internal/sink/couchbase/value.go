package couchbase

import (
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
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
	row := rowMetaOf(r, i)
	for _, col := range p.schema.Columns {
		if m, ok := p.meta[col.Name]; ok {
			v, err := metaValue(m.From, row, p.sourceTable)
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
		v, ok := r.Value(col.Name, i)
		if !ok {
			continue
		}
		if ct, ok := p.cast.Target(col.Name); ok {
			cv, err := ct.Convert(v)
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

// rowMeta is the per-row metadata view metaValue needs, read straight from
// the wire record. Transport-envelope fields (stream/shard/headers) are nil
// on the wire path: their fallback semantics are unchanged.
type rowMeta struct {
	Op         rowchange.Op
	Position   string
	CommitTS   time.Time
	IngestTS   time.Time
	Snapshot   bool
	EnrichMiss bool
}

func rowMetaOf(r *transport.BatchReader, i int) rowMeta {
	commitTS, _ := r.CommitTS(i)
	ingestTS, _ := r.IngestTS(i)
	return rowMeta{
		Op:       r.Op(i),
		Position: r.Position(i),
		CommitTS: commitTS,
		IngestTS: ingestTS,
		Snapshot: r.Snapshot(i),
	}
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

// metaValue resolves one metadata key to its concrete value for a rowchange.
// Mirrors the ClickHouse and Iceberg projections — same keys, same nil
// semantics. Time values stay time.Time: encoding/json renders RFC3339.
func metaValue(key core.MetadataKey, c rowMeta, sourceTable string) (any, error) {
	switch key {
	case core.MetaOp:
		return c.Op.String(), nil
	case core.MetaCommitTS:
		if c.CommitTS.IsZero() {
			return nil, nil
		}
		return c.CommitTS, nil
	case core.MetaIngestTS:
		if c.IngestTS.IsZero() {
			return nil, nil
		}
		return c.IngestTS, nil
	case core.MetaPosition:
		if c.Position == "" {
			return nil, nil
		}
		return c.Position, nil
	case core.MetaSourceTable:
		return sourceTable, nil
	case core.MetaPhase:
		if c.Snapshot {
			return "snapshot", nil
		}
		return "stream", nil
	case core.MetaStream:
		// Wire path: no transport envelope; the source table IS the stream.
		return sourceTable, nil
	case core.MetaShard:
		return nil, nil
	case core.MetaSeq:
		if c.Position == "" {
			return nil, nil
		}
		return c.Position, nil // CDC: the event coordinate (GTID/LSN)
	case core.MetaMsgTS:
		return nil, nil
	case core.MetaMsgKey:
		return nil, nil
	case core.MetaHeaders:
		return nil, nil
	case core.MetaEnrichMiss:
		if c.EnrichMiss {
			return true, nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown metadata key %q", key)
	}
}
