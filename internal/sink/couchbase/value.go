package couchbase

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
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
func (p *tablePlan) buildDoc(c change.Change) (map[string]any, map[string]any, error) {
	data := make(map[string]any, len(p.schema.Columns))
	meta := make(map[string]any, len(p.meta))
	for _, col := range p.schema.Columns {
		if m, ok := p.meta[col.Name]; ok {
			v, err := metaValue(m.From, c, p.sourceTable)
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
		v, ok := c.After[col.Name]
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

// jsonValue converts a canonical Go value into its JSON document form.
// Nested composites recurse; leaves use their natural JSON encoding, with
// two deliberate exceptions: UUIDs render hyphenated (queryable, not
// base64 noise) and []byte leaves inside composites fall to encoding/json's
// base64 (documented).
func jsonValue(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case bool, string, int32, int64, float32, float64, time.Time:
		return v, nil
	case []byte:
		// A bare 16-byte value is a UUID by convention (the canonical
		// KindUUID wire form); longer binaries stay base64 via encoding/json.
		if len(t) == 16 {
			return formatUUID(t), nil
		}
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

// formatUUID renders 16 raw bytes as the canonical hyphenated form — the
// same rendering the Iceberg sink parses back with hex (uuidToBytes there,
// formatting here).
func formatUUID(b []byte) string {
	dst := make([]byte, 36)
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])
	return string(dst)
}

// metaValue resolves one metadata key to its concrete value for a change.
// Mirrors the ClickHouse and Iceberg projections — same keys, same nil
// semantics. Time values stay time.Time: encoding/json renders RFC3339.
func metaValue(key core.MetadataKey, c change.Change, sourceTable string) (any, error) {
	switch key {
	case core.MetaOp:
		return c.Op.String(), nil
	case core.MetaCommitTS:
		if c.CommitTS.IsZero() {
			return nil, nil
		}
		return c.CommitTS, nil
	case core.MetaIngestTS:
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
		if c.Transport != nil && c.Transport.Stream != "" {
			return c.Transport.Stream, nil
		}
		return sourceTable, nil // CDC: the source table IS the stream
	case core.MetaShard:
		if c.Transport == nil || c.Transport.Shard == "" {
			return nil, nil
		}
		return c.Transport.Shard, nil
	case core.MetaSeq:
		if c.Transport != nil && c.Transport.Seq != "" {
			return c.Transport.Seq, nil
		}
		if c.Position == "" {
			return nil, nil
		}
		return c.Position, nil // CDC: the event coordinate (GTID/LSN)
	case core.MetaMsgTS:
		if c.Transport == nil || c.Transport.MsgTS.IsZero() {
			return nil, nil
		}
		return c.Transport.MsgTS, nil
	case core.MetaMsgKey:
		if c.Transport == nil {
			return nil, nil
		}
		return c.Transport.MsgKey, nil
	case core.MetaHeaders:
		if c.Transport == nil || c.Transport.Headers == "" {
			return nil, nil
		}
		return c.Transport.Headers, nil
	case core.MetaEnrichMiss:
		if c.EnrichMiss {
			return true, nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown metadata key %q", key)
	}
}
