// Columnar decoding: the direct-to-Arrow sibling of Decoder (#733). A decoder
// that can write a record's fields straight into a transport.RowEncoder
// implements ColumnarDecoder and the Kafka reader prefers it, so the live path
// never materializes a per-record rowchange.Change or re-encodes one through
// the source puller. The decode cores are shared: the map path stays intact
// for callers that still need []rowchange.Change.
package decoder

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
)

// ColumnarDecoder is the optional direct-to-Arrow sibling of Decoder (#733).
// DecodeInto resolves the record's source key (a Debezium envelope source, or
// "" for a topic-keyed decoder), asks enc for that table's encoder, and
// appends each decoded row straight into its Arrow builders — no per-row
// map[string]any buffered between decode and encode.
//
// It returns the number of rows appended: 0 for a tombstone, a skipped op, or
// an unmapped source. Every row is stamped with its operation, commit time and
// the given position. A record the direct path cannot encode without changing
// behavior (a shape the schema cannot represent, a missing target schema, or a
// delete that needs the map path's key backfill) returns ErrShapeDrift, and the
// caller re-encodes that record on the map path — which reproduces the row
// puller exactly.
type ColumnarDecoder interface {
	DecodeInto(rec *kgo.Record, enc Encoder, position string) (int, error)
}

// Encoder resolves a decoded record's source key to the Arrow encoder for its
// target table, built from that table's canonical schema. It reports whether
// the source maps to a configured target (mapped=false means the record is
// skipped, as the row path skips an unmapped change). A nil encoder with
// mapped=true means the target has no canonical schema; the decoder then
// returns ErrShapeDrift so the caller falls back to the map path, which infers
// the shape from the rows.
type Encoder func(sourceKey string) (enc *transport.RowEncoder, mapped bool, err error)

// ErrShapeDrift marks a record the direct path cannot encode without changing
// behavior. The caller re-encodes it on the map path, which gates and merges
// schema drift, backfills a delete's key and guards a delete with no primary
// key exactly as the row puller did.
var ErrShapeDrift = errors.New("decoder: record needs the map path")

// ErrEncode marks a value a declared column's Arrow builder rejected. It is not
// a decode error — the record parsed — so the caller must fail the run rather
// than skip the record, exactly as the row puller's RecordFromChanges fails it.
var ErrEncode = errors.New("decoder: value cannot be encoded")

// appendImage writes a full decoded row image (a Debezium after/before object)
// into enc's schema columns: a column the image carries gets its value, any
// other (an enrichment column, a column the source omitted) is NULL. An image
// key the schema lacks is drift, returned before any cell is written so a
// fallback cannot leave a partial row.
func appendImage(enc *transport.RowEncoder, image map[string]any) error {
	cols := enc.Schema().Columns
	for name := range image {
		if _, ok := enc.Column(name); !ok {
			return ErrShapeDrift
		}
	}
	// A struct-valued column may carry a field the schema's struct lacks, which
	// only the map path's nested drift check sees. Decline it before any append
	// so the caller re-encodes through the puller, which fails loud.
	for i := range cols {
		if cols[i].Type.Kind != core.KindStruct {
			continue
		}
		if _, isMap := image[cols[i].Name].(map[string]any); isMap {
			return ErrShapeDrift
		}
	}
	for i := range cols {
		v, ok := image[cols[i].Name]
		if !ok {
			enc.AppendNull(i)
			continue
		}
		if err := enc.AppendValue(i, v); err != nil {
			return fmt.Errorf("%w: %v", ErrEncode, err)
		}
	}
	return nil
}

// appendProjected appends a projected image (declared fields read from doc) to
// enc's matching columns and marks each in filled. Every declared field must
// exist in the schema; a miss reports through miss (unless Required, which
// fails). All lookups and validation run before any append, so a record the
// caller then skips (a required miss, a decode error) never leaves a partial
// row in the encoder.
func appendProjected(enc *transport.RowEncoder, fields []Field, doc map[string]any, miss MissFn, filled []bool) error {
	for _, f := range fields {
		if _, ok := enc.Column(f.Name); !ok {
			return ErrShapeDrift
		}
	}
	vals := make([]any, len(fields))
	have := make([]bool, len(fields))
	for i, f := range fields {
		v, ok := lookup(doc, f.path())
		if !ok {
			if f.Required {
				return &ErrFieldMissing{Column: f.Name, Path: f.path()}
			}
			if miss != nil {
				miss(f.Name)
			}
			continue
		}
		vals[i], have[i] = v, true
	}
	// A struct-valued field may carry a nested field the schema lacks; only the
	// map path's nested drift check sees it. Decline before any append.
	cols := enc.Schema().Columns
	for i, f := range fields {
		if !have[i] {
			continue
		}
		col, _ := enc.Column(f.Name)
		if cols[col].Type.Kind != core.KindStruct {
			continue
		}
		if _, isMap := vals[i].(map[string]any); isMap {
			return ErrShapeDrift
		}
	}
	for i, f := range fields {
		col, _ := enc.Column(f.Name)
		if !have[i] {
			enc.AppendNull(col)
		} else if err := enc.AppendValue(col, vals[i]); err != nil {
			return fmt.Errorf("%w: %v", ErrEncode, err)
		}
		filled[col] = true
	}
	return nil
}

// fillNulls writes NULL to every schema column not yet filled for the row.
func fillNulls(enc *transport.RowEncoder, filled []bool) {
	for i := range filled {
		if !filled[i] {
			enc.AppendNull(i)
		}
	}
}

// appendAllNull writes NULL to every schema column.
func appendAllNull(enc *transport.RowEncoder) {
	for i := range enc.Schema().Columns {
		enc.AppendNull(i)
	}
}
