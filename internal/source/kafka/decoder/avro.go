package decoder

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/change"
)

// Avro decodes Confluent-Avro messages: a 1-byte magic (0x00), a 4-byte
// big-endian schema id, then the binary Avro payload. The schema is resolved
// from the registry by id and cached (schemas are immutable per id) and the
// payload is decoded into the canonical value shape — records become
// map[string]any, arrays []any, scalars their Go types — which the sink's
// composite writer consumes directly. Every message is an insert: the record
// IS the row. (A Debezium-Avro envelope, where the message wraps
// before/after, is a future envelope layer on top.)
type Avro struct {
	registry SchemaRegistry
	cache    sync.Map // int -> avro.Schema
}

// NewAvroDecoder builds an Avro decoder over a schema registry.
func NewAvroDecoder(registry SchemaRegistry) *Avro {
	return &Avro{registry: registry}
}

// ErrBadWireFormat marks a value that does not carry the Confluent wire
// header — the topic is not Confluent-Avro, or the value is corrupt.
type ErrBadWireFormat struct{}

func (e *ErrBadWireFormat) Error() string {
	return "avro: message is missing the Confluent wire header (magic 0x00 + 4-byte schema id)"
}

func (d *Avro) Decode(rec *kgo.Record) ([]change.Change, error) {
	v := rec.Value
	if len(v) < 5 || v[0] != 0x00 {
		return nil, &ErrBadWireFormat{}
	}
	id := int(binary.BigEndian.Uint32(v[1:5]))
	payload := v[5:]

	var schema avro.Schema
	if cached, ok := d.cache.Load(id); ok {
		schema = cached.(avro.Schema)
	} else {
		s, err := d.registry.Get(rec.Context, id)
		if err != nil {
			return nil, err
		}
		d.cache.Store(id, s)
		schema = s
	}

	var after map[string]any
	if err := avro.Unmarshal(schema, payload, &after); err != nil {
		return nil, fmt.Errorf("avro: decode payload (schema id %d): %w", id, err)
	}
	return []change.Change{{
		Op:    change.OpInsert,
		After: after,
	}}, nil
}
