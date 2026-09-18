package decoder

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/internal/rowchange"
)

// Avro decodes Confluent-Avro messages: a 1-byte magic (0x00), a 4-byte
// big-endian schema id, then the binary Avro payload. The schema is resolved
// from the registry by id and cached (schemas are immutable per id) and the
// payload is decoded into the canonical value shape — records become
// map[string]any, arrays []any, scalars their Go types — which the sink's
// composite writer consumes directly. Every message is an insert: the record
// IS the row. (A Debezium-Avro envelope, where the message wraps
// before/after, is a future envelope layer on top.)
//
// One Reader consumes every topic its spec tables name through a single
// shared decoder, so field selection is declared per topic: ByTopic
// projects the decoded record onto a subset for a topic that has an entry,
// and keeps every field for one that does not. The registry schema is the
// source of truth for a topic's record shape, and the spec is a projection
// of it, so a column present in the record but not declared is dropped
// rather than tripping schema drift downstream. There is no raw-blob
// equivalent of Raw's KeepPayload here — the pre-decode bytes are Confluent
// wire format (magic byte + schema id + binary), unreadable without the
// registry, so storing them would land a column nobody can query.
type Avro struct {
	registry SchemaRegistry
	cache    sync.Map // int -> avro.Schema

	// ByTopic declares field selection per topic. A topic absent from the
	// map (including when ByTopic itself is nil, the zero value) keeps every
	// field the schema decodes.
	ByTopic map[string]TopicExtraction
	// Miss is called for every declared field a record does not carry (a
	// column declared that the registry schema does not have). Optional.
	Miss MissFn
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

func (d *Avro) Decode(rec *kgo.Record) ([]rowchange.Change, error) {
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

	if spec, ok := d.ByTopic[rec.Topic]; ok && len(spec.Fields) > 0 {
		projected, err := Extract(after, spec.Fields, d.Miss)
		if err != nil {
			return nil, fmt.Errorf("avro: project schema id %d: %w", id, err)
		}
		after = projected
	}

	return []rowchange.Change{{
		Op:    rowchange.OpInsert,
		After: after,
	}}, nil
}
