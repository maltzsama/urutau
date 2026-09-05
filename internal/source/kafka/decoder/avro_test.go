package decoder

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/change"
)

// fakeRegistry serves schemas by id and counts fetches.
type fakeRegistry struct {
	mu      sync.Mutex
	schemas map[int]avro.Schema
	fetches map[int]int
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{schemas: map[int]avro.Schema{}, fetches: map[int]int{}}
}

func (r *fakeRegistry) add(id int, json string) {
	s, err := avro.Parse(json)
	if err != nil {
		panic(err)
	}
	r.schemas[id] = s
}

func (r *fakeRegistry) Get(_ context.Context, id int) (avro.Schema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fetches[id]++
	if s, ok := r.schemas[id]; ok {
		return s, nil
	}
	return nil, &ErrUnknownSchema{ID: id}
}

func (r *fakeRegistry) fetchCount(id int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fetches[id]
}

const orderAvroSchema = `{"type":"record","name":"order","fields":[
	{"name":"id","type":"long"},
	{"name":"cust","type":{"type":"record","name":"cust","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"long"}
	]}},
	{"name":"tags","type":{"type":"array","items":"string"}},
	{"name":"note","type":["null","string"]}
]}`

// confluentValue wraps an avro payload with the Confluent wire header.
func confluentValue(id int, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = 0x00
	binary.BigEndian.PutUint32(out[1:5], uint32(id))
	copy(out[5:], payload)
	return out
}

// encode encodes a Go value under a schema into avro binary.
func encode(t *testing.T, schemaJSON string, v any) []byte {
	t.Helper()
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var buf bytes.Buffer
	enc := avro.NewEncoderForSchema(s, &buf)
	if err := enc.Encode(v); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return buf.Bytes()
}

// A Confluent-Avro nested record decodes into the map/[] shape the sink
// composite writer consumes.
func TestAvroDecodeNestedRecord(t *testing.T) {
	reg := newFakeRegistry()
	reg.add(1, orderAvroSchema)
	d := NewAvroDecoder(reg)

	payload := encode(t, orderAvroSchema, map[string]any{
		"id":   int64(1),
		"cust": map[string]any{"name": "ana", "age": int64(30)},
		"tags": []any{"a", "b"},
		"note": nil,
	})
	rec := &kgo.Record{Value: confluentValue(1, payload)}

	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(changes) != 1 || changes[0].Op != change.OpInsert {
		t.Fatalf("changes = %+v, want one insert", changes)
	}
	after := changes[0].After
	if after["id"] != int64(1) {
		t.Fatalf("id = %v (%T), want int64 1", after["id"], after["id"])
	}
	cust, ok := after["cust"].(map[string]any)
	if !ok || cust["name"] != "ana" {
		t.Fatalf("cust = %#v, want nested map with name=ana", after["cust"])
	}
	if tags, ok := after["tags"].([]any); !ok || len(tags) != 2 {
		t.Fatalf("tags = %#v, want []any of 2", after["tags"])
	}
}

// The schema is cached per id: the registry is hit once even across decodes.
func TestAvroRegistryCachesByID(t *testing.T) {
	reg := newFakeRegistry()
	reg.add(7, orderAvroSchema)
	d := NewAvroDecoder(reg)

	payload := encode(t, orderAvroSchema, map[string]any{
		"id":   int64(1),
		"cust": map[string]any{"name": "ana", "age": int64(1)},
		"tags": []any{"x"},
		"note": nil,
	})
	for i := 0; i < 3; i++ {
		if _, err := d.Decode(&kgo.Record{Value: confluentValue(7, payload)}); err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
	}
	if got := reg.fetchCount(7); got != 1 {
		t.Fatalf("registry fetches = %d, want 1 (schema is immutable per id)", got)
	}
}

// A value without the Confluent header is a distinct, non-retryable error.
func TestAvroDecodeBadWireFormat(t *testing.T) {
	d := NewAvroDecoder(newFakeRegistry())
	_, err := d.Decode(&kgo.Record{Value: []byte("no header here")})
	if _, ok := err.(*ErrBadWireFormat); !ok {
		t.Fatalf("err = %v, want ErrBadWireFormat", err)
	}
}

// A schema id the registry does not know surfaces as ErrUnknownSchema.
func TestAvroDecodeUnknownSchemaID(t *testing.T) {
	d := NewAvroDecoder(newFakeRegistry())
	payload := encode(t, orderAvroSchema, map[string]any{
		"id":   int64(1),
		"cust": map[string]any{"name": "ana", "age": int64(1)},
		"tags": []any{},
		"note": nil,
	})
	_, err := d.Decode(&kgo.Record{Value: confluentValue(99, payload)})
	if _, ok := err.(*ErrUnknownSchema); !ok {
		t.Fatalf("err = %v, want ErrUnknownSchema", err)
	}
}
