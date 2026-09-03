// Package decoder defines the Kafka message decoding contract. A decoder
// takes a raw Kafka record (from any wire format: debezium-json, avro,
// protobuf, …) and produces the pipeline's canonical Change events.
package decoder

import (
	"github.com/maltzsama/urutau/internal/change"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Decoder turns a single Kafka record into zero or more pipeline changes.
// The decoder is responsible for mapping the source message's operation
// type, key, and payload into the Change struct. A single record may
// produce multiple changes (e.g. a debezium outbox envelope wrapping a
// batch).
type Decoder interface {
	Decode(record *kgo.Record) ([]change.Change, error)
}
