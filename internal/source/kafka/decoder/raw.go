// Raw lands the message envelope with the payload opaque by default: the
// record value is carried as a string, no JSON parsing, no schema inference.
// This is the bronze-landing decoder — the point is to NOT understand the
// content, so payloads that are not even valid JSON land verbatim. The
// transport envelope (topic, partition, offset, timestamp, key, headers) is
// attached by the reader, not here.
//
// One Reader consumes every topic its spec tables name through a single
// shared decoder, so extraction is declared per topic: ByTopic switches a
// topic on when it has an entry, off (fully opaque) when it does not. Once a
// topic has an entry, its payload MUST parse as JSON — the two modes (byte
// passthrough vs. field extraction) are mutually exclusive per topic, since
// an unparseable payload extracting to all-NULL columns would look identical
// to a parseable one whose fields happen to be absent.
package decoder

import (
	"bytes"
	"encoding/json"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/internal/rowchange"
)

// Raw is the passthrough decoder. Every message is an insert — raw landing
// has no update or delete semantics, the log is the data. A tombstone
// (nil value) becomes a row with a NULL payload, which is the faithful
// record of the fact.
type Raw struct {
	// ByTopic declares field extraction per topic. A topic absent from the
	// map (including when ByTopic itself is nil, the zero value) stays fully
	// opaque: one "payload" column, the raw bytes, unconditionally.
	ByTopic map[string]TopicExtraction
	// Miss is called for every declared field a message does not carry — see
	// MissFn. Optional.
	Miss MissFn
}

func (d *Raw) Decode(r *kgo.Record) ([]rowchange.Change, error) {
	spec, ok := d.ByTopic[r.Topic]
	if !ok {
		return []rowchange.Change{{
			Op:    rowchange.OpInsert,
			After: map[string]any{"payload": rawValue(r.Value)},
		}}, nil
	}

	after, err := d.extract(r.Value, spec)
	if err != nil {
		return nil, err
	}
	return []rowchange.Change{{Op: rowchange.OpInsert, After: after}}, nil
}

// extract parses the payload as JSON and projects it onto the declared
// fields. A tombstone (nil value) lands every declared column as NULL — the
// same "no payload, no fields" — rather than failing.
func (d *Raw) extract(value []byte, spec TopicExtraction) (map[string]any, error) {
	if value == nil {
		return Extract(map[string]any{}, spec.Fields, d.Miss)
	}

	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, &ErrNotJSON{Err: err}
	}

	out, err := Extract(doc, spec.Fields, d.Miss)
	if err != nil {
		return nil, err
	}
	if spec.KeepPayload {
		out["payload"] = string(value)
	}
	return out, nil
}

// ErrNotJSON marks a payload that could not be parsed as JSON when field
// extraction is declared. This is a hard failure, not a per-field miss: a
// payload extraction was declared for is presumed to be JSON, and one that
// is not means the topic does not hold what the spec says it does.
type ErrNotJSON struct{ Err error }

func (e *ErrNotJSON) Error() string {
	return "raw: field extraction is declared but the payload is not valid JSON: " + e.Err.Error()
}

func (e *ErrNotJSON) Unwrap() error { return e.Err }

// rawValue carries a nil value as a NULL payload (tombstone) rather than an
// empty string: the absence of a payload is a fact in its own right.
func rawValue(v []byte) any {
	if v == nil {
		return nil
	}
	return string(v)
}
