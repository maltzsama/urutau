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
	"fmt"
	"io"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
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
		out := make(map[string]any, len(spec.Fields)+1)
		for _, field := range spec.Fields {
			out[field.Name] = nil
		}
		if spec.KeepPayload {
			out["payload"] = nil
		}
		return out, nil
	}

	doc, err := parseJSONDoc(value)
	if err != nil {
		return nil, err
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

// parseJSONDoc decodes one JSON object with json.Number preserved. A payload
// ending exactly at a buffer boundary leaves trailing bytes only in the
// reader, so both the decoder's buffer and the remainder are checked, or
// `{"a":1}EXTRA` would slip through (issue #507).
func parseJSONDoc(value []byte) (map[string]any, error) {
	var doc map[string]any
	r := bytes.NewReader(value)
	dec := json.NewDecoder(r)
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, &ErrNotJSON{Err: err}
	}
	remaining, _ := io.ReadAll(io.MultiReader(dec.Buffered(), r))
	if len(bytes.TrimLeft(remaining, " \t\n\r")) > 0 {
		return nil, &ErrNotJSON{Err: fmt.Errorf("raw: trailing data after JSON document")}
	}
	return doc, nil
}

// DecodeInto implements ColumnarDecoder: the raw payload is written straight
// into the target's Arrow builders, with no rowchange.Change between. An
// opaque topic lands its single "payload" column; an extracting topic projects
// the declared fields. Every message is an insert — raw landing has no update
// or delete semantics.
func (d *Raw) DecodeInto(rec *kgo.Record, e Encoder, position string) (int, error) {
	enc, mapped, err := e("")
	if err != nil {
		return 0, err
	}
	if !mapped {
		return 0, nil
	}
	if enc == nil {
		return 0, ErrShapeDrift
	}
	meta := transport.RowMeta{Op: rowchange.OpInsert, Position: position}

	spec, extracting := d.ByTopic[rec.Topic]
	if !extracting {
		// Opaque passthrough: the raw bytes are the "payload" column.
		col, ok := enc.Column("payload")
		if !ok {
			return 0, ErrShapeDrift
		}
		filled := make([]bool, len(enc.Schema().Columns))
		if rec.Value == nil {
			enc.AppendNull(col)
		} else {
			enc.AppendBytes(col, rec.Value)
		}
		filled[col] = true
		fillNulls(enc, filled)
		enc.EndRow(meta)
		return 1, nil
	}

	// Extraction: a tombstone lands every declared column (and payload) NULL.
	if rec.Value == nil {
		appendAllNull(enc)
		enc.EndRow(meta)
		return 1, nil
	}
	doc, err := parseJSONDoc(rec.Value)
	if err != nil {
		return 0, err
	}
	// Resolve every destination before appending anything, so a record the
	// direct path declines never leaves a partial row in the encoder.
	payloadCol := -1
	if spec.KeepPayload {
		col, ok := enc.Column("payload")
		if !ok {
			return 0, ErrShapeDrift
		}
		payloadCol = col
	}
	filled := make([]bool, len(enc.Schema().Columns))
	if err := appendProjected(enc, spec.Fields, doc, d.Miss, filled); err != nil {
		return 0, err
	}
	if payloadCol >= 0 {
		enc.AppendBytes(payloadCol, rec.Value)
		filled[payloadCol] = true
	}
	fillNulls(enc, filled)
	enc.EndRow(meta)
	return 1, nil
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
