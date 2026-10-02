// Package decoder implements the debezium-json decoder for Kafka CDC
// messages. The wire format is the Debezium JSON envelope: a top-level
// object with "op" (c/u/d/r/t), "before", "after", "source" (with
// "ts_ms", "db", "table"), and "transaction" fields. Only c (create),
// u (update), and d (delete) map to pipeline changes; others are skipped.
package decoder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/twmb/franz-go/pkg/kgo"
)

// debeziumEnvelope is the top-level structure of a Debezium JSON message.
type debeziumEnvelope struct {
	Op          string           `json:"op"`
	Before      *json.RawMessage `json:"before"`
	After       *json.RawMessage `json:"after"`
	Source      debeziumSource   `json:"source"`
	TimestampMs int64            `json:"ts_ms"`
}

type debeziumSource struct {
	TsMs  int64  `json:"ts_ms"`
	DB    string `json:"db"`
	Table string `json:"table"`
}

// DebeziumJSON decodes Kafka records in the Debezium JSON envelope format.
type DebeziumJSON struct {
	// TopicToTable maps a Kafka topic name to the pipeline target table.
	// If empty, the decoder uses the source.db + source.table from the
	// envelope.
	TopicToTable map[string]string
}

// Decode implements Decoder.
func (d *DebeziumJSON) Decode(record *kgo.Record) ([]rowchange.Change, error) {
	if len(record.Value) == 0 {
		return nil, nil // tombstone (null value): no row change
	}
	value := record.Value
	// A Kafka Connect JsonConverter with schemas.enable=true (the default)
	// wraps the envelope as {"schema":…,"payload":{…}}. Unwrap it, or the op
	// is empty and every record is silently skipped.
	var root map[string]json.RawMessage
	if err := json.Unmarshal(value, &root); err != nil {
		return nil, fmt.Errorf("debezium-json: unmarshal: %w", err)
	}
	if payload, ok := root["payload"]; ok {
		value = payload
	}

	var env debeziumEnvelope
	if err := decodeJSON(value, &env); err != nil {
		return nil, fmt.Errorf("debezium-json: unmarshal: %w", err)
	}

	var op rowchange.Op
	switch env.Op {
	case "c", "r":
		op = rowchange.OpInsert
	case "u":
		op = rowchange.OpUpdate
	case "d":
		op = rowchange.OpDelete
	case "t", "m":
		return nil, nil // truncate / message: no row change
	default:
		return nil, &ErrNotEnvelope{Op: env.Op}
	}

	table := d.resolveTable(record.Topic, env)
	target := env.Source.DB + "." + env.Source.Table
	if table != "" {
		target = table
	}

	// source.ts_ms is the origin commit time; ts_ms is when Debezium
	// processed the event. Prefer the origin, fall back to processing.
	tsMs := env.Source.TsMs
	if tsMs == 0 {
		tsMs = env.TimestampMs
	}
	commitTS := time.UnixMilli(tsMs)
	ingestTS := time.Now()

	after, err := decodeObject(env.After)
	if err != nil {
		return nil, fmt.Errorf("debezium-json: unmarshal after: %w", err)
	}
	before, err := decodeObject(env.Before)
	if err != nil {
		return nil, fmt.Errorf("debezium-json: unmarshal before: %w", err)
	}

	// Build key from the after image (create/update) or before image (delete).
	keyImage := after
	if op == rowchange.OpDelete {
		keyImage = before
	}

	c := rowchange.Change{
		Op:       op,
		Table:    target,
		After:    after,
		Before:   before,
		CommitTS: commitTS,
		IngestTS: ingestTS,
	}
	if keyImage != nil {
		c.Key = extractKey(keyImage)
	}
	return []rowchange.Change{c}, nil
}

// ErrNotEnvelope marks a message that is not a Debezium envelope — an empty op
// or one this decoder does not know. It is fatal: the topic's format is an
// assertion, and silently skipping every record is total data loss.
type ErrNotEnvelope struct{ Op string }

func (e *ErrNotEnvelope) Error() string {
	if e.Op == "" {
		return "debezium-json: message has no op — not a Debezium envelope"
	}
	return fmt.Sprintf("debezium-json: unknown op %q", e.Op)
}

// decodeJSON decodes one JSON document with json.Number preserved, so a bigint
// above 2^53 does not lose precision through float64.
func decodeJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

// decodeObject decodes a row image, normalizing json.Number to int64/float64.
func decodeObject(raw *json.RawMessage) (map[string]any, error) {
	if raw == nil || len(*raw) == 0 || string(*raw) == "null" {
		return nil, nil
	}
	var m map[string]any
	if err := decodeJSON(*raw, &m); err != nil {
		return nil, err
	}
	normalizeNumbers(m)
	return m, nil
}

// normalizeNumbers converts json.Number values to int64 (or float64 when not
// integral) recursively, preserving bigint precision.
func normalizeNumbers(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	case map[string]any:
		for k, vv := range t {
			t[k] = normalizeNumbers(vv)
		}
		return t
	case []any:
		for i, vv := range t {
			t[i] = normalizeNumbers(vv)
		}
		return t
	default:
		return v
	}
}

func (d *DebeziumJSON) resolveTable(topic string, env debeziumEnvelope) string {
	// Check the TopicToTable map first — allows explicit topic → target
	// mapping when the envelope's source table doesn't match the pipeline.
	if t, ok := d.TopicToTable[topic]; ok {
		return t
	}
	// Fall back to the envelope's source table if available.
	if env.Source.Table != "" {
		return env.Source.DB + "." + env.Source.Table
	}
	return ""
}

// extractKey pulls the primary key values from a debezium key or value
// object. The key image is a JSON object, and encoding/json into
// map[string]any discards field order — so the values here come out in map
// iteration order. Callers that need positional meaning (composite primary
// keys) must rebuild the tuple with OrderKey against the declared key
// columns.
func extractKey(m map[string]any) []any {
	key := make([]any, 0, len(m))
	for _, v := range m {
		key = append(key, v)
	}
	return key
}

// OrderKey rebuilds a change's key tuple in primary-key order, reading the
// values from the row image (after; before for deletes). Downstream
// consumers treat the key as positional — per-key collapse joins values
// into a map key, and the sink's equality deletes index the tuple by
// primary-key column — so a shuffled tuple duplicates rows and deletes the
// wrong ones.
func OrderKey(c *rowchange.Change, pk []string) {
	if c == nil || len(pk) == 0 {
		return
	}
	src := c.After
	if src == nil {
		src = c.Before
	}
	if src == nil {
		return
	}
	key := make([]any, len(pk))
	for i, col := range pk {
		key[i] = src[col]
	}
	c.Key = key
}
