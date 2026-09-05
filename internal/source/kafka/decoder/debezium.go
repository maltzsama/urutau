// Package decoder implements the debezium-json decoder for Kafka CDC
// messages. The wire format is the Debezium JSON envelope: a top-level
// object with "op" (c/u/d/r/t), "before", "after", "source" (with
// "ts_ms", "db", "table"), and "transaction" fields. Only c (create),
// u (update), and d (delete) map to pipeline changes; others are skipped.
package decoder

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/change"
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
func (d *DebeziumJSON) Decode(record *kgo.Record) ([]change.Change, error) {
	var env debeziumEnvelope
	if err := json.Unmarshal(record.Value, &env); err != nil {
		return nil, fmt.Errorf("debezium-json: unmarshal: %w", err)
	}

	var op change.Op
	switch env.Op {
	case "c", "r":
		op = change.OpInsert
	case "u":
		op = change.OpUpdate
	case "d":
		op = change.OpDelete
	default:
		return nil, nil // skip non-CDC operations (t, etc.)
	}

	table := d.resolveTable(record.Key, env)
	target := env.Source.DB + "." + env.Source.Table
	if table != "" {
		target = table
	}

	commitTS := time.UnixMilli(env.TimestampMs)
	ingestTS := time.Now()

	var after map[string]any
	if env.After != nil {
		if err := json.Unmarshal(*env.After, &after); err != nil {
			return nil, fmt.Errorf("debezium-json: unmarshal after: %w", err)
		}
	}

	var before map[string]any
	if env.Before != nil {
		if err := json.Unmarshal(*env.Before, &before); err != nil {
			return nil, fmt.Errorf("debezium-json: unmarshal before: %w", err)
		}
	}

	// Build key from the after image (create/update) or before image (delete).
	keyImage := after
	if op == change.OpDelete {
		keyImage = before
	}

	pos := &position{
		topic:  record.Topic,
		part:   record.Partition,
		offset: record.Offset,
	}

	c := change.Change{
		Op:       op,
		Table:    target,
		After:    after,
		Before:   before,
		CommitTS: commitTS,
		IngestTS: ingestTS,
	}

	// Extract key from after/before using the primary key columns from
	// the envelope — debezium puts them in the key record. For now, use
	// all values from the key image as the change key.
	if keyImage != nil {
		c.Key = extractKey(keyImage)
	}

	_ = pos // stored in the change's position field via the reader
	return []change.Change{c}, nil
}

func (d *DebeziumJSON) resolveTable(_ []byte, env debeziumEnvelope) string {
	// Use the envelope's source table if available.
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
func OrderKey(c *change.Change, pk []string) {
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

// position is a lightweight offset carrier for the decoder. The reader
// wraps it in a proper position.Offsets.
type position struct {
	topic  string
	part   int32
	offset int64
}
