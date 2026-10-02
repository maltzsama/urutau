package decoder

import (
	"errors"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDebeziumJSONCreate(t *testing.T) {
	d := &DebeziumJSON{}
	msg := []byte(`{
		"op": "c",
		"before": null,
		"after": {"id": 1, "name": "alice"},
		"source": {"ts_ms": 1700000000000, "db": "shop", "table": "users"},
		"ts_ms": 1700000000000
	}`)
	rec := &kgo.Record{Topic: "db.shop.users", Value: msg}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	c := changes[0]
	if c.Op.String() != "insert" {
		t.Errorf("op = %q, want insert", c.Op)
	}
	if c.Table != "shop.users" {
		t.Errorf("table = %q, want shop.users", c.Table)
	}
	if c.After["id"] != int64(1) {
		t.Errorf("after.id = %v (%T), want int64(1)", c.After["id"], c.After["id"])
	}
	if c.After["name"] != "alice" {
		t.Errorf("after.name = %v, want alice", c.After["name"])
	}
	if c.CommitTS.Before(time.UnixMilli(1700000000000)) || c.CommitTS.After(time.UnixMilli(1700000000001)) {
		t.Errorf("commitTS = %v, want ~1700000000000", c.CommitTS)
	}
}

func TestDebeziumJSONUpdate(t *testing.T) {
	d := &DebeziumJSON{}
	msg := []byte(`{
		"op": "u",
		"before": {"id": 1, "name": "alice"},
		"after": {"id": 1, "name": "bob"},
		"source": {"ts_ms": 1700000001000, "db": "shop", "table": "users"},
		"ts_ms": 1700000001000
	}`)
	rec := &kgo.Record{Topic: "db.shop.users", Value: msg}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if changes[0].Op.String() != "update" {
		t.Errorf("op = %q, want update", changes[0].Op)
	}
	if changes[0].Before["name"] != "alice" {
		t.Errorf("before.name = %v, want alice", changes[0].Before["name"])
	}
	if changes[0].After["name"] != "bob" {
		t.Errorf("after.name = %v, want bob", changes[0].After["name"])
	}
}

func TestDebeziumJSONDelete(t *testing.T) {
	d := &DebeziumJSON{}
	msg := []byte(`{
		"op": "d",
		"before": {"id": 1, "name": "bob"},
		"after": null,
		"source": {"ts_ms": 1700000002000, "db": "shop", "table": "users"},
		"ts_ms": 1700000002000
	}`)
	rec := &kgo.Record{Topic: "db.shop.users", Value: msg}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if changes[0].Op.String() != "delete" {
		t.Errorf("op = %q, want delete", changes[0].Op)
	}
	if changes[0].After != nil {
		t.Errorf("after = %v, want nil for delete", changes[0].After)
	}
	if changes[0].Before == nil {
		t.Error("before should not be nil for delete")
	}
}

func TestDebeziumJSONSkipsTransactionOp(t *testing.T) {
	d := &DebeziumJSON{}
	msg := []byte(`{"op": "t", "source": {"ts_ms": 0, "db": "", "table": ""}, "ts_ms": 0}`)
	rec := &kgo.Record{Value: msg}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Errorf("got %d changes, want 0 for transaction op", len(changes))
	}
}

func TestDebeziumJSONBadJSON(t *testing.T) {
	d := &DebeziumJSON{}
	rec := &kgo.Record{Value: []byte("not json")}
	_, err := d.Decode(rec)
	if err == nil {
		t.Error("expected error for bad JSON")
	}
}

func TestDebeziumJSONCustomTableMapping(t *testing.T) {
	d := &DebeziumJSON{
		TopicToTable: map[string]string{
			"db.shop.users": "raw.users",
		},
	}
	msg := []byte(`{
		"op": "c",
		"after": {"id": 1},
		"source": {"ts_ms": 0, "db": "shop", "table": "users"},
		"ts_ms": 0
	}`)
	rec := &kgo.Record{Topic: "db.shop.users", Value: msg}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	// The topic → target mapping wins over the envelope's source table.
	if changes[0].Table != "raw.users" {
		t.Errorf("table = %q, want raw.users", changes[0].Table)
	}
}

// The raw key tuple inherits JSON object disorder. OrderKey must rebuild it
// in the declared primary-key order: downstream consumers treat the key as
// positional (collapse map keys, equality-delete tuples).
func TestOrderKeyCompositeInsert(t *testing.T) {
	c := &rowchange.Change{
		Op:    rowchange.OpInsert,
		After: map[string]any{"tenant": "t1", "id": float64(7), "v": "x"},
	}
	OrderKey(c, []string{"tenant", "id"})
	if c.Key[0] != "t1" || c.Key[1] != float64(7) {
		t.Fatalf("key = %v, want [t1 7]", c.Key)
	}
}

func TestOrderKeyDeleteReadsBefore(t *testing.T) {
	c := &rowchange.Change{
		Op:     rowchange.OpDelete,
		Before: map[string]any{"tenant": "t1", "id": float64(7)},
	}
	OrderKey(c, []string{"tenant", "id"})
	if c.Key[0] != "t1" || c.Key[1] != float64(7) {
		t.Fatalf("key = %v, want [t1 7] from the before image", c.Key)
	}
}

func TestOrderKeyNoopCases(t *testing.T) {
	// No declared PK: leave the tuple alone.
	c := &rowchange.Change{After: map[string]any{"id": float64(1)}, Key: []any{"raw"}}
	OrderKey(c, nil)
	if c.Key[0] != "raw" {
		t.Fatalf("key = %v, want untouched", c.Key)
	}
	// No row image: nothing to read from.
	d := &rowchange.Change{Op: rowchange.OpDelete, Key: []any{"raw"}}
	OrderKey(d, []string{"id"})
	if d.Key[0] != "raw" {
		t.Fatalf("key = %v, want untouched", d.Key)
	}
	// Nil change must not panic.
	OrderKey(nil, []string{"id"})
}

// A Kafka Connect JsonConverter with schemas.enable=true wraps the envelope in
// {"schema":…,"payload":…}; the decoder must unwrap it instead of seeing an
// empty op and skipping every record.
func TestDebeziumJSONUnwrapsConnectEnvelope(t *testing.T) {
	d := &DebeziumJSON{}
	msg := []byte(`{
		"schema": {"type": "struct"},
		"payload": {
			"op": "c",
			"after": {"id": 1},
			"source": {"ts_ms": 1700000000000, "db": "shop", "table": "users"},
			"ts_ms": 1700000000000
		}
	}`)
	changes, err := d.Decode(&kgo.Record{Topic: "db.shop.users", Value: msg})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(changes) != 1 || changes[0].Op != rowchange.OpInsert {
		t.Fatalf("changes = %+v, want one insert", changes)
	}
}

// A tombstone (null value) is a delete with no payload: no error, no change.
func TestDebeziumJSONTombstoneIsQuiet(t *testing.T) {
	d := &DebeziumJSON{}
	changes, err := d.Decode(&kgo.Record{Topic: "db.shop.users", Value: nil})
	if err != nil || changes != nil {
		t.Fatalf("tombstone: changes=%v err=%v, want nil,nil", changes, err)
	}
}

// A bigint above 2^53 must keep its exact value, not round through float64.
func TestDebeziumJSONBigintPrecision(t *testing.T) {
	d := &DebeziumJSON{}
	msg := []byte(`{"op":"c","after":{"id":9007199254740993},"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
	changes, err := d.Decode(&kgo.Record{Topic: "s.t", Value: msg})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := changes[0].After["id"]; got != int64(9007199254740993) {
		t.Fatalf("id = %v (%T), want int64(9007199254740993)", got, got)
	}
}

// An empty or unknown op is not a Debezium envelope: fatal, never a silent skip.
func TestDebeziumJSONUnknownOpIsFatal(t *testing.T) {
	d := &DebeziumJSON{}
	for _, msg := range []string{
		`{"after":{"id":1},"source":{"db":"s","table":"t"}}`,
		`{"op":"x","after":{"id":1},"source":{"db":"s","table":"t"}}`,
	} {
		_, err := d.Decode(&kgo.Record{Topic: "s.t", Value: []byte(msg)})
		var ne *ErrNotEnvelope
		if !errors.As(err, &ne) {
			t.Fatalf("msg %s: err = %v, want ErrNotEnvelope", msg, err)
		}
	}
}

// source.ts_ms (origin commit) wins over ts_ms (processing time).
func TestDebeziumJSONUsesSourceTsMs(t *testing.T) {
	d := &DebeziumJSON{}
	msg := []byte(`{"op":"c","after":{"id":1},"source":{"ts_ms":1000,"db":"s","table":"t"},"ts_ms":2000}`)
	changes, err := d.Decode(&kgo.Record{Topic: "s.t", Value: msg})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := changes[0].CommitTS; !got.Equal(time.UnixMilli(1000)) {
		t.Fatalf("commitTS = %v, want source.ts_ms 1000", got)
	}
}

// An empty (non-null) value is not a tombstone: it must not be skipped as a
// quiet no-op.
func TestDebeziumJSONEmptyValueIsFatal(t *testing.T) {
	d := &DebeziumJSON{}
	_, err := d.Decode(&kgo.Record{Topic: "s.t", Value: []byte{}})
	var ne *ErrNotEnvelope
	if !errors.As(err, &ne) {
		t.Fatalf("empty value err = %v, want ErrNotEnvelope", err)
	}
}

// A present source.ts_ms of zero is a legitimate origin time, not "absent".
func TestDebeziumJSONZeroSourceTsIsPresent(t *testing.T) {
	d := &DebeziumJSON{}
	msg := []byte(`{"op":"c","after":{"id":1},"source":{"ts_ms":0,"db":"s","table":"t"},"ts_ms":2000}`)
	changes, err := d.Decode(&kgo.Record{Topic: "s.t", Value: msg})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := changes[0].CommitTS; !got.Equal(time.UnixMilli(0)) {
		t.Fatalf("commitTS = %v, want source.ts_ms 0", got)
	}
}
