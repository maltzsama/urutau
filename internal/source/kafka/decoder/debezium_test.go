package decoder

import (
	"testing"
	"time"

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
	if c.After["id"] != float64(1) {
		t.Errorf("after.id = %v, want 1", c.After["id"])
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
	// Envelope source takes precedence when available.
	if changes[0].Table != "shop.users" {
		t.Errorf("table = %q, want shop.users", changes[0].Table)
	}
}
