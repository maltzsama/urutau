package enrich

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/spec"
)

// A destination declared as string (what AddColumns does, matching the sink
// schema) must stay string whether the reference is cold or hot: a cold utf8
// column and a hot int64 one made the worker's pending concat fail, killing
// the worker (issue #564).
func TestColdAndHotDestColumnsShareTheDeclaredType(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) { c.Select = []string{"name", "tier"} })
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	decl := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64, Nullable: true}},
			{Name: "q", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "users.name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "users.tier", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}
	join := func() *dpint.Batch {
		t.Helper()
		rec, encErr := transport.RecordFromChanges([]rowchange.Change{searchEvent(1, int64(1))}, decl, nil)
		if encErr != nil {
			t.Fatalf("encode: %v", encErr)
		}
		in := &dpint.Batch{Table: "events", Record: rec, Mode: dataplane.UpsertMode}
		out, jErr := s.ColumnarJoin(context.Background(), in)
		in.Release()
		if jErr != nil {
			t.Fatalf("ColumnarJoin: %v", jErr)
		}
		return out
	}

	cold := join()
	if cold == nil {
		t.Fatal("cold join dropped the row")
	}
	assertDestType(t, cold, "users.tier", arrow.BinaryTypes.String)
	cold.Release()

	// Load the reference (tier is int64 there): the hot dest must still be a
	// string column, and the value its canonical rendering.
	loader := fakeRows(t, []map[string]any{{"id": int64(1), "name": "ana", "tier": int64(3)}})
	if err := s.UseLoader("users", loader); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !s.refs[0].isHot() {
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() {
		t.Fatal("reference did not go hot")
	}

	hot := join()
	if hot == nil {
		t.Fatal("hot join dropped the row")
	}
	defer hot.Release()
	assertDestType(t, hot, "users.tier", arrow.BinaryTypes.String)
	rows, derr := transport.DecodeBatch(hot.Record, "events", []string{"id"})
	if derr != nil {
		t.Fatalf("decode: %v", derr)
	}
	if rows[0].After["users.tier"] != "3" {
		t.Fatalf("users.tier = %#v, want \"3\"", rows[0].After["users.tier"])
	}
}

func assertDestType(t *testing.T, b *dpint.Batch, col string, want arrow.DataType) {
	t.Helper()
	for i := 0; i < b.Record.Schema().NumFields(); i++ {
		if f := b.Record.Schema().Field(i); f.Name == col {
			if !arrow.TypeEqual(f.Type, want) {
				t.Fatalf("dest %s type = %s, want %s", col, f.Type, want)
			}
			return
		}
	}
	t.Fatalf("dest %s not in the output", col)
}
