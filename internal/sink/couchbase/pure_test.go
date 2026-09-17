package couchbase

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
)

func TestJsonValueAllTypes(t *testing.T) {
	cases := []struct {
		name string
		in   any
	}{
		{"nil", nil},
		{"bool", true},
		{"string", "hello"},
		{"int32", int32(42)},
		{"int64", int64(42)},
		{"float32", float32(1.5)},
		{"float64", 3.14},
		{"time", time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := jsonValue(tc.in)
			if err != nil {
				t.Fatalf("jsonValue(%T): %v", tc.in, err)
			}
			// For nil, got must also be nil.
			if tc.in == nil && got != nil {
				t.Errorf("jsonValue(nil) = %v, want nil", got)
			}
		})
	}

	// bytes pass through as-is (no base64 encoding at this stage).
	b := []byte{0xde, 0xad}
	got, err := jsonValue(b)
	if err != nil {
		t.Fatalf("jsonValue(bytes): %v", err)
	}
	if !reflect.DeepEqual(got, b) {
		t.Errorf("jsonValue(bytes) = %v, want %v", got, b)
	}

	// Nested map.
	got, err = jsonValue(map[string]any{"a": int64(1)})
	if err != nil {
		t.Fatalf("jsonValue(map): %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok || m["a"] != int64(1) {
		t.Errorf("jsonValue(map) = %v", got)
	}

	// Nested slice.
	got, err = jsonValue([]any{int64(1), int64(2)})
	if err != nil {
		t.Fatalf("jsonValue(slice): %v", err)
	}
	s, ok := got.([]any)
	if !ok || len(s) != 2 {
		t.Errorf("jsonValue(slice) = %v", got)
	}

	// Unsupported type.
	if _, err := jsonValue(struct{}{}); err == nil {
		t.Error("jsonValue(struct): want error")
	}
}

func TestMetaValueCouchbaseAllKeys(t *testing.T) {
	now := time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC)
	c := rowMeta{
		Op:       1,
		Position: "gtid:1",
		CommitTS: now,
		IngestTS: now,
		Snapshot: false,
		Phase:    "stream",
	}

	cases := []struct {
		key core.MetadataKey
	}{
		{core.MetaOp},
		{core.MetaCommitTS},
		{core.MetaIngestTS},
		{core.MetaPosition},
		{core.MetaSourceTable},
		{core.MetaPhase},
		{core.MetaStream},
		{core.MetaShard},
		{core.MetaSeq},
		{core.MetaMsgTS},
		{core.MetaMsgKey},
		{core.MetaHeaders},
		{core.MetaEnrichMiss},
	}
	for _, tc := range cases {
		_, err := metaValue(tc.key, c, "src.t")
		if err != nil {
			t.Errorf("metaValue(%q): %v", tc.key, err)
		}
	}

	// Unknown key errors.
	if _, err := metaValue("unknown", c, "t"); err == nil {
		t.Error("unknown metadata key: want error")
	}

	// Zero commit TS returns nil.
	zero := rowMeta{CommitTS: time.Time{}}
	got, err := metaValue(core.MetaCommitTS, zero, "t")
	if err != nil {
		t.Fatalf("zero commitTS: %v", err)
	}
	if got != nil {
		t.Errorf("zero commitTS = %v, want nil", got)
	}

	// Empty position returns nil for position and seq.
	empty := rowMeta{}
	got, err = metaValue(core.MetaPosition, empty, "t")
	if err != nil {
		t.Fatalf("empty position: %v", err)
	}
	if got != nil {
		t.Errorf("empty position = %v, want nil", got)
	}
	got, err = metaValue(core.MetaSeq, empty, "t")
	if err != nil {
		t.Fatalf("empty seq: %v", err)
	}
	if got != nil {
		t.Errorf("empty seq = %v, want nil", got)
	}

	// Phase fallback to snapshot.
	snap := rowMeta{Snapshot: true}
	got, err = metaValue(core.MetaPhase, snap, "t")
	if err != nil {
		t.Fatalf("snapshot phase: %v", err)
	}
	if got != core.PhaseSnapshot {
		t.Errorf("snapshot phase = %v, want %v", got, core.PhaseSnapshot)
	}

	// Phase fallback to stream.
	stream := rowMeta{Snapshot: false}
	got, err = metaValue(core.MetaPhase, stream, "t")
	if err != nil {
		t.Fatalf("stream phase: %v", err)
	}
	if got != core.PhaseStream {
		t.Errorf("stream phase = %v, want %v", got, core.PhaseStream)
	}

	// No phase, no snapshot defaults to stream.
	noPhase := rowMeta{}
	got, err = metaValue(core.MetaPhase, noPhase, "t")
	if err != nil {
		t.Fatalf("no phase: %v", err)
	}
	if got != core.PhaseStream {
		t.Errorf("no phase = %v, want %v", got, core.PhaseStream)
	}

	// EnrichMiss true returns true.
	em := rowMeta{EnrichMiss: true}
	got, err = metaValue(core.MetaEnrichMiss, em, "t")
	if err != nil {
		t.Fatalf("enrich miss: %v", err)
	}
	if got != true {
		t.Errorf("enrich miss = %v, want true", got)
	}

	// EnrichMiss false returns nil.
	emFalse := rowMeta{EnrichMiss: false}
	got, err = metaValue(core.MetaEnrichMiss, emFalse, "t")
	if err != nil {
		t.Fatalf("enrich miss false: %v", err)
	}
	if got != nil {
		t.Errorf("enrich miss false = %v, want nil", got)
	}
}

func TestTranslateKVErr(t *testing.T) {
	// nil returns nil.
	if got := translateKVErr(nil); got != nil {
		t.Errorf("translateKVErr(nil) = %v, want nil", got)
	}

	// errNotFound returns a not-found style error.
	got := translateKVErr(errNotFound)
	if got == nil {
		t.Fatal("translateKVErr(errNotFound) = nil")
	}
	if !errors.Is(got, errNotFound) {
		t.Errorf("translateKVErr(errNotFound) should wrap errNotFound")
	}

	// Other errors pass through.
	other := errors.New("some other error")
	got = translateKVErr(other)
	if !errors.Is(got, other) {
		t.Errorf("translateKVErr(other) should wrap the original error")
	}
}

func TestCouchbaseIdent(t *testing.T) {
	s := &Sink{scope: "default"}

	// Qualified scope.collection.
	scope, coll, err := s.ident("analytics.events")
	if err != nil {
		t.Fatalf("ident: %v", err)
	}
	if scope != "analytics" || coll != "events" {
		t.Errorf("ident = %q/%q, want analytics/events", scope, coll)
	}

	// Bare name falls back to default scope.
	scope, coll, err = s.ident("orders")
	if err != nil {
		t.Fatalf("ident bare: %v", err)
	}
	if scope != "default" || coll != "orders" {
		t.Errorf("ident bare = %q/%q, want default/orders", scope, coll)
	}

	// Too many dots.
	_, _, err = s.ident("a.b.c")
	if err == nil {
		t.Error("triple-qualified: want error")
	}
}

func TestValidateSchemaCouchbase(t *testing.T) {
	// Clean schema passes.
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}}
	if err := validateSchema(schema, nil); err != nil {
		t.Errorf("clean schema: %v", err)
	}

	// Reserved data column.
	schema.Columns = append(schema.Columns, core.Column{
		Name: reservedField, Type: core.ColumnType{Kind: core.KindString}},
	)
	if err := validateSchema(schema, nil); err == nil {
		t.Error("reserved data column: want error")
	}

	// Reserved metadata destination.
	schema = core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}}
	if err := validateSchema(schema, []core.MetadataColumn{{From: core.MetaOp, As: reservedField}}); err == nil {
		t.Error("reserved metadata dest: want error")
	}
}

func TestControlWritePreservesExistingProps(t *testing.T) {
	prev := &controlDoc{
		Position:  "old",
		UpdatedAt: time.Now(),
		Properties: map[string]string{
			"existing": "value",
		},
	}
	now := time.Now()
	got := controlWrite(prev, batchInfo{Position: "new"}, now)

	if got.Position != "new" {
		t.Errorf("position = %q, want new", got.Position)
	}
	if got.Properties["existing"] != "value" {
		t.Errorf("existing property lost")
	}
	if got.UpdatedAt != now {
		t.Errorf("UpdatedAt not updated")
	}
}

func TestControlWriteSnapshotState(t *testing.T) {
	got := controlWrite(nil, batchInfo{
		Position:        "g1:1",
		SnapshotState:   "complete",
		SnapshotPending: []uint32{1, 2},
	}, time.Now())

	if got.Properties["cdc.snapshot.state"] != "complete" {
		t.Errorf("snapshot state = %q", got.Properties["cdc.snapshot.state"])
	}
	if got.Properties["cdc.snapshot.pending"] == "" {
		t.Error("snapshot pending missing")
	}
}

func TestPropertiesOfMissingDoc(t *testing.T) {
	kv := newFakeKV()
	props, err := propertiesOf(t.Context(), kv)
	if err != nil {
		t.Fatalf("propertiesOf: %v", err)
	}
	if len(props) != 0 {
		t.Errorf("empty doc properties = %v, want empty", props)
	}
}

func TestSetPropertiesEmptyNoop(t *testing.T) {
	kv := newFakeKV()
	err := setProperties(t.Context(), kv, core.TableRef{Target: "t"}, nil, time.Now)
	if err != nil {
		t.Fatalf("setProperties empty: %v", err)
	}
	if len(kv.docs) != 0 {
		t.Error("empty props should not write")
	}
}

func TestSetPropertiesRequiresTarget(t *testing.T) {
	kv := newFakeKV()
	err := setProperties(t.Context(), kv, core.TableRef{}, map[string]string{"k": "v"}, time.Now)
	if err == nil {
		t.Error("empty target: want error")
	}
}
