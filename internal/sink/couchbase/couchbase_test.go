package couchbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// fakeKV is an in-memory kvStore with JSON fidelity: documents round-trip
// through marshal/unmarshal so reads see exactly what Couchbase would.
// hook, when set, is called before every write and can inject the crash
// points the recovery tests rely on.
type fakeKV struct {
	docs map[string]json.RawMessage
	hook func(op string, id string) error
}

func newFakeKV() *fakeKV { return &fakeKV{docs: map[string]json.RawMessage{}} }

func (f *fakeKV) upsert(_ context.Context, id string, doc any) error {
	if f.hook != nil {
		if err := f.hook("upsert", id); err != nil {
			return err
		}
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	f.docs[id] = b
	return nil
}

func (f *fakeKV) remove(_ context.Context, id string) error {
	if f.hook != nil {
		if err := f.hook("remove", id); err != nil {
			return err
		}
	}
	if _, ok := f.docs[id]; !ok {
		return errNotFound
	}
	delete(f.docs, id)
	return nil
}

func (f *fakeKV) get(_ context.Context, id string, out any) (bool, error) {
	b, ok := f.docs[id]
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return false, err
	}
	return true, nil
}

// fakeTx stages mutations and only applies them when fn returns nil — the
// rollback semantics of a real transaction. A failed run leaves the store
// byte-identical, which is the property the atomic tests assert.
type fakeTx struct {
	store *fakeKV
	hook  func() error // non-nil error after the batch is staged
}

func (t *fakeTx) run(_ context.Context, fn func(tx kvStore) error) error {
	staged := map[string]json.RawMessage{}
	removed := map[string]bool{}
	tx := &stagedTx{staged: staged, removed: removed, base: t.store}
	if err := fn(tx); err != nil {
		return err // nothing staged reaches the store: rollback
	}
	if t.hook != nil {
		if err := t.hook(); err != nil {
			return err
		}
	}
	for id := range removed {
		delete(t.store.docs, id)
	}
	for id, b := range staged {
		t.store.docs[id] = b
	}
	return nil
}

type stagedTx struct {
	staged  map[string]json.RawMessage
	removed map[string]bool
	base    *fakeKV
}

func (s *stagedTx) upsert(_ context.Context, id string, doc any) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	s.staged[id] = b
	delete(s.removed, id)
	return nil
}

func (s *stagedTx) remove(_ context.Context, id string) error {
	if _, ok := s.staged[id]; !ok {
		if _, ok := s.base.docs[id]; !ok && !s.removed[id] {
			return errNotFound
		}
	}
	s.removed[id] = true
	delete(s.staged, id)
	return nil
}

func (s *stagedTx) get(_ context.Context, id string, out any) (bool, error) {
	if b, ok := s.staged[id]; ok {
		return true, json.Unmarshal(b, out)
	}
	if s.removed[id] {
		return false, nil
	}
	return s.base.get(context.Background(), id, out)
}

// plan is the shared write plan for the tests: two data columns plus a
// metadata column, keyed on id.
func plan(meta map[string]core.MetadataColumn) *tablePlan {
	return &tablePlan{
		schema: core.Schema{Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "ingest_ts", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		}},
		meta:        meta,
		sourceTable: "src.orders",
		pk:          []string{"id"},
	}
}

func metaIngest() map[string]core.MetadataColumn {
	return map[string]core.MetadataColumn{
		"ingest_ts": {From: core.MetaIngestTS, As: "ingest_ts"},
	}
}

func upsertBatch(pos string, rows ...int64) *dataplane.Batch {
	cb := rowchange.Batch{Table: "orders", Position: pos, Mode: rowchange.UpsertMode}
	for _, id := range rows {
		cb.Changes = append(cb.Changes, rowchange.Change{
			Op: rowchange.OpInsert, Key: []any{id},
			After:    map[string]any{"id": id, "v": fmt.Sprintf("v%d", id)},
			IngestTS: time.Unix(1700000000, 0).UTC(),
		})
	}
	cs := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
	}, PrimaryKey: []string{"id"}}
	dpb, err := dpint.BatchFromChangeBatch(cb, cs)
	if err != nil {
		panic(err)
	}
	return dpb
}

func deleteBatch(pos string, ids ...int64) *dataplane.Batch {
	cb := rowchange.Batch{Table: "orders", Position: pos, Mode: rowchange.UpsertMode}
	for _, id := range ids {
		cb.Changes = append(cb.Changes, rowchange.Change{Op: rowchange.OpDelete, Key: []any{id}})
	}
	cs := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
	}, PrimaryKey: []string{"id"}}
	dpb, err := dpint.BatchFromChangeBatch(cb, cs)
	if err != nil {
		panic(err)
	}
	return dpb
}

// TestFastCommitDataThenControl: the batch lands as documents, the control
// document carries the position and the metadata sub-object is where the
// contract says it is.
func TestFastCommitDataThenControl(t *testing.T) {
	kv := newFakeKV()
	w := newTableWriter(kv, nil, plan(metaIngest()), nil)

	if err := w.Commit(context.Background(), upsertBatch("g1:42", 1, 2)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(kv.docs) != 3 {
		t.Fatalf("documents = %d, want 3 (2 data + 1 control)", len(kv.docs))
	}
	var ctrl controlDoc
	if found, err := kv.get(context.Background(), controlKey, &ctrl); err != nil || !found {
		t.Fatalf("control doc missing: found=%v err=%v", found, err)
	}
	if ctrl.Position != "g1:42" {
		t.Fatalf("control position = %q, want g1:42", ctrl.Position)
	}
	var doc map[string]any
	if _, err := kv.get(context.Background(), "[1]", &doc); err != nil {
		t.Fatalf("doc [1]: %v", err)
	}
	if doc["v"] != "v1" {
		t.Fatalf("doc[1].v = %v", doc["v"])
	}
	sub, ok := doc[reservedField].(map[string]any)
	if !ok {
		t.Fatalf("doc has no %q sub-object: %v", reservedField, doc)
	}
	if sub["ingest_ts"] != "2023-11-14T22:13:20Z" {
		t.Fatalf("metadata ingest_ts = %v", sub["ingest_ts"])
	}
	if _, direct := doc["ingest_ts"]; direct {
		t.Fatalf("metadata leaked to the document top level: %v", doc)
	}
}

// TestFastCrashBeforeControlKeepsPositionBack: the failure injection fires
// on the CONTROL write only — data documents are in place, the position is
// not advanced. A replay of the same batch rewrites the same documents
// (no duplication is possible with key-addressed upserts) and advances the
// position. This is the fast-mode recovery contract.
func TestFastCrashBeforeControlKeepsPositionBack(t *testing.T) {
	kv := newFakeKV()
	w := newTableWriter(kv, nil, plan(metaIngest()), nil)

	kv.hook = func(op, id string) error {
		if id == controlKey {
			return errors.New("simulated crash before control write")
		}
		return nil
	}
	if err := w.Commit(context.Background(), upsertBatch("g1:42", 1, 2)); err == nil {
		t.Fatal("commit should fail at the control write")
	}
	var ctrl controlDoc
	if found, _ := kv.get(context.Background(), controlKey, &ctrl); found {
		t.Fatal("position advanced past data on a failed commit")
	}

	// Restart: replay the same batch with no fault.
	kv.hook = nil
	w2 := newTableWriter(kv, nil, plan(metaIngest()), nil)
	if err := w2.Commit(context.Background(), upsertBatch("g1:42", 1, 2)); err != nil {
		t.Fatalf("replay commit: %v", err)
	}
	dataDocs := 0
	for id := range kv.docs {
		if id != controlKey {
			dataDocs++
		}
	}
	if dataDocs != 2 {
		t.Fatalf("data documents after replay = %d, want 2 (idempotent, no duplicates)", dataDocs)
	}
	if found, _ := kv.get(context.Background(), controlKey, &ctrl); !found || ctrl.Position != "g1:42" {
		t.Fatalf("position after replay = %q (found=%v), want g1:42", ctrl.Position, found)
	}
}

// TestFastDeleteToleratesReplay: removing an already-removed key is
// success — the replay story re-runs deletes that already landed.
func TestFastDeleteToleratesReplay(t *testing.T) {
	kv := newFakeKV()
	w := newTableWriter(kv, nil, plan(metaIngest()), nil)
	if err := w.Commit(context.Background(), upsertBatch("g1:1", 7)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := w.Commit(context.Background(), deleteBatch("g1:2", 7)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// The same delete again (replayed batch): not an error.
	if err := w.Commit(context.Background(), deleteBatch("g1:2", 7)); err != nil {
		t.Fatalf("replayed delete: %v", err)
	}
	if _, ok := kv.docs["[7]"]; ok {
		t.Fatal("deleted document still present")
	}
}

// TestAtomicFailureLeavesNoTrace: a mid-batch failure rolls the whole
// transaction back — neither the data documents nor the position exist
// after a failed atomic commit.
func TestAtomicFailureLeavesNoTrace(t *testing.T) {
	kv := newFakeKV()
	tx := &fakeTx{store: kv, hook: func() error { return errors.New("commit-phase failure") }}
	w := newTableWriter(kv, tx, plan(metaIngest()), nil)

	if err := w.Commit(context.Background(), upsertBatch("g1:9", 1, 2, 3)); err == nil {
		t.Fatal("atomic commit should fail")
	}
	if len(kv.docs) != 0 {
		t.Fatalf("store not clean after rollback: %v", kv.docs)
	}

	// And a healthy attempt commits everything, control document included.
	tx.hook = nil
	if err := w.Commit(context.Background(), upsertBatch("g1:9", 1)); err != nil {
		t.Fatalf("atomic commit: %v", err)
	}
	var ctrl controlDoc
	if found, _ := kv.get(context.Background(), controlKey, &ctrl); !found || ctrl.Position != "g1:9" {
		t.Fatalf("position after atomic commit = %q (found=%v)", ctrl.Position, found)
	}
}

// TestControlPreservesProperties: a commit must not erase the snapshot
// orchestrator's properties — the control document is the merge point of
// two writers (commit path and SetProperties).
func TestControlPreservesProperties(t *testing.T) {
	kv := newFakeKV()
	w := newTableWriter(kv, nil, plan(nil), nil)
	ctx := context.Background()

	if err := setProperties(ctx, kv, core.TableRef{Target: "orders"}, map[string]string{"cdc.snapshot.state": "in_progress", "cdc.snapshot.bounds": `{"chunk":1}`}, time.Now); err != nil {
		t.Fatalf("set properties: %v", err)
	}
	if err := w.Commit(ctx, upsertBatch("g1:1", 1)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	props, err := propertiesOf(ctx, kv)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props["cdc.snapshot.state"] != "in_progress" {
		t.Fatalf("properties lost by commit: %v", props)
	}

	// And the batch's own snapshot state merges in the same write.
	b := upsertBatch("g1:2", 2)
	b.SnapshotState = "complete"
	b.SnapshotPending = []uint32{1, 2, 3}
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("snapshot commit: %v", err)
	}
	props, err = propertiesOf(ctx, kv)
	if err != nil {
		t.Fatalf("properties 2: %v", err)
	}
	if props["cdc.snapshot.state"] != "complete" {
		t.Fatalf("snapshot state did not advance with the position: %v", props)
	}
	if props["cdc.snapshot.pending"] == "" {
		t.Fatal("pending list missing")
	}
}

// TestDocKeyRules: deterministic, type-unambiguous, length-guarded.
func TestDocKeyRules(t *testing.T) {
	cases := []struct {
		name    string
		a, b    []any
		same    bool
		tooLong bool
	}{
		{"int stable", []any{int64(1)}, []any{int64(1)}, true, false},
		{"int vs string", []any{int64(1)}, []any{"1"}, false, false},
		{"composite order matters", []any{"a", int64(2)}, []any{int64(2), "a"}, false, false},
		{"composite separators safe", []any{"a", "b"}, []any{"a\x1fb"}, false, false},
		{"nil vs empty string", []any{nil}, []any{""}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ka, err := docKey(tc.a)
			if err != nil {
				t.Fatalf("key a: %v", err)
			}
			kb, err := docKey(tc.b)
			if err != nil {
				t.Fatalf("key b: %v", err)
			}
			if (ka == kb) != tc.same {
				t.Fatalf("keys %q and %q: same=%v", ka, kb, tc.same)
			}
			if !strings.HasPrefix(ka, "[") {
				t.Fatalf("data key %q must start with '[' (control-key namespace)", ka)
			}
		})
	}
	long := rowchange.Change{Key: []any{strings.Repeat("x", maxKeyLen)}}
	if _, err := docKey(long.Key); err == nil {
		t.Fatal("oversized key accepted")
	}
}

// TestBuildDocValueForms: the canonical value → JSON conversions the sink
// promises — UUID as hyphenated string (cast-converted), decimal as
// canonical text, nested composites recursive, cast plan applied before
// serialization.
func TestBuildDocValueForms(t *testing.T) {
	alloc := memory.NewGoAllocator()
	p := &tablePlan{
		schema: core.Schema{Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "uid", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "amount", Type: core.ColumnType{Kind: core.KindDecimal, Precision: 10, Scale: 2}},
			{Name: "addr", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
				{Name: "city", Type: core.ColumnType{Kind: core.KindString}},
			}}},
			{Name: "tags", Type: core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindString}}},
			{Name: "vm", Type: core.ColumnType{Kind: core.KindMap, KeyType: &core.ColumnType{Kind: core.KindString}, ValueType: &core.ColumnType{Kind: core.KindInt64}}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		}},
		sourceTable: "src.orders",
	}
	// Build a wire record carrying the row: the wire forms are exactly
	// what the codec decodes — decimal as Decimal128, uuid as string,
	// composites as arrow struct/list/map.
	data, err := transport.CoreSchemaToArrow(p.schema)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(alloc, data)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(1)
	bld.Field(1).(*array.StringBuilder).Append("018f6a1e-7d3f-7aa1-bb3a-5f3f5e6a7b8c") // UUID arrives as string after cast
	if err := bld.Field(2).(*array.Decimal128Builder).AppendValueFromString("123.45"); err != nil {
		t.Fatalf("decimal: %v", err)
	}
	sb := bld.Field(3).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("Curitiba")
	lb := bld.Field(4).(*array.ListBuilder)
	lb.Append(true)
	for _, s := range []string{"a", "b"} {
		lb.ValueBuilder().(*array.StringBuilder).Append(s)
	}
	mb := bld.Field(5).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("k")
	mb.ItemBuilder().(*array.Int64Builder).Append(3)
	bld.Field(6).(*array.StringBuilder).Append("hi")
	for i := 7; i < int(data.NumFields()); i++ {
		bld.Field(i).AppendNull()
	}
	rec := bld.NewRecordBatch()
	defer rec.Release()

	reader, err := transport.NewBatchReader(rec, []string{"id"})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	data0, meta, err := p.buildDoc(reader, 0)
	if err != nil {
		t.Fatalf("buildDoc: %v", err)
	}
	_ = data0
	if len(meta) != 0 {
		t.Fatalf("unexpected metadata: %v", meta)
	}
	if data0["uid"] != "018f6a1e-7d3f-7aa1-bb3a-5f3f5e6a7b8c" {
		t.Fatalf("uuid = %v, want hyphenated form", data0["uid"])
	}
	if data0["amount"] != "123.45" {
		t.Fatalf("decimal = %v, want canonical text", data0["amount"])
	}
	addr, ok := data0["addr"].(map[string]any)
	if !ok || addr["city"] != "Curitiba" {
		t.Fatalf("nested struct = %v", data0["addr"])
	}
	tags, ok := data0["tags"].([]any)
	if !ok || len(tags) != 2 {
		t.Fatalf("nested list = %v", data0["tags"])
	}
	if !reflect.DeepEqual(data0["vm"], map[string]any{"k": int64(3)}) {
		t.Fatalf("nested map = %v", data0["vm"])
	}

	// Cast applies before serialization: decimal to string stays exact
	// text even though the wire already delivers canonical text.
	p.cast, _ = core.ParseCastPolicy(map[string]string{"amount": "string"})
	data0, _, err = p.buildDoc(reader, 0)
	if err != nil {
		t.Fatalf("buildDoc cast: %v", err)
	}
	if data0["amount"] != "123.45" {
		t.Fatalf("cast amount = %v", data0["amount"])
	}
}

// TestReservedFieldRejected: a data column or metadata destination named
// _urutau would collide with the metadata sub-object — rejected at ensure
// time with a named error, not silently merged.
func TestReservedFieldRejected(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: reservedField, Type: core.ColumnType{Kind: core.KindString}},
	}}
	err := validateSchema(schema, nil)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("reserved data column accepted: %v", err)
	}
	err = validateSchema(schemaWithoutReserved(), []core.MetadataColumn{{From: core.MetaOp, As: reservedField}})
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("reserved metadata destination accepted: %v", err)
	}
}

func schemaWithoutReserved() core.Schema {
	return core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}}
}

// TestParseCommitModeRules covers the option parsing contract.
func TestParseCommitModeRules(t *testing.T) {
	fast, err := parseCommitMode("")
	if err != nil || fast {
		t.Fatalf("empty: fast=%v err=%v", fast, err)
	}
	fast, err = parseCommitMode("fast")
	if err != nil || fast {
		t.Fatalf("fast: fast=%v err=%v", fast, err)
	}
	fast, err = parseCommitMode("atomic")
	if err != nil || !fast {
		t.Fatalf("atomic: fast=%v err=%v", fast, err)
	}
	if _, err := parseCommitMode("eventual"); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

// A cast column absent from the wire must fail loudly (FIX-DOC v2 A): the
// sink would otherwise skip the cast and serialize the raw value.
func TestBuildDocErrorsOnMissingCastColumn(t *testing.T) {
	p := &tablePlan{
		schema: core.Schema{Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "ghost", Type: core.ColumnType{Kind: core.KindString}}, // cast target, absent from the wire
		}},
		sourceTable: "src.t",
	}
	p.cast, _ = core.ParseCastPolicy(map[string]string{"ghost": "string"})

	data, err := transport.CoreSchemaToArrow(core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.NewGoAllocator(), data)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(1)
	for j := 1; j < int(data.NumFields()); j++ {
		bld.Field(j).AppendNull()
	}
	rec := bld.NewRecordBatch()
	defer rec.Release()
	reader, err := transport.NewBatchReader(rec, []string{"id"})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}

	_, _, err = p.buildDoc(reader, 0)
	if err == nil || !strings.Contains(err.Error(), `"ghost"`) || !strings.Contains(err.Error(), "kind not found") {
		t.Fatalf("missing cast column must error citing the column, got %v", err)
	}
}
