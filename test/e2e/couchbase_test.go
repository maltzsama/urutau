package e2e

// Couchbase sink acceptance (CR-041), opened through driver.OpenSink so the
// tests also prove the driver is reachable via the real import chain
// (builtin), not just within its own package. Reads go back through a
// second gocb client — the write path is never trusted on its own report.
//
// Unlike ClickHouse, the Couchbase container cannot self-initialize from
// env vars: couchbase-setup (compose) runs cluster-init and creates the
// bucket after the HTTP port answers, which can lag behind `compose up
// --wait`. cbSink therefore polls OpenSink with a bounded retry — the only
// Go-side wait in the suite, and only for this driver.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	gocb "github.com/couchbase/gocb/v2"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

const (
	cbConnStr   = "couchbase://localhost"
	cbUser      = "urutau"
	cbPass      = "urutaupass"
	cbBucket    = "lakehouse"
	cbPosition  = "_urutau::position"
	cbTimeout   = 90 * time.Second
	cbOpenTries = 45
)

func cbSpec(commit spec.CommitMode) *spec.Spec {
	return &spec.Spec{
		Sink: spec.Sink{
			Type:         "couchbase",
			URI:          cbConnStr,
			Namespace:    cbBucket,
			ClientID:     cbUser,
			ClientSecret: cbPass,
			CommitMode:   commit,
		},
	}
}

// cbSink opens the sink through the registry, retrying while the setup
// container finishes cluster-init/bucket-create.
func cbSink(t *testing.T, ctx context.Context, commit spec.CommitMode) sink.Sink {
	t.Helper()
	var lastErr error
	for i := 0; i < cbOpenTries; i++ {
		s, err := driver.OpenSink(ctx, cbSpec(commit))
		if err == nil {
			return s
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("open sink: %v", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("open sink after %d tries: %v", cbOpenTries, lastErr)
	return nil // unreachable; Fatalf exits
}

// cbClient is the independent read-back client (never the sink's own
// connection). Readiness waits on mgmt+KV only — the same contract the
// sink declares; the unmapped query port must not block a read.
func cbClient(t *testing.T) (*gocb.Cluster, *gocb.Bucket) {
	t.Helper()
	cluster, err := gocb.Connect(cbConnStr, gocb.ClusterOptions{Username: cbUser, Password: cbPass})
	if err != nil {
		t.Fatalf("read-back connect: %v", err)
	}
	t.Cleanup(func() { _ = cluster.Close(nil) })
	mgmtReady := &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeManagement},
	}
	kvReady := &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeKeyValue},
	}
	if err := cluster.WaitUntilReady(cbTimeout, mgmtReady); err != nil {
		t.Fatalf("read-back cluster: %v", err)
	}
	b := cluster.Bucket(cbBucket)
	if err := b.WaitUntilReady(cbTimeout, kvReady); err != nil {
		t.Fatalf("read-back bucket: %v", err)
	}
	return cluster, b
}

// cbDrop drops the test's collection from a previous run; each test uses a
// distinct collection name so EnsureTable's fresh create never races a drop.
func cbDrop(t *testing.T, cluster *gocb.Cluster, coll string) {
	t.Helper()
	_, err := cluster.Buckets().GetBucket(cbBucket, nil)
	if err != nil {
		return // bucket not there yet; nothing to drop
	}
	_ = cluster.Bucket(cbBucket).CollectionsV2().DropCollection("_default", coll, nil)
}

func cbDoc(t *testing.T, b *gocb.Bucket, coll, key string) (map[string]any, bool) {
	t.Helper()
	res, err := b.Scope("_default").Collection(coll).Get(key, &gocb.GetOptions{Timeout: cbTimeout})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, false
		}
		t.Fatalf("read-back %s: %v", key, err)
	}
	var doc map[string]any
	if err := res.Content(&doc); err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
	return doc, true
}

func cbReadPosition(t *testing.T, b *gocb.Bucket, coll string) (string, bool) {
	t.Helper()
	res, err := b.Scope("_default").Collection(coll).Get(cbPosition, &gocb.GetOptions{Timeout: cbTimeout})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return "", false
		}
		t.Fatalf("read-back control: %v", err)
	}
	var doc struct {
		Position string `json:"position"`
	}
	if err := res.Content(&doc); err != nil {
		t.Fatalf("decode control: %v", err)
	}
	return doc.Position, true
}

// cbOrdersSchema is the canonical schema every basic test writes: id + v.
func cbOrdersSchema() (core.Schema, core.TableRef) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
	ref := core.TableRef{Source: "src.orders", Target: "cb_orders", PrimaryKey: []string{"id"}}
	return schema, ref
}

func cbWriter(t *testing.T, ctx context.Context, s sink.Sink, ref core.TableRef, meta []core.MetadataColumn) sink.TableWriter {
	t.Helper()
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, meta)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func cbRow(id int64, v, pos string) change.Batch {
	return change.Batch{
		Table: "cb_orders", Position: pos, Mode: change.UpsertMode,
		Upserts: []change.Change{{
			Op: change.OpInsert, Key: []any{id},
			After:    map[string]any{"id": id, "v": v},
			IngestTS: time.Now(),
		}},
	}
}

// TestCouchbaseSinkUpsertAndResume: upserts land as documents, an update of
// the same key replaces in place (native upsert — no merge, no FINAL), and
// the control document carries the resume position.
func TestCouchbaseSinkUpsertAndResume(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), cbTimeout)
	defer cancel()
	cluster, b := cbClient(t)
	cbDrop(t, cluster, "cb_orders")

	s := cbSink(t, ctx, "")
	defer func() { _ = s.Close() }()
	schema, ref := cbOrdersSchema()
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := cbWriter(t, ctx, s, ref, nil)

	if err := w.Commit(ctx, cbRow(1, "a", "0/1")); err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	if err := w.Commit(ctx, cbRow(1, "a2", "0/2")); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	doc, ok := cbDoc(t, b, "cb_orders", "[1]")
	if !ok {
		t.Fatal("document [1] missing")
	}
	if doc["v"] != "a2" {
		t.Fatalf("value after update = %v, want a2 (immediate, not eventual)", doc["v"])
	}
	pos, ok := cbReadPosition(t, b, "cb_orders")
	if !ok || pos != "0/2" {
		t.Fatalf("control position = %q (found=%v), want 0/2", pos, ok)
	}
	if got, err := s.Position(ctx, ref); err != nil || got != "0/2" {
		t.Fatalf("sink.Position = %q, %v; want 0/2", got, err)
	}
}

// TestCouchbaseSinkDelete: OpDelete removes the document outright — not a
// tombstone, not eventual. A replayed delete is a no-op, not an error.
func TestCouchbaseSinkDelete(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), cbTimeout)
	defer cancel()
	cluster, b := cbClient(t)
	cbDrop(t, cluster, "cb_orders")

	s := cbSink(t, ctx, "")
	defer func() { _ = s.Close() }()
	schema, ref := cbOrdersSchema()
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := cbWriter(t, ctx, s, ref, nil)

	if err := w.Commit(ctx, cbRow(9, "x", "0/1")); err != nil {
		t.Fatalf("commit: %v", err)
	}
	del := change.Batch{
		Table: "cb_orders", Position: "0/2", Mode: change.UpsertMode,
		Deletes: []change.Change{{Op: change.OpDelete, Key: []any{int64(9)}}},
	}
	if err := w.Commit(ctx, del); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := cbDoc(t, b, "cb_orders", "[9]"); ok {
		t.Fatal("document survived its delete")
	}
	if err := w.Commit(ctx, del); err != nil {
		t.Fatalf("replayed delete: %v", err)
	}
}

// TestCouchbaseSinkFastRecovery: the crash window — data documents in
// place, control document not yet written. Simulated by deleting the
// control document behind the sink's back, then replaying the batch:
// documents rewrite identically (no duplicates are possible with
// key-addressed upserts) and the position advances exactly once.
func TestCouchbaseSinkFastRecovery(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), cbTimeout)
	defer cancel()
	cluster, b := cbClient(t)
	cbDrop(t, cluster, "cb_orders")

	s := cbSink(t, ctx, "")
	defer func() { _ = s.Close() }()
	schema, ref := cbOrdersSchema()
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := cbWriter(t, ctx, s, ref, nil)

	if err := w.Commit(ctx, cbRow(1, "a", "0/1")); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Simulate the crash between data and control write.
	if _, err := b.Scope("_default").Collection("cb_orders").Remove(cbPosition, nil); err != nil {
		t.Fatalf("drop control: %v", err)
	}
	if _, ok := cbReadPosition(t, b, "cb_orders"); ok {
		t.Fatal("control document survived the simulated crash")
	}
	// Restart: same batch again.
	w2 := cbWriter(t, ctx, s, ref, nil)
	if err := w2.Commit(ctx, cbRow(1, "a", "0/1")); err != nil {
		t.Fatalf("replay: %v", err)
	}
	doc, ok := cbDoc(t, b, "cb_orders", "[1]")
	if !ok || doc["v"] != "a" {
		t.Fatalf("document after replay = %v, %v", doc, ok)
	}
	pos, ok := cbReadPosition(t, b, "cb_orders")
	if !ok || pos != "0/1" {
		t.Fatalf("position after replay = %q (found=%v), want 0/1", pos, ok)
	}
}

// TestCouchbaseSinkAtomicMode: commitMode atomic — a mid-batch failure
// (one valid upsert staged, then an unaddressable key) rolls back the
// whole attempt: no data documents, no position advance. Then a clean
// batch commits with data and position together.
func TestCouchbaseSinkAtomicMode(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*cbTimeout)
	defer cancel()
	cluster, b := cbClient(t)
	cbDrop(t, cluster, "cb_atomic")

	s := cbSink(t, ctx, spec.CommitModeAtomic)
	defer func() { _ = s.Close() }()
	schema, ref := cbOrdersSchema()
	ref.Target = "cb_atomic"
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := cbWriter(t, ctx, s, ref, nil)

	bad := change.Batch{
		Table: ref.Target, Position: "0/1", Mode: change.UpsertMode,
		Upserts: []change.Change{
			{Op: change.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}, IngestTS: time.Now()},
			{Op: change.OpInsert, Key: []any{strings.Repeat("x", 300)}, After: map[string]any{"id": int64(2), "v": "b"}, IngestTS: time.Now()},
		},
	}
	if err := w.Commit(ctx, bad); err == nil {
		t.Fatal("oversized key must fail the batch")
	}
	if _, ok := cbDoc(t, b, "cb_atomic", "[1]"); ok {
		t.Fatal("staged upsert survived a rolled-back transaction")
	}
	if _, ok := cbReadPosition(t, b, "cb_atomic"); ok {
		t.Fatal("position advanced on a rolled-back transaction")
	}

	good := change.Batch{
		Table: ref.Target, Position: "0/2", Mode: change.UpsertMode,
		Upserts: []change.Change{
			{Op: change.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}, IngestTS: time.Now()},
			{Op: change.OpInsert, Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "v": "b"}, IngestTS: time.Now()},
		},
	}
	if err := w.Commit(ctx, good); err != nil {
		t.Fatalf("atomic commit: %v", err)
	}
	if _, ok := cbDoc(t, b, "cb_atomic", "[2]"); !ok {
		t.Fatal("atomic batch data missing")
	}
	pos, ok := cbReadPosition(t, b, "cb_atomic")
	if !ok || pos != "0/2" {
		t.Fatalf("position = %q (found=%v), want 0/2", pos, ok)
	}
}

// TestCouchbaseSinkAppendRequiresPK: append mode has no collapse to lean
// on, and a document cannot be addressed without a key — ensure-time
// rejection, pause-and-alert per the ClickHouse precedent.
func TestCouchbaseSinkAppendRequiresPK(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), cbTimeout)
	defer cancel()
	s := cbSink(t, ctx, "")
	defer func() { _ = s.Close() }()
	schema := core.Schema{Columns: []core.Column{
		{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
	}}
	ref := core.TableRef{Source: "src.events", Target: "cb_nopk"}
	err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.AppendMode)
	if err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("append without PK: want primary-key error, got %v", err)
	}
}

// TestCouchbaseSinkNestedTypes: Couchbase is JSON — Struct/List/Map land
// natively, no escape valve, readable back as navigable documents.
func TestCouchbaseSinkNestedTypes(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), cbTimeout)
	defer cancel()
	cluster, b := cbClient(t)
	cbDrop(t, cluster, "cb_nested")

	s := cbSink(t, ctx, "")
	defer func() { _ = s.Close() }()
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "addr", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "city", Type: core.ColumnType{Kind: core.KindString}},
		}}},
		{Name: "tags", Type: core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindString}}},
	}}
	ref := core.TableRef{Source: "src.users", Target: "cb_nested", PrimaryKey: []string{"id"}}
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := cbWriter(t, ctx, s, ref, nil)
	batch := change.Batch{
		Table: ref.Target, Position: "0/1", Mode: change.UpsertMode,
		Upserts: []change.Change{{
			Op: change.OpInsert, Key: []any{int64(1)},
			After: map[string]any{
				"id":   int64(1),
				"addr": map[string]any{"city": "Curitiba"},
				"tags": []any{"alpha", "beta"},
			},
			IngestTS: time.Now(),
		}},
	}
	if err := w.Commit(ctx, batch); err != nil {
		t.Fatalf("commit: %v", err)
	}
	doc, ok := cbDoc(t, b, "cb_nested", "[1]")
	if !ok {
		t.Fatal("nested document missing")
	}
	addr, ok := doc["addr"].(map[string]any)
	if !ok || addr["city"] != "Curitiba" {
		t.Fatalf("nested struct = %v", doc["addr"])
	}
	tags, ok := doc["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "alpha" {
		t.Fatalf("nested list = %v", doc["tags"])
	}
}

// TestCouchbaseSinkMetadataSubObject: a data column named op coexists with
// the op metadata — the reserved sub-object keeps them apart, and the
// control document stays readable.
func TestCouchbaseSinkMetadataSubObject(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), cbTimeout)
	defer cancel()
	cluster, b := cbClient(t)
	cbDrop(t, cluster, "cb_meta")

	s := cbSink(t, ctx, "")
	defer func() { _ = s.Close() }()
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "op", Type: core.ColumnType{Kind: core.KindString, Nullable: true}}, // collides by name only
		{Name: "cdc_op", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
	ref := core.TableRef{Source: "src.orders", Target: "cb_meta", PrimaryKey: []string{"id"}}
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := cbWriter(t, ctx, s, ref, []core.MetadataColumn{{From: core.MetaOp, As: "cdc_op"}})
	batch := change.Batch{
		Table: ref.Target, Position: "0/1", Mode: change.UpsertMode,
		Upserts: []change.Change{{
			Op: change.OpInsert, Key: []any{int64(1)},
			After:    map[string]any{"id": int64(1), "op": "DATA", "cdc_op": "DATA"},
			IngestTS: time.Now(),
		}},
	}
	if err := w.Commit(ctx, batch); err != nil {
		t.Fatalf("commit: %v", err)
	}
	doc, ok := cbDoc(t, b, "cb_meta", "[1]")
	if !ok {
		t.Fatal("meta document missing")
	}
	if doc["op"] != "DATA" {
		t.Fatalf("data column op overwritten: %v", doc["op"])
	}
	sub, ok := doc["_urutau"].(map[string]any)
	if !ok || sub["cdc_op"] != "insert" {
		t.Fatalf("metadata sub-object = %v", doc["_urutau"])
	}
}

// TestCouchbaseSinkDurabilityMajority: every durable write acknowledges at
// majority durability — on the single-node 0-replica e2e bucket that
// reduces to the active node, and it must succeed rather than raise
// DurabilityImpossible (the failure the compose comments document).
func TestCouchbaseSinkDurabilityMajority(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), cbTimeout)
	defer cancel()
	cluster, _ := cbClient(t)
	cbDrop(t, cluster, "cb_durable")

	s := cbSink(t, ctx, "")
	defer func() { _ = s.Close() }()
	schema, ref := cbOrdersSchema()
	ref.Target = "cb_durable"
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := cbWriter(t, ctx, s, ref, nil)
	for i := int64(1); i <= 5; i++ {
		if err := w.Commit(ctx, cbRow(i, fmt.Sprintf("v%d", i), fmt.Sprintf("0/%d", i))); err != nil {
			t.Fatalf("durable commit %d: %v", i, err)
		}
	}
}

// TestCouchbaseSinkPropertiesRoundTrip: the snapshot state machine's
// property bag rides the control document — set, read back, overwritten.
func TestCouchbaseSinkPropertiesRoundTrip(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), cbTimeout)
	defer cancel()
	s := cbSink(t, ctx, "")
	defer func() { _ = s.Close() }()
	schema, ref := cbOrdersSchema()
	ref.Target = "cb_props"
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := s.SetProperties(ctx, ref, map[string]string{"cdc.snapshot.state": "in_progress"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	props, err := s.Properties(ctx, ref)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if props["cdc.snapshot.state"] != "in_progress" {
		t.Fatalf("props = %v", props)
	}
	if err := s.SetProperties(ctx, ref, map[string]string{"cdc.snapshot.state": "complete"}); err != nil {
		t.Fatalf("set 2: %v", err)
	}
	props, err = s.Properties(ctx, ref)
	if err != nil {
		t.Fatalf("get 2: %v", err)
	}
	if props["cdc.snapshot.state"] != "complete" {
		t.Fatalf("props after overwrite = %v", props)
	}
}
