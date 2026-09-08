package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// ClickHouse sink acceptance (CR-040). Every read-back uses FINAL or
// OPTIMIZE ... FINAL: without it, ReplacingMergeTree dedup is eventual and
// the test would measure the background merge clock, not Urutau's
// correctness. The sink is opened through driver.OpenSink so the test also
// proves the driver is reachable via the real import chain (builtin), not
// just within its own package.

const chDSN = "clickhouse://localhost:9002?password=clickpass"

func chSink(t *testing.T, ctx context.Context) sink.Sink {
	t.Helper()
	s, err := driver.OpenSink(ctx, &spec.Spec{
		Sink: spec.Sink{Type: "clickhouse", URI: chDSN, Namespace: "lakehouse"},
	})
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	return s
}

func chStdDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("clickhouse", chDSN)
	if err != nil {
		t.Fatalf("std open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func chDrop(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	if _, err := db.Exec("DROP TABLE IF EXISTS lakehouse." + table); err != nil {
		t.Fatalf("drop %s: %v", table, err)
	}
}

func chQueryInt(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func chOrdersSchema() (core.Schema, core.TableRef) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			// One metadata column: proves the projection against the
			// change header on a real engine, not only in unit fakes.
			{Name: "ingest_ts", Type: core.ColumnType{Kind: core.KindTimestampTZ, Nullable: true}},
		},
	}
	ref := core.TableRef{Source: "src.orders", Target: "lakehouse.ch_orders", PrimaryKey: []string{"id"}}
	return schema, ref
}

func chWriter(t *testing.T, ctx context.Context, s sink.Sink, ref core.TableRef, meta []core.MetadataColumn) sink.TableWriter {
	t.Helper()
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, meta)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	return w
}

var chMeta = []core.MetadataColumn{{From: core.MetaIngestTS, As: "ingest_ts"}}

// TestClickHouseSinkUpsertAndResume: upserts land, the collapsed batch is
// one insert, an update of the same PK resolves to the newest value under
// FINAL, and argMax(position, seq) returns the latest commit coordinate.
func TestClickHouseSinkUpsertAndResume(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := chSink(t, ctx)
	defer func() { _ = s.Close() }()
	db := chStdDB(t)
	chDrop(t, db, "ch_orders")

	schema, ref := chOrdersSchema()
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := chWriter(t, ctx, s, ref, chMeta)
	defer func() { _ = w.Close() }()

	row := func(id int64, v, pos string) change.Batch {
		return change.Batch{
			Table: ref.Target, Position: pos,
			Upserts: []change.Change{{
				Op: change.OpInsert, Table: ref.Target, Key: []any{id},
				After: map[string]any{"id": id, "v": v}, IngestTS: time.Now(),
			}},
		}
	}
	if err := w.Commit(ctx, toDPBatch(row(1, "a", "0/1"))); err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	if err := w.Commit(ctx, toDPBatch(row(2, "b", "0/2"))); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	if got := chQueryInt(t, db, "SELECT count() FROM lakehouse.ch_orders FINAL"); got != 2 {
		t.Fatalf("rows after two upserts = %d, want 2", got)
	}

	// Update the same PK: collapsed batch, one row, newest value wins.
	if err := w.Commit(ctx, toDPBatch(row(1, "a2", "0/3"))); err != nil {
		t.Fatalf("commit update: %v", err)
	}
	var v string
	if err := db.QueryRow("SELECT v FROM lakehouse.ch_orders FINAL WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if v != "a2" {
		t.Fatalf("value = %q, want a2", v)
	}

	// Resume: the latest commit coordinate, decoded through the position
	// contract in the caller — here, opaque-string equality suffices.
	pos, err := s.Position(ctx, ref)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if pos != "0/3" {
		t.Fatalf("position = %q, want 0/3", pos)
	}
}

// TestClickHouseSinkDelete: the tombstone hides the row under FINAL and
// OPTIMIZE ... FINAL removes it physically — the operator owns that
// maintenance, the sink only marks it correctly.
func TestClickHouseSinkDelete(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := chSink(t, ctx)
	defer func() { _ = s.Close() }()
	db := chStdDB(t)
	chDrop(t, db, "ch_orders")

	schema, ref := chOrdersSchema()
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := chWriter(t, ctx, s, ref, chMeta)
	defer func() { _ = w.Close() }()

	commit := func(b change.Batch) {
		t.Helper()
		if err := w.Commit(ctx, toDPBatch(b)); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	commit(change.Batch{
		Table: ref.Target, Position: "0/1",
		Upserts: []change.Change{{Op: change.OpInsert, Table: ref.Target, Key: []any{int64(7)},
			After: map[string]any{"id": int64(7), "v": "doomed"}, IngestTS: time.Now()}},
	})
	commit(change.Batch{
		Table: ref.Target, Position: "0/2",
		Deletes: []change.Change{{Op: change.OpDelete, Table: ref.Target, Key: []any{int64(7)}}},
	})

	if got := chQueryInt(t, db, "SELECT count() FROM lakehouse.ch_orders FINAL WHERE id = 7"); got != 0 {
		t.Fatalf("deleted row visible under FINAL = %d, want 0", got)
	}
	// Physical cleanup is the operator's maintenance, not the sink's: on
	// ClickHouse 24.8 clean_deleted_rows is an obsolete no-op and removing
	// the tombstone takes OPTIMIZE ... FINAL CLEANUP (experimental flag).
	// The tombstone row stays, correctly hidden from FINAL reads.
	// One merge (OPTIMIZE FINAL) dedups the key to its winning version: the
	// tombstone, still physically present but permanently hidden.
	if _, err := db.Exec("OPTIMIZE TABLE lakehouse.ch_orders FINAL"); err != nil {
		t.Fatalf("optimize: %v", err)
	}
	if got := chQueryInt(t, db, "SELECT count() FROM lakehouse.ch_orders WHERE id = 7"); got != 1 {
		t.Fatalf("rows after merge = %d, want 1 (the tombstone)", got)
	}
	if got := chQueryInt(t, db, "SELECT count() FROM lakehouse.ch_orders FINAL WHERE id = 7"); got != 0 {
		t.Fatalf("deleted row visible under FINAL = %d, want 0", got)
	}
}

// TestClickHouseSinkAppendMode: the DDL tells the truth about the write
// shape — plain MergeTree, no tombstone machinery — while position and seq
// stay (ordering for the resume read, not versioning).
func TestClickHouseSinkAppendMode(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := chSink(t, ctx)
	defer func() { _ = s.Close() }()
	db := chStdDB(t)
	chDrop(t, db, "ch_orders")

	schema, ref := chOrdersSchema()
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.AppendMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := chWriter(t, ctx, s, ref, chMeta)
	defer func() { _ = w.Close() }()

	for i, pos := range []string{"0/1", "0/2"} {
		b := change.Batch{Table: ref.Target, Position: pos, Mode: change.AppendMode}
		b.Upserts = []change.Change{{Op: change.OpInsert, Table: ref.Target, Key: nil,
			After: map[string]any{"id": int64(i + 1), "v": fmt.Sprintf("row-%d", i+1)}, IngestTS: time.Now()}}
		if err := w.Commit(ctx, toDPBatch(b)); err != nil {
			t.Fatalf("commit %d: %v", i+1, err)
		}
	}
	if got := chQueryInt(t, db, "SELECT count() FROM lakehouse.ch_orders"); got != 2 {
		t.Fatalf("append rows = %d, want 2", got)
	}
	var typ string
	if err := db.QueryRow(
		"SELECT type FROM system.columns WHERE database='lakehouse' AND table='ch_orders' AND name='is_deleted'").Scan(&typ); err == nil {
		t.Fatalf("append table carries is_deleted (%s) — versioning machinery leaked into a log table", typ)
	}
	pos, err := s.Position(ctx, ref)
	if err != nil || pos != "0/2" {
		t.Fatalf("append position = %q, %v; want 0/2", pos, err)
	}
}

// TestClickHouseSinkProgressRoundTrip: snapshot progress is an optimization
// record — write/read through the sink's property surface must round-trip,
// and it must never be needed to decide correctness.
func TestClickHouseSinkProgressRoundTrip(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := chSink(t, ctx)
	defer func() { _ = s.Close() }()

	schema, ref := chOrdersSchema()
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	props := map[string]string{
		"urutau.snapshot.state":   "in-progress",
		"urutau.snapshot.pending": "0,1,2",
	}
	if err := s.SetProperties(ctx, ref, props); err != nil {
		t.Fatalf("set properties: %v", err)
	}
	got, err := s.Properties(ctx, ref)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	for k, want := range props {
		if got[k] != want {
			t.Errorf("progress %q = %q, want %q", k, got[k], want)
		}
	}
}

// TestClickHouseSinkFailedBatchIsAtomic proves the whole-batch failure path
// of the writer: one bad value fails the batch BEFORE any block is sent, and
// nothing from it reaches the table — not the valid rows of the same batch.
// (The engine's single-partition single-block INSERT is the second half of
// the atomicity story, documented at §4; a cross-partition multi-part insert
// is where it weakens, by documented design. The native protocol does not
// re-validate value ranges server-side — decimal overflow lands silently —
// so client-side failure is the mechanism this sink can and must prove.)
func TestClickHouseSinkFailedBatchIsAtomic(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := chSink(t, ctx)
	defer func() { _ = s.Close() }()
	db := chStdDB(t)
	chDrop(t, db, "ch_atomic")

	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "amount", Type: core.ColumnType{Kind: core.KindDecimal, Precision: 4, Scale: 2, Nullable: true}},
	}}
	ref := core.TableRef{Source: "src.payments", Target: "lakehouse.ch_atomic", PrimaryKey: []string{"id"}}
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := chWriter(t, ctx, s, ref, nil)
	defer func() { _ = w.Close() }()

	b := change.Batch{Table: ref.Target, Position: "0/1",
		Upserts: []change.Change{
			{Op: change.OpInsert, Table: ref.Target, Key: []any{int64(1)},
				After: map[string]any{"id": int64(1), "amount": "12.34"}, IngestTS: time.Now()},
			// A structured object cannot be a decimal: coerce fails on this
			// row, after the good row is already buffered in the batch.
			{Op: change.OpInsert, Table: ref.Target, Key: []any{int64(2)},
				After: map[string]any{"id": int64(2), "amount": map[string]any{"bad": true}}, IngestTS: time.Now()},
		}}
	if err := w.Commit(ctx, toDPBatch(b)); err == nil {
		t.Fatal("batch with a non-encodable decimal: want error")
	}
	if got := chQueryInt(t, db, "SELECT count() FROM lakehouse.ch_atomic FINAL"); got != 0 {
		t.Fatalf("failed batch leaked %d rows — atomicity broken", got)
	}
}

// TestClickHouseSinkControlWithoutFinal is the §8.6 control: the SAME table,
// read WITHOUT FINAL, can legitimately disagree with the FINAL read until a
// background merge resolves the versions. This documents why every other
// test in this file asserts under FINAL (or after OPTIMIZE FINAL) — a no-
// FINAL assertion would measure the merge clock. Timing-dependent by nature,
// so it only checks that both reads are *reachable*, never that they differ.
func TestClickHouseSinkControlWithoutFinal(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := chSink(t, ctx)
	defer func() { _ = s.Close() }()
	db := chStdDB(t)
	chDrop(t, db, "ch_orders")

	schema, ref := chOrdersSchema()
	if err := s.EnsureTable(ctx, ref, schema, nil, core.CastPolicy{}, change.UpsertMode); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	w := chWriter(t, ctx, s, ref, chMeta)
	defer func() { _ = w.Close() }()

	b := change.Batch{Table: ref.Target, Position: "0/1",
		Upserts: []change.Change{{Op: change.OpInsert, Table: ref.Target, Key: []any{int64(9)},
			After: map[string]any{"id": int64(9), "v": "v1"}, IngestTS: time.Now()}}}
	if err := w.Commit(ctx, toDPBatch(b)); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Both reads must succeed; FINAL is the authoritative one.
	var vNoFinal, vFinal string
	if err := db.QueryRow("SELECT v FROM lakehouse.ch_orders WHERE id = 9").Scan(&vNoFinal); err != nil {
		t.Fatalf("no-final read: %v", err)
	}
	if err := db.QueryRow("SELECT v FROM lakehouse.ch_orders FINAL WHERE id = 9").Scan(&vFinal); err != nil {
		t.Fatalf("final read: %v", err)
	}
	if vFinal != "v1" {
		t.Fatalf("final read = %q, want v1", vFinal)
	}
	_ = vNoFinal // may or may not have merged yet — that is the point
}
