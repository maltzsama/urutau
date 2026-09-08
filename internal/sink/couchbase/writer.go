package couchbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// maxKeyLen is Couchbase's document ID ceiling. A primary key tuple that
// renders longer than this cannot be addressed, and silently truncating it
// would make two different rows one document — so it is a loud error.
const maxKeyLen = 250

// errNotFound is the seam-level not-found. The real KV adapter translates
// gocb's ErrDocumentNotFound into it so the writer (and the unit tests'
// fake) share one sentinel instead of importing gocb semantics.
var errNotFound = errors.New("couchbase: document not found")

// kvStore is the read/write seam the writer commits through: the minimal
// surface of gocb's Collection the sink needs. The transaction attempt
// context satisfies the same interface, so applyData writes through a
// plain collection in fast mode and through the attempt context in atomic
// mode without branching. Small enough that the unit tests fake it and
// inject failures at exact points (a data write lost, the control write
// lost) to prove the recovery contract.
type kvStore interface {
	upsert(ctx context.Context, id string, doc any) error
	remove(ctx context.Context, id string) error
	get(ctx context.Context, id string, out any) (found bool, err error)
}

// txRunner executes fn atomically: either every mutation lands or none.
type txRunner interface {
	run(ctx context.Context, fn func(tx kvStore) error) error
}

// tablePlan is a table's resolved write plan: the schema EnsureTable
// resolved (metadata columns included — the writer walks it to know WHAT
// to project, the meta map tells it WHERE each lands), plus the cast plan
// and key order. It is immutable after EnsureTable.
type tablePlan struct {
	schema      core.Schema
	meta        map[string]core.MetadataColumn
	cast        core.CastPolicy
	pk          []string
	sourceTable string
}

// tableWriter commits one table's batches. Two sequencing modes, one
// invariant (the position must never advance past durably written data):
//
//   - fast (default): data documents first, the position-carrying control
//     document LAST. A crash in between leaves the position un-advanced;
//     the restart replays the batch, and because every mutation is
//     upsert-by-key (or remove-by-key) the replay rewrites the same
//     documents — reprocessing cost, never duplication.
//   - atomic: everything inside one distributed transaction, so a failure
//     mid-batch leaves no trace at all.
type tableWriter struct {
	kv   kvStore
	txns txRunner // nil in fast mode
	plan *tablePlan
	now  func() time.Time
}

func newTableWriter(kv kvStore, txns txRunner, plan *tablePlan, now func() time.Time) *tableWriter {
	if now == nil {
		now = time.Now
	}
	return &tableWriter{kv: kv, txns: txns, plan: plan, now: now}
}

// docKey renders a change's primary key tuple as the document ID. JSON
// encoding is deterministic for identical values, unambiguous across types
// ("1" vs 1 vs 1.0), and never merges adjacent elements — the collision
// rules the worker's own keyString exists for. Data keys start with "[",
// so they cannot collide with the control document's "_urutau::" prefix.
func docKey(c rowchange.Change) (string, error) {
	b, err := json.Marshal(c.Key)
	if err != nil {
		return "", fmt.Errorf("couchbase: encode key %v: %w", c.Key, err)
	}
	if len(b) > maxKeyLen {
		return "", fmt.Errorf("couchbase: key %s exceeds %d bytes (%d) — shorten the primary key", b, maxKeyLen, len(b))
	}
	return string(b), nil
}

// Commit writes one collapsed batch. See the type comment for the two
// sequencing modes; Batch.Mode needs no branch here — append-mode batches
// arrive with upserts only (the worker rewrote or dropped the deletes).
//
// QUARANTINE: the RecordBatch→rowchange.Batch unpack is a bridge that dies
// when the Couchbase sink consumes RecordBatch directly.
func (w *tableWriter) Commit(ctx context.Context, b *dataplane.Batch) error {
	// Unpack the columnar batch back to row-oriented changes.
	// QUARANTINE: this bridge dies when the Couchbase sink consumes RecordBatch directly.
	cb, err := w.unpackBatch(b)
	if err != nil {
		return fmt.Errorf("couchbase: unpack: %w", err)
	}

	if w.txns != nil {
		return w.commitAtomic(ctx, cb)
	}
	return w.commitFast(ctx, cb)
}

// unpackBatch converts a columnar dataplane.Batch back to a row-oriented
// rowchange.Batch. QUARANTINE: dies when the Couchbase sink consumes
// RecordBatch directly.
func (w *tableWriter) unpackBatch(b *dataplane.Batch) (rowchange.Batch, error) {
	if b.Record == nil || b.Record.NumRows() == 0 {
		return rowchange.Batch{Table: b.Table, Position: string(b.Watermark)}, nil
	}

	rows, _, err := transport.DecodeBatch(b.Record, nil, w.plan.pk)
	if err != nil {
		return rowchange.Batch{}, err
	}

	var upserts, deletes []rowchange.Change
	for _, r := range rows {
		switch r.Op {
		case rowchange.OpDelete:
			deletes = append(deletes, r)
		default:
			upserts = append(upserts, r)
		}
	}

	return rowchange.Batch{
		Table:           b.Table,
		Upserts:         upserts,
		Deletes:         deletes,
		Position:        string(b.Watermark),
		Mode:            rowchange.UpsertMode,
		SnapshotState:   b.SnapshotState,
		SnapshotPending: b.SnapshotPending,
	}, nil
}

// commitFast is the default path: every data mutation durably placed, then
// the control document. The control read-modify-write preserves properties
// the snapshot orchestrator recorded between commits.
func (w *tableWriter) commitFast(ctx context.Context, b rowchange.Batch) error {
	if err := applyData(ctx, w.kv, w.plan, b); err != nil {
		return err
	}
	prev, err := readControl(ctx, w.kv)
	if err != nil {
		return err
	}
	return w.kv.upsert(ctx, controlKey, controlWrite(prev, b, w.now()))
}

// commitAtomic runs the whole batch — data documents AND the control
// document — inside one distributed transaction. A failure anywhere rolls
// everything back; the position can never separate from its data.
func (w *tableWriter) commitAtomic(ctx context.Context, b rowchange.Batch) error {
	return w.txns.run(ctx, func(tx kvStore) error {
		if err := applyData(ctx, tx, w.plan, b); err != nil {
			return err
		}
		prev, err := readControl(ctx, tx)
		if err != nil {
			return err
		}
		return tx.upsert(ctx, controlKey, controlWrite(prev, b, w.now()))
	})
}

// applyData applies the batch's mutations through the given surface: one
// upsert per surviving row (document = data fields + reserved "_urutau"
// metadata sub-object), one remove per deleted key. A not-found remove is
// success — the replay story re-runs deletes that already landed.
func applyData(ctx context.Context, kv kvStore, plan *tablePlan, b rowchange.Batch) error {
	for _, u := range b.Upserts {
		key, err := docKey(u)
		if err != nil {
			return err
		}
		data, meta, err := plan.buildDoc(u)
		if err != nil {
			return fmt.Errorf("upsert key %s: %w", key, err)
		}
		if len(meta) > 0 {
			data[reservedField] = meta
		}
		if err := kv.upsert(ctx, key, data); err != nil {
			return fmt.Errorf("upsert key %s: %w", key, err)
		}
	}
	for _, d := range b.Deletes {
		key, err := docKey(d)
		if err != nil {
			return err
		}
		err = kv.remove(ctx, key)
		if err == nil || errors.Is(err, errNotFound) {
			continue
		}
		return fmt.Errorf("remove key %s: %w", key, err)
	}
	return nil
}

// Close is a no-op: the writer holds no connection of its own — the Sink
// owns the cluster handle and its lifecycle.
func (w *tableWriter) Close() error { return nil }
