package couchbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/faultinject"
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

// errCASMismatch is a lost optimistic-concurrency race: the control document
// changed between the read and the write, so the caller retries.
var errCASMismatch = errors.New("couchbase: control document changed concurrently")

// maxControlCASRetries bounds the optimistic retry loop.
const maxControlCASRetries = 16

// casStore is the optimistic-concurrency surface the control document needs
// in fast mode. It is optional: a transaction (txAttempt) serializes its own
// read-modify-write, so it does not implement it.
type casStore interface {
	// getCAS reads id and returns its CAS token; found=false when the
	// document does not exist (cas is then 0).
	getCAS(ctx context.Context, id string, out any) (found bool, cas uint64, err error)
	// replaceCAS writes doc only if the document still has the given CAS
	// (0 = insert); a concurrent change returns errCASMismatch.
	replaceCAS(ctx context.Context, id string, doc any, cas uint64) error
}

// mergeControl read-modify-writes the control document. When the store
// supports CAS (fast mode) it retries a lost race, so the worker's commit and
// the coordinator's snapshot bookkeeping cannot overwrite each other (issue
// #566). Otherwise (a transaction) the read-modify-write is already
// serialized. A nil result from apply writes nothing.
func mergeControl(ctx context.Context, kv kvStore, apply func(prev *controlDoc) *controlDoc) error {
	if cs, ok := kv.(casStore); ok {
		for attempt := 0; attempt < maxControlCASRetries; attempt++ {
			var prev controlDoc
			found, cas, err := cs.getCAS(ctx, controlKey, &prev)
			if err != nil {
				return err
			}
			var p *controlDoc
			if found {
				p = &prev
			}
			next := apply(p)
			if next == nil {
				return nil
			}
			if err := cs.replaceCAS(ctx, controlKey, next, cas); err != nil {
				if errors.Is(err, errCASMismatch) {
					continue // someone else wrote; re-read and re-merge
				}
				return err
			}
			return nil
		}
		return fmt.Errorf("couchbase: control document: %d CAS retries exhausted (heavy contention)", maxControlCASRetries)
	}
	prev, err := readControl(ctx, kv)
	if err != nil {
		return err
	}
	next := apply(prev)
	if next == nil {
		return nil
	}
	return kv.upsert(ctx, controlKey, next)
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
	// target is the qualified sink target (scope.collection), used only to
	// label the commit-boundary fault points with the table a crash hit.
	target string
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
	// owner is the worker group that owns this partition (ref.Owner); empty
	// in collapsed mode. When set, the control document records this
	// partition's position in its per-owner map (WK-001 C7).
	owner string
}

func newTableWriter(kv kvStore, txns txRunner, plan *tablePlan, owner string, now func() time.Time) *tableWriter {
	if now == nil {
		now = time.Now
	}
	return &tableWriter{kv: kv, txns: txns, plan: plan, owner: owner, now: now}
}

// docKey renders a change's primary key tuple as the document ID. JSON
// encoding is deterministic for identical values, unambiguous across types
// ("1" vs 1 vs 1.0), and never merges adjacent elements — the collision
// rules the worker's own keyString exists for. Data keys start with "[",
// so they cannot collide with the control document's "_urutau::" prefix.
func docKey(key []any) (string, error) {
	b, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("couchbase: encode key %v: %w", key, err)
	}
	if len(b) > maxKeyLen {
		return "", fmt.Errorf("couchbase: key %s exceeds %d bytes (%d) — shorten the primary key", b, maxKeyLen, len(b))
	}
	return string(b), nil
}

// Commit writes one collapsed batch. See the type comment for the two
// sequencing modes; Batch.Mode needs no branch here — append-mode batches
// arrive with upserts only (the worker rewrote or dropped the deletes).
// Column-oriented: the reader validates the wire schema once and the
// commit paths read per-row values straight from the record — no
// rowchange intermediate.
func (w *tableWriter) Commit(ctx context.Context, b *dataplane.Batch) error {
	reader, err := transport.NewBatchReader(b.Record, w.plan.pk)
	if err != nil {
		return fmt.Errorf("couchbase: %w", err)
	}
	info := batchInfo{
		Position:        string(b.Watermark),
		Seq:             b.Seq,
		Owner:           w.owner,
		SnapshotState:   b.SnapshotState,
		SnapshotPending: b.SnapshotPending,
	}

	if w.txns != nil {
		return w.commitAtomic(ctx, reader, info)
	}
	return w.commitFast(ctx, reader, info)
}

// batchInfo carries the batch-level control metadata the commit paths
// persist with the data: the position, the owning worker group, and the
// resumable-backfill state.
type batchInfo struct {
	Position        string
	Seq             uint64
	Owner           string
	SnapshotState   string
	SnapshotPending []uint32
}

// commitFast is the default path: every data mutation durably placed, then
// the control document. The control read-modify-write preserves properties
// the snapshot orchestrator recorded between commits.
func (w *tableWriter) commitFast(ctx context.Context, r *transport.BatchReader, info batchInfo) error {
	if err := applyData(ctx, w.kv, w.plan, r); err != nil {
		return err
	}
	// The fast path's only commit window: the data documents are durable, the
	// control document that carries the position is not yet written. The
	// fault point is armed by the commit-boundary matrix; in every other build
	// it is empty. A crash here must replay, never lose: the restart re-applies
	// the same key-addressed upserts/removes and then advances the position
	// (see the Couchbase section of commit-boundaries.md).
	faultinject.At(faultinject.WorkerCouchbaseDataBeforeControl,
		"table", w.plan.target, "seq", info.Seq, "position", info.Position)
	// The commit path and the coordinator's snapshot bookkeeping both write
	// this document; the CAS loop keeps one from discarding the other's
	// position or snapshot fields (issue #566).
	return mergeControl(ctx, w.kv, func(prev *controlDoc) *controlDoc {
		return controlWrite(prev, info, w.now())
	})
}

// commitAtomic runs the whole batch — data documents AND the control
// document — inside one distributed transaction. A failure anywhere rolls
// everything back; the position can never separate from its data.
func (w *tableWriter) commitAtomic(ctx context.Context, r *transport.BatchReader, info batchInfo) error {
	return w.txns.run(ctx, func(tx kvStore) error {
		if err := applyData(ctx, tx, w.plan, r); err != nil {
			return err
		}
		prev, err := readControl(ctx, tx)
		if err != nil {
			return err
		}
		return tx.upsert(ctx, controlKey, controlWrite(prev, info, w.now()))
	})
}

// applyData applies the batch's mutations through the given surface: one
// upsert per surviving row (document = data fields + reserved "_urutau"
// metadata sub-object), one remove per deleted key. A not-found remove is
// success — the replay story re-runs deletes that already landed.
func applyData(ctx context.Context, kv kvStore, plan *tablePlan, r *transport.BatchReader) error {
	for i := range r.NumRows() {
		key, err := docKey(r.Key(i))
		if err != nil {
			return err
		}
		if r.Op(i) == rowchange.OpDelete {
			err = kv.remove(ctx, key)
			if err == nil || errors.Is(err, errNotFound) {
				continue
			}
			return fmt.Errorf("remove key %s: %w", key, err)
		}
		data, meta, err := plan.buildDoc(r, i)
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
	return nil
}

// Close is a no-op: the writer holds no connection of its own — the Sink
// owns the cluster handle and its lifecycle.
func (w *tableWriter) Close() error { return nil }
