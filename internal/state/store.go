// Package state provides a bbolt-backed store for CDC positions and
// snapshot handoff metadata.
//
// Layout:
//
//	positions/            (top-level bucket)
//	  └─ <pipeline>/      (bucket per pipeline)
//	      └─ <table> = {"offset":"<b64>","committedAtNs":...}
//	snapshots/            (top-level bucket)
//	  └─ <pipeline>/
//	      └─ <table> = {"endOffset":"<b64>","schema":"<b64 IPC>","capturedAt":...}
//
// Nested buckets instead of concatenated keys: no collision encoding
// needed, and per-pipeline scans and pipeline-wide deletes come free.
//
// DURABILITY: bbolt fsyncs on every commit by default and this store
// keeps it. The committed position IS the checkpoint (contract I2,
// CR-069 §3.3): bolt.NoSync risks file corruption on power loss — not
// just losing the last commit — and is FORBIDDEN on this file. If commit
// throughput ever matters, the lever is db.Batch. Never NoSync.
//
// Offsets are opaque cookies (contract §8.2): this store never inspects
// or compares them.
//
// ROLE AND ARBITRATION (audit §3 — the decision, written down):
//
// The project's default is "the position lives in the sink, atomic with the
// data" (design §17.3) — no parallel store that can diverge. This store is
// the EXCEPTION, and only for one case: external plugin sinks that cannot
// persist a position themselves. For those, Urutau commits the offset here
// AFTER the sink confirms a flush (contract §9) — never before, the same
// ordering invariant as the Iceberg path.
//
// Arbitrage rule when both exist for the same pipeline/table:
//
//  1. If the sink reports a committed position (sink.Position != ""),
//     that is the source of truth — it was written atomically with the
//     data. The store's copy, if any, is ignored.
//  2. Only when the sink has no position capability (plugin sinks) does
//     the store become authoritative.
//  3. On resume, the sink position wins; the store is consulted only as
//     the fallback.
//
// The two are never merged. One wins per table. This rule keeps the
// single-source-of-truth property the project is built on.
//
// STATUS: this package is NOT yet wired into the runtime. It exists for
// the plugin-sink position path; wiring it (or removing it) is a separate
// change. Until then no code path can give the false impression that the
// problem is solved.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
	bolt_errors "go.etcd.io/bbolt/errors"
)

var (
	// ErrNotFound means no state exists for the key — the caller then
	// starts from snapshot / earliest available (contract §6: "fromOffset
	// absent").
	ErrNotFound = errors.New("state: not found")

	positionBucket = []byte("positions")
	snapshotBucket = []byte("snapshots")
)

// Store is a bbolt-backed state store. Safe for concurrent use — bbolt
// itself is the synchronization boundary (one writer, N non-blocking
// readers via MVCC). No mutex here: one would serialize reads against
// writes, which is precisely what bbolt's MVCC exists to avoid.
type Store struct {
	db   *bolt.DB
	path string
}

// Open opens or creates the database at path.
//
// Timeout is load-bearing: without it, a second Urutau instance (or a
// stale file lock) blocks Open FOREVER — a zombie with no error.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("state: open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{positionBucket, snapshotBucket} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("state: create bucket %s: %w", b, err)
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("state: init: %w", err)
	}
	return &Store{db: db, path: path}, nil
}

func (s *Store) Path() string { return s.path }

// positionValue is the stored form of a committed position. JSON with
// base64 offsets: one marshal per table per batch (not per row), and it
// buys human-readable bbolt dumps — "why is this pipeline stuck since
// Tuesday" becomes answerable from the file alone.
type positionValue struct {
	Offset        []byte `json:"offset"`
	CommittedAtNs int64  `json:"committedAtNs"`
}

// GetPosition returns the committed position for (pipeline, table) —
// the watermark of CR-069 §3.3, the fromOffset of contract §6.
// ErrNotFound: no position committed yet; the caller starts from
// snapshot / earliest available.
func (s *Store) GetPosition(pipeline, table string) ([]byte, error) {
	var offset []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := subBucket(tx, positionBucket, pipeline)
		if b == nil {
			return ErrNotFound
		}
		v := b.Get([]byte(table))
		if v == nil {
			return ErrNotFound
		}
		var val positionValue
		if err := json.Unmarshal(v, &val); err != nil {
			return fmt.Errorf("state: corrupt position for %q: %w", table, err)
		}
		if len(val.Offset) == 0 {
			return fmt.Errorf("state: empty stored offset for %q", table)
		}
		offset = val.Offset
		return nil
	})
	if err != nil {
		return nil, err
	}
	return offset, nil
}

// Positions returns every committed position of a pipeline — startup
// resume and status reporting.
func (s *Store) Positions(pipeline string) (map[string][]byte, error) {
	positions := map[string][]byte{}
	err := s.db.View(func(tx *bolt.Tx) error {
		b := subBucket(tx, positionBucket, pipeline)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			var val positionValue
			if err := json.Unmarshal(v, &val); err != nil {
				return fmt.Errorf("state: corrupt position for %q: %w", k, err)
			}
			positions[string(k)] = val.Offset
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return positions, nil
}

// CommitPositions atomically commits the position of every table in the
// map — ONE bbolt transaction, all-or-nothing. This is the ONLY write
// path for positions: a crash mid-commit leaves the previous state
// intact (PR-5 acceptance, contract I2).
//
// The one call site that matters (supervisor, PR-7/8):
//
//	if err := sink.Flush(ctx); err != nil { ... }        // no commit
//	if err := st.CommitPositions(p, watermarks); err != nil { ... }
//	// watermarks: map[table][]byte — Batch.Watermark of CR-069 §3.3,
//	// captured at RECEIVE time, before any transform.
func (s *Store) CommitPositions(pipeline string, positions map[string][]byte) error {
	if pipeline == "" {
		return errors.New("state: pipeline required")
	}
	if len(positions) == 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.Bucket(positionBucket).CreateBucketIfNotExists([]byte(pipeline))
		if err != nil {
			return fmt.Errorf("state: pipeline bucket: %w", err)
		}
		now := time.Now().UnixNano()
		for table, offset := range positions {
			if table == "" {
				return fmt.Errorf("state: empty table name in commit")
			}
			if len(offset) == 0 {
				return fmt.Errorf("state: empty offset for table %q", table)
			}
			v, err := json.Marshal(positionValue{Offset: offset, CommittedAtNs: now})
			if err != nil {
				return fmt.Errorf("state: marshal position for %q: %w", table, err)
			}
			if err := b.Put([]byte(table), v); err != nil {
				return fmt.Errorf("state: put position for %q: %w", table, err)
			}
		}
		return nil
	})
}

// SnapshotMetadata is the snapshot→CDC handoff point for (pipeline,
// table) — contract §7. Written BEFORE the snapshot is consumed (crash
// mid-snapshot re-runs from this point), not after completion.
type SnapshotMetadata struct {
	EndOffset  []byte    `json:"endOffset"`
	Schema     []byte    `json:"schema,omitempty"`
	CapturedAt time.Time `json:"capturedAt"`
}

// SetSnapshot persists the handoff metadata for (pipeline, table).
func (s *Store) SetSnapshot(pipeline, table string, meta *SnapshotMetadata) error {
	if meta == nil || len(meta.EndOffset) == 0 {
		return errors.New("state: snapshot metadata requires a non-empty endOffset")
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("state: marshal snapshot: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		pb, err := tx.Bucket(snapshotBucket).CreateBucketIfNotExists([]byte(pipeline))
		if err != nil {
			return fmt.Errorf("state: pipeline bucket: %w", err)
		}
		return pb.Put([]byte(table), b)
	})
}

// GetSnapshot retrieves the handoff metadata for (pipeline, table).
// ErrNotFound: no snapshot has been started for this table.
func (s *Store) GetSnapshot(pipeline, table string) (*SnapshotMetadata, error) {
	var meta *SnapshotMetadata
	err := s.db.View(func(tx *bolt.Tx) error {
		b := subBucket(tx, snapshotBucket, pipeline)
		if b == nil {
			return ErrNotFound
		}
		v := b.Get([]byte(table))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &meta)
	})
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// DeletePipeline removes ALL state of a pipeline — positions and
// snapshots — in one transaction. `urutau pipeline reset` territory.
func (s *Store) DeletePipeline(pipeline string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, top := range [][]byte{positionBucket, snapshotBucket} {
			if err := tx.Bucket(top).DeleteBucket([]byte(pipeline)); err != nil {
				if errors.Is(err, bolt_errors.ErrBucketNotFound) {
					continue
				}
				return fmt.Errorf("state: delete pipeline: %w", err)
			}
		}
		return nil
	})
}

// Close closes the database. In-flight transactions complete first
// (bbolt semantics); calls after Close fail with ErrDatabaseNotOpen.
func (s *Store) Close() error {
	return s.db.Close()
}

// subBucket returns the pipeline's bucket under top, or nil when the
// pipeline has no state yet.
func subBucket(tx *bolt.Tx, top []byte, pipeline string) *bolt.Bucket {
	pb := tx.Bucket(top)
	if pb == nil {
		return nil
	}
	return pb.Bucket([]byte(pipeline))
}
