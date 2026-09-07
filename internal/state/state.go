// Package state provides a bbolt-backed key-value store for persisting
// CDC positions, snapshot metadata, and pipeline state.
package state

import (
	"encoding/json"
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"
)

var (
	positionBucket = []byte("positions")
	snapshotBucket = []byte("snapshots")
)

// Store is a bbolt-backed state store. Safe for concurrent use.
type Store struct {
	db   *bolt.DB
	mu   sync.RWMutex
	path string
}

// Open opens or creates a bbolt database at the given path.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("state: open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(positionBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(snapshotBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("state: create buckets: %w", err)
	}
	return &Store{db: db, path: path}, nil
}

// Position stores a CDC position for a given pipeline.
func (s *Store) Position(pipeline string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var val []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(positionBucket).Get([]byte(pipeline))
		if v != nil {
			val = append([]byte{}, v...)
		}
		return nil
	})
	return val, err
}

// SetPosition stores a CDC position for a given pipeline.
func (s *Store) SetPosition(pipeline string, position []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(positionBucket).Put([]byte(pipeline), position)
	})
}

// SnapshotMetadata is metadata about a completed snapshot for handoff.
type SnapshotMetadata struct {
	EndOffset string            `json:"endOffset"`
	Schema    map[string]string `json:"schema,omitempty"`
}

// GetSnapshot retrieves snapshot metadata for a given table.
func (s *Store) GetSnapshot(table string) (*SnapshotMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var meta SnapshotMetadata
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(snapshotBucket).Get([]byte(table))
		if v == nil {
			return nil
		}
		found = true
		return json.Unmarshal(v, &meta)
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &meta, nil
}

// SetSnapshot stores snapshot metadata for a given table.
func (s *Store) SetSnapshot(table string, meta *SnapshotMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("state: marshal snapshot: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(snapshotBucket).Put([]byte(table), b)
	})
}

// DeletePosition removes a position entry.
func (s *Store) DeletePosition(pipeline string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(positionBucket).Delete([]byte(pipeline))
	})
}

// DeleteSnapshot removes a snapshot entry.
func (s *Store) DeleteSnapshot(table string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(snapshotBucket).Delete([]byte(table))
	})
}

// Close closes the underlying database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}
