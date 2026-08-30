// Package change defines the change event that flows from source decoding
// to sink writing: one row-level operation with its before/after images,
// primary key, and source position.
package change

import "time"

type Op uint8

const (
	OpInsert Op = iota
	OpUpdate
	OpDelete
)

func (o Op) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Change mirrors a single row-level event. Key holds the primary key values
// in table-spec order; After is nil for deletes and Before is only carried
// when needed (mutable-column filters). Position is the source coordinate
// (GTID | LSN) the event came from; CommitTS is the source commit timestamp.
type Change struct {
	Op       Op
	Table    string // target table ("namespace.name")
	Key      []any
	After    map[string]any
	Before   map[string]any
	Position string
	CommitTS time.Time
	// Window tags the change as part of a DBLog snapshot window. Nil for
	// plain stream events.
	Window *Window
}

// Window carries DBLog snapshot-window signaling on a change. The runner
// tags live events that fall inside [low, high] with InWindow so the
// batcher discards the superseded snapshot row, and emits a Closes marker
// (with no row payload) once the reader has provably caught up past high.
type Window struct {
	ChunkID  uint32
	InWindow bool
	Closes   bool
}

// Collapsed is the reduced state of a batch after per-key collapse: the last
// operation for each key wins.
type Collapsed struct {
	// Upserts holds the surviving rows: the last operation per key is an
	// insert or an update, so the row must exist with its final value and
	// older versions must be equality-deleted by the sink.
	Upserts []Change

	// Deletes holds keys whose last operation is a delete: equality delete
	// only — a data row must never be emitted for them, since a delete file
	// committed together with the data it means to remove is unreliable
	// across Iceberg implementations (see the spike finding).
	Deletes []Change
}

// Keys returns every primary key in the batch: upserts (whose older versions
// must be deleted) followed by pure deletes.
func (c Collapsed) Keys() [][]any {
	keys := make([][]any, 0, len(c.Upserts)+len(c.Deletes))
	for _, u := range c.Upserts {
		keys = append(keys, u.Key)
	}
	for _, d := range c.Deletes {
		keys = append(keys, d.Key)
	}
	return keys
}

// Batch is the unit handed to a committer: the collapsed changes of one
// table plus the source position reached when the batch closed.
type Batch struct {
	Table    string
	Upserts  []Change
	Deletes  []Change
	Position string
}
