package snapshot

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
)

// SnapshotState is the durable state machine for the DBLog snapshot phase.
//
//	not_started ──► in_progress ──► complete
//	     │              │  ▲
//	     │              └──┘ (resume: read bounds + pending)
//	     └──► (adopt: skip directly to complete)
type SnapshotState string

const (
	StateNotStarted SnapshotState = "not_started"
	StateInProgress SnapshotState = "in_progress"
	StateComplete   SnapshotState = "complete"
)

// SnapshotProps are the Iceberg table properties that persist snapshot
// progress. They are written atomically with the commit that advances
// position, keeping crash safety by construction.
const (
	PropSnapshotState   = "cdc.snapshot.state"
	PropSnapshotBounds  = "cdc.snapshot.bounds"
	PropSnapshotPending = "cdc.snapshot.pending"
	PropSnapshotStarted = "cdc.snapshot.started"
)

// SnapshotProgress is the durable snapshot state read from Iceberg table
// properties. It tells the pipeline whether a snapshot is in progress,
// what the deterministic bounds are, and which chunks remain pending.
type SnapshotProgress struct {
	State   SnapshotState
	Bounds  [][]any  // deterministic bounds, calculated once at snapshot start
	Pending []uint32 // chunk IDs still to be processed
	Started string   // ISO 8601 timestamp of snapshot start
}

// IsComplete returns true when the snapshot phase is done.
func (sp *SnapshotProgress) IsComplete() bool {
	return sp.State == StateComplete || sp.State == ""
}

// HasPending returns true when there are chunks left to process.
func (sp *SnapshotProgress) HasPending() bool {
	return len(sp.Pending) > 0
}

// ReadSnapshotProgress reads snapshot state from Iceberg table properties.
func ReadSnapshotProgress(props iceberg.Properties) (*SnapshotProgress, error) {
	sp := &SnapshotProgress{
		State: StateComplete, // default: no snapshot in progress
	}
	state := props[PropSnapshotState]
	if state == "" || state == string(StateComplete) {
		return sp, nil
	}
	sp.State = SnapshotState(state)
	if sp.State != StateNotStarted && sp.State != StateInProgress {
		return nil, fmt.Errorf("snapshot: unknown state %q", state)
	}
	if sp.State == StateNotStarted {
		return sp, nil
	}
	// Parse bounds.
	boundsJSON := props[PropSnapshotBounds]
	if boundsJSON != "" {
		var bounds [][]json.RawMessage
		if err := json.Unmarshal([]byte(boundsJSON), &bounds); err != nil {
			return nil, fmt.Errorf("snapshot: parse bounds: %w", err)
		}
		sp.Bounds = make([][]any, len(bounds))
		for i, b := range bounds {
			row := make([]any, len(b))
			for j, cell := range b {
				var v any
				if err := json.Unmarshal(cell, &v); err != nil {
					return nil, fmt.Errorf("snapshot: parse bound[%d][%d]: %w", i, j, err)
				}
				row[j] = v
			}
			sp.Bounds[i] = row
		}
	}
	// Parse pending.
	pendingJSON := props[PropSnapshotPending]
	if pendingJSON != "" {
		var pending []uint32
		if err := json.Unmarshal([]byte(pendingJSON), &pending); err != nil {
			return nil, fmt.Errorf("snapshot: parse pending: %w", err)
		}
		sp.Pending = pending
	}
	sp.Started = props[PropSnapshotStarted]
	return sp, nil
}

// EncodeSnapshotProgress serializes snapshot progress into Iceberg
// table properties.
func EncodeSnapshotProgress(sp *SnapshotProgress) iceberg.Properties {
	props := iceberg.Properties{
		PropSnapshotState: string(sp.State),
	}
	if sp.State == StateComplete || sp.State == StateNotStarted {
		return props
	}
	if len(sp.Bounds) > 0 {
		boundsJSON, err := json.Marshal(sp.Bounds)
		if err != nil {
			return props // best effort — bounds are informational
		}
		props[PropSnapshotBounds] = string(boundsJSON)
	}
	if len(sp.Pending) > 0 {
		pendingJSON, err := json.Marshal(sp.Pending)
		if err != nil {
			return props
		}
		props[PropSnapshotPending] = string(pendingJSON)
	}
	if sp.Started != "" {
		props[PropSnapshotStarted] = sp.Started
	}
	return props
}

// EncodePending serializes only the pending list — for atomic commit
// that removes one chunk from pending without rewriting the entire state.
func EncodePending(pending []uint32) string {
	if len(pending) == 0 {
		return "[]"
	}
	b, err := json.Marshal(pending)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// PendingFromProperty decodes a pending list from a table property.
func PendingFromProperty(s string) []uint32 {
	if s == "" || s == "[]" {
		return nil
	}
	var pending []uint32
	if err := json.Unmarshal([]byte(s), &pending); err != nil {
		return nil
	}
	return pending
}

// RemoveFromPending removes chunkID from the pending list and returns
// the new sorted list.
func RemoveFromPending(pending []uint32, chunkID uint32) []uint32 {
	out := make([]uint32, 0, len(pending))
	for _, id := range pending {
		if id != chunkID {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SnapshotStateForTable reads the snapshot progress for a given table
// from Iceberg properties. Returns a completed snapshot if no state is
// stored.
func SnapshotStateForTable(props iceberg.Properties) (*SnapshotProgress, error) {
	return ReadSnapshotProgress(props)
}

// FormatBoundsForLog renders bounds as a compact log string: "N chunks [0, 1, ..., N-1]".
func FormatBoundsForLog(bounds [][]any) string {
	return fmt.Sprintf("%d chunks", len(bounds))
}

// PendingIDs renders a pending list as a compact string: "[3,4,5]".
func PendingIDs(pending []uint32) string {
	parts := make([]string, len(pending))
	for i, id := range pending {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// ParseISO8601 parses an ISO 8601 timestamp string.
func ParseISO8601(s string) (time.Time, error) {
	return time.Parse(time.RFC3339, s)
}
