package snapshot

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
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

// ReadSnapshotProgress reads snapshot state from table properties. The
// plain map type keeps this source-side package free of the sink library
// (the architecture wall); iceberg.Properties is assignable to it.
func ReadSnapshotProgress(props map[string]string) (*SnapshotProgress, error) {
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
	// Parse bounds. Cells are kind-tagged: a plain JSON round trip would
	// turn every number into float64, and the chunk SELECT binds these
	// values back into SQL comparisons — the types must survive.
	boundsJSON := props[PropSnapshotBounds]
	if boundsJSON != "" {
		var raw [][]any
		if err := json.Unmarshal([]byte(boundsJSON), &raw); err != nil {
			return nil, fmt.Errorf("snapshot: parse bounds: %w", err)
		}
		sp.Bounds = make([][]any, len(raw))
		for i, b := range raw {
			row := make([]any, len(b))
			for j, cell := range b {
				v, ok := decodeBoundCell(cell)
				if !ok {
					return nil, fmt.Errorf("snapshot: bound[%d][%d]: unsupported value %v", i, j, cell)
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

// EncodeSnapshotProgress serializes snapshot progress into table
// properties (the caller passes it to the sink's property writer).
func EncodeSnapshotProgress(sp *SnapshotProgress) map[string]string {
	props := map[string]string{
		PropSnapshotState: string(sp.State),
	}
	if sp.State == StateComplete || sp.State == StateNotStarted {
		return props
	}
	if len(sp.Bounds) > 0 {
		tagged := make([][]any, len(sp.Bounds))
		for i, b := range sp.Bounds {
			row := make([]any, len(b))
			for j, cell := range b {
				v, ok := encodeBoundCell(cell)
				if !ok {
					return props // unsupported type: bounds stay absent, resume recalculates
				}
				row[j] = v
			}
			tagged[i] = row
		}
		boundsJSON, err := json.Marshal(tagged)
		if err != nil {
			return props
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
// from its properties. Returns a completed snapshot if no state is stored.
func SnapshotStateForTable(props map[string]string) (*SnapshotProgress, error) {
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

// encodeBoundCell tags a bound value with its kind for JSON storage. The
// tags exist because JSON numbers decode as float64 and a bounds round
// trip must return the exact types the chunker produced — these values
// are bound back into SQL comparisons on resume. Integer variants (int,
// int32, uint64) and raw []byte cells are folded into their canonical
// tagged forms; an unhandled type returns false and the caller drops the
// bounds rather than persist something unround-trippable.
func encodeBoundCell(v any) (any, bool) {
	switch t := v.(type) {
	case nil:
		return nil, true
	case int:
		return map[string]any{"i": strconv.FormatInt(int64(t), 10)}, true
	case int32:
		return map[string]any{"i": strconv.FormatInt(int64(t), 10)}, true
	case int64:
		return map[string]any{"i": strconv.FormatInt(t, 10)}, true
	case uint64:
		return map[string]any{"u": strconv.FormatUint(t, 10)}, true
	case float64:
		return map[string]any{"f": t}, true
	case string:
		return map[string]any{"s": t}, true
	case []byte:
		return map[string]any{"s": string(t)}, true
	case bool:
		return map[string]any{"b": t}, true
	case time.Time:
		return map[string]any{"t": t.UTC().Format(time.RFC3339Nano)}, true
	default:
		return nil, false
	}
}

// decodeBoundCell reverses encodeBoundCell.
func decodeBoundCell(v any) (any, bool) {
	if v == nil {
		return nil, true
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	if s, ok := m["i"].(string); ok {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, false
		}
		return n, true
	}
	if s, ok := m["u"].(string); ok {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, false
		}
		return n, true
	}
	if f, ok := m["f"].(float64); ok {
		return f, true
	}
	if s, ok := m["s"].(string); ok {
		return s, true
	}
	if b, ok := m["b"].(bool); ok {
		return b, true
	}
	if s, ok := m["t"].(string); ok {
		ts, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, false
		}
		return ts, true
	}
	return nil, false
}
