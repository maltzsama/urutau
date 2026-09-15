package couchbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/position"
)

// controlKey is the single control document per collection: it carries the
// committed CDC position and the snapshot state machine. It lives in the
// SAME collection as the data (drop the collection, drop the state — the
// property the e2e isolation leans on) and its key cannot collide with a
// data document: data keys are JSON arrays ("[...]"), this starts with "_".
const controlKey = "_urutau::position"

// controlDoc is the JSON body of the control document. DOCUMENTED
// CONTRACT: like the ClickHouse progress table, it is a resume record,
// NEVER a source of data correctness — losing it costs a re-snapshot, never
// a wrong row. The invariant that keeps it honest is the write ORDER:
// position only advances after the batch's data is durably in place.
type controlDoc struct {
	Position   string            `json:"position,omitempty"`
	Positions  map[string]string `json:"positions,omitempty"` // owner → position (WK-001 C7)
	UpdatedAt  time.Time         `json:"updated_at,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

// controlWrite merges a committed batch into the control document. The
// batch's snapshot state (if any) travels WITH the position in this single
// document write — one KV mutation, inherently atomic, the guarantee the
// Iceberg sink spends a two-phase commit on and ClickHouse gets from the
// single-insert-per-partition rule. Existing properties are preserved:
// the control document is the merge point of the commit path and the
// snapshot orchestrator's SetProperties calls.
//
// The per-partition map (WK-001 C7) holds one position per worker group.
// Under commitMode: atomic the read-modify-write runs inside the
// transaction, so two partitions cannot lose each other's entry. The scalar
// Position is kept alongside for compatibility (workers==1, and a downgrade
// that only reads it).
func controlWrite(prev *controlDoc, info batchInfo, now time.Time) *controlDoc {
	ctrl := &controlDoc{Properties: map[string]string{}}
	if prev != nil {
		if prev.Properties != nil {
			ctrl.Properties = prev.Properties
		}
		if prev.Positions != nil {
			ctrl.Positions = prev.Positions
		}
	}
	ctrl.Position = info.Position
	if info.Owner != "" {
		if ctrl.Positions == nil {
			ctrl.Positions = map[string]string{}
		}
		ctrl.Positions[info.Owner] = info.Position
	}
	ctrl.UpdatedAt = now
	if info.SnapshotState != "" {
		ctrl.Properties[snapshot.PropSnapshotState] = info.SnapshotState
	}
	if info.SnapshotPending != nil {
		ctrl.Properties[snapshot.PropSnapshotPending] = snapshot.EncodePending(info.SnapshotPending)
	}
	return ctrl
}

// readControl fetches the control document. not-found yields (nil, nil):
// never written is a normal state, not an error.
func readControl(ctx context.Context, kv kvStore) (*controlDoc, error) {
	var doc controlDoc
	found, err := kv.get(ctx, controlKey, &doc)
	if err != nil {
		return nil, fmt.Errorf("couchbase: read control: %w", err)
	}
	if !found {
		return nil, nil
	}
	return &doc, nil
}

// positionOf reads the committed position for one collection from the
// control document — one Get, O(1), no scan, no aggregation. When the
// per-partition map is present (WK-001 C7) it returns the MinSafe across
// owners, so a lagging partition is never resumed past; otherwise the scalar
// (a pre-C7 document, or workers==1). Empty string means the collection was
// never written and needs the snapshot. When ownerCount > 1 and the map is
// incomplete, it returns "" — no safe minimum — rather than a MinSafe over a
// subset that could advance past the missing owner.
func positionOf(ctx context.Context, kv kvStore, sourceKind string, ownerCount int) (string, error) {
	doc, err := readControl(ctx, kv)
	if err != nil || doc == nil {
		return "", err
	}
	if len(doc.Positions) > 0 {
		if ownerCount > 1 && len(doc.Positions) < ownerCount {
			return "", nil
		}
		parsed := make([]position.Position, 0, len(doc.Positions))
		for owner, p := range doc.Positions {
			pp, perr := position.Parse(sourceKind, p)
			if perr != nil {
				return "", fmt.Errorf("couchbase: position owner %s: %w", owner, perr)
			}
			parsed = append(parsed, pp)
		}
		best, err := position.MinSafe(parsed)
		if err != nil {
			return "", err
		}
		if best != nil {
			return best.String(), nil
		}
	}
	return doc.Position, nil
}

// seedPositions records a baseline for every owner in owners that has no
// committed position, using the minimum of the owners that do. A fresh table
// (no owner committed) is left alone — it snapshots. Called at boot, before
// any worker commits, so the read-modify-write is safe.
func seedPositions(ctx context.Context, kv kvStore, sourceKind string, owners []string) error {
	if len(owners) < 2 {
		return nil
	}
	doc, err := readControl(ctx, kv)
	if err != nil {
		return err
	}
	if doc == nil || len(doc.Positions) == 0 {
		return nil // fresh table: nothing to seed
	}
	parsed := make([]position.Position, 0, len(doc.Positions))
	for _, p := range doc.Positions {
		pp, err := position.Parse(sourceKind, p)
		if err != nil {
			return fmt.Errorf("couchbase: seed: %w", err)
		}
		parsed = append(parsed, pp)
	}
	best, err := position.MinSafe(parsed)
	if err != nil {
		return err
	}
	baseline := best.String()

	merged := make(map[string]string, len(doc.Positions)+len(owners))
	for o, p := range doc.Positions {
		merged[o] = p
	}
	changed := false
	for _, owner := range owners {
		if _, ok := merged[owner]; ok {
			continue
		}
		merged[owner] = baseline
		changed = true
	}
	if !changed {
		return nil
	}
	doc.Positions = merged
	return kv.upsert(ctx, controlKey, doc)
}

// propertiesOf reads the property map (snapshot progress resume). Missing
// control document yields an empty map with no error — the contract the
// PropertyGetter declares.
func propertiesOf(ctx context.Context, kv kvStore) (map[string]string, error) {
	doc, err := readControl(ctx, kv)
	if err != nil || doc == nil {
		return map[string]string{}, err
	}
	if doc.Properties == nil {
		return map[string]string{}, nil
	}
	return doc.Properties, nil
}

// setProperties merges orchestrator bookkeeping into the control document
// (read-modify-write). The snapshot orchestrator is the single writer of
// properties per table, so no CAS loop is needed; the writes are durable
// at the sink's configured durability level.
func setProperties(ctx context.Context, kv kvStore, ref core.TableRef, props map[string]string, now func() time.Time) error {
	if len(props) == 0 {
		return nil
	}
	if ref.Target == "" {
		return errors.New("couchbase: properties: target is required")
	}
	prev, err := readControl(ctx, kv)
	if err != nil {
		return err
	}
	ctrl := &controlDoc{Properties: map[string]string{}}
	if prev != nil {
		ctrl = prev
		if ctrl.Properties == nil {
			ctrl.Properties = map[string]string{}
		}
	}
	for k, v := range props {
		ctrl.Properties[k] = v
	}
	ctrl.UpdatedAt = now()
	return kv.upsert(ctx, controlKey, ctrl)
}
