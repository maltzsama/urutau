package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/source"
)

// ProgressStore is where a table's snapshot progress is kept: the sink's
// table properties.
type ProgressStore interface {
	Properties(ctx context.Context, ref core.TableRef) (map[string]string, error)
	SetProperties(ctx context.Context, ref core.TableRef, props map[string]string) error
}

// PlanError is a failure to read or record a table's snapshot progress. Op
// says which.
type PlanError struct {
	Op  string
	Err error
}

func (e *PlanError) Error() string { return fmt.Sprintf("%s: %v", e.Op, e.Err) }
func (e *PlanError) Unwrap() error { return e.Err }

// RangePlan is the chunks a range snapshot covers and those still to do.
type RangePlan struct {
	Chunks []source.Chunk
	Todo   map[uint32]bool
	// Resumed says the plan continues an in-progress snapshot.
	Resumed bool
}

// PlanRange returns the chunks a table's snapshot covers and which of them
// are still to do. An in-progress snapshot recorded under the same partition
// ranges resumes: its bounds are reused and only its pending chunks remain.
// Otherwise the source's bounds are read afresh, and the new snapshot's
// progress — bounds, ranges and every chunk pending — is recorded before its
// first chunk; each window then commits the chunks still to do after it. A
// nil store keeps no progress: every snapshot starts afresh.
func PlanRange(ctx context.Context, store ProgressStore, chunker source.ChunkSource, ref core.TableRef, ranges []source.Chunk, now time.Time) (RangePlan, error) {
	layout := PartitionLayout(ranges)
	if store != nil {
		props, err := store.Properties(ctx, ref)
		if err != nil {
			return RangePlan{}, &PlanError{Op: "snapshot progress", Err: err}
		}
		sp, perr := ReadSnapshotProgress(props)
		if perr == nil && Resumable(sp) && props[PropSnapshotPartitions] == layout {
			all := Chunks(sp.Bounds)
			todo := map[uint32]bool{}
			for _, id := range sp.Pending {
				todo[id] = true
			}
			if len(todo) == 0 {
				if todo, err = AllChunkRefs(all, ranges); err != nil {
					return RangePlan{}, err
				}
			}
			return RangePlan{Chunks: all, Todo: todo, Resumed: true}, nil
		}
	}
	bounds, err := chunker.Bounds(ctx)
	if err != nil {
		return RangePlan{}, err
	}
	all := Chunks(bounds)
	todo, err := AllChunkRefs(all, ranges)
	if err != nil {
		return RangePlan{}, err
	}
	if store != nil {
		props, err := EncodeSnapshotProgress(&SnapshotProgress{
			State: StateInProgress, Bounds: bounds, Pending: SortedRefs(todo),
			Started: now.UTC().Format(time.RFC3339),
		})
		if err != nil {
			return RangePlan{}, &PlanError{Op: "record snapshot progress", Err: err}
		}
		props[PropSnapshotPartitions] = layout
		if err := store.SetProperties(ctx, ref, props); err != nil {
			return RangePlan{}, &PlanError{Op: "record snapshot progress", Err: err}
		}
	}
	return RangePlan{Chunks: all, Todo: todo}, nil
}
