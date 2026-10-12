package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/source"
)

// propStore is a table's properties in memory.
type propStore struct {
	props   map[string]string
	readErr error
	setErr  error
	sets    int
}

func (s *propStore) Properties(context.Context, core.TableRef) (map[string]string, error) {
	return s.props, s.readErr
}

func (s *propStore) SetProperties(_ context.Context, _ core.TableRef, props map[string]string) error {
	if s.setErr != nil {
		return s.setErr
	}
	s.sets++
	if s.props == nil {
		s.props = map[string]string{}
	}
	for k, v := range props {
		s.props[k] = v
	}
	return nil
}

func planSource() *fakeSource {
	f := &fakeSource{}
	for id := int64(1); id <= 6; id++ {
		f.rows = append(f.rows, map[string]any{"id": id})
	}
	return f
}

var planRef = core.TableRef{Source: "app.orders", Target: "raw.orders"}

// A fresh snapshot plans every chunk and records its progress, with the
// partition ranges it was chunked for, before its first chunk.
func TestPlanRangeRecordsAFreshSnapshot(t *testing.T) {
	store := &propStore{}
	ranges := []source.Chunk{{}}
	plan, err := PlanRange(context.Background(), store, planSource(), planRef, ranges, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Resumed || len(plan.Chunks) == 0 || len(plan.Todo) != len(plan.Chunks) {
		t.Fatalf("plan = %+v, want every chunk to do and not resumed", plan)
	}
	if store.sets != 1 || store.props[PropSnapshotPartitions] != PartitionLayout(ranges) {
		t.Fatalf("progress recorded %d time(s) with partitions %q", store.sets, store.props[PropSnapshotPartitions])
	}
	sp, err := ReadSnapshotProgress(store.props)
	if err != nil || sp.State != StateInProgress || len(sp.Pending) != len(plan.Todo) {
		t.Fatalf("recorded progress = %+v, %v; want in progress with every chunk pending", sp, err)
	}
}

// An in-progress snapshot recorded under the same ranges resumes with only
// its pending chunks, and records nothing; under other ranges it starts over.
func TestPlanRangeResumesOnlyUnderTheSameRanges(t *testing.T) {
	store := &propStore{}
	ranges := []source.Chunk{{}}
	first, err := PlanRange(context.Background(), store, planSource(), planRef, ranges, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Two chunks done: the progress a window's commit would leave.
	pending := SortedRefs(first.Todo)[2:]
	props, err := EncodeSnapshotProgress(&SnapshotProgress{State: StateInProgress, Bounds: [][]any{{int64(1)}, {int64(3)}, {int64(5)}}, Pending: pending})
	if err != nil {
		t.Fatal(err)
	}
	props[PropSnapshotPartitions] = PartitionLayout(ranges)
	store.props, store.sets = props, 0

	resumed, err := PlanRange(context.Background(), store, planSource(), planRef, ranges, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.Resumed || len(resumed.Todo) != len(pending) || store.sets != 0 {
		t.Fatalf("plan = %+v after %d write(s); want resumed with %d chunk(s) to do and nothing recorded", resumed, store.sets, len(pending))
	}

	split := []source.Chunk{{High: []any{int64(4)}}, {Low: []any{int64(4)}}}
	fresh, err := PlanRange(context.Background(), store, planSource(), planRef, split, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Resumed || store.sets != 1 || store.props[PropSnapshotPartitions] != PartitionLayout(split) {
		t.Fatalf("plan under other ranges = %+v after %d write(s); want a fresh snapshot recorded under them", fresh, store.sets)
	}
}

// Without a store every snapshot starts afresh; a store that fails is
// reported with the step that failed.
func TestPlanRangeStoreFailures(t *testing.T) {
	ranges := []source.Chunk{{}}
	plan, err := PlanRange(context.Background(), nil, planSource(), planRef, ranges, time.Now())
	if err != nil || plan.Resumed || len(plan.Todo) == 0 {
		t.Fatalf("plan without a store = %+v, %v", plan, err)
	}

	boom := errors.New("catalog down")
	var pe *PlanError
	_, err = PlanRange(context.Background(), &propStore{readErr: boom}, planSource(), planRef, ranges, time.Now())
	if !errors.As(err, &pe) || pe.Op != "snapshot progress" || !errors.Is(err, boom) {
		t.Fatalf("read failure = %v, want a PlanError for the read", err)
	}
	_, err = PlanRange(context.Background(), &propStore{setErr: boom}, planSource(), planRef, ranges, time.Now())
	if !errors.As(err, &pe) || pe.Op != "record snapshot progress" || !errors.Is(err, boom) {
		t.Fatalf("write failure = %v, want a PlanError for the record", err)
	}
}
