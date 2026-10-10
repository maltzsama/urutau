package runner

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/sink"
)

// stateSink holds per-table positions and properties.
type stateSink struct {
	sink.Sink
	positions map[string]string
	props     map[string]map[string]string
}

func (s *stateSink) Position(_ context.Context, ref core.TableRef) (string, error) {
	return s.positions[ref.Target], nil
}

func (s *stateSink) Properties(_ context.Context, ref core.TableRef) (map[string]string, error) {
	out := map[string]string{}
	for k, v := range s.props[ref.Target] {
		out[k] = v
	}
	return out, nil
}

func (s *stateSink) SetProperties(_ context.Context, ref core.TableRef, p map[string]string) error {
	if s.props[ref.Target] == nil {
		s.props[ref.Target] = map[string]string{}
	}
	for k, v := range p {
		s.props[ref.Target][k] = v
	}
	return nil
}

// Before the stream starts, every table about to be snapshotted is marked
// not_started, except one an earlier run left in_progress (its bounds make
// the snapshot resumable). The tables that already hold rows are reported:
// their snapshot must take the upsert path.
func TestMarkSnapshotsPending(t *testing.T) {
	snk := &stateSink{
		positions: map[string]string{"left": "0/20"},
		props: map[string]map[string]string{
			"left": {snapshot.PropSnapshotState: string(snapshot.StateInProgress), snapshot.PropSnapshotPending: "[3]"},
		},
	}
	held, err := markSnapshotsPending(context.Background(), snk, []core.TableRef{{Target: "fresh"}, {Target: "left"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := snk.props["fresh"][snapshot.PropSnapshotState]; got != string(snapshot.StateNotStarted) {
		t.Errorf("fresh: state %q, want not_started", got)
	}
	if got := snk.props["left"][snapshot.PropSnapshotState]; got != string(snapshot.StateInProgress) {
		t.Errorf("left: state %q, want in_progress kept", got)
	}
	if held["fresh"] || !held["left"] {
		t.Errorf("held rows = %v, want only left", held)
	}
}
