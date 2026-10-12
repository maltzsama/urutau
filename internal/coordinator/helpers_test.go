package coordinator

import (
	"context"
	"sync"
	"testing"

	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"

	route "github.com/maltzsama/urutau/internal/routing"
)

// ownerKey returns an int64 primary key that the production rendezvous owner
// function assigns to owner index want among names. Tests use it instead of
// hardcoding a key/range split, so they keep asserting per-owner behavior
// without depending on the hash's arithmetic.
func ownerKey(t *testing.T, names []string, want int) int64 {
	t.Helper()
	for i := int64(0); i < 1<<20; i++ {
		o, err := route.OwnerOfKey([]any{i}, names)
		if err != nil {
			t.Fatalf("OwnerOfKey(%d): %v", i, err)
		}
		if o == want {
			return i
		}
	}
	t.Fatalf("no key routes to owner %d of %v", want, names)
	return 0
}

// ownerKeys returns n distinct int64 keys that all route to owner index want.
func ownerKeys(t *testing.T, names []string, want, n int) []int64 {
	t.Helper()
	out := make([]int64, 0, n)
	for i := int64(0); len(out) < n; i++ {
		o, err := route.OwnerOfKey([]any{i}, names)
		if err != nil {
			t.Fatalf("OwnerOfKey(%d): %v", i, err)
		}
		if o == want {
			out = append(out, i)
		}
	}
	return out
}

// setRouteForTest publishes one table's owners into the routing snapshot.
// Production routing is published by boot and by ScaleTable.
func (c *Coordinator) setRouteForTest(target string, owners []*workerState) {
	c.publishRouting(c.loadRouting().WithOwners(target, owners))
}

// setRangesForTest replaces the whole ranges map in the routing snapshot,
// matching the old c.partitionRanges = map[...] assignment.
func (c *Coordinator) setRangesForTest(ranges map[string][]source.Chunk) {
	c.publishRouting(c.loadRouting().WithRanges(ranges))
}

// neverCaughtReader never reaches the high watermark, so the caught-up proof
// would poll until its window timeout unless the wait is aborted. polled
// closes on the first Synced call, so a test can coordinate with the wait
// instead of racing a sleep.
type neverCaughtReader struct {
	once   sync.Once
	polled chan struct{}
}

func (r *neverCaughtReader) Synced() position.Position {
	r.once.Do(func() { close(r.polled) })
	return position.MustGTID("")
}

func (r *neverCaughtReader) Master(context.Context) (position.Position, error) {
	return position.MustGTID(""), nil
}

func (r *neverCaughtReader) OpenWindow(context.Context, uint32) {}
func (r *neverCaughtReader) ClearWindow()                       {}

var _ source.SourceReader = (*neverCaughtReader)(nil)
