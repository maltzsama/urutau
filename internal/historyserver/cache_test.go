package historyserver

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/internal/eventlog"
)

// countingStore wraps a fakeStore and counts how many times the underlying
// store is read.
type countingStore struct {
	fakeStore
	reads int
}

func (s *countingStore) ReadRunTrail(ctx context.Context, pipeline, runID string) (eventlog.Trail, error) {
	s.reads++
	return s.fakeStore.ReadRunTrail(ctx, pipeline, runID)
}

// #330/#331: a sealed run is immutable, so the second read — and every page
// after the first — is served from memory.
func TestCachedStoreCachesSealedRuns(t *testing.T) {
	inner := &countingStore{fakeStore: fakeStore{
		events: []eventlog.Event{{Kind: "job_started"}}, sealed: true, emitted: 1,
	}}
	cs := newCachedStore(inner, 4)

	for i := 0; i < 3; i++ {
		if _, err := cs.ReadRunTrail(context.Background(), "shop", "r1"); err != nil {
			t.Fatal(err)
		}
	}
	if inner.reads != 1 {
		t.Fatalf("store reads = %d, want 1 (a sealed run is cached)", inner.reads)
	}
}

// An unsealed run may still be growing, so it is never cached.
func TestCachedStoreDoesNotCacheUnsealedRuns(t *testing.T) {
	inner := &countingStore{fakeStore: fakeStore{events: []eventlog.Event{{Kind: "job_started"}}}}
	cs := newCachedStore(inner, 4)

	for i := 0; i < 3; i++ {
		if _, err := cs.ReadRunTrail(context.Background(), "shop", "r1"); err != nil {
			t.Fatal(err)
		}
	}
	if inner.reads != 3 {
		t.Fatalf("store reads = %d, want 3 (an unsealed run is never cached)", inner.reads)
	}
}

// The cache is bounded: the least recently used run is evicted past the cap.
func TestCachedStoreEvictsLRU(t *testing.T) {
	inner := &countingStore{fakeStore: fakeStore{sealed: true}}
	cs := newCachedStore(inner, 2)

	_, _ = cs.ReadRunTrail(context.Background(), "shop", "r1")
	_, _ = cs.ReadRunTrail(context.Background(), "shop", "r2")
	_, _ = cs.ReadRunTrail(context.Background(), "shop", "r3") // evicts r1
	_, _ = cs.ReadRunTrail(context.Background(), "shop", "r1") // must re-read
	if inner.reads != 4 {
		t.Fatalf("store reads = %d, want 4 (r1 was evicted)", inner.reads)
	}
}
