package historyserver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maltzsama/urutau/internal/eventlog"
)

// countingStore wraps a fakeStore and counts how many times the underlying
// store is read.
type countingStore struct {
	fakeStore
	reads atomic.Int64
}

func (s *countingStore) ReadRunTrail(ctx context.Context, pipeline, runID string) (eventlog.Trail, error) {
	s.reads.Add(1)
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
	if got := inner.reads.Load(); got != 1 {
		t.Fatalf("store reads = %d, want 1 (a sealed run is cached)", got)
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
	if got := inner.reads.Load(); got != 3 {
		t.Fatalf("store reads = %d, want 3 (an unsealed run is never cached)", got)
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
	if got := inner.reads.Load(); got != 4 {
		t.Fatalf("store reads = %d, want 4 (r1 was evicted)", got)
	}
}

// Concurrent misses for the same run are deduped: one S3 read, not one per
// request.
func TestCachedStoreDedupesConcurrentReads(t *testing.T) {
	inner := &countingStore{fakeStore: fakeStore{sealed: true}}
	cs := newCachedStore(inner, 4)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cs.ReadRunTrail(context.Background(), "shop", "r1")
		}()
	}
	wg.Wait()
	if got := inner.reads.Load(); got != 1 {
		t.Fatalf("store reads = %d, want 1 (concurrent misses deduped)", got)
	}
}
