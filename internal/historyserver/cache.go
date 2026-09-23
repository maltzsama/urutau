package historyserver

import (
	"container/list"
	"context"
	"sync"

	"github.com/maltzsama/urutau/internal/eventlog"
)

// defaultRetainedRuns bounds the trail cache when the config is silent. It
// mirrors Spark's spark.history.retainedApplications: enough runs stay warm
// that browsing a run's pages does not re-read it, without pinning memory to
// an unbounded number of trails.
const defaultRetainedRuns = 32

// trailCache is a bounded LRU of decoded trails keyed by "<pipeline>/<runID>".
//
// Only SEALED trails are stored: a sealed run is immutable — the writer's
// run_sealed marker is its last line and rotated objects are never rewritten —
// so serving it from memory stays correct forever. An unsealed run may still
// be growing (its current object is re-uploaded on every emit), so it is never
// cached. This is what turns the O(N) S3 read + JSONL parse into a map lookup
// (issue #330) and makes paginating a run O(page) instead of O(N) per page
// (issue #331).
type trailCache struct {
	mu    sync.Mutex
	max   int
	ll    *list.List // front = most recently used
	items map[string]*list.Element
}

type cacheEntry struct {
	key   string
	trail eventlog.Trail
}

func newTrailCache(max int) *trailCache {
	if max <= 0 {
		max = defaultRetainedRuns
	}
	return &trailCache{max: max, ll: list.New(), items: make(map[string]*list.Element, max)}
}

// get returns the cached trail and moves it to the front.
func (c *trailCache) get(key string) (eventlog.Trail, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return eventlog.Trail{}, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry).trail, true
}

// put stores the trail, evicting the least recently used entry past the cap.
func (c *trailCache) put(key string, t eventlog.Trail) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		el.Value.(*cacheEntry).trail = t
		c.ll.MoveToFront(el)
		return
	}
	c.items[key] = c.ll.PushFront(&cacheEntry{key: key, trail: t})
	for c.ll.Len() > c.max {
		oldest := c.ll.Back()
		if oldest == nil {
			break
		}
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*cacheEntry).key)
	}
}

// cachedStore adds a trailCache in front of a Store. Discovery
// (ListPipelines/ListRuns) is a cheap S3 list and is not cached.
type cachedStore struct {
	inner Store
	cache *trailCache
}

func newCachedStore(inner Store, retained int) *cachedStore {
	return &cachedStore{inner: inner, cache: newTrailCache(retained)}
}

func (s *cachedStore) ListPipelines(ctx context.Context) ([]eventlog.PipelineSummary, error) {
	return s.inner.ListPipelines(ctx)
}

func (s *cachedStore) ListRuns(ctx context.Context, pipeline string) ([]eventlog.RunSummary, error) {
	return s.inner.ListRuns(ctx, pipeline)
}

func (s *cachedStore) ReadRunTrail(ctx context.Context, pipeline, runID string) (eventlog.Trail, error) {
	key := pipeline + "/" + runID
	if t, ok := s.cache.get(key); ok {
		return t, nil
	}
	t, err := s.inner.ReadRunTrail(ctx, pipeline, runID)
	if err != nil {
		return eventlog.Trail{}, err
	}
	if t.Sealed {
		s.cache.put(key, t)
	}
	return t, nil
}
