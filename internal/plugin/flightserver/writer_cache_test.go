package flightserver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/sink"
)

// fakeTableWriter is a sink.TableWriter for the cache tests. It records its
// calls and flags a Commit that overlaps another, which a correctly serialized
// committer never does.
type fakeTableWriter struct {
	closes   atomic.Int64
	commits  atomic.Int64
	inCommit atomic.Int32
	overlap  atomic.Bool
}

func (w *fakeTableWriter) Commit(context.Context, *dataplane.Batch) error {
	if !w.inCommit.CompareAndSwap(0, 1) {
		w.overlap.Store(true)
	}
	// Hold the "commit" long enough that a concurrent entry would be seen.
	time.Sleep(time.Millisecond)
	w.inCommit.Store(0)
	w.commits.Add(1)

	return nil
}

func (w *fakeTableWriter) Close() error {
	w.closes.Add(1)

	return nil
}

// fakeOpener stands in for the wrapped sink.Sink's Writer.
type fakeOpener struct{ opens atomic.Int64 }

func (o *fakeOpener) Writer(context.Context, core.TableRef, core.CastPolicy, []core.MetadataColumn) (sink.TableWriter, error) {
	o.opens.Add(1)

	return &fakeTableWriter{}, nil
}

func newCache() writerCache { return writerCache{tables: map[string]*tableWriter{}} }

// gRPC serves each DoPut stream in its own goroutine, so writerFor and the map
// it shares are reached concurrently. Before #478 the map was unguarded: two
// streams raced it and the race detector failed (or the server panicked with
// "concurrent map writes"). Every caller must still observe exactly one
// committer per table.
func TestWriterCacheIsConcurrencySafe(t *testing.T) {
	cache := newCache()
	opener := &fakeOpener{}

	const streams = 64
	writers := make([]sink.TableWriter, streams)

	var wg sync.WaitGroup

	for i := range streams {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			w, err := cache.writerFor(context.Background(), opener, core.TableRef{Target: "raw.orders"})
			if err != nil {
				t.Errorf("writerFor: %v", err)

				return
			}
			writers[i] = w
		}(i)
	}
	wg.Wait()

	for i, w := range writers {
		if w == nil {
			t.Fatalf("stream %d got a nil writer", i)
		}
		if w != writers[0] {
			t.Fatalf("stream %d got a different writer than stream 0: writerFor must return one committer per table", i)
		}
	}
}

// closeAll shares the lock with writerFor; the bots flagged their interleaving
// (an open completing after shutdown). This runs them together under the race
// detector and asserts every committer a stream actually received is the one
// shared for the table.
func TestWriterCacheCloseAllRacesWriterFor(t *testing.T) {
	cache := newCache()
	opener := &fakeOpener{}

	const streams = 64
	got := make([]sink.TableWriter, streams)

	var wg sync.WaitGroup

	for i := range streams {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			w, err := cache.writerFor(context.Background(), opener, core.TableRef{Target: "raw.orders"})
			if err != nil {
				if !errors.Is(err, errWriterCacheClosed) {
					t.Errorf("writerFor: %v", err)
				}

				return
			}
			got[i] = w
		}(i)
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		cache.closeAll()
	}()

	wg.Wait()

	var first sink.TableWriter

	for _, w := range got {
		if w == nil {
			continue
		}
		if first == nil {
			first = w

			continue
		}
		if w != first {
			t.Fatal("streams observed different committers for one table")
		}
	}
}

// A sink.TableWriter is not safe to call Commit on concurrently; the cache
// hands the same committer to every stream for a table, so it must serialize
// those calls itself.
func TestWriterCacheSerializesPerTableCommits(t *testing.T) {
	cache := newCache()

	w, err := cache.writerFor(context.Background(), &fakeOpener{}, core.TableRef{Target: "raw.orders"})
	if err != nil {
		t.Fatalf("writerFor: %v", err)
	}

	const commits = 16

	var wg sync.WaitGroup

	for range commits {
		wg.Add(1)

		go func() {
			defer wg.Done()
			_ = w.Commit(context.Background(), &dataplane.Batch{Table: "raw.orders"})
		}()
	}
	wg.Wait()

	fw, ok := w.(*tableWriter).w.(*fakeTableWriter)
	if !ok {
		t.Fatalf("unexpected writer type %T", w.(*tableWriter).w)
	}
	if fw.overlap.Load() {
		t.Fatal("two Commit calls entered the shared committer concurrently; the cache must serialize them")
	}
	if got := fw.commits.Load(); got != commits {
		t.Fatalf("commits = %d, want %d", got, commits)
	}
}

// After shutdown the cache must reject new streams and release what it held,
// so a committer opened by an in-flight stream is closed rather than leaked
// (the closeAll/writerFor interleaving the bots flagged).
func TestWriterCacheClosesAndRejectsAfterShutdown(t *testing.T) {
	cache := newCache()

	w, err := cache.writerFor(context.Background(), &fakeOpener{}, core.TableRef{Target: "raw.orders"})
	if err != nil {
		t.Fatalf("writerFor: %v", err)
	}

	cache.closeAll()
	cache.closeAll() // idempotent

	if _, err := cache.writerFor(context.Background(), &fakeOpener{}, core.TableRef{Target: "raw.orders"}); !errors.Is(err, errWriterCacheClosed) {
		t.Fatalf("writerFor after shutdown = %v, want errWriterCacheClosed", err)
	}

	fw, ok := w.(*tableWriter).w.(*fakeTableWriter)
	if !ok {
		t.Fatalf("unexpected writer type %T", w.(*tableWriter).w)
	}
	if got := fw.closes.Load(); got != 1 {
		t.Fatalf("underlying committer closed %d times, want 1", got)
	}
}
