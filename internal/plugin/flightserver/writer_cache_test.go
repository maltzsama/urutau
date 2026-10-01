package flightserver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/sink"
)

// fakeTableWriter is a no-op sink.TableWriter for the cache tests.
type fakeTableWriter struct{ closes atomic.Int64 }

func (*fakeTableWriter) Commit(context.Context, *dataplane.Batch) error { return nil }

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

// gRPC serves each DoPut stream in its own goroutine, so writerFor, closeAll
// and the map they share are all reached concurrently. Before #478 the map was
// unguarded: two streams raced it and the race detector failed (or the server
// panicked with "concurrent map writes"). Every caller must still observe
// exactly one writer per table.
func TestWriterCacheIsConcurrencySafe(t *testing.T) {
	cache := writerCache{writers: map[string]sink.TableWriter{}}
	opener := &fakeOpener{}

	const streams = 64
	writers := make([]sink.TableWriter, streams)

	var wg sync.WaitGroup

	for i := range streams {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			w, err := cache.writerFor(context.Background(), opener, "raw.orders")
			if err != nil {
				t.Errorf("writerFor: %v", err)

				return
			}
			writers[i] = w
		}(i)
	}

	// closeAll iterates the same map, so it must share the lock too.
	for range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			cache.closeAll()
		}()
	}

	wg.Wait()

	for i, w := range writers {
		if w == nil {
			t.Fatalf("stream %d got a nil writer", i)
		}
		if w != writers[0] {
			t.Fatalf("stream %d got a different writer than stream 0: writerFor must return one writer per table", i)
		}
	}
}
