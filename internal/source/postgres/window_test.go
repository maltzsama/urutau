package postgres

import (
	"sync"
	"testing"

	"github.com/maltzsama/urutau/position"
)

// TestCurrentWindowTagsOnlyPastLowLSN drives the DBLog window predicate: only
// transactions whose commit LSN is strictly past the low watermark are tagged
// InWindow; a transaction at or before it (already reflected in the chunk
// SELECT) must not be tagged.
func TestCurrentWindowTagsOnlyPastLowLSN(t *testing.T) {
	r := &Reader{winMu: sync.Mutex{}}
	low := position.MustLSN("0/40")
	r.winMu.Lock()
	r.winOpen = true
	r.winChunk = 3
	r.winLow = low
	r.winMu.Unlock()

	// Transaction committed at or before the low LSN: not in the window.
	r.curLSN = *position.MustLSN("0/40")
	if w := r.currentWindow(); w != nil {
		t.Fatalf("at-low transaction must not be InWindow: %+v", w)
	}

	// Transaction committed strictly past the low LSN: InWindow for the
	// chunk.
	r.curLSN = *position.MustLSN("0/41")
	if w := r.currentWindow(); w == nil || !w.InWindow || w.ChunkID != 3 {
		t.Fatalf("past-low transaction must be InWindow: %+v", w)
	}

	// A missing watermark (capture failed) falls back to tagging everything
	// — over-tagging is safe.
	r.winMu.Lock()
	r.winLow = nil
	r.winMu.Unlock()
	r.curLSN = 0
	if w := r.currentWindow(); w == nil || !w.InWindow {
		t.Fatalf("missing watermark must fall back to tagging: %+v", w)
	}

	// Window closed: nothing is tagged.
	r.ClearWindow()
	if w := r.currentWindow(); w != nil {
		t.Fatalf("closed window must not tag: %+v", w)
	}
}
