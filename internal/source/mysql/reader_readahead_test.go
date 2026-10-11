package mysql

import "testing"

// go-mysql reads binlog events ahead of the reader into a channel whose
// capacity it counts in events, 10,240 by default. Each queued event keeps its
// whole packet, so with wide rows a lagging reader pins hundreds of megabytes
// that no flow budget sees. The reader must bound that read-ahead itself.
func TestCanalConfigBoundsTheEventReadAhead(t *testing.T) {
	cfg := canalConfig(Config{Addr: "db:3306", User: "u", ServerID: 7}, nil)
	if cfg.EventCacheCount <= 0 {
		t.Fatal("EventCacheCount is unset: go-mysql falls back to 10,240 events of read-ahead")
	}
	if cfg.EventCacheCount > 512 {
		t.Fatalf("EventCacheCount = %d, want a small bound: every queued event holds its packet", cfg.EventCacheCount)
	}
}
