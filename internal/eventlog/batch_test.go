package eventlog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// decodeLines splits a put body into decoded JSON objects.
func decodeLines(t *testing.T, body string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// EmitBatch enqueues every record of the batch with consecutive seqs; a Flush
// uploads them together, and a caller-supplied ts wins over the flush time —
// the log trail stamps each record itself, so a batch flushed later keeps the
// log's time.
func TestEmitBatch(t *testing.T) {
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", 0, p)
	at := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

	batch := []map[string]any{
		{"ts": at.Format(time.RFC3339Nano), "level": "INFO", "msg": "one"},
		{"ts": at.Add(time.Second).Format(time.RFC3339Nano), "level": "WARN", "msg": "two"},
	}
	if err := r.EmitBatch(context.Background(), KindLog, batch); err != nil {
		t.Fatalf("EmitBatch: %v", err)
	}
	if err := r.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := p.Calls(); got != 1 {
		t.Fatalf("PUTs = %d, want 1 — a batch flushes as one upload", got)
	}
	lines := decodeLines(t, string(p.lastBody))
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(lines))
	}
	for i, want := range []string{"one", "two"} {
		if got := lines[i]["kind"]; got != KindLog {
			t.Errorf("line %d kind = %v, want %s", i, got, KindLog)
		}
		if got := lines[i]["seq"]; got != float64(i+1) {
			t.Errorf("line %d seq = %v, want %d", i, got, i+1)
		}
		if got := lines[i]["msg"]; got != want {
			t.Errorf("line %d msg = %v, want %q", i, got, want)
		}
		if lines[i]["run_id"] == "" {
			t.Errorf("line %d has no run_id", i)
		}
	}
	// The record's own timestamp survives; it is not the flush time.
	if got, want := lines[0]["ts"], at.Format(time.RFC3339Nano); got != want {
		t.Errorf("ts = %v, want the caller's %s", got, want)
	}
	if got, want := lines[1]["ts"], at.Add(time.Second).Format(time.RFC3339Nano); got != want {
		t.Errorf("ts = %v, want the caller's %s", got, want)
	}

	// Seqs continue across Emit and EmitBatch, so a reader never mistakes the
	// join for lost events.
	if err := r.Emit(context.Background(), KindCommit, map[string]any{"table": "t"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := r.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := p.Calls(); got != 2 {
		t.Fatalf("PUTs = %d, want 2 — the flushed object is sealed once", got)
	}
	lines = decodeLines(t, string(p.lastBody))
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1 (a new object)", len(lines))
	}
	if got := lines[0]["seq"]; got != float64(3) {
		t.Errorf("seq after batch = %v, want 3", got)
	}
	// An Emit without a caller ts still gets the writer's own.
	if lines[0]["ts"] == "" {
		t.Error("Emit did not stamp ts")
	}
}

func TestEmitBatchEmpty(t *testing.T) {
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", 0, p)
	if err := r.EmitBatch(context.Background(), KindLog, nil); err != nil {
		t.Fatalf("empty EmitBatch: %v", err)
	}
	if got := p.Calls(); got != 0 {
		t.Fatalf("PUTs = %d, want 0 — an empty batch is a no-op", got)
	}
}
