package eventlog

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		uri    string
		bucket string
		prefix string
		err    string
	}{
		{"s3://mybucket/prefix", "mybucket", "prefix", ""},
		{"s3://mybucket/a/b/c", "mybucket", "a/b/c", ""},
		{"s3://mybucket", "mybucket", "", ""},
		{"s3://mybucket/", "mybucket", "", ""},
		{"s3:///prefix", "", "", "lacks a bucket"},
		{"http://mybucket/prefix", "", "", "must be s3://"},
		{"", "", "", "must be s3://"},
	}
	for _, tt := range tests {
		t.Run(tt.uri, func(t *testing.T) {
			bucket, prefix, err := parseURI(tt.uri)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("parseURI(%q): got err %v, want %q", tt.uri, err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseURI(%q): unexpected error: %v", tt.uri, err)
			}
			if bucket != tt.bucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.bucket)
			}
			if prefix != tt.prefix {
				t.Errorf("prefix = %q, want %q", prefix, tt.prefix)
			}
		})
	}
}

func TestParseURIKeyFormat(t *testing.T) {
	bucket, prefix, err := parseURI("s3://mybucket/events/")
	if err != nil {
		t.Fatal(err)
	}
	if bucket != "mybucket" {
		t.Fatalf("bucket = %q", bucket)
	}
	trimmed := strings.TrimSuffix(prefix, "/")
	key := trimmed + "/run-123/events.jsonl"
	if key != "events/run-123/events.jsonl" {
		t.Fatalf("key = %q", key)
	}
}

// fakePutter captures Put calls for assertions.
type fakePutter struct {
	mu       sync.Mutex
	calls    int
	lastBody []byte
	lastKey  string
	err      error
}

func (f *fakePutter) Put(_ context.Context, bucket, key string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastBody = make([]byte, len(body))
	copy(f.lastBody, body)
	f.lastKey = key
	return f.err
}

func (f *fakePutter) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestEmitAccumulatesAndUploads(t *testing.T) {
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", p)

	ctx := context.Background()
	if err := r.Emit(ctx, "job_started", map[string]any{"pipeline": "test"}); err != nil {
		t.Fatal(err)
	}
	if p.Calls() != 1 {
		t.Fatalf("calls = %d, want 1", p.Calls())
	}

	if err := r.Emit(ctx, "commit", map[string]any{"table": "users"}); err != nil {
		t.Fatal(err)
	}
	if p.Calls() != 2 {
		t.Fatalf("calls = %d, want 2", p.Calls())
	}

	// Second upload should be larger (contains both events).
	p.mu.Lock()
	body := string(p.lastBody)
	p.mu.Unlock()
	if !strings.Contains(body, "job_started") || !strings.Contains(body, "commit") {
		t.Fatalf("body does not contain both events: %s", body)
	}
	if r.Emitted() != 2 {
		t.Fatalf("emitted = %d, want 2", r.Emitted())
	}
}

func TestEmitClosedReturnsError(t *testing.T) {
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", p)

	r.Close()
	if err := r.Emit(context.Background(), "job_stopped", nil); err == nil {
		t.Fatal("expected error on closed run")
	}
	if p.Calls() != 0 {
		t.Fatalf("calls = %d, want 0 (no upload after close)", p.Calls())
	}
}

func TestEmitBestEffort(t *testing.T) {
	p := &fakePutter{err: context.DeadlineExceeded}
	r := NewWithPutter("bucket", "prefix", p)

	err := r.Emit(context.Background(), "job_started", nil)
	// Emit returns the error (caller decides to log or ignore).
	if err == nil {
		t.Fatal("expected error from failing putter")
	}
	// Buffer still accumulated the event.
	if r.Emitted() != 1 {
		t.Fatalf("emitted = %d, want 1 (buffer grows even on put failure)", r.Emitted())
	}
}

func TestCloseIdempotent(t *testing.T) {
	r := NewWithPutter("bucket", "prefix", &fakePutter{})
	r.Close()
	r.Close() // should not panic
}

func TestNewRunID(t *testing.T) {
	id := newRunID()
	if id == "" {
		t.Fatal("newRunID returned empty string")
	}
	// Format: YYYYMMDDTHHMMSS-hexhexhexhex
	if len(id) < 20 {
		t.Fatalf("run id too short: %q", id)
	}
	if !strings.Contains(id, "-") {
		t.Fatalf("run id missing separator: %q", id)
	}
}

func TestEmitTimestamp(t *testing.T) {
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", p)

	before := time.Now().UTC()
	if err := r.Emit(context.Background(), "test_event", nil); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()

	p.mu.Lock()
	body := string(p.lastBody)
	p.mu.Unlock()

	// Body should contain a timestamp in RFC3339Nano format.
	if !strings.Contains(body, `"ts"`) {
		t.Fatal("body missing ts field")
	}

	_ = before
	_ = after
}

// orderPutter records every PUT (key + body) in arrival order and sleeps on
// the first call: with concurrent Emits, the first PUT in flight finishes
// last, so a lost lock order would let a later PUT land before it.
type orderPutter struct {
	mu     sync.Mutex
	bodies []string
	keys   []string
	call   int
	delay  time.Duration
}

func (p *orderPutter) Put(_ context.Context, _ string, key string, body []byte) error {
	p.mu.Lock()
	n := p.call
	p.call++
	p.mu.Unlock()
	if n == 0 && p.delay > 0 {
		time.Sleep(p.delay)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = append(p.keys, key)
	p.bodies = append(p.bodies, string(body))
	return nil
}

func (p *orderPutter) snapshot() (keys, bodies []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.keys...), append([]string(nil), p.bodies...)
}

// E-1: two concurrent Emits must land their PUTs in append order — the one
// that appends first PUTs first, even though it finishes last. Without
// putMu held across the PUT (acquired before mu.Unlock), the slower first
// PUT could land after a later one and S3's last-writer-wins would drop the
// earlier events.
func TestConcurrentEmitPreservesPUTOrder(t *testing.T) {
	p := &orderPutter{delay: 50 * time.Millisecond}
	r := NewWithPutter("bucket", "prefix", p)

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := r.Emit(context.Background(), "commit", map[string]any{"n": i}); err != nil {
				t.Errorf("emit %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	keys, bodies := p.snapshot()
	if len(bodies) != n {
		t.Fatalf("puts = %d, want %d", len(bodies), n)
	}
	// Every PUT carries the whole buffer, so the last one lands with all n
	// lines — including the event whose PUT was held up by the delay.
	last := bodies[len(bodies)-1]
	if got := strings.Count(last, "\n"); got != n {
		t.Fatalf("last PUT has %d lines, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		if !strings.Contains(last, fmt.Sprintf(`"n":%d`, i)) {
			t.Fatalf("last PUT missing event %d: %s", i, last)
		}
	}
	_ = keys
}
