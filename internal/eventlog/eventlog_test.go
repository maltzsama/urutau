package eventlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
	mu        sync.Mutex
	calls     int
	lastBody  []byte
	lastKey   string
	putKeys   []string // every key, in PUT order
	putBodies []string // every body, in PUT order
	err       error
}

func (f *fakePutter) Put(_ context.Context, bucket, key string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastBody = make([]byte, len(body))
	copy(f.lastBody, body)
	f.lastKey = key
	f.putKeys = append(f.putKeys, key)
	f.putBodies = append(f.putBodies, string(body))
	return f.err
}

func (f *fakePutter) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newTestRun builds a writer with a fake putter and closes it when the test
// ends, so every flusher goroutine is stopped.
func newTestRun(t *testing.T, bucket, prefix string, max int64, p putter) *Run {
	t.Helper()
	r := NewWithPutter(bucket, prefix, max, p)
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// The key convention: the pipeline segment sits between the root prefix and
// the run, so listing alone discovers pipelines then runs.
func TestRunBaseKey(t *testing.T) {
	tests := []struct {
		prefix, pipeline, want string
	}{
		{"p", "", "p/run-abc/"},
		{"/p/", "", "p/run-abc/"},
		{"p", "shop", "p/shop/run-abc/"},
		{"p/", "/shop/", "p/shop/run-abc/"},
		{"", "shop", "shop/run-abc/"},
		{"", "", "run-abc/"},
	}
	for _, tt := range tests {
		if got := runBaseKey(tt.prefix, tt.pipeline, "abc"); got != tt.want {
			t.Errorf("runBaseKey(%q, %q) = %q, want %q", tt.prefix, tt.pipeline, got, tt.want)
		}
	}
}

// Emit only enqueues: nothing reaches S3 until Flush. Two flushed events land
// in the objects the flusher seals, and Emitted counts both.
func TestEmitFlushesOnDemand(t *testing.T) {
	p := &fakePutter{}
	r := newTestRun(t, "bucket", "prefix", 0, p)
	ctx := context.Background()

	if err := r.Emit(ctx, "job_started", map[string]any{"pipeline": "test"}); err != nil {
		t.Fatal(err)
	}
	if p.Calls() != 0 {
		t.Fatalf("calls = %d before flush, want 0 — Emit must not upload", p.Calls())
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if p.Calls() != 1 {
		t.Fatalf("calls = %d, want 1 after flush", p.Calls())
	}

	if err := r.Emit(ctx, "commit", map[string]any{"table": "users"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if p.Calls() != 2 {
		t.Fatalf("calls = %d, want 2", p.Calls())
	}
	p.mu.Lock()
	body := string(p.lastBody)
	p.mu.Unlock()
	if !strings.Contains(body, "commit") || !strings.Contains(body, "users") {
		t.Fatalf("body does not contain the second event: %s", body)
	}
	if r.Emitted() != 2 {
		t.Fatalf("emitted = %d, want 2", r.Emitted())
	}
}

func TestEmitClosedReturnsError(t *testing.T) {
	p := &fakePutter{}
	r := newTestRun(t, "bucket", "prefix", 0, p)

	_ = r.Close()
	before := p.Calls()
	if err := r.Emit(context.Background(), "job_stopped", nil); err == nil {
		t.Fatal("expected error on closed run")
	}
	if p.Calls() != before {
		t.Fatalf("calls = %d, want %d (no upload after close)", p.Calls(), before)
	}
}

// A failing putter must not block or error Emit (it only enqueues); the
// failure surfaces on Flush/Close, and the event is still counted.
func TestEmitBestEffort(t *testing.T) {
	p := &fakePutter{err: context.DeadlineExceeded}
	r := newTestRun(t, "bucket", "prefix", 0, p)
	ctx := context.Background()

	if err := r.Emit(ctx, "job_started", nil); err != nil {
		t.Fatalf("Emit must not block on the putter: %v", err)
	}
	if r.Emitted() != 1 {
		t.Fatalf("emitted = %d, want 1", r.Emitted())
	}
	if err := r.Flush(ctx); err == nil {
		t.Fatal("Flush must surface the put failure")
	}
	_ = r.Close()
}

func TestCloseIdempotent(t *testing.T) {
	r := newTestRun(t, "bucket", "prefix", 0, &fakePutter{})
	_ = r.Close()
	_ = r.Close() // should not panic
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
	r := newTestRun(t, "bucket", "prefix", 0, p)
	if err := r.Emit(context.Background(), "test_event", nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	body := string(p.lastBody)
	p.mu.Unlock()
	if !strings.Contains(body, `"ts"`) {
		t.Fatalf("body missing ts field: %s", body)
	}
}

// Concurrent Emits marshal under the lock, so the flushed object carries every
// accepted event once, with contiguous seqs in order.
func TestConcurrentEmitPreservesOrder(t *testing.T) {
	p := &fakePutter{}
	r := newTestRun(t, "bucket", "prefix", 0, p)

	const n = 200
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
	if err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	p.mu.Lock()
	body := p.lastBody
	p.mu.Unlock()
	events, err := parseEvents(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != n {
		t.Fatalf("events = %d, want %d", len(events), n)
	}
	for i, e := range events {
		if e.Seq != i+1 {
			t.Fatalf("event %d seq = %d, want %d (queue order must match seq)", i, e.Seq, i+1)
		}
	}
}

// Cumulative PUT bytes are O(total event bytes): an object is written once and
// never rewritten with its own contents plus one more event (issue #548).
func TestFlushKeepsBytesLinear(t *testing.T) {
	const threshold = 1024
	p := &fakePutter{}
	r := newTestRun(t, "bucket", "prefix", threshold, p)

	const n = 10_000
	for i := 0; i < n; i++ {
		if err := r.Emit(context.Background(), "commit", map[string]any{"n": i}); err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
	}
	if err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.Emitted() != n {
		t.Fatalf("emitted = %d, want %d", r.Emitted(), n)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int
	for _, b := range p.putBodies {
		total += len(b)
	}
	// Each event is written exactly once, so the cumulative PUT bytes stay
	// under an upper bound on the raw payload. A regression to per-event
	// re-uploads would blow past this by orders of magnitude.
	maxLine, err := json.Marshal(map[string]any{
		"n": n, "ts": time.Now().UTC().Format(time.RFC3339Nano),
		"run_id": r.ID(), "kind": "commit", "seq": n,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bound := n * (len(maxLine) + 1); total > bound {
		t.Fatalf("cumulative PUT bytes = %d, want <= %d (each event uploaded once)", total, bound)
	}
}

// The flusher seals objects under fixed-width, monotonically increasing keys,
// so the trail reads in creation order as plain strings.
func TestFlushKeySequence(t *testing.T) {
	p := &fakePutter{}
	r := newTestRun(t, "bucket", "prefix", 1<<20, p)
	ctx := context.Background()

	// One event per Flush makes the object sequence deterministic.
	for i := 0; i < 4; i++ {
		if err := r.Emit(ctx, "commit", map[string]any{"n": i}); err != nil {
			t.Fatal(err)
		}
		if err := r.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = r.Close()

	p.mu.Lock()
	defer p.mu.Unlock()
	var objects []string
	seen := map[string]bool{}
	for _, k := range p.putKeys {
		suffix := k[strings.LastIndex(k, "/")+1:]
		if !seen[suffix] {
			seen[suffix] = true
			objects = append(objects, suffix)
		}
	}
	want := []string{"events-000000.jsonl", "events-000001.jsonl", "events-000002.jsonl", "events-000003.jsonl"}
	if len(objects) < len(want) {
		t.Fatalf("objects = %d, want at least %d: %v", len(objects), len(want), objects)
	}
	for i, w := range want {
		if objects[i] != w {
			t.Fatalf("object[%d] = %q, want %q", i, objects[i], w)
		}
	}
}

// A Close racing many Emits accepts a prefix of them and writes every accepted
// event plus the seal marker across the objects it seals — never fewer lines.
func TestCloseSerializesWithInFlightEmits(t *testing.T) {
	p := &fakePutter{}
	r := newTestRun(t, "bucket", "prefix", 0, p)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = r.Emit(context.Background(), "commit", map[string]any{"n": i}) // closed errors are fine
		}(i)
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		time.Sleep(5 * time.Millisecond)
		_ = r.Close()
	}()
	wg.Wait()
	<-closed
	_ = r.Close() // idempotency only now

	accepted := r.Emitted()
	if accepted == 0 {
		t.Fatal("no events accepted")
	}
	p.mu.Lock()
	var lines int
	for _, b := range p.putBodies {
		lines += strings.Count(b, "\n")
	}
	p.mu.Unlock()
	if lines != accepted+1 {
		t.Fatalf("trail lines = %d, want %d (accepted events + seal marker)", lines, accepted+1)
	}
}

// MaxObjectBytes <= 0 is clamped to the default in both constructors.
func TestMaxObjectBytesClampedToDefault(t *testing.T) {
	for _, bad := range []int64{0, -1, -8 << 20} {
		p := &fakePutter{}
		r := newTestRun(t, "bucket", "prefix", bad, p)
		if r.maxObjectBytes != defaultMaxObjectBytes {
			t.Fatalf("NewWithPutter(%d): maxObjectBytes = %d, want %d", bad, r.maxObjectBytes, defaultMaxObjectBytes)
		}
	}

	// The clamping lives in newRun, which both New and NewWithPutter call.
	r := newRun("b", "p/run-x/", "x", -5, 0, 0, &fakePutter{}, slog.Default())
	t.Cleanup(func() { _ = r.Close() })
	if r.maxObjectBytes != defaultMaxObjectBytes {
		t.Fatalf("newRun: maxObjectBytes = %d, want %d", r.maxObjectBytes, defaultMaxObjectBytes)
	}
	if r.flushEvery != defaultFlushInterval {
		t.Fatalf("newRun: flushEvery = %s, want %s", r.flushEvery, defaultFlushInterval)
	}
	if cap(r.ch) != defaultQueueSize {
		t.Fatalf("newRun: queue size = %d, want %d", cap(r.ch), defaultQueueSize)
	}
}

// Under a failing upload the buffer is capped, not grown without bound, and
// the trim leaves an events_dropped marker so a reader sees the loss
// (issues #230, #333).
func TestBufferBoundedUnderOutage(t *testing.T) {
	r := &Run{id: "x", maxObjectBytes: 64, maxBufferBytes: 256, log: slog.Default()}
	line := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n")
	for i := 0; i < 100; i++ {
		r.buf = append(r.buf, line...) // ~4900 bytes, well past the 256-byte cap
	}
	r.boundBuffer()
	if int64(len(r.buf)) > r.maxBufferBytes+200 {
		t.Fatalf("buffer = %d bytes, want <= %d", len(r.buf), r.maxBufferBytes)
	}
	if !bytes.Contains(r.buf, []byte(`"kind":"events_dropped"`)) {
		t.Fatalf("a trim left no events_dropped marker: %s", r.buf)
	}
}

// gatePutter blocks its first PUT until released, so a burst can fill the
// queue while the flusher is stuck.
type gatePutter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (g *gatePutter) Put(_ context.Context, _, _ string, _ []byte) error {
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return nil
}

// A full queue drops and counts events instead of blocking the caller
// (issue #548: the emit path must never stall the pipeline).
func TestQueueFullDropsAndCounts(t *testing.T) {
	g := &gatePutter{entered: make(chan struct{}), release: make(chan struct{})}
	// A one-byte object cap flushes the first line immediately, blocking the
	// flusher in Put; a two-slot queue then fills.
	r := newRun("b", "p/run-x/", "x", 1, time.Hour, 2, g, slog.Default())

	if err := r.Emit(context.Background(), "e", nil); err != nil {
		t.Fatal(err)
	}
	<-g.entered // the flusher is inside its first PUT
	for i := 0; i < 20; i++ {
		if err := r.Emit(context.Background(), "e", nil); err != nil {
			t.Fatal(err)
		}
	}
	if r.Dropped() == 0 {
		t.Fatal("expected the queue to drop and count events")
	}
	close(g.release)
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// #333: every event carries a contiguous, monotonic seq, and seq is lifted out
// of Fields like the other reserved keys.
func TestEmitAssignsContiguousSeq(t *testing.T) {
	p := &fakePutter{}
	r := newTestRun(t, "b", "p", 1<<20, p)
	for i := 0; i < 5; i++ {
		if err := r.Emit(context.Background(), "commit", map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	body := p.lastBody
	p.mu.Unlock()
	events, err := parseEvents(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("events = %d, want 5", len(events))
	}
	for i, e := range events {
		if e.Seq != i+1 {
			t.Fatalf("event %d seq = %d, want %d", i, e.Seq, i+1)
		}
		if _, ok := e.Fields["seq"]; ok {
			t.Fatalf("seq leaked into Fields: %+v", e.Fields)
		}
	}
}

// #333: Close writes a run_sealed terminal marker recording the emitted count,
// so a reader can verify completeness and tell a sealed run from an abandoned
// one.
func TestCloseWritesSealMarker(t *testing.T) {
	p := &fakePutter{}
	r := newTestRun(t, "b", "p", 1<<20, p)
	_ = r.Emit(context.Background(), "job_started", nil)
	_ = r.Emit(context.Background(), "commit", nil)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	body := p.lastBody
	p.mu.Unlock()
	events, err := parseEvents(body)
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != KindRunSealed {
		t.Fatalf("last event = %q, want %q", last.Kind, KindRunSealed)
	}
	if got := last.Fields["emitted"]; got != float64(2) {
		t.Fatalf("seal emitted = %v, want 2", got)
	}
}

// #231: Close surfaces a best-effort PUT failure instead of swallowing it.
func TestCloseReturnsPutError(t *testing.T) {
	p := &fakePutter{err: errors.New("down")}
	r := newTestRun(t, "b", "p", 1<<20, p)
	_ = r.Emit(context.Background(), "commit", map[string]any{"n": 1})
	if err := r.Close(); err == nil {
		t.Fatal("Close must return the PUT failure")
	}
}

// #232: s3://bucket and s3://bucket/ must derive the same base key.
func TestParseURINormalizesTrailingSlash(t *testing.T) {
	b1, p1, err := parseURI("s3://bucket")
	if err != nil {
		t.Fatal(err)
	}
	b2, p2, err := parseURI("s3://bucket/")
	if err != nil {
		t.Fatal(err)
	}
	if b1 != b2 || p1 != p2 {
		t.Fatalf("trailing slash changed the parse: (%q,%q) vs (%q,%q)", b1, p1, b2, p2)
	}
	_, p3, err := parseURI("s3://bucket/prefix/")
	if err != nil || p3 != "prefix" {
		t.Fatalf("prefix = %q, err %v; want prefix", p3, err)
	}
}

// The fixed-width key format sorts in creation order beyond 100 objects
// (%02d would make events-100 sort before events-99).
func TestRotationKeySortOrder(t *testing.T) {
	p := &fakePutter{}
	r := newTestRun(t, "bucket", "prefix", 1<<20, p)
	ctx := context.Background()

	// One Flush per event makes the object sequence deterministic and long
	// enough to cover the width overflow.
	for i := 0; i < 150; i++ {
		if err := r.Emit(ctx, "commit", map[string]any{"n": i}); err != nil {
			t.Fatal(err)
		}
		if err := r.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	p.mu.Lock()
	keys := append([]string(nil), p.putKeys...)
	p.mu.Unlock()

	// Distinct keys in first-PUT (creation) order.
	var created []string
	seen := map[string]bool{}
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			created = append(created, k)
		}
	}
	if len(created) < 100 {
		t.Fatalf("only %d objects; want 100+ to cover the width overflow", len(created))
	}
	for i := 1; i < len(created); i++ {
		if created[i-1] >= created[i] {
			t.Fatalf("keys do not sort in creation order at %d: %q >= %q", i, created[i-1], created[i])
		}
	}
}
