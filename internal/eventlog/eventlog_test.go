package eventlog

import (
	"context"
	"fmt"
	"sort"
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

func TestEmitAccumulatesAndUploads(t *testing.T) {
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", 0, p)

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
	r := NewWithPutter("bucket", "prefix", 0, p)

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
	r := NewWithPutter("bucket", "prefix", 0, p)

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
	r := NewWithPutter("bucket", "prefix", 0, &fakePutter{})
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
	r := NewWithPutter("bucket", "prefix", 0, p)

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

func (p *orderPutter) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.bodies...)
}

// E-1: two concurrent Emits must land their PUTs in append order — the one
// that appends first PUTs first, even though it finishes last. Without
// putMu held across the PUT (acquired before mu.Unlock), the slower first
// PUT could land after a later one and S3's last-writer-wins would drop the
// earlier events.
func TestConcurrentEmitPreservesPUTOrder(t *testing.T) {
	p := &orderPutter{delay: 50 * time.Millisecond}
	r := NewWithPutter("bucket", "prefix", 0, p)

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

	bodies := p.snapshot()
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
}

// failNthPutter fails exactly one PUT (by call index), recording everything.
type failNthPutter struct {
	mu     sync.Mutex
	failOn int // 0-based call index
	call   int
	keys   []string
	bodies []string
}

func (p *failNthPutter) Put(_ context.Context, _ string, key string, body []byte) error {
	p.mu.Lock()
	n := p.call
	p.call++
	p.keys = append(p.keys, key)
	p.bodies = append(p.bodies, string(body))
	p.mu.Unlock()
	if n == p.failOn {
		return context.DeadlineExceeded
	}
	return nil
}

// E-2 (1): the last Emit's PUT failed (best-effort contract), so its line is
// still in the buffer. Close must re-upload the buffer — a graceful shutdown
// loses exactly the final event otherwise.
func TestCloseReFlushesFailedLastEvent(t *testing.T) {
	p := &failNthPutter{failOn: 2} // third PUT fails
	r := NewWithPutter("bucket", "prefix", 0, p)

	ctx := context.Background()
	if err := r.Emit(ctx, "one", nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Emit(ctx, "two", nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Emit(ctx, "last", nil); err == nil {
		t.Fatal("expected the failing PUT to error the emit")
	}

	r.Close()

	p.mu.Lock()
	defer p.mu.Unlock()
	last := p.bodies[len(p.bodies)-1]
	if !strings.Contains(last, `"kind":"last"`) {
		t.Fatalf("close PUT does not carry the failed event: %s", last)
	}
	if !strings.HasSuffix(p.keys[len(p.keys)-1], "events-000000.jsonl") {
		t.Fatalf("close PUT key = %q", p.keys[len(p.keys)-1])
	}
}

// E-2 (2): a Close concurrent with in-flight Emits must produce a final
// object containing every event accepted before the flip — never a smaller
// body overwriting a bigger in-flight PUT.
func TestCloseSerializesWithInFlightEmits(t *testing.T) {
	p := &orderPutter{delay: 5 * time.Millisecond}
	r := NewWithPutter("bucket", "prefix", 0, p)

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
		time.Sleep(10 * time.Millisecond)
		r.Close()
	}()
	wg.Wait()
	<-closed  // the first Close finished, including its final PUT
	r.Close() // idempotency only now

	accepted := r.Emitted()
	if accepted == 0 {
		t.Fatal("no events accepted")
	}
	// The Close made exactly one PUT beyond the accepted emits — proving it
	// actually ran, not merely that the emit PUTs covered the buffer.
	if bodies := p.snapshot(); len(bodies) != accepted+1 {
		t.Fatalf("PUTs = %d, want %d (accepted emits + one close)", len(bodies), accepted+1)
	}
	// Every PUT body is a prefix of the final buffer (appends only grow it),
	// so the largest body must carry exactly the accepted events.
	bodies := p.snapshot()
	maxLines := 0
	for _, b := range bodies {
		if c := strings.Count(b, "\n"); c > maxLines {
			maxLines = c
		}
	}
	if maxLines != accepted {
		t.Fatalf("largest PUT body has %d lines, want %d (accepted)", maxLines, accepted)
	}
}

// ── E-3: rotation by size ─────────────────────────────────────────────

// O(n × threshold), not O(n²): each PUT carries at most ~threshold bytes,
// so the cumulative PUT bytes of a 10k-event run stay in the low MBs.
func TestRotationKeepsBytesLinear(t *testing.T) {
	const threshold = 1024
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", threshold, p)

	const n = 10_000
	for i := 0; i < n; i++ {
		if err := r.Emit(context.Background(), "commit", map[string]any{"n": i}); err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
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
	// Linear bound: every body is at most ~threshold + one line.
	if total > n*(threshold+128) {
		t.Fatalf("cumulative PUT bytes = %d, want O(n × threshold) ≤ %d", total, n*(threshold+128))
	}
}

// Three-plus rotations with the literal key sequence: the crossing event
// travels in the object being closed, the next Emit opens the next object,
// and the trail reads events-000000.jsonl, events-000001.jsonl, … in
// lexicographic order.
func TestRotationKeySequence(t *testing.T) {
	const threshold = 256
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", threshold, p)

	for i := 0; i < 40; i++ {
		if err := r.Emit(context.Background(), "commit", map[string]any{
			"n":   i,
			"pad": "0123456789012345678901234567890123456789",
		}); err != nil {
			t.Fatal(err)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	// Distinct object keys in first-PUT order.
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
		t.Fatalf("rotations = %d objects, want at least %d: %v", len(objects), len(want), objects)
	}
	for i, w := range want {
		if objects[i] != w {
			t.Fatalf("object[%d] = %q, want %q", i, objects[i], w)
		}
	}
	// The original object is not empty.
	if strings.Count(p.putBodies[0], "\n") == 0 {
		t.Fatal("the first object is empty")
	}
	// The current object key is the latest rotated one.
	if got := r.ObjectKey(); !strings.HasSuffix(got, fmt.Sprintf("events-%06d.jsonl", r.seq)) || r.seq == 0 {
		t.Fatalf("ObjectKey = %q, seq = %d; want the current rotated object", got, r.seq)
	}
}

// Close after a rotation re-flushes the buffer into the CURRENT object, not
// back into the first object.
func TestCloseAfterRotationFlushesCurrentObject(t *testing.T) {
	const threshold = 256
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", threshold, p)

	// Rotate at least once.
	for r.seq == 0 {
		if err := r.Emit(context.Background(), "commit", map[string]any{
			"n":   1,
			"pad": "0123456789012345678901234567890123456789",
		}); err != nil {
			t.Fatal(err)
		}
	}
	// One small event in the fresh current object — below the threshold, so
	// the buffer is non-empty at Close and the close PUT is observable.
	if err := r.Emit(context.Background(), "final", nil); err != nil {
		t.Fatal(err)
	}
	current := r.ObjectKey() // single-threaded test; r.seq read without mu

	r.Close()

	p.mu.Lock()
	defer p.mu.Unlock()
	lastKey, lastBody := p.putKeys[len(p.putKeys)-1], p.putBodies[len(p.putBodies)-1]
	if lastKey != current {
		t.Fatalf("close PUT went to %q, want the current object %q", lastKey, current)
	}
	if !strings.Contains(lastBody, `"kind":"final"`) {
		t.Fatalf("close PUT body does not carry the last event: %s", lastBody)
	}
}

// MaxObjectBytes <= 0 is clamped to the default in both constructors — a
// misconfigured threshold must never degrade to a rotation per event.
func TestMaxObjectBytesClampedToDefault(t *testing.T) {
	for _, bad := range []int64{0, -1, -8 << 20} {
		p := &fakePutter{}
		r := NewWithPutter("bucket", "prefix", bad, p)
		if r.maxObjectBytes != defaultMaxObjectBytes {
			t.Fatalf("NewWithPutter(%d): maxObjectBytes = %d, want %d", bad, r.maxObjectBytes, defaultMaxObjectBytes)
		}
		// A burst of small events must not rotate: one object only.
		for i := 0; i < 50; i++ {
			if err := r.Emit(context.Background(), "commit", nil); err != nil {
				t.Fatal(err)
			}
		}
		p.mu.Lock()
		keys := append([]string(nil), p.putKeys...)
		p.mu.Unlock()
		for _, k := range keys {
			if !strings.HasSuffix(k, "events-000000.jsonl") {
				t.Fatalf("clamped run rotated to %q", k)
			}
		}
	}

	// New takes the same clamp (the AWS config loads offline; no client
	// calls happen at construction).
	r, err := New(context.Background(), Config{URI: "s3://bucket/prefix", MaxObjectBytes: -5})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if r.maxObjectBytes != defaultMaxObjectBytes {
		t.Fatalf("New: maxObjectBytes = %d, want %d", r.maxObjectBytes, defaultMaxObjectBytes)
	}
}

// flakyPutter fails the first failFirst PUTs (transient), recording all.
type flakyPutter struct {
	mu        sync.Mutex
	failFirst int
	call      int
	keys      []string
	bodies    []string
}

func (p *flakyPutter) Put(_ context.Context, _ string, key string, body []byte) error {
	p.mu.Lock()
	n := p.call
	p.call++
	p.keys = append(p.keys, key)
	p.bodies = append(p.bodies, string(body))
	p.mu.Unlock()
	if n < p.failFirst {
		return context.DeadlineExceeded
	}
	return nil
}

// lastPerKey returns the final body written to each object key.
func (p *flakyPutter) lastPerKey() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]string, len(p.keys))
	for i, k := range p.keys {
		out[k] = p.bodies[i]
	}
	return out
}

// R-1 (1): a rotated object whose PUT fails is retained and retried by the
// next Emit — no event of the sealed object is lost.
func TestRotationRetainsFailedObject(t *testing.T) {
	const threshold = 200
	p := &flakyPutter{failFirst: 1} // the first rotation's PUT fails
	r := NewWithPutter("bucket", "prefix", threshold, p)

	const n = 12
	for i := 0; i < n; i++ {
		_ = r.Emit(context.Background(), "commit", map[string]any{ // failures are best-effort
			"n":   i,
			"pad": strings.Repeat("x", 40),
		})
	}
	// The retained object must have been delivered on a later retry: every
	// event appears in the final body of some object.
	var all strings.Builder
	for _, b := range p.lastPerKey() {
		all.WriteString(b)
	}
	for i := 0; i < n; i++ {
		if !strings.Contains(all.String(), fmt.Sprintf(`"n":%d`, i)) {
			t.Fatalf("event %d missing from the trail: %s", i, all.String())
		}
	}
}

// R-1 (2) regression: a NON-rotated current object whose PUT fails keeps its
// buffer — the next Emit re-PUTs the accumulated buffer, as before.
func TestNonRotatedPutFailureRetriesViaBuffer(t *testing.T) {
	p := &flakyPutter{failFirst: 1}
	r := NewWithPutter("bucket", "prefix", 0, p) // clamped to the default: never rotates here

	if err := r.Emit(context.Background(), "one", nil); err == nil {
		t.Fatal("expected the failing PUT to error the first emit")
	}
	if err := r.Emit(context.Background(), "two", nil); err != nil {
		t.Fatalf("second emit: %v", err)
	}
	last := p.lastPerKey()
	var body string
	for _, b := range last {
		if strings.Contains(b, `"kind":"two"`) {
			body = b
		}
	}
	if !strings.Contains(body, `"kind":"one"`) || !strings.Contains(body, `"kind":"two"`) {
		t.Fatalf("buffer was not re-PUT whole: %s", body)
	}
}

// R-1 (3): a rotated object left pending when no further Emit happens is
// delivered by Close.
func TestCloseDeliversPendingObject(t *testing.T) {
	p := &flakyPutter{failFirst: 1}
	r := NewWithPutter("bucket", "prefix", 1, p) // threshold 1: every emit rotates

	if err := r.Emit(context.Background(), "final", nil); err == nil {
		t.Fatal("expected the rotation PUT to fail")
	}
	r.Close()

	// The pending object (the first key) carries the event.
	last := p.lastPerKey()
	body, ok := last["prefix/run-"+r.ID()+"/events-000000.jsonl"]
	if !ok {
		// key layout differs in tests? fall back to scanning for the marker
		for _, b := range last {
			if strings.Contains(b, `"kind":"final"`) {
				body = b
			}
		}
	}
	if !strings.Contains(body, `"kind":"final"`) {
		t.Fatalf("Close did not deliver the pending object: %v", last)
	}
}

// R-2: fixed-width keys sort in creation order as plain strings — catches
// both the "events.jsonl" dot-vs-dash inversion and the %02d width overflow
// (which the creation-order test never exercised).
func TestRotationKeySortOrder(t *testing.T) {
	const threshold = 32
	p := &fakePutter{}
	r := NewWithPutter("bucket", "prefix", threshold, p)

	// Enough events to cross 100 rotations: that is the width-overflow case
	// (%02d would make events-100 sort before events-99), beyond the dot-vs-
	// dash inversion the first key already exposes.
	for i := 0; i < 300; i++ {
		if err := r.Emit(context.Background(), "commit", map[string]any{"n": i, "pad": strings.Repeat("y", 20)}); err != nil {
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
	sorted := append([]string(nil), created...)
	sort.Strings(sorted)
	for i := range created {
		if created[i] != sorted[i] {
			t.Fatalf("keys do not sort in creation order at %d: created=%q sorted=%q", i, created[i], sorted[i])
		}
	}
}
