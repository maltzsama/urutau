package eventlog

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// fakePutter captures uploads; failures are scripted per call.
type fakePutter struct {
	mu    sync.Mutex
	puts  []string // bodies, in order
	failN int      // fail the first N puts
}

func (f *fakePutter) Put(_ context.Context, bucket, key string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return errFake{}
	}
	f.puts = append(f.puts, bucket+"|"+key+"|"+string(body))
	return nil
}

func (f *fakePutter) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.puts...)
}

type errFake struct{}

func (errFake) Error() string { return "fake put failure" }

func TestEmitUploadsGrowingTrail(t *testing.T) {
	fp := &fakePutter{}
	r := NewWithPutter("warehouse", "trails", fp)

	if err := r.Emit(context.Background(), KindJobStarted, map[string]any{"tables": 2}); err != nil {
		t.Fatalf("emit job_started: %v", err)
	}
	if err := r.Emit(context.Background(), KindCommit, map[string]any{"table": "orders", "rows": 3}); err != nil {
		t.Fatalf("emit commit: %v", err)
	}

	puts := fp.calls()
	if len(puts) != 2 {
		t.Fatalf("got %d puts, want 2", len(puts))
	}

	// Each put replaces the object with the full trail: line count grows.
	if n := strings.Count(puts[0], "\n"); n != 1 {
		t.Errorf("first put has %d lines, want 1", n)
	}
	if n := strings.Count(puts[1], "\n"); n != 2 {
		t.Errorf("second put has %d lines, want 2", n)
	}

	// Every put lands on the same key: <prefix>/run-<id>/events.jsonl.
	key := "trails/run-" + r.ID() + "/events.jsonl"
	for i, p := range puts {
		if !strings.HasPrefix(p, "warehouse|"+key+"|") {
			t.Errorf("put %d bucket/key mismatch: %q", i, p)
		}
	}
}

func TestEmitEventShape(t *testing.T) {
	fp := &fakePutter{}
	r := NewWithPutter("b", "p", fp)
	if err := r.Emit(context.Background(), KindResume, map[string]any{"from": "none"}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	// Body is the third field: bucket|key|body.
	parts := strings.SplitN(fp.calls()[0], "|", 3)
	var ev map[string]any
	if err := json.Unmarshal([]byte(parts[2]), &ev); err != nil {
		t.Fatalf("line is not json: %v", err)
	}
	if ev["kind"] != KindResume {
		t.Errorf("kind = %v", ev["kind"])
	}
	if ev["run_id"] != r.ID() {
		t.Errorf("run_id = %v, want %s", ev["run_id"], r.ID())
	}
	if _, ok := ev["ts"]; !ok {
		t.Error("ts missing")
	}
	if ev["from"] != "none" {
		t.Errorf("payload field lost: %v", ev)
	}
}

func TestEmitFailureIsSurfacedNotFatal(t *testing.T) {
	fp := &fakePutter{failN: 1}
	r := NewWithPutter("b", "p", fp)
	if err := r.Emit(context.Background(), KindJobStarted, nil); err == nil {
		t.Fatal("expected error from failing put")
	}
	if r.Emitted() != 1 {
		t.Errorf("emitted = %d, want 1 (counted despite upload failure)", r.Emitted())
	}
	// Recovery: the next emit retries the whole trail, so no line is lost.
	if err := r.Emit(context.Background(), KindCommit, nil); err != nil {
		t.Fatalf("recovery emit: %v", err)
	}
	if n := strings.Count(fp.calls()[0], "\n"); n != 2 {
		t.Errorf("recovery put has %d lines, want 2 (both events)", n)
	}
}

func TestCloseSealsRun(t *testing.T) {
	r := NewWithPutter("b", "p", &fakePutter{})
	r.Close()
	if err := r.Emit(context.Background(), KindCommit, nil); err == nil {
		t.Fatal("emit after close must fail")
	}
}

func TestParseURI(t *testing.T) {
	cases := []struct {
		uri     string
		bucket  string
		prefix  string
		wantErr bool
	}{
		{uri: "s3://bucket", bucket: "bucket"},
		{uri: "s3://bucket/trails", bucket: "bucket", prefix: "trails"},
		{uri: "s3://bucket/a/b", bucket: "bucket", prefix: "a/b"},
		{uri: "http://x/y", wantErr: true},
		{uri: "s3://", wantErr: true},
		{uri: "s3:///prefix", wantErr: true},
	}
	for _, tc := range cases {
		bucket, prefix, err := parseURI(tc.uri)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: want error", tc.uri)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", tc.uri, err)
			continue
		}
		if bucket != tc.bucket || prefix != tc.prefix {
			t.Errorf("%q: got (%q, %q), want (%q, %q)", tc.uri, bucket, prefix, tc.bucket, tc.prefix)
		}
	}
}

func TestNewBadURIFailsFast(t *testing.T) {
	if _, err := New(context.Background(), Config{URI: "not-s3"}); err == nil {
		t.Fatal("expected construction error for non-s3 uri")
	}
}

func TestRunIDsAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := NewWithPutter("b", "p", &fakePutter{}).ID()
		if seen[id] {
			t.Fatalf("duplicate run id %q", id)
		}
		seen[id] = true
	}
}
