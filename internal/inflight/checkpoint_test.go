package inflight

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestCheckpointKeyFormat(t *testing.T) {
	// Verify the key format matches the expected S3 structure.
	c := &Checkpoint{prefix: "checkpoints"}
	runID := "2026-01-01T00:00:00Z-abc123"
	worker := "w1"
	key := strings.TrimPrefix(c.prefix+"/", "/") + runID + "/" + worker + "/manifest.json"
	want := "checkpoints/2026-01-01T00:00:00Z-abc123/w1/manifest.json"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}

	// Empty prefix.
	c2 := &Checkpoint{prefix: ""}
	key2 := strings.TrimPrefix(c2.prefix+"/", "/") + runID + "/" + worker + "/manifest.json"
	if strings.HasPrefix(key2, "/") {
		t.Fatalf("empty prefix produces leading slash: %q", key2)
	}
	want2 := "2026-01-01T00:00:00Z-abc123/w1/manifest.json"
	if key2 != want2 {
		t.Fatalf("key = %q, want %q", key2, want2)
	}
}

// TestCheckpointMarkCleanOnlyOnSuccess covers audit #12: a failed PutObject
// must leave the index dirty, or a persistent S3 outage would clear the
// dirty flag for an upload that never happened and stop checkpoints forever.
func TestCheckpointMarkCleanOnlyOnSuccess(t *testing.T) {
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cp := &Checkpoint{
		Interval: 10 * time.Millisecond,
		bucket:   "b",
		prefix:   "",
		client: s3.NewFromConfig(aws.Config{}, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
			o.UsePathStyle = true
			o.Region = "us-east-1"
			o.Retryer = aws.NopRetryer{}
			o.Credentials = credentials.NewStaticCredentialsProvider("k", "s", "")
		}),
	}
	idx := NewPositionIndex("run")
	idx.Add(Batch{ID: 1, Table: "t", Bytes: 10})
	index := map[string]*PositionIndex{"w": idx}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		cp.Run(ctx, "run", func() map[string]*PositionIndex { return index }, slog.Default())
		close(done)
	}()

	fail.Store(true)
	time.Sleep(80 * time.Millisecond)
	if !idx.Dirty() {
		t.Fatal("index must stay dirty while uploads fail")
	}
	fail.Store(false)
	deadline := time.Now().Add(2 * time.Second)
	for idx.Dirty() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if idx.Dirty() {
		t.Fatal("index must clear once an upload succeeds")
	}
	cancel()
	<-done
}

// TestCheckpointSeesDynamicallyAddedWorker covers issue #554: the checkpoint
// must re-read the live owner set every tick, so an owner registered by a
// scale-out (#312) is checkpointed from the next tick on — a boot-time
// snapshot never saw it.
func TestCheckpointSeesDynamicallyAddedWorker(t *testing.T) {
	var mu sync.Mutex
	paths := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths[r.URL.Path] = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cp := &Checkpoint{
		Interval: 10 * time.Millisecond,
		bucket:   "b",
		prefix:   "",
		client: s3.NewFromConfig(aws.Config{}, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
			o.UsePathStyle = true
			o.Region = "us-east-1"
			o.Retryer = aws.NopRetryer{}
			o.Credentials = credentials.NewStaticCredentialsProvider("k", "s", "")
		}),
	}

	boot := NewPositionIndex("run")
	boot.Add(Batch{ID: 1, Table: "t", Bytes: 10})
	owners := map[string]*PositionIndex{"w0": boot}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		cp.Run(ctx, "run", func() map[string]*PositionIndex {
			mu.Lock()
			defer mu.Unlock()
			out := make(map[string]*PositionIndex, len(owners))
			for k, v := range owners {
				out[k] = v
			}
			return out
		}, slog.Default())
		close(done)
	}()

	waitPath := func(substr string) bool {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			found := false
			for p := range paths {
				if strings.Contains(p, substr) {
					found = true
					break
				}
			}
			mu.Unlock()
			if found {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return false
	}

	if !waitPath("/w0/manifest.json") {
		t.Fatal("boot worker not checkpointed")
	}

	// A scale-out registers w1 after boot.
	added := NewPositionIndex("run")
	added.Add(Batch{ID: 2, Table: "t", Bytes: 10})
	mu.Lock()
	owners["w1"] = added
	mu.Unlock()

	if !waitPath("/w1/manifest.json") {
		t.Fatal("dynamically added worker never checkpointed (issue #554)")
	}
	cancel()
	<-done
}

func TestParseS3URI(t *testing.T) {
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
			bucket, prefix, err := parseS3URI(tt.uri)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("parseS3URI(%q): got err %v, want %q", tt.uri, err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseS3URI(%q): unexpected error: %v", tt.uri, err)
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

func TestParseS3URIKeyNoLeadingSlash(t *testing.T) {
	// An empty prefix must not produce a key with a leading slash.
	bucket, prefix, err := parseS3URI("s3://mybucket")
	if err != nil {
		t.Fatal(err)
	}
	if bucket != "mybucket" {
		t.Fatalf("bucket = %q", bucket)
	}
	// Simulate key construction (same logic as checkpoint.Run).
	key := strings.TrimPrefix(prefix+"/", "/") + "run1" + "/worker1" + "/manifest.json"
	if strings.HasPrefix(key, "/") {
		t.Fatalf("key has leading slash: %q", key)
	}
	if key != "run1/worker1/manifest.json" {
		t.Fatalf("key = %q, want run1/worker1/manifest.json", key)
	}
}

func TestParseS3URIKeyWithPrefix(t *testing.T) {
	bucket, prefix, err := parseS3URI("s3://mybucket/checkpoints/")
	if err != nil {
		t.Fatal(err)
	}
	if bucket != "mybucket" {
		t.Fatalf("bucket = %q", bucket)
	}
	// prefix after TrimSuffix("/") in newCheckpoint = "checkpoints"
	trimmed := strings.TrimSuffix(prefix, "/")
	key := strings.TrimPrefix(trimmed+"/", "/") + "run1" + "/worker1" + "/manifest.json"
	if key != "checkpoints/run1/worker1/manifest.json" {
		t.Fatalf("key = %q", key)
	}
}
