package coordinator

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/maltzsama/urutau/position"
)

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
	// Simulate key construction (same logic as checkpoint.run).
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

func TestPositionIndexDirty(t *testing.T) {
	idx := newPositionIndex("run-1")
	if idx.Dirty() {
		t.Fatal("new index should not be dirty")
	}

	// add makes it dirty
	idx.add(inflightBatch{id: 1, table: "t1", bytes: 100})
	if !idx.Dirty() {
		t.Fatal("add should set dirty")
	}

	// Manifest does NOT clear dirty (checkpoint writes it later)
	_ = idx.Manifest()
	if !idx.Dirty() {
		t.Fatal("Manifest should not clear dirty")
	}

	// MarkClean clears it
	idx.MarkClean()
	if idx.Dirty() {
		t.Fatal("MarkClean should clear dirty")
	}

	// truncate on new table makes it dirty
	idx.truncate("t1", positionFixture("pos-1"))
	if !idx.Dirty() {
		t.Fatal("truncate should set dirty")
	}

	idx.MarkClean()

	// duplicate truncate (same position) should NOT set dirty
	idx.truncate("t1", positionFixture("pos-1"))
	if idx.Dirty() {
		t.Fatal("duplicate truncate should not set dirty")
	}
}

// positionFixture is a minimal Position implementation for tests.
type positionFixture string

func (p positionFixture) String() string { return string(p) }
func (p positionFixture) Compare(other position.Position) int {
	return strings.Compare(string(p), other.(positionFixture).String())
}
func (p positionFixture) Contains(position.Position) bool { return false }

func TestCheckpointKeyFormat(t *testing.T) {
	// Verify the key format matches the expected S3 structure.
	c := &checkpoint{prefix: "checkpoints"}
	runID := "2026-01-01T00:00:00Z-abc123"
	worker := "w1"
	key := strings.TrimPrefix(c.prefix+"/", "/") + runID + "/" + worker + "/manifest.json"
	want := "checkpoints/2026-01-01T00:00:00Z-abc123/w1/manifest.json"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}

	// Empty prefix.
	c2 := &checkpoint{prefix: ""}
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

	cp := &checkpoint{
		interval: 10 * time.Millisecond,
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
	idx := newPositionIndex("run")
	idx.add(inflightBatch{id: 1, table: "t", bytes: 10})
	index := map[string]*positionIndex{"w": idx}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { cp.run(ctx, "run", index, slog.Default()); close(done) }()

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
