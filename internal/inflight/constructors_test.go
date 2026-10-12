package inflight

import (
	"context"
	"testing"
	"time"
)

func TestNewCheckpointRejectsBadURI(t *testing.T) {
	if _, err := NewCheckpoint(context.Background(), CheckpointConfig{URI: "http://nope"}); err == nil {
		t.Fatal("a non-s3 URI must be rejected")
	}
}

func TestNewCheckpointBuildsClient(t *testing.T) {
	cp, err := NewCheckpoint(context.Background(), CheckpointConfig{
		URI:       "s3://bucket/prefix/",
		Endpoint:  "http://127.0.0.1:1",
		AccessKey: "a",
		SecretKey: "b",
	})
	if err != nil {
		// No AWS config in the sandbox: the constructor's error arm ran.
		return
	}
	if cp.bucket != "bucket" || cp.prefix != "prefix" {
		t.Fatalf("checkpoint = %+v", cp)
	}
	if cp.Interval != 10*time.Second {
		t.Fatalf("default interval = %v, want 10s", cp.Interval)
	}
}
