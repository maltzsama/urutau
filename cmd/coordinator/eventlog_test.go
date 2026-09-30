package main

import "testing"

// Issue #465: --eventlog-endpoint points the trail at an S3-compatible store.
func TestEventlogEndpointReachesTheTrail(t *testing.T) {
	cfg := eventlogConfig("s3://trails/urutau", "http://rustfs:9000")
	if cfg == nil || cfg.URI != "s3://trails/urutau" || cfg.Endpoint != "http://rustfs:9000" {
		t.Fatalf("eventlog config = %+v, want the URI and the endpoint", cfg)
	}
	if eventlogConfig("", "http://rustfs:9000") != nil {
		t.Fatal("an endpoint without --eventlog must not turn the trail on")
	}
}
