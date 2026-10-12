package connect

import (
	"testing"
	"time"
)

func TestResolveMaxThreads(t *testing.T) {
	if got := resolveMaxThreads(5); got != 5 {
		t.Fatalf("resolveMaxThreads(5) = %d, want 5", got)
	}
	if got := resolveMaxThreads(0); got < 1 || got > maxMaxThreads {
		t.Fatalf("resolveMaxThreads(0) = %d, want 1..%d", got, maxMaxThreads)
	}
	if got := resolveMaxThreads(1000); got != maxMaxThreads {
		t.Fatalf("resolveMaxThreads(1000) = %d, want %d", got, maxMaxThreads)
	}
}

func TestResolveRetryCount(t *testing.T) {
	if got := resolveRetryCount(0); got != defaultRetryCount {
		t.Fatalf("resolveRetryCount(0) = %d, want %d", got, defaultRetryCount)
	}
	if got := resolveRetryCount(7); got != 7 {
		t.Fatalf("resolveRetryCount(7) = %d, want 7", got)
	}
	if got := resolveRetryCount(-1); got != 0 {
		t.Fatalf("resolveRetryCount(-1) = %d, want 0", got)
	}
}

func TestResolveInitialWaitTime(t *testing.T) {
	if got := resolveInitialWaitTime(0); got != DefaultInitialWait {
		t.Fatalf("resolveInitialWaitTime(0) = %v, want %v", got, DefaultInitialWait)
	}
	if got := resolveInitialWaitTime(120); got != 120*time.Second {
		t.Fatalf("resolveInitialWaitTime(120) = %v, want 120s", got)
	}
	if got := resolveInitialWaitTime(5); got != minInitialWait {
		t.Fatalf("resolveInitialWaitTime(5) = %v, want the %v floor", got, minInitialWait)
	}
}
