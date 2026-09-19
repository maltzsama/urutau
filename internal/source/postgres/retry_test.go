package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	errs "github.com/maltzsama/urutau/internal/errors"
)

func TestRetryBackoffCapped(t *testing.T) {
	// Never negative, never above the 30s cap, even for a huge retryCount
	// where 2^attempt * time.Second would overflow time.Duration.
	for _, attempt := range []int{0, 1, 4, 5, 6, 34, 35, 100} {
		d := retryBackoff(attempt)
		if d <= 0 {
			t.Fatalf("retryBackoff(%d) = %v, want > 0", attempt, d)
		}
		if d > 30*time.Second {
			t.Fatalf("retryBackoff(%d) = %v, want <= 30s", attempt, d)
		}
	}
	if got := retryBackoff(0); got != time.Second {
		t.Fatalf("retryBackoff(0) = %v, want 1s", got)
	}
	if got := retryBackoff(5); got != 30*time.Second {
		t.Fatalf("retryBackoff(5) = %v, want 30s", got)
	}
}

func TestRetryTransientErr(t *testing.T) {
	attempts := 0
	transient := &pgconn.PgError{Code: "08006"}
	err := retryTransientErr(context.Background(), 3, func() error {
		attempts++
		if attempts == 1 {
			return transient
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestIsTransientSQLState(t *testing.T) {
	cases := []struct {
		code string
		want bool
	}{
		{"08006", true},  // connection failure
		{"08001", true},  // sqlclient unable to establish
		{"53300", true},  // too many connections
		{"53200", true},  // out of memory
		{"57P01", true},  // admin shutdown
		{"57P03", true},  // cannot connect now
		{"40001", true},  // serialization failure
		{"40P01", true},  // deadlock
		{"28P01", false}, // invalid password
		{"3D000", false}, // database does not exist
		{"42601", false}, // syntax error
		{"42P01", false}, // undefined table
		{"", false},
		{"2", false},
	}
	for _, c := range cases {
		if got := errs.ClassifySQLState(c.code).Transient(); got != c.want {
			t.Errorf("ClassifySQLState(%q).Transient() = %v, want %v", c.code, got, c.want)
		}
	}
}

func TestIsTransientPgError(t *testing.T) {
	if !isTransient(&pgconn.PgError{Code: "08006"}) {
		t.Fatal("connection exception must be transient")
	}
	if isTransient(&pgconn.PgError{Code: "28P01"}) {
		t.Fatal("invalid password must not be transient")
	}
	// A wrapped PgError is still classified.
	wrapped := fmt.Errorf("connect: %w", &pgconn.PgError{Code: "53300"})
	if !isTransient(wrapped) {
		t.Fatal("wrapped transient PgError must be transient")
	}
}

func TestIsTransientNetworkError(t *testing.T) {
	if !isTransient(&net.OpError{Op: "dial", Err: errors.New("connection refused")}) {
		t.Fatal("net.Error must be transient")
	}
	if !isTransient(context.DeadlineExceeded) {
		t.Fatal("deadline exceeded must be transient")
	}
	if isTransient(errors.New("plain error")) {
		t.Fatal("a plain error must not be transient")
	}
	if isTransient(nil) {
		t.Fatal("nil must not be transient")
	}
}

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
	if got := resolveInitialWaitTime(0); got != defaultInitialWait {
		t.Fatalf("resolveInitialWaitTime(0) = %v, want %v", got, defaultInitialWait)
	}
	if got := resolveInitialWaitTime(120); got != 120*time.Second {
		t.Fatalf("resolveInitialWaitTime(120) = %v, want 120s", got)
	}
	if got := resolveInitialWaitTime(5); got != minInitialWait {
		t.Fatalf("resolveInitialWaitTime(5) = %v, want the %v floor", got, minInitialWait)
	}
}

func TestClassifyPgError(t *testing.T) {
	if f, ok := classifyPgError(&pgconn.PgError{Code: "28P01"}); !ok || f != errs.AuthFailed {
		t.Fatalf("classifyPgError(28P01) = %v, %v; want AuthFailed, true", f, ok)
	}
	if f, ok := classifyPgError(&pgconn.PgError{Code: "40001"}); !ok || f != errs.ConcurrencyConflict {
		t.Fatalf("classifyPgError(40001) = %v, %v; want ConcurrencyConflict, true", f, ok)
	}
	if _, ok := classifyPgError(errors.New("plain")); ok {
		t.Fatal("a non-PgError must not be claimed")
	}
}

func TestRetryTransientPermanentNotRetried(t *testing.T) {
	attempts := 0
	perm := &pgconn.PgError{Code: "28P01"} // invalid password
	_, err := retryTransient(context.Background(), 5, func() (int, error) {
		attempts++
		return 0, perm
	})
	if !errors.Is(err, perm) {
		t.Fatalf("err = %v, want the permanent error", err)
	}
	if attempts != 1 {
		t.Fatalf("permanent error retried %d times, want 1", attempts)
	}
}

func TestRetryTransientSucceedsAfterRetry(t *testing.T) {
	attempts := 0
	transient := &pgconn.PgError{Code: "08006"} // connection failure
	v, err := retryTransient(context.Background(), 3, func() (int, error) {
		attempts++
		if attempts == 1 {
			return 0, transient
		}
		return 42, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != 42 || attempts != 2 {
		t.Fatalf("v=%d attempts=%d, want 42/2", v, attempts)
	}
}

func TestRetryTransientExhaustsBudget(t *testing.T) {
	attempts := 0
	transient := &pgconn.PgError{Code: "53300"} // too many connections
	_, err := retryTransient(context.Background(), 1, func() (int, error) {
		attempts++
		return 0, transient
	})
	if !errors.Is(err, transient) {
		t.Fatalf("err = %v, want the transient error", err)
	}
	if attempts != 2 { // initial + 1 retry
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestRetryTransientContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	transient := &pgconn.PgError{Code: "08006"}
	_, err := retryTransient(ctx, 5, func() (int, error) {
		attempts++
		return 0, transient
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}
