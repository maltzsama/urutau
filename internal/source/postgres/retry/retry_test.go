package retry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRetryBackoffCapped(t *testing.T) {
	// Never negative, never above the 30s cap, even for a huge retryCount
	// where 2^attempt * time.Second would overflow time.Duration.
	for _, attempt := range []int{0, 1, 4, 5, 6, 34, 35, 100} {
		d := Backoff(attempt)
		if d <= 0 {
			t.Fatalf("Backoff(%d) = %v, want > 0", attempt, d)
		}
		if d > 30*time.Second {
			t.Fatalf("Backoff(%d) = %v, want <= 30s", attempt, d)
		}
	}
	if got := Backoff(0); got != time.Second {
		t.Fatalf("Backoff(0) = %v, want 1s", got)
	}
	if got := Backoff(5); got != 30*time.Second {
		t.Fatalf("Backoff(5) = %v, want 30s", got)
	}
}

func TestRetryTransientErr(t *testing.T) {
	attempts := 0
	transient := &pgconn.PgError{Code: "08006"}
	err := DoErr(context.Background(), 3, func() error {
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
		if got := retryableSQLState(c.code); got != c.want {
			t.Errorf("retryableSQLState(%q) = %v, want %v", c.code, got, c.want)
		}
	}
}

func TestIsTransientPgError(t *testing.T) {
	if !IsTransient(&pgconn.PgError{Code: "08006"}) {
		t.Fatal("connection exception must be transient")
	}
	if IsTransient(&pgconn.PgError{Code: "28P01"}) {
		t.Fatal("invalid password must not be transient")
	}
	// A wrapped PgError is still classified.
	wrapped := fmt.Errorf("connect: %w", &pgconn.PgError{Code: "53300"})
	if !IsTransient(wrapped) {
		t.Fatal("wrapped transient PgError must be transient")
	}
}

func TestIsTransientNetworkError(t *testing.T) {
	if !IsTransient(&net.OpError{Op: "dial", Err: errors.New("connection refused")}) {
		t.Fatal("net.Error must be transient")
	}
	if !IsTransient(context.DeadlineExceeded) {
		t.Fatal("deadline exceeded must be transient")
	}
	if IsTransient(errors.New("plain error")) {
		t.Fatal("a plain error must not be transient")
	}
	if IsTransient(nil) {
		t.Fatal("nil must not be transient")
	}
}

func TestIsTransientNonPgError(t *testing.T) {
	// A plain error has no SQLSTATE; the shared rules decide (not retryable).
	if IsTransient(errors.New("plain")) {
		t.Fatal("a plain error must not be transient")
	}
}

func TestRetryTransientPermanentNotRetried(t *testing.T) {
	attempts := 0
	perm := &pgconn.PgError{Code: "28P01"} // invalid password
	_, err := Do(context.Background(), 5, func() (int, error) {
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
	v, err := Do(context.Background(), 3, func() (int, error) {
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
	_, err := Do(context.Background(), 1, func() (int, error) {
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
	_, err := Do(ctx, 5, func() (int, error) {
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
