package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

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
		if got := transientSQLState(c.code); got != c.want {
			t.Errorf("transientSQLState(%q) = %v, want %v", c.code, got, c.want)
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
