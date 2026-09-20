package errors

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestRetryable(t *testing.T) {
	if Retryable(nil) {
		t.Fatal("nil must not be retryable")
	}
	if Retryable(ErrNoData) {
		t.Fatal("ErrNoData must not be retryable")
	}
	if Retryable(errors.New("plain")) {
		t.Fatal("a plain error must not be retryable")
	}
	if !Retryable(&net.OpError{Op: "dial", Err: errors.New("refused")}) {
		t.Fatal("a net.Error must be retryable")
	}
	if !Retryable(context.DeadlineExceeded) {
		t.Fatal("a deadline must be retryable")
	}
}
