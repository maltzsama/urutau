// Package errors decides whether a source failure is worth retrying. The
// pipeline asks one question — retry or fail — so this package exposes that
// one decision, not a taxonomy: a source classifies its own driver errors and
// falls back here for the rest.
package errors

import (
	"context"
	stderrors "errors"
	"net"
)

// ErrNoData marks a source that produced no data within its configured wait
// (#154). It is non-retryable: the operator must fix the configuration.
var ErrNoData = stderrors.New("source produced no data")

// Retryable reports whether err is worth retrying. A permanent failure (bad
// credentials, a missing object, a malformed query) is never retried — it does
// not heal with backoff. A source with driver-specific codes classifies those
// first and calls this only for the remainder.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	if stderrors.Is(err, ErrNoData) {
		return false
	}
	var netErr net.Error
	if stderrors.As(err, &netErr) {
		return true
	}
	return stderrors.Is(err, context.DeadlineExceeded)
}
