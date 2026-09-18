package postgres

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// retryBackoff returns the exponential backoff for attempt n (0-based):
// 1s, 2s, 4s, 8s, 16s, then 30s. The exponent is capped BEFORE the shift:
// 2^n seconds overflows time.Duration (int64 nanoseconds) around n=34,
// yielding a negative delay that would fire immediately. 30s needs at most
// 2^5, so anything beyond is the cap.
func retryBackoff(attempt int) time.Duration {
	if attempt >= 5 {
		return 30 * time.Second
	}
	return time.Duration(1<<uint(attempt)) * time.Second
}

// retryTransient runs op up to retries+1 times, retrying only transient
// errors with exponential backoff. A permanent error, or an exhausted
// budget, returns the last error. ctx cancellation stops the loop.
//
// op must be safe to re-run from scratch: the caller retries the whole
// operation, not a partial result.
func retryTransient[T any](ctx context.Context, retries int, op func() (T, error)) (T, error) {
	var zero T
	for attempt := 0; ; attempt++ {
		v, err := op()
		if err == nil {
			return v, nil
		}
		if !isTransient(err) || attempt >= retries {
			return zero, err
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(retryBackoff(attempt)):
		}
	}
}

// retryTransientErr is retryTransient for operations with no result value.
func retryTransientErr(ctx context.Context, retries int, op func() error) error {
	_, err := retryTransient(ctx, retries, func() (struct{}, error) {
		return struct{}{}, op()
	})
	return err
}

// isTransient reports whether a connection/query error is worth retrying.
// Retrying a permanent error (bad password, missing database, syntax error)
// only delays the inevitable failure, so classification is explicit rather
// than "retry everything".
//
// Classification is by SQLSTATE class, per PostgreSQL's error-code appendix:
//   - class 08 — connection exception
//   - class 53 — insufficient resources (e.g. 53300 too many connections)
//   - class 57 — operator intervention (57P01 admin shutdown, 57P03 cannot
//     connect now)
//   - 40001 serialization failure, 40P01 deadlock detected
//
// Network-level failures (net.Error) and a connect deadline are also
// transient. This is the minimal subset #166 needs; issue #159 grows it into
// a shared errs package.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return transientSQLState(pgErr.Code)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

// transientSQLState classifies a SQLSTATE code.
func transientSQLState(code string) bool {
	if len(code) < 2 {
		return false
	}
	switch code[:2] {
	case "08", "53", "57":
		return true
	}
	switch code {
	case "40001", "40P01":
		return true
	}
	return false
}
