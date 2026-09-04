package iceberg

import (
	"errors"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
)

// isRetryableError classifies transient catalog/store errors as retryable
// and terminal schema/type errors as not.
func TestIsRetryableError(t *testing.T) {
	transient := []error{
		errors.New("iceberg: rest catalog 500 Internal Server Error"),
		errors.New("iceberg: 503 Service Unavailable"),
		errors.New("s3: SlowDown: please reduce your request rate"),
		errors.New("s3: RequestTimeout"),
		errors.New("rpc error: connection refused"),
		errors.New("s3: read: EOF"),
		errors.New("i/o timeout"),
		table.ErrCommitFailed,
	}
	for _, e := range transient {
		if !isRetryableError(e) {
			t.Errorf("%v should be retryable", e)
		}
	}

	terminal := []error{
		errors.New("iceberg: column x: unknown canonical kind"),
		errors.New("iceberg: create table: schema mismatch"),
		errors.New("iceberg: no such table"),
	}
	for _, e := range terminal {
		if isRetryableError(e) {
			t.Errorf("%v should be terminal", e)
		}
	}
}

// backoffDuration grows with the attempt, is bounded by the cap plus
// jitter, and never returns a duration small enough to panic Int64N.
func TestBackoffDuration(t *testing.T) {
	// Bounded: base or above, never beyond cap + 25% jitter.
	for attempt := 0; attempt < 10; attempt++ {
		d := backoffDuration(200*time.Millisecond, attempt)
		if d < 200*time.Millisecond {
			t.Fatalf("attempt %d: backoff %v below base", attempt, d)
		}
		if d > 38*time.Second { // 30s cap + 25% jitter on the capped value
			t.Fatalf("attempt %d: backoff %v exceeds cap+jitter", attempt, d)
		}
	}
	// The multiplier grows with the attempt until the cap: early attempts
	// must stay well under later capped ones.
	if got := backoffDuration(200*time.Millisecond, 0); got > time.Second {
		t.Fatalf("attempt 0 backoff %v too large", got)
	}
	if got := backoffDuration(200*time.Millisecond, 7); got < 20*time.Second {
		t.Fatalf("attempt 7 backoff %v too small (should be near the 25.6s base cap)", got)
	}
	// The minimum allowed base must not panic (jitter division).
	for attempt := 0; attempt < 3; attempt++ {
		if d := backoffDuration(4*time.Nanosecond, attempt); d <= 0 {
			t.Fatalf("backoff %v must be positive", d)
		}
	}
}
