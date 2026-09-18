package mysql

import (
	"testing"
	"time"
)

func TestNormalize(t *testing.T) {
	if got := normalize([]byte("abc")); got != "abc" {
		t.Fatalf("normalize([]byte) = %v, want string", got)
	}
	if got := normalize(int64(5)); got != int64(5) {
		t.Fatalf("normalize(int64) changed the value")
	}
}

func TestPlaceholders(t *testing.T) {
	if got := placeholders(3); got != "?, ?, ?" {
		t.Fatalf("placeholders(3) = %q", got)
	}
}

// The snapshot query parses in UTC; normalizeSnapshot re-tags naive temporals
// into the operator's location so a snapshot row matches the CDC decode of the
// same row (issue #139).
func TestNormalizeSnapshotTemporal(t *testing.T) {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	utc := time.Date(2023, 1, 8, 12, 30, 45, 0, time.UTC)

	// DATETIME: the same wall clock, re-tagged to loc.
	if got := normalizeSnapshot(utc, "DATETIME", loc).(time.Time); !got.Equal(time.Date(2023, 1, 8, 12, 30, 45, 0, loc)) {
		t.Fatalf("DATETIME = %v", got)
	}
	// TIMESTAMP: the same instant.
	if got := normalizeSnapshot(utc, "TIMESTAMP", loc).(time.Time); !got.Equal(utc) {
		t.Fatalf("TIMESTAMP = %v, want %v", got, utc)
	}
	// DATE: midnight in loc.
	d := time.Date(2023, 1, 8, 0, 0, 0, 0, time.UTC)
	if got := normalizeSnapshot(d, "DATE", loc).(time.Time); !got.Equal(time.Date(2023, 1, 8, 0, 0, 0, 0, loc)) {
		t.Fatalf("DATE = %v", got)
	}
	// Non-temporal values fall back to normalize.
	if got := normalizeSnapshot([]byte("x"), "VARCHAR", loc); got != "x" {
		t.Fatalf("VARCHAR = %v, want x", got)
	}
}
