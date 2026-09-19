package errors

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestClassifySQLStateCategories(t *testing.T) {
	cases := []struct {
		code string
		want Failure
	}{
		{"28P01", AuthFailed},
		{"28000", AuthFailed},
		{"3D000", ObjectNotFound},
		{"3F000", ObjectNotFound},
		{"42P01", ObjectNotFound},
		{"42703", ObjectNotFound},
		{"42501", PermissionDenied},
		{"42601", SourceReadError},
		{"42883", SourceReadError},
		{"42804", SchemaUnsupported},
		{"42846", SchemaUnsupported},
		{"42P18", SchemaUnsupported},
		{"53300", ResourceExhausted},
		{"53100", ResourceExhausted},
		{"53200", ResourceExhausted},
		{"54000", ResourceExhausted},
		{"55006", ConcurrencyConflict},
		{"55P03", ConcurrencyConflict},
		{"40001", ConcurrencyConflict},
		{"40P01", ConcurrencyConflict},
		{"57P01", NetworkUnreachable},
		{"57P03", NetworkUnreachable},
		{"XX000", InternalError},
		{"08006", NetworkUnreachable}, // class fallback
		{"53000", ResourceExhausted},  // class fallback
		{"57123", NetworkUnreachable}, // class fallback
		{"42601", SourceReadError},
		{"99999", SourceReadError}, // unknown -> read error (permanent)
		{"", SourceReadError},
		{"2", SourceReadError},
	}
	for _, c := range cases {
		if got := ClassifySQLState(c.code); got != c.want {
			t.Errorf("ClassifySQLState(%q) = %v, want %v", c.code, got, c.want)
		}
	}
}

func TestFailureTransient(t *testing.T) {
	transient := []Failure{NetworkUnreachable, ResourceExhausted, ConcurrencyConflict}
	for _, f := range transient {
		if !f.Transient() {
			t.Errorf("%v.Transient() = false, want true", f)
		}
	}
	permanent := []Failure{Unknown, AuthFailed, ObjectNotFound, PermissionDenied, SourceReadError, SchemaUnsupported, InternalError, NoData}
	for _, f := range permanent {
		if f.Transient() {
			t.Errorf("%v.Transient() = true, want false", f)
		}
	}
}

func TestFailureString(t *testing.T) {
	if got := AuthFailed.String(); got != "AuthFailed" {
		t.Fatalf("AuthFailed.String() = %q", got)
	}
	if got := Failure(200).String(); got != "Unknown" {
		t.Fatalf("out-of-range Failure.String() = %q, want Unknown", got)
	}
}

func TestClassifyNoDataSentinel(t *testing.T) {
	wrapped := errors.New("postgres: no WAL message: " + ErrNoData.Error())
	if got := Classify(ErrNoData); got != NoData {
		t.Fatalf("Classify(ErrNoData) = %v, want NoData", got)
	}
	if got := Classify(wrapped); got != Unknown {
		// A plain string match must not classify; only errors.Is does.
		t.Fatalf("Classify(string-matching error) = %v, want Unknown", got)
	}
}

func TestClassifyFallbackNetwork(t *testing.T) {
	if got := Classify(&net.OpError{Op: "dial", Err: errors.New("refused")}); got != NetworkUnreachable {
		t.Fatalf("Classify(net.Error) = %v, want NetworkUnreachable", got)
	}
	if got := Classify(context.DeadlineExceeded); got != NetworkUnreachable {
		t.Fatalf("Classify(deadline) = %v, want NetworkUnreachable", got)
	}
	if got := Classify(nil); got != Unknown {
		t.Fatalf("Classify(nil) = %v, want Unknown", got)
	}
	if got := Classify(errors.New("plain")); got != Unknown {
		t.Fatalf("Classify(plain) = %v, want Unknown", got)
	}
}

func TestClassifyRegisteredClassifier(t *testing.T) {
	// A classifier that claims only errors carrying a specific marker. It is
	// registered for the process, so the assertion is limited to its own error.
	marker := errors.New("marker")
	RegisterClassifier(func(err error) (Failure, bool) {
		if errors.Is(err, marker) {
			return PermissionDenied, true
		}
		return Unknown, false
	})
	if got := Classify(marker); got != PermissionDenied {
		t.Fatalf("Classify(marker) = %v, want PermissionDenied", got)
	}
}
