// Package errors classifies source and driver errors into actionable failure
// categories, so the pipeline can decide whether to retry, abort, or skip
// instead of inspecting raw driver error strings.
//
// A source registers a classifier from an init() function; Classify then walks
// the registered classifiers in order and returns the first category claimed.
// The package is a leaf (standard library only) so any source package can
// import it without violating the architecture wall.
package errors

import (
	"context"
	stderrors "errors"
	"net"
)

// Failure is the actionable category of an error.
type Failure uint8

const (
	// Unknown is the zero value: no classifier claimed the error.
	Unknown Failure = iota
	// AuthFailed: bad credentials or an authentication rejection.
	AuthFailed
	// ObjectNotFound: the database, schema, table, or column is missing.
	ObjectNotFound
	// PermissionDenied: the user lacks a required privilege.
	PermissionDenied
	// SourceReadError: a query failed for a reason the source cannot fix.
	SourceReadError
	// SchemaUnsupported: a column type or protocol shape the decoder cannot map.
	SchemaUnsupported
	// ResourceExhausted: the server is out of a finite resource.
	ResourceExhausted
	// ConcurrencyConflict: a serialization failure or deadlock; safe to retry.
	ConcurrencyConflict
	// NetworkUnreachable: a connection-level failure; safe to retry.
	NetworkUnreachable
	// InternalError: a server-side bug (XX000).
	InternalError
	// NoData: the source produced no data within the configured wait; a
	// misconfiguration signal, never retried.
	NoData
)

// String names the category.
func (f Failure) String() string {
	switch f {
	case AuthFailed:
		return "AuthFailed"
	case ObjectNotFound:
		return "ObjectNotFound"
	case PermissionDenied:
		return "PermissionDenied"
	case SourceReadError:
		return "SourceReadError"
	case SchemaUnsupported:
		return "SchemaUnsupported"
	case ResourceExhausted:
		return "ResourceExhausted"
	case ConcurrencyConflict:
		return "ConcurrencyConflict"
	case NetworkUnreachable:
		return "NetworkUnreachable"
	case InternalError:
		return "InternalError"
	case NoData:
		return "NoData"
	default:
		return "Unknown"
	}
}

// Transient reports whether a failure is worth retrying. Retrying a permanent
// failure (bad password, missing database, syntax error) only delays the
// inevitable, so classification is explicit rather than "retry everything".
func (f Failure) Transient() bool {
	switch f {
	case NetworkUnreachable, ResourceExhausted, ConcurrencyConflict:
		return true
	default:
		return false
	}
}

// ErrNoData marks a source that produced no data within its configured wait.
// It is non-retryable: the operator must fix the configuration.
var ErrNoData = stderrors.New("source produced no data")

// Classifier maps an error to a Failure. ok is false when the error is not
// the classifier's to claim, so the next classifier is tried.
type Classifier func(error) (Failure, bool)

var classifiers []Classifier

// RegisterClassifier appends a classifier. It is meant to be called from a
// source package's init(), so Classify sees it without a wiring layer.
func RegisterClassifier(c Classifier) {
	classifiers = append(classifiers, c)
}

// Classify returns the failure category for err. It checks the package's own
// sentinels first, then each registered classifier, and finally the shared
// network/deadline fallback.
func Classify(err error) Failure {
	if err == nil {
		return Unknown
	}
	if stderrors.Is(err, ErrNoData) {
		return NoData
	}
	for _, c := range classifiers {
		if f, ok := c(err); ok {
			return f
		}
	}
	var netErr net.Error
	if stderrors.As(err, &netErr) {
		return NetworkUnreachable
	}
	if stderrors.Is(err, context.DeadlineExceeded) {
		return NetworkUnreachable
	}
	return Unknown
}

// ClassifySQLState maps a PostgreSQL SQLSTATE code to a failure category, per
// PostgreSQL's error-code appendix. A source-specific classifier extracts the
// code and delegates here, keeping the mapping shared.
func ClassifySQLState(code string) Failure {
	switch code {
	case "28P01", "28000":
		return AuthFailed
	case "3D000", "3F000", "42P01", "42703":
		return ObjectNotFound
	case "42501":
		return PermissionDenied
	case "42601", "42883":
		return SourceReadError
	case "42804", "42846", "42P18":
		return SchemaUnsupported
	case "53300", "53100", "53200", "54000":
		return ResourceExhausted
	case "55006", "55P03", "40001", "40P01":
		return ConcurrencyConflict
	case "57P01", "57P03":
		return NetworkUnreachable
	case "XX000":
		return InternalError
	}
	// Class-level fallback: 08 connection exception, 53 insufficient
	// resources, 57 operator intervention.
	if len(code) >= 2 {
		switch code[:2] {
		case "08":
			return NetworkUnreachable
		case "53":
			return ResourceExhausted
		case "57":
			return NetworkUnreachable
		}
	}
	return SourceReadError
}
