package kafka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
)

func TestClassifyFetchPermanent(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int16
	}{
		{"UNKNOWN_TOPIC_OR_PARTITION", 3},
		{"TOPIC_AUTHORIZATION_FAILED", 29},
		{"SASL_AUTHENTICATION_FAILED", 58},
		{"UNSUPPORTED_SASL_MECHANISM", 33},
	} {
		err := &kerr.Error{Code: tc.code, Message: tc.name}
		if got := classifyFetch(err); got != fetchPermanent {
			t.Errorf("%s: classifyFetch = %v, want fetchPermanent", tc.name, got)
		}
	}
}

func TestClassifyFetchTransient(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int16
	}{
		{"BROKER_NOT_AVAILABLE", 8},
		{"NETWORK_EXCEPTION", 13},
		{"COORDINATOR_NOT_AVAILABLE", 15},
		{"NOT_LEADER_FOR_PARTITION", 6},
		{"REQUEST_TIMED_OUT", 7},
	} {
		err := &kerr.Error{Code: tc.code, Message: tc.name}
		if got := classifyFetch(err); got != fetchTransient {
			t.Errorf("%s: classifyFetch = %v, want fetchTransient", tc.name, got)
		}
	}
}

// OFFSET_OUT_OF_RANGE is neither: the data is gone, so retrying is pointless
// and failing without saying why leaves the operator guessing.
func TestClassifyFetchPositionLost(t *testing.T) {
	err := &kerr.Error{Code: 1, Message: "OFFSET_OUT_OF_RANGE"}
	if got := classifyFetch(err); got != fetchPositionLost {
		t.Errorf("classifyFetch = %v, want fetchPositionLost", got)
	}
}

// A dial or TLS failure carries no protocol code. It is transient: the
// broker may come back, and a genuinely broken endpoint surfaces through a
// bounded retry rather than a silent spin.
func TestClassifyFetchNonProtocolIsTransient(t *testing.T) {
	if got := classifyFetch(errors.New("dial tcp: connection refused")); got != fetchTransient {
		t.Errorf("classifyFetch = %v, want fetchTransient", got)
	}
}

// Classification must survive wrapping — franz-go returns errors nested in
// fetch and partition context.
func TestClassifyFetchUnwrapsWrappedError(t *testing.T) {
	wrapped := fmt.Errorf("fetching from broker: %w",
		fmt.Errorf("partition 3: %w", &kerr.Error{Code: 3, Message: "UNKNOWN_TOPIC_OR_PARTITION"}))
	if got := classifyFetch(wrapped); got != fetchPermanent {
		t.Errorf("classifyFetch(wrapped) = %v, want fetchPermanent", got)
	}
}

// A topic pointed at the wrong registry, or an Avro decoder pointed at a
// non-Avro topic, fails every record the same way. Dropping them decoded
// nothing while reporting success.
func TestDecodeIsFatalForPermanentDecoderErrors(t *testing.T) {
	if !decodeIsFatal(&decoder.ErrBadWireFormat{}) {
		t.Error("ErrBadWireFormat must fail the reader, not drop the record")
	}
	if !decodeIsFatal(&decoder.ErrUnknownSchema{ID: 42}) {
		t.Error("ErrUnknownSchema must fail the reader, not drop the record")
	}
}

// Wrapping must not hide the classification.
func TestDecodeIsFatalUnwraps(t *testing.T) {
	wrapped := fmt.Errorf("decoding record: %w", &decoder.ErrUnknownSchema{ID: 7})
	if !decodeIsFatal(wrapped) {
		t.Error("a wrapped permanent decoder error is still fatal")
	}
}

// A single malformed payload is not a reason to stop the pipeline — that one
// record is dropped and logged.
func TestDecodeIsFatalFalseForOrdinaryError(t *testing.T) {
	if decodeIsFatal(errors.New("unexpected end of JSON input")) {
		t.Error("an ordinary decode error must not fail the reader")
	}
}

func TestErrPositionLostMessageAndUnwrap(t *testing.T) {
	inner := &kerr.Error{Code: 1, Message: "OFFSET_OUT_OF_RANGE"}
	err := &ErrPositionLost{Topic: "orders", Err: inner}
	if got := err.Error(); got == "" {
		t.Fatal("ErrPositionLost must describe itself")
	}
	if !errors.Is(err.Unwrap(), error(inner)) {
		t.Error("ErrPositionLost must unwrap to the protocol error")
	}
	var target *kerr.Error
	if !errors.As(err, &target) || target.Code != 1 {
		t.Error("ErrPositionLost must stay inspectable as a kerr.Error")
	}
}
