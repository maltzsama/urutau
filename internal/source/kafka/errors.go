package kafka

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
)

// fetchClass is what the consume loop does about a fetch error.
type fetchClass int

const (
	// fetchTransient is a broker, leader or coordinator that is not serving
	// right now. Retrying the same fetch is the remedy, after a delay.
	fetchTransient fetchClass = iota
	// fetchPermanent is a condition no amount of retrying resolves: the topic
	// does not exist, the principal lacks a right, the credentials are wrong.
	// It must fail the pipeline so an operator sees it.
	fetchPermanent
	// fetchPositionLost is retention having passed the offset we asked for.
	// The data is gone; resuming needs a decision (re-read from the start, or
	// stop), not a retry.
	fetchPositionLost
)

// permanentCodes are Kafka protocol errors that retrying cannot fix. Codes
// are spelled as franz-go's own kerr table spells them. Anything absent is
// treated as transient — a code with no rule is far more likely to be a
// broker hiccup than a misconfiguration, and the retry is bounded anyway.
var permanentCodes = map[int16]struct{}{
	58: {}, // SASL_AUTHENTICATION_FAILED
	34: {}, // ILLEGAL_SASL_STATE
	66: {}, // DELEGATION_TOKEN_EXPIRED
	29: {}, // TOPIC_AUTHORIZATION_FAILED
	30: {}, // GROUP_AUTHORIZATION_FAILED
	31: {}, // CLUSTER_AUTHORIZATION_FAILED
	65: {}, // DELEGATION_TOKEN_AUTHORIZATION_FAILED
	3:  {}, // UNKNOWN_TOPIC_OR_PARTITION
	17: {}, // INVALID_TOPIC_EXCEPTION
	33: {}, // UNSUPPORTED_SASL_MECHANISM
	35: {}, // UNSUPPORTED_VERSION
	43: {}, // UNSUPPORTED_FOR_MESSAGE_FORMAT
	76: {}, // UNSUPPORTED_COMPRESSION_TYPE
	54: {}, // SECURITY_DISABLED
}

// classifyFetch decides what to do about a fetch error. A non-protocol error
// (dial failure, TLS, a closed connection) is transient: the broker may come
// back, and a permanent network misconfiguration surfaces as a bounded retry
// that exhausts rather than a silent spin.
func classifyFetch(err error) fetchClass {
	var protocolErr *kerr.Error
	if !errors.As(err, &protocolErr) {
		return fetchTransient
	}
	// OFFSET_OUT_OF_RANGE: retention passed the offset we asked for.
	if protocolErr.Code == 1 {
		return fetchPositionLost
	}
	if _, ok := permanentCodes[protocolErr.Code]; ok {
		return fetchPermanent
	}
	return fetchTransient
}

// decodeIsFatal reports whether a decoder error must fail the reader rather
// than drop the record. The decoders already mark the conditions no retry
// resolves:
//   - ErrBadWireFormat: the topic is not Confluent-Avro.
//   - ErrUnknownSchema: the registry does not know the schema id.
//   - ErrNotJSON: field extraction is declared on a topic whose payload is
//     not JSON — the spec's assertion about the topic's shape is wrong.
//   - ErrFieldMissing: a column declared Required is absent from the
//     payload (raw) or the registry schema (avro projection) — a contract
//     violation, not an occasional gap.
//
// Every one of these means every record on the topic fails the same way.
// Dropping them would decode nothing while reporting success, and would
// stall the position at the last decodable record.
func decodeIsFatal(err error) bool {
	var badWire *decoder.ErrBadWireFormat
	var unknownSchema *decoder.ErrUnknownSchema
	var notJSON *decoder.ErrNotJSON
	var fieldMissing *decoder.ErrFieldMissing
	return errors.As(err, &badWire) || errors.As(err, &unknownSchema) ||
		errors.As(err, &notJSON) || errors.As(err, &fieldMissing)
}

// ErrPositionLost marks retention having passed the consumer's offset. The
// records between the stored position and the log start are gone, so
// resuming is a decision (re-read from the start and accept the gap, or
// stop) rather than something the reader can pick on its own.
type ErrPositionLost struct {
	Topic string
	Err   error
}

func (e *ErrPositionLost) Error() string {
	return fmt.Sprintf("kafka: position lost on topic %q: retention passed the stored offset: %v", e.Topic, e.Err)
}

func (e *ErrPositionLost) Unwrap() error { return e.Err }
