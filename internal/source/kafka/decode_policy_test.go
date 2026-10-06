package kafka

import (
	"errors"
	"testing"

	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
)

// The decode-error policy: the default fails the run on any decoder
// rejection; skip drops a single malformed record (and counts it) but still
// fails on a fundamentally-broken decoder (issue #558).
func TestNoteDecodeErrorPolicy(t *testing.T) {
	malformed := errors.New("malformed record")
	broken := &decoder.ErrBadWireFormat{}

	// Default (fail): a malformed record ends the run, and is counted.
	r := &Reader{}
	if !r.noteDecodeError(malformed) {
		t.Fatal("the default policy must fail on a decode error")
	}
	if r.DecodeErrors() != 1 {
		t.Fatalf("DecodeErrors = %d, want 1", r.DecodeErrors())
	}

	// skip: a malformed record is dropped, not fatal.
	s := &Reader{skipDecodeErrors: true}
	if s.noteDecodeError(malformed) {
		t.Fatal("under skip a malformed record must not end the run")
	}
	if s.DecodeErrors() != 1 {
		t.Fatalf("DecodeErrors = %d, want 1", s.DecodeErrors())
	}
	// A broken decoder ends the run even under skip.
	if !s.noteDecodeError(broken) {
		t.Fatal("a fundamentally-broken decoder must end the run even under skip")
	}
}
