package iceberg

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go"
)

// C5.1: the descriptor framing is a magic byte followed by two
// length-prefixed file lists (deletes, appends). Empty payloads are the
// deterministic case — no DataFile encoding involved.
func TestEncodeStagedEmptyFraming(t *testing.T) {
	b, err := encodeStaged(stagedPayload{}, iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{stagedMagic, 0, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(b, want) {
		t.Fatalf("encodeStaged = %v, want %v", b, want)
	}
}
