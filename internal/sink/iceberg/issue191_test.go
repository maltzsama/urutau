package iceberg

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go"
)

// #191: an absent uint32 list must round-trip as nil, not an empty non-nil
// slice — otherwise the CommitStaged "last non-empty wins" aggregation and the
// empty-cycle guard are broken.
func TestReadUint32ListEmptyIsNil(t *testing.T) {
	var buf bytes.Buffer
	writeUint32(&buf, 0)
	got, err := readUint32List(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("readUint32List(count=0) = %v, want nil", got)
	}
}

func TestEncodeDecodeStagedEmptyPendingIsNil(t *testing.T) {
	b, err := encodeStaged(stagedPayload{snapshotState: "x"}, iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	p, err := decodeStaged(b, iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if p.snapshotPending != nil {
		t.Fatalf("an unset pending list decoded to %v, want nil", p.snapshotPending)
	}
}
