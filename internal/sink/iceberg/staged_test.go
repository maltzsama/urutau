package iceberg

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go"
)

// C5.1/C5.2: the descriptor framing is a magic byte, the snapshot state
// (string + pending list) and two length-prefixed file lists (deletes,
// appends). Empty payloads are the deterministic case — no DataFile encoding.
func TestEncodeStagedEmptyFraming(t *testing.T) {
	b, err := encodeStaged(stagedPayload{}, iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{stagedMagic, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(b, want) {
		t.Fatalf("encodeStaged = %v, want %v", b, want)
	}
}

// The snapshot state round-trips through the descriptor (the files need real
// DataFiles; the state does not).
func TestEncodeDecodeStagedState(t *testing.T) {
	b, err := encodeStaged(stagedPayload{snapshotState: "in_progress", snapshotPending: []uint32{3, 7}}, iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	p, err := decodeStaged(b, iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if p.snapshotState != "in_progress" {
		t.Fatalf("state = %q, want in_progress", p.snapshotState)
	}
	if len(p.snapshotPending) != 2 || p.snapshotPending[0] != 3 || p.snapshotPending[1] != 7 {
		t.Fatalf("pending = %v, want [3 7]", p.snapshotPending)
	}
	if len(p.deletes) != 0 || len(p.appends) != 0 {
		t.Fatalf("files = %d/%d, want 0/0", len(p.deletes), len(p.appends))
	}
}

func TestDecodeStagedRejectsBadMagic(t *testing.T) {
	if _, err := decodeStaged([]byte{0x00, 0, 0, 0, 0}, iceberg.PartitionSpec{}, nil, 2); err == nil {
		t.Fatal("a descriptor without the magic must be rejected")
	}
}
