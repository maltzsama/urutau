package iceberg

import (
	"bytes"
	"encoding/binary"
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

func TestDecodeStagedRejectsOversizedLengths(t *testing.T) {
	// A length field that claims far more bytes than the payload holds must
	// be rejected before it drives an allocation.
	buf := []byte{stagedMagic}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], 1<<30) // 1 GiB claimed, nothing follows
	buf = append(buf, n[:]...)
	if _, err := decodeStaged(buf, iceberg.PartitionSpec{}, nil, 2); err == nil {
		t.Fatal("an oversized string length must be rejected")
	}

	// Same for a file-list count: it cannot exceed the remaining bytes / 4.
	buf = []byte{stagedMagic}
	binary.BigEndian.PutUint32(n[:], 0) // empty snapshot state
	buf = append(buf, n[:]...)
	binary.BigEndian.PutUint32(n[:], 0) // empty pending list
	buf = append(buf, n[:]...)
	binary.BigEndian.PutUint32(n[:], 1<<30) // delete-file count
	buf = append(buf, n[:]...)
	if _, err := decodeStaged(buf, iceberg.PartitionSpec{}, nil, 2); err == nil {
		t.Fatal("an oversized file count must be rejected")
	}
}
