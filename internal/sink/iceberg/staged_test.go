package iceberg

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// C5.1/C5.2: the descriptor framing is a magic byte, the spec/schema
// fingerprint, the snapshot state (string + pending list) and two
// length-prefixed file lists (deletes, appends). Empty payloads are the
// deterministic case — no DataFile encoding.
func TestEncodeStagedEmptyFraming(t *testing.T) {
	b, err := encodeStaged(stagedPayload{}, iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	// magic + specID + schema len + state len + pending count + deletes count + appends count
	want := []byte{
		stagedMagic,
		0, 0, 0, 0, // spec ID
		0, 0, 0, 0, // schema string length
		0, 0, 0, 0, // snapshot state length
		0, 0, 0, 0, // pending list count
		0, 0, 0, 0, // delete file count
		0, 0, 0, 0, // append file count
	}
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
	var n [4]byte
	put := func(buf []byte, v uint32) []byte {
		binary.BigEndian.PutUint32(n[:], v)
		return append(buf, n[:]...)
	}

	// A string length that claims far more bytes than the payload holds must
	// be rejected before it drives an allocation. Layout: magic, spec ID,
	// then the oversized schema-string length.
	buf := []byte{stagedMagic}
	buf = put(buf, 0)     // spec ID matches
	buf = put(buf, 1<<30) // schema string length: 1 GiB claimed, nothing follows
	if _, err := decodeStaged(buf, iceberg.PartitionSpec{}, nil, 2); err == nil {
		t.Fatal("an oversized string length must be rejected")
	}

	// Same for a file-list count: it cannot exceed the remaining bytes / 4.
	buf = []byte{stagedMagic}
	buf = put(buf, 0)     // spec ID
	buf = put(buf, 0)     // empty schema string
	buf = put(buf, 0)     // empty snapshot state
	buf = put(buf, 0)     // empty pending list
	buf = put(buf, 1<<30) // delete-file count
	if _, err := decodeStaged(buf, iceberg.PartitionSpec{}, nil, 2); err == nil {
		t.Fatal("an oversized file count must be rejected")
	}
}

// TestDecodeStagedRejectsSpecOrSchemaDrift pins issue #124: the descriptor
// fingerprints the spec and schema the files were encoded against, so a table
// that evolved between staging and committing is rejected instead of silently
// committing mis-typed partition values.
func TestDecodeStagedRejectsSpecOrSchemaDrift(t *testing.T) {
	b, err := encodeStaged(stagedPayload{}, iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Same spec (ID 0) and schema (empty): decodes.
	if _, err := decodeStaged(b, iceberg.PartitionSpec{}, nil, 2); err != nil {
		t.Fatalf("matching fingerprint must decode: %v", err)
	}
	// The table gained a schema: the fingerprint no longer matches.
	if _, err := decodeStaged(b, iceberg.PartitionSpec{}, testSchema(t), 2); err == nil {
		t.Fatal("a schema change since staging must be rejected")
	}
}

// testSchema builds a minimal non-empty schema for the fingerprint test.
func testSchema(t *testing.T) *iceberg.Schema {
	t.Helper()
	return iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
}

// TestCycleCommittedDetectsALandedCommit pins issue #123: a retry after a lost
// catalog response must not re-add the cycle's files (appends are not
// idempotent).
func TestCycleCommittedDetectsALandedCommit(t *testing.T) {
	key := cycleKey(nil, nil, "gtid:1-9")
	if key == "" {
		t.Fatal("cycleKey must be non-empty")
	}
	if cycleKey(nil, nil, "gtid:1-9") != key {
		t.Fatal("cycleKey must be stable")
	}
	if cycleKey(nil, nil, "gtid:1-10") == key {
		t.Fatal("different positions must yield different cycle keys")
	}

	// Fast path: the table already holds the cycle's position.
	if !cycleCommitted(iceberg.Properties{"cdc.position": "gtid:1-9"}, nil, key, "gtid:1-9") {
		t.Fatal("a table already holding the cycle's position must be detected")
	}
	// The head snapshot carries the cycle key.
	head := &table.Snapshot{SnapshotID: 7, Summary: &table.Summary{
		Operation:  table.OpAppend,
		Properties: iceberg.Properties{"cdc.cycle": key},
	}}
	if !cycleCommitted(nil, head, key, "") {
		t.Fatal("a head snapshot carrying the cycle key must be detected")
	}
	// Nothing committed yet, or a different cycle.
	if cycleCommitted(nil, nil, key, "gtid:1-9") {
		t.Fatal("an uncommitted cycle must not be reported as committed")
	}
	other := &table.Snapshot{SnapshotID: 8, Summary: &table.Summary{
		Properties: iceberg.Properties{"cdc.cycle": "other"},
	}}
	if cycleCommitted(nil, other, key, "") {
		t.Fatal("a different cycle key must not match")
	}
}
