package iceberg

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go"
)

// #194: the descriptor schema fingerprint is a structural hash, not
// iceberg.Schema.String(), so it is stable across an iceberg-go formatting
// change and still distinguishes every schema that differs in a field ID,
// name, type or requiredness.
func TestSchemaFingerprintStructural(t *testing.T) {
	base := func() *iceberg.Schema {
		return iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String},
		)
	}

	if schemaFingerprint(nil) != "" {
		t.Fatal("a nil schema must fingerprint empty")
	}
	a, b := base(), base()
	if schemaFingerprint(a) != schemaFingerprint(b) {
		t.Fatal("equal schemas must fingerprint equally")
	}

	changedID := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String},
	)
	if schemaFingerprint(base()) == schemaFingerprint(changedID) {
		t.Fatal("a changed field ID must change the fingerprint")
	}

	changedType := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String},
	)
	if schemaFingerprint(base()) == schemaFingerprint(changedType) {
		t.Fatal("a changed field type must change the fingerprint")
	}

	changedName := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id2", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String},
	)
	if schemaFingerprint(base()) == schemaFingerprint(changedName) {
		t.Fatal("a changed field name must change the fingerprint")
	}

	// A field added inside a nested struct must change it too — the recursion
	// descends into nested types.
	nested := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "s", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 4, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
		}}},
	)
	if schemaFingerprint(base()) == schemaFingerprint(nested) {
		t.Fatal("an added nested struct must change the fingerprint")
	}
}

// A v2 (string-fingerprinted) descriptor is still accepted on decode, so an
// older worker's descriptor reaching a newer coordinator keeps committing.
func TestDecodeStagedV2StillAccepted(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(stagedMagicV2)
	writeUint32(&buf, 0)                 // spec ID
	writeString(&buf, schemaString(nil)) // v2 fingerprints the library string
	writeString(&buf, "in_progress")     // snapshot state
	writeUint32List(&buf, []uint32{5})
	if err := writeFileList(&buf, nil, iceberg.PartitionSpec{}, nil, 2); err != nil {
		t.Fatal(err)
	}
	if err := writeFileList(&buf, nil, iceberg.PartitionSpec{}, nil, 2); err != nil {
		t.Fatal(err)
	}

	p, err := decodeStaged(buf.Bytes(), iceberg.PartitionSpec{}, nil, 2)
	if err != nil {
		t.Fatalf("v2 descriptor must decode: %v", err)
	}
	if p.snapshotState != "in_progress" || len(p.snapshotPending) != 1 || p.snapshotPending[0] != 5 {
		t.Fatalf("v2 round-trip = %+v", p)
	}
}
