package postgres

import (
	"encoding/binary"
	"testing"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/source"
)

// encodeTuple renders a text-format pgoutput tuple.
func encodeTuple(vals ...string) []byte {
	var b []byte
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(vals)))
	b = append(b, n[:]...)
	for _, v := range vals {
		var l [4]byte
		b = append(b, 't')
		binary.BigEndian.PutUint32(l[:], uint32(len(v)))
		b = append(b, l[:]...)
		b = append(b, v...)
	}
	return b
}

// updatePayload renders a pgoutput Update message with a full old tuple.
func updatePayload(rel uint32, old, new []string) []byte {
	var b []byte
	var r [4]byte
	binary.BigEndian.PutUint32(r[:], rel)
	b = append(b, r[:]...)
	b = append(b, 'O')
	b = append(b, encodeTuple(old...)...)
	b = append(b, 'N')
	b = append(b, encodeTuple(new...)...)
	return b
}

func keyChangeReader(upsert bool) *Reader {
	st := orderState()
	proj, _ := newProjection(nil, nil, st)
	return &Reader{
		cfg: Config{UpsertTargets: map[string]bool{"raw.orders": upsert}},
		relByID: map[uint32]relEntry{1: {
			state: st,
			ref:   source.TableRef{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}},
			proj:  proj,
		}},
	}
}

// An upsert UPDATE that changes the primary key must delete the old key before
// writing the new one.
func TestUpdateKeyChangeDeletesOldKeyUpsert(t *testing.T) {
	r := keyChangeReader(true)
	payload := updatePayload(1, []string{"7", "old", "1.0", "t"}, []string{"8", "new", "2.0", "t"})
	if err := r.handleUpdate(payload); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}
	if len(r.txn) != 2 {
		t.Fatalf("txn = %d changes, want a delete of the old key then the update", len(r.txn))
	}
	if r.txn[0].Op != rowchange.OpDelete || r.txn[0].Key[0] != int64(7) {
		t.Fatalf("first change = %+v, want a delete of key 7", r.txn[0])
	}
	if r.txn[1].Op != rowchange.OpUpdate || r.txn[1].Key[0] != int64(8) {
		t.Fatalf("second change = %+v, want the update of key 8", r.txn[1])
	}
}

// An append target keeps the old row; the key change must not emit a delete.
func TestUpdateKeyChangeAppendKeepsOldKey(t *testing.T) {
	r := keyChangeReader(false)
	payload := updatePayload(1, []string{"7", "old", "1.0", "t"}, []string{"8", "new", "2.0", "t"})
	if err := r.handleUpdate(payload); err != nil {
		t.Fatalf("handleUpdate: %v", err)
	}
	if len(r.txn) != 1 {
		t.Fatalf("txn = %d changes, want only the update for an append target", len(r.txn))
	}
	if r.txn[0].Op != rowchange.OpUpdate || r.txn[0].Key[0] != int64(8) {
		t.Fatalf("change = %+v, want the update of key 8", r.txn[0])
	}
}
