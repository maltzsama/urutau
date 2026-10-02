package couchbase

import (
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
)

// A Date/Time/UUID arrives from the wire in its native form, and a bare JSON
// number/base64 would corrupt the document; jsonValueKind renders the
// canonical text instead (issue #484). A plain Int32 stays a number.
func TestJSONValueKindWireForms(t *testing.T) {
	if got, err := jsonValueKind(core.KindDate, int32(20630)); err != nil || got != time.Unix(20630*86400, 0).UTC().Format("2006-01-02") {
		t.Fatalf("date = %v, %v; want %s", got, err, time.Unix(20630*86400, 0).UTC().Format("2006-01-02"))
	}

	if got, err := jsonValueKind(core.KindTime, int64(3661000000)); err != nil || got != "01:01:01" {
		t.Fatalf("time = %v, %v; want 01:01:01", got, err)
	}

	uuid := []byte{0x0b, 0x1c, 0x2d, 0x3e, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x66, 0x66}
	if got, err := jsonValueKind(core.KindUUID, uuid); err != nil || got != "0b1c2d3e-1111-2222-3333-444455556666" {
		t.Fatalf("uuid = %v, %v", got, err)
	}

	if got, err := jsonValueKind(core.KindInt32, int32(20630)); err != nil || got != int32(20630) {
		t.Fatalf("plain int32 = %v, %v; a non-date int32 must stay a number", got, err)
	}

	// uint64 is no longer rejected.
	if got, err := jsonValue(uint64(18446744073709551615)); err != nil || got != uint64(18446744073709551615) {
		t.Fatalf("uint64 = %v, %v", got, err)
	}
}
