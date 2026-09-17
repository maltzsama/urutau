package postgres

// Coverage for the Postgres source's pure helpers and the reader's
// confirmed-LSN/synced-floor bookkeeping.

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

func TestCapabilities(t *testing.T) {
	c := capabilities()
	if !c.Snapshot || !c.ChunkQuery || !c.Stream || !c.BeforeImage {
		t.Fatalf("capabilities = %+v", c)
	}
	if len(c.Modes) != 1 || c.Modes[0] != source.ModeCDC {
		t.Fatalf("modes = %v", c.Modes)
	}
}

func TestParsePosition(t *testing.T) {
	a := Source{}
	pos, err := a.ParsePosition("0/1A2B")
	if err != nil {
		t.Fatalf("ParsePosition: %v", err)
	}
	if pos == nil {
		t.Fatal("nil position")
	}
	if _, err := a.ParsePosition("not-an-lsn"); err == nil {
		t.Fatal("a malformed LSN must be rejected")
	}
}

func TestPublicationFor(t *testing.T) {
	if got := publicationFor("urutau_slot"); got != "urutau_slot_pub" {
		t.Fatalf("publicationFor = %q", got)
	}
}

func TestSplitSource(t *testing.T) {
	s, tb, ok := splitSource("public.orders")
	if !ok || s != "public" || tb != "orders" {
		t.Fatalf("splitSource = %q, %q, %v", s, tb, ok)
	}
	if _, _, ok := splitSource("orders"); ok {
		t.Fatal("a name without a schema separator must not split")
	}
}

func TestDecodeStringToBytes(t *testing.T) {
	b, err := decodeStringToBytes("deadbeef")
	if err != nil {
		t.Fatalf("decodeStringToBytes: %v", err)
	}
	if hex.EncodeToString(b) != "deadbeef" {
		t.Fatalf("decoded = %x", b)
	}
	if _, err := decodeStringToBytes("zz"); err == nil {
		t.Fatal("a non-hex string must be rejected")
	}
}

func TestCleanNumeric(t *testing.T) {
	if got := cleanNumeric("$1,234.56"); got != "1234.56" {
		t.Fatalf("cleanNumeric = %q", got)
	}
	if got := cleanNumeric(" 42 "); got != "42" {
		t.Fatalf("cleanNumeric(spaces) = %q", got)
	}
	if got := cleanNumeric("(5)"); !strings.HasPrefix(got, "-") {
		t.Fatalf("a parenthesized numeric must become negative, got %q", got)
	}
}

func TestConfirmedLSN(t *testing.T) {
	r := &Reader{}
	if got := r.confirmedLSN(); got != 0 {
		t.Fatalf("confirmedLSN(nil callback) = %v, want 0", got)
	}

	r.SetConfirmed(func() position.Position { return nil })
	if got := r.confirmedLSN(); got != 0 {
		t.Fatalf("confirmedLSN(nil position) = %v, want 0", got)
	}

	r.SetConfirmed(func() position.Position { return position.MustLSN("0/10") })
	if got := r.confirmedLSN(); got != position.LSN(16) {
		t.Fatalf("confirmedLSN = %v, want 16", got)
	}

	r.SetConfirmed(func() position.Position { return &position.Offsets{} })
	if got := r.confirmedLSN(); got != 0 {
		t.Fatalf("confirmedLSN(non-LSN) = %v, want 0", got)
	}
}

func TestAdvanceSyncedFloor(t *testing.T) {
	synced := position.LSN(10)
	r := &Reader{synced: &synced}

	r.advanceSyncedFloor(pglogrepl.LSN(20))
	if *r.synced != 20 {
		t.Fatalf("synced = %v, want 20", *r.synced)
	}

	// A lower LSN never moves the floor backwards.
	r.advanceSyncedFloor(pglogrepl.LSN(5))
	if *r.synced != 20 {
		t.Fatalf("synced = %v, want 20 after a retrograde LSN", *r.synced)
	}

	// Mid-transaction the floor must hold.
	r.txn = []*rowchange.Change{{}}
	r.advanceSyncedFloor(pglogrepl.LSN(30))
	if *r.synced != 20 {
		t.Fatalf("synced = %v, want 20 while a transaction is buffering", *r.synced)
	}
}
