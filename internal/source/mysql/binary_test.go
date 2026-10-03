package mysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/schema"
)

// The binlog writes a fixed BINARY(n) without its trailing 0x00 padding; the
// target is FixedSizeBinary(n). normalizeCol must repad, or the encoder
// panics on a short value (#562).
func TestNormalizeColRepadsFixedBinary(t *testing.T) {
	col := schema.TableColumn{Name: "b", Type: schema.TYPE_BINARY, FixedSize: 4}
	got, ok := normalizeCol(col, []byte{0x01, 0x02}, time.UTC).([]byte)
	if !ok || len(got) != 4 {
		t.Fatalf("fixed binary = %v (ok=%v), want 4 bytes", got, ok)
	}
	if got[0] != 0x01 || got[1] != 0x02 || got[2] != 0 || got[3] != 0 {
		t.Fatalf("fixed binary = %v, want 01 02 00 00", got)
	}
}

// A varbinary (no fixed size) keeps its byte-preserving string form, as
// before the repad (#562). Assert the exact type: the documented contract is
// a string, so a regression to []byte fails here (Sourcery nitpick).
func TestNormalizeColLeavesVarBinary(t *testing.T) {
	col := schema.TableColumn{Name: "b", Type: schema.TYPE_BINARY, FixedSize: 0}
	got, ok := normalizeCol(col, []byte{0x01}, time.UTC).(string)
	if !ok {
		t.Fatalf("normalizeCol must return the byte-preserving string for a varbinary, got %T",
			normalizeCol(col, []byte{0x01}, time.UTC))
	}
	if b := []byte(got); len(b) != 1 || b[0] != 0x01 {
		t.Fatalf("varbinary = %v, want the raw byte", b)
	}
}
