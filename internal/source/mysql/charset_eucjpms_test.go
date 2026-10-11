package mysql

import (
	"bufio"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"golang.org/x/text/encoding/japanese"
)

// eucjpmsServerTable reads charsetgen/eucjpms.tsv: every multi-byte sequence
// a real MySQL 8.4 decodes, and the UTF-8 it decodes to.
func eucjpmsServerTable(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("charsetgen/eucjpms.tsv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()
	table := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(strings.TrimSpace(sc.Text()), "\t")
		if len(parts) != 2 {
			t.Fatalf("malformed line %q", sc.Text())
		}
		seq, err1 := hex.DecodeString(parts[0])
		want, err2 := hex.DecodeString(parts[1])
		if err1 != nil || err2 != nil {
			t.Fatalf("bad hex in %q", sc.Text())
		}
		table[string(seq)] = string(want)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return table
}

// CDC must decode eucjpms exactly as the server does for the backfill: every
// sequence the server decodes gives the same text here.
func TestEucjpmsDecodesEverySequenceAsTheServerDoes(t *testing.T) {
	table := eucjpmsServerTable(t)
	if len(table) != 15078 {
		t.Fatalf("server table has %d sequences, want 15078", len(table))
	}
	for seq, want := range table {
		if got := decodeString([]byte(seq), "eucjpms_japanese_ci"); got != want {
			t.Fatalf("sequence %X decodes to %q, want %q", seq, got, want)
		}
	}
}

// The sequences where eucjpms and plain EUC-JP disagree are the reason the
// charset has its own decoder. Each of them must follow the server, not
// x/text's EUC-JP.
func TestEucjpmsFollowsTheServerWhereEUCJPDiffers(t *testing.T) {
	table := eucjpmsServerTable(t)
	plain := japanese.EUCJP.NewDecoder()
	differ := 0
	for seq, want := range table {
		other, err := plain.Bytes([]byte(seq))
		if err == nil && string(other) == want {
			continue
		}
		differ++
		if got := decodeString([]byte(seq), "eucjpms_japanese_ci"); got != want {
			t.Errorf("sequence %X decodes to %q, want the server's %q (plain EUC-JP gives %q)", seq, got, want, other)
		}
	}
	if differ == 0 {
		t.Fatal("no sequence differs from plain EUC-JP; the comparison is not exercising the vendor rows")
	}
	t.Logf("%d of %d sequences differ from plain EUC-JP", differ, len(table))
}

func TestEucjpmsDecodesMixedText(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		want string
	}{
		{"kanji", []byte{0xc6, 0xfc, 0xcb, 0xdc}, "日本"},
		{"ascii and kanji", []byte{'a', 0xc6, 0xfc, '1'}, "a日1"},
		{"NEC circled digit", []byte{0xad, 0xa1}, "①"},
		{"half-width kana", []byte{0x8e, 0xb1}, "ｱ"},
		{"supplementary plane", []byte{0x8f, 0xb0, 0xa1}, "丂"},
		{"duplicate encoding of ≒", []byte{0xad, 0xf0}, "≒"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeString(tc.raw, "eucjpms_japanese_ci"); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A malformed or unassigned sequence keeps the raw bytes, like every other
// charset here: the raw form is recoverable, a substitute is not.
func TestEucjpmsKeepsRawBytesForInvalidSequences(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"truncated two-byte", []byte{0xa1}},
		{"bad trail byte", []byte{0xa1, 0x20}},
		{"truncated supplementary", []byte{0x8f, 0xa1}},
		{"unassigned main-plane position", []byte{0xa9, 0xa1}},
		{"unassigned supplementary position", []byte{0x8f, 0xa1, 0xa1}},
		{"kana trail out of range", []byte{0x8e, 0xa0}},
		{"lone high byte", []byte{0x80}},
		{"valid then invalid", []byte{0xc6, 0xfc, 0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeString(tc.raw, "eucjpms_japanese_ci"); got != string(tc.raw) {
				t.Errorf("got %q, want the raw bytes %q", got, tc.raw)
			}
		})
	}
}
