package mysql

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/position"
)

const readerTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func ordersTable() *schema.Table {
	return &schema.Table{
		Schema: "shop",
		Name:   "orders",
		Columns: []schema.TableColumn{
			{Name: "id"},
			{Name: "v"},
			{Name: "amount"},
		},
	}
}

var ordersRef = TableRef{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}

func newTestReader(out chan<- rowchange.Change) *Reader {
	return &Reader{out: out, bySrc: map[string]TableRef{"shop.orders": ordersRef}, done: make(chan struct{})}
}

func TestDecodeInsert(t *testing.T) {
	r := newTestReader(nil)
	tbl := ordersTable()
	row := []any{int64(7), []byte("seven"), 1.5}

	c := r.decode(ordersRef, tbl, rowchange.OpInsert, row, nil, "u:1-3")

	if c.Op != rowchange.OpInsert || c.Table != "raw.orders" || c.Position != "u:1-3" {
		t.Fatalf("change = %+v", c)
	}
	if len(c.Key) != 1 || c.Key[0] != int64(7) {
		t.Fatalf("key = %v, want [7]", c.Key)
	}
	if c.After["v"] != "seven" {
		t.Fatalf("v = %v (%T), want normalized string", c.After["v"], c.After["v"])
	}
	if c.After["amount"] != 1.5 {
		t.Fatalf("amount = %v", c.After["amount"])
	}
	if c.Before != nil {
		t.Fatalf("insert must not carry Before: %+v", c.Before)
	}
}

func TestDecodeDeleteKeepsBeforeOnly(t *testing.T) {
	r := newTestReader(nil)
	tbl := ordersTable()
	row := []any{int64(7), []byte("seven"), 1.5}

	c := r.decode(ordersRef, tbl, rowchange.OpDelete, row, nil, "u:1-4")

	if c.Op != rowchange.OpDelete {
		t.Fatalf("op = %v", c.Op)
	}
	if c.After != nil {
		t.Fatalf("delete must not carry After: %+v", c.After)
	}
	if c.Key[0] != int64(7) {
		t.Fatalf("delete key must come from the old row: %v", c.Key)
	}
}

func TestDecodeUpdateCarriesBeforeAndAfter(t *testing.T) {
	r := newTestReader(nil)
	tbl := ordersTable()
	before := []any{int64(7), []byte("old"), 1.0}
	after := []any{int64(7), []byte("new"), 2.0}

	c := r.decode(ordersRef, tbl, rowchange.OpUpdate, after, before, "u:1-5")

	if c.After["v"] != "new" || c.Before["v"] != "old" {
		t.Fatalf("update before/after = %v / %v", c.Before, c.After)
	}
	if c.After["amount"] != 2.0 || c.Before["amount"] != 1.0 {
		t.Fatalf("update amounts = %v / %v", c.Before["amount"], c.After["amount"])
	}
}

func TestOnRowRoutesDecodeAndPosition(t *testing.T) {
	out := make(chan rowchange.Change, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-9"

	tbl := ordersTable()
	e := &canal.RowsEvent{
		Table:  tbl,
		Action: canal.InsertAction,
		Rows: [][]any{
			{int64(1), []byte("a"), 1.0},
			{int64(2), []byte("b"), 2.0},
		},
	}
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	for i, wantID := range []int64{1, 2} {
		c := <-out
		if c.Op != rowchange.OpInsert || c.Table != "raw.orders" {
			t.Fatalf("row %d: %+v", i, c)
		}
		if c.Key[0] != wantID || c.Position != "u:1-9" {
			t.Fatalf("row %d: key %v pos %q", i, c.Key, c.Position)
		}
	}
}

func TestOnRowUpdatePairsAndUnregisteredTable(t *testing.T) {
	out := make(chan rowchange.Change, 8)
	r := newTestReader(out)
	r.curGTID = "u:2-2"

	e := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.UpdateAction,
		Rows: [][]any{
			{int64(1), []byte("old"), 1.0},
			{int64(1), []byte("new"), 2.0},
		},
	}
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c := <-out
	if c.Op != rowchange.OpUpdate || c.Before["v"] != "old" || c.After["v"] != "new" {
		t.Fatalf("update change = %+v", c)
	}

	// A table outside the spec must be silently skipped.
	other := &canal.RowsEvent{
		Table:  &schema.Table{Schema: "other", Name: "t", Columns: []schema.TableColumn{{Name: "id"}}},
		Action: canal.InsertAction,
		Rows:   [][]any{{int64(1)}},
	}
	if err := r.OnRow(other); err != nil {
		t.Fatalf("OnRow other: %v", err)
	}
	select {
	case c := <-out:
		t.Fatalf("unregistered table leaked: %+v", c)
	default:
	}
}

func TestOnGTIDAccumulatesCumulativeSet(t *testing.T) {
	r := newTestReader(nil)
	r.mergeGTID(position.MustGTID(readerTestUUID + ":1-2"))
	for i := 3; i <= 5; i++ {
		r.mergeGTID(position.MustGTID(fmt.Sprintf("%s:%d", readerTestUUID, i)))
	}

	want := position.MustGTID(readerTestUUID + ":1-5").String()
	if r.curGTID != want {
		t.Fatalf("curGTID = %q, want cumulative %q", r.curGTID, want)
	}
}

// TestOnRowWindowTagsOnlyPastLowWatermark drives the DBLog window predicate:
// only transactions strictly past the low watermark (the master's executed
// GTID set captured at OpenWindow) are tagged InWindow. A transaction at or
// before the watermark is already reflected in the chunk SELECT and must not
// be tagged, or the snapshot row would be discarded for a stale value.
func TestOnRowWindowTagsOnlyPastLowWatermark(t *testing.T) {
	out := make(chan rowchange.Change, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-9"
	low := position.MustGTID(readerTestUUID + ":1-5")
	r.winMu.Lock()
	r.winOpen = true
	r.winChunk = 0
	r.winLow = low
	r.winMu.Unlock()

	tbl := ordersTable()
	e := &canal.RowsEvent{
		Table:  tbl,
		Action: canal.InsertAction,
		Rows:   [][]any{{int64(1), []byte("a"), 1.0}},
	}

	// A transaction PAST the low watermark is tagged InWindow.
	r.curTxn = position.MustGTID(readerTestUUID + ":1-7")
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c := <-out
	if c.Window == nil || !c.Window.InWindow || c.Window.ChunkID != 0 {
		t.Fatalf("past-low event must be InWindow: %+v", c.Window)
	}

	// A transaction AT the low watermark is NOT tagged (its effect is
	// already in the SELECT).
	r.curTxn = position.MustGTID(readerTestUUID + ":1-5")
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c = <-out
	if c.Window != nil {
		t.Fatalf("at-low event must NOT be InWindow: %+v", c.Window)
	}

	// A missing watermark (master capture failed) falls back to tagging
	// everything — over-tagging is safe.
	r.winMu.Lock()
	r.winLow = nil
	r.winMu.Unlock()
	r.curTxn = position.MustGTID(readerTestUUID + ":1-1")
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c = <-out
	if c.Window == nil || !c.Window.InWindow {
		t.Fatalf("missing watermark must fall back to tagging: %+v", c.Window)
	}

	// Window closed: nothing is tagged.
	r.ClearWindow()
	r.curTxn = position.MustGTID(readerTestUUID + ":1-8")
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c = <-out
	if c.Window != nil {
		t.Fatalf("closed window must not tag: %+v", c.Window)
	}
}

// enumSetTable mirrors a table with one ENUM and one SET column, as canal
// populates them from the table definition.
func enumSetTable() *schema.Table {
	return &schema.Table{
		Schema: "shop",
		Name:   "orders",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "status", Type: schema.TYPE_ENUM, EnumValues: []string{"new", "paid", "shipped"}},
			{Name: "tags", Type: schema.TYPE_SET, SetValues: []string{"gift", "fragile", "express"}},
		},
	}
}

// The binlog encodes ENUM as a 1-based ordinal and SET as a bitmask, while
// the backfill's SELECT returns both as text. Both feed the same target
// column, which mapColumnType maps to KindString — and the sink's
// StringifyScalar converts an int64 to its decimal digits without error. So
// an undecoded ordinal does not fail anywhere: it silently writes "2" where
// the snapshot of the same row wrote "paid".
func TestRowToMapDecodesEnumAndSet(t *testing.T) {
	tbl := enumSetTable()

	// status = 'paid' (2nd member), tags = 'gift,express' (bits 0 and 2).
	got := rowToMap(tbl, []any{int64(7), int64(2), int64(0b101)})

	if got["status"] != "paid" {
		t.Errorf("status = %#v, want \"paid\" — an ENUM ordinal must decode to its member, "+
			"or CDC writes the ordinal where the backfill writes the text", got["status"])
	}
	if got["tags"] != "gift,express" {
		t.Errorf("tags = %#v, want \"gift,express\" — a SET bitmask must decode to its "+
			"comma-joined members in declaration order", got["tags"])
	}
}

// Edge cases that must not invent a value: MySQL stores 0 for an ENUM value
// rejected on insert, and an ordinal or bit past the member list means the
// column was altered between the TableMapEvent and this decode.
func TestRowToMapEnumSetEdgeCases(t *testing.T) {
	tbl := enumSetTable()

	if got := rowToMap(tbl, []any{int64(1), int64(0), int64(0)}); got["status"] != "" {
		t.Errorf("ENUM index 0 = %#v, want \"\" (MySQL's marker for a rejected value)", got["status"])
	}
	if got := rowToMap(tbl, []any{int64(1), int64(99), int64(0)}); got["status"] != int64(99) {
		t.Errorf("out-of-range ENUM index = %#v, want the raw value kept rather than a "+
			"fabricated member", got["status"])
	}
	// Bit 3 has no member (only 3 declared); the members that do resolve stay.
	if got := rowToMap(tbl, []any{int64(1), int64(1), int64(0b1001)}); got["tags"] != "gift" {
		t.Errorf("SET with an unknown bit = %#v, want \"gift\"", got["tags"])
	}
	// An empty SET is the empty string, matching what SELECT returns.
	if got := rowToMap(tbl, []any{int64(1), int64(1), int64(0)}); got["tags"] != "" {
		t.Errorf("empty SET = %#v, want \"\"", got["tags"])
	}
}

// A column whose definition carries no members (canal could not introspect
// it) must fall through to the plain normalizer rather than dropping data.
func TestRowToMapEnumWithoutMembersFallsThrough(t *testing.T) {
	tbl := &schema.Table{
		Schema: "shop", Name: "orders",
		Columns: []schema.TableColumn{{Name: "status", Type: schema.TYPE_ENUM}},
	}
	if got := rowToMap(tbl, []any{int64(2)}); got["status"] != int64(2) {
		t.Errorf("ENUM without members = %#v, want the raw value", got["status"])
	}
}

// A text column's bytes arrive in the column's own character set, with no
// conversion — the binlog does not transcode. The backfill's SELECT does get
// transcoded (its connection is utf8mb4, so the server converts), so a
// non-UTF-8 column landed as mojibake from CDC and as correct text from the
// snapshot: the same split ENUM and SET had.
func TestRowToMapDecodesNonUTF8Charsets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		collation string
		raw       []byte
		want      string
	}{
		// MySQL's "latin1" is cp1252, not ISO-8859-1: byte 0x80 is the euro
		// sign, which ISO-8859-1 would decode as a control character.
		{"latin1 accents", "latin1_swedish_ci", []byte{0x4a, 0xe9, 0x72, 0xf4, 0x6d, 0x65}, "Jérôme"},
		{"latin1 euro", "latin1_swedish_ci", []byte{0x80}, "€"},
		{"cp1251 cyrillic", "cp1251_general_ci", []byte{0xcc, 0xee, 0xf1, 0xea, 0xe2, 0xe0}, "Москва"},
		{"utf8mb4 passthrough", "utf8mb4_general_ci", []byte("José"), "José"},
		{"ascii passthrough", "ascii_general_ci", []byte("plain"), "plain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := &schema.Table{Columns: []schema.TableColumn{
				{Name: "v", Type: schema.TYPE_STRING, Collation: tc.collation},
			}}
			got := rowToMap(tbl, []any{tc.raw})["v"]
			if got != tc.want {
				t.Errorf("v = %q, want %q — a %s column must decode to UTF-8, or CDC writes "+
					"mojibake where the backfill writes correct text", got, tc.want, tc.collation)
			}
		})
	}
}

// Binary columns are bytes, not text: reinterpreting them through a charset
// decoder would corrupt them. They must survive byte for byte.
func TestRowToMapLeavesBinaryBytesAlone(t *testing.T) {
	raw := []byte{0xff, 0xfe, 0x00, 0x80, 0x41}
	tbl := &schema.Table{Columns: []schema.TableColumn{
		{Name: "b", Type: schema.TYPE_BINARY, Collation: "binary"},
	}}
	got, ok := rowToMap(tbl, []any{raw})["b"].(string)
	if !ok || !bytes.Equal([]byte(got), raw) {
		t.Errorf("binary column = %#v, want the original bytes %v preserved", got, raw)
	}
}

// An unknown or absent collation must pass the bytes through rather than
// guess: the raw form is still recoverable, a bad guess is not.
func TestRowToMapUnknownCollationPassesThrough(t *testing.T) {
	raw := []byte{0xe9, 0x41}
	// eucjpms is multi-byte and tracked separately (see charset.go's doc
	// comment); "notacharset" stands in for anything genuinely unmodeled.
	for _, collation := range []string{"", "eucjpms_japanese_ci", "notacharset_general_ci"} {
		tbl := &schema.Table{Columns: []schema.TableColumn{
			{Name: "v", Type: schema.TYPE_STRING, Collation: collation},
		}}
		got, _ := rowToMap(tbl, []any{raw})["v"].(string)
		if !bytes.Equal([]byte(got), raw) {
			t.Errorf("collation %q: got %q, want the raw bytes untouched", collation, got)
		}
	}
}

// Multi-byte East Asian character sets. x/text's decoders follow the
// WHATWG/Unicode indexes; MySQL maintains its own tables, which agree for
// the overwhelming majority of code points. These pin the common cases.
func TestRowToMapDecodesMultibyteCharsets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		collation string
		raw       []byte
		want      string
	}{
		{"sjis", "sjis_japanese_ci", []byte{0x93, 0xfa, 0x96, 0x7b}, "日本"},
		// cp932 is Microsoft's Shift-JIS superset; katakana encodes identically.
		{"cp932", "cp932_japanese_ci", []byte{0x83, 0x65, 0x83, 0x58, 0x83, 0x67}, "テスト"},
		// ujis is MySQL's name for EUC-JP.
		{"ujis", "ujis_japanese_ci", []byte{0xc6, 0xfc, 0xcb, 0xdc}, "日本"},
		{"gbk", "gbk_chinese_ci", []byte{0xd6, 0xd0, 0xb9, 0xfa}, "中国"},
		{"gb18030", "gb18030_chinese_ci", []byte{0xd6, 0xd0, 0xb9, 0xfa}, "中国"},
		{"big5", "big5_chinese_ci", []byte{0xa4, 0xa4, 0xa4, 0xe5}, "中文"},
		{"euckr", "euckr_korean_ci", []byte{0xc7, 0xd1, 0xb1, 0xb9}, "한국"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := &schema.Table{Columns: []schema.TableColumn{
				{Name: "v", Type: schema.TYPE_STRING, Collation: tc.collation},
			}}
			if got := rowToMap(tbl, []any{tc.raw})["v"]; got != tc.want {
				t.Errorf("v = %q, want %q", got, tc.want)
			}
		})
	}
}

// eucjpms is MySQL's Microsoft-flavored EUC-JP: it differs from plain EUC-JP
// in exactly the vendor rows x/text does not model, so it must pass through
// rather than decode a handful of characters wrongly.
func TestRowToMapEucjpmsStillPassesThrough(t *testing.T) {
	raw := []byte{0xc6, 0xfc, 0xcb, 0xdc}
	tbl := &schema.Table{Columns: []schema.TableColumn{
		{Name: "v", Type: schema.TYPE_STRING, Collation: "eucjpms_japanese_ci"},
	}}
	got, _ := rowToMap(tbl, []any{raw})["v"].(string)
	if !bytes.Equal([]byte(got), raw) {
		t.Errorf("eucjpms = %q, want the raw bytes passed through", got)
	}
}

// gb2312 is the subset GBK extends, and MySQL's tis620 is the Thai code
// page Windows-874 encodes; both reuse a decoder already in the table.
func TestRowToMapDecodesSubsetCharsets(t *testing.T) {
	for _, tc := range []struct {
		name, collation string
		raw             []byte
		want            string
	}{
		{"gb2312", "gb2312_chinese_ci", []byte{0xd6, 0xd0, 0xb9, 0xfa}, "中国"},
		{"tis620", "tis620_thai_ci", []byte{0xa1, 0xa2, 0xa3}, "กขฃ"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := &schema.Table{Columns: []schema.TableColumn{
				{Name: "v", Type: schema.TYPE_STRING, Collation: tc.collation},
			}}
			if got := rowToMap(tbl, []any{tc.raw})["v"]; got != tc.want {
				t.Errorf("v = %q, want %q", got, tc.want)
			}
		})
	}
}

// macce (Mac Central Europe) decodes through its own generated table
// rather than charmap.Macintosh (Mac Roman): they differ. Byte 0x80 is "Ä"
// in Mac Central Europe and a different character in Mac Roman — decoding
// through the wrong table would have corrupted exactly the accented
// characters the charset exists for. Value verified against a real MySQL
// 8.4 server (see internal/source/mysql/charsetgen).
func TestRowToMapDecodesMacce(t *testing.T) {
	tbl := &schema.Table{Columns: []schema.TableColumn{
		{Name: "v", Type: schema.TYPE_STRING, Collation: "macce_general_ci"},
	}}
	if got := rowToMap(tbl, []any{[]byte{0x80}})["v"]; got != "Ä" {
		t.Errorf("macce 0x80 = %q, want %q", got, "Ä")
	}
}

// TestRowToMapDecodesGeneratedCharsets covers the 7 single-byte character
// sets with no golang.org/x/text decoder (armscii8, dec8, geostd8, hp8,
// keybcs2, swe7, macce). Each is generated from a real MySQL 8.4 server
// (internal/source/mysql/charsetgen), not hand-transcribed or borrowed from
// a lookalike standard — the risk that made macce and eucjpms unsafe to
// treat casually. Values below were independently verified against the
// server, not just copied from the generated file.
func TestRowToMapDecodesGeneratedCharsets(t *testing.T) {
	for _, tc := range []struct {
		name, collation string
		raw             byte
		want            string
	}{
		{"armscii8 Armenian", "armscii8_general_ci", 0xC0, "Ը"},
		{"dec8 accented", "dec8_swedish_ci", 0xC0, "À"},
		{"geostd8 euro", "geostd8_general_ci", 0x80, "€"},
		{"hp8 accented", "hp8_english_ci", 0xC0, "â"},
		{"keybcs2 Czech", "keybcs2_general_ci", 0x80, "Č"},
		{"swe7 accented", "swe7_swedish_ci", 0x40, "É"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := &schema.Table{Columns: []schema.TableColumn{
				{Name: "v", Type: schema.TYPE_STRING, Collation: tc.collation},
			}}
			if got := rowToMap(tbl, []any{[]byte{tc.raw}})["v"]; got != tc.want {
				t.Errorf("%s byte 0x%02X = %q, want %q", tc.name, tc.raw, got, tc.want)
			}
		})
	}
}

// swe7 is a strict 7-bit set: every byte 0x80-0xFF has no assignment.
// MySQL itself converts such a byte to "?" without error, but this
// package's decodeString policy is to keep unmappable bytes raw (more
// recoverable than a lossy substitute), so it must fall through untouched
// rather than becoming "?".
func TestRowToMapGeneratedCharsetUnassignedByteFallsThrough(t *testing.T) {
	tbl := &schema.Table{Columns: []schema.TableColumn{
		{Name: "v", Type: schema.TYPE_STRING, Collation: "swe7_swedish_ci"},
	}}
	raw := []byte{0x80}
	got, _ := rowToMap(tbl, []any{raw})["v"].(string)
	if !bytes.Equal([]byte(got), raw) {
		t.Errorf("swe7 unassigned byte = %q, want the raw byte kept (not \"?\")", got)
	}
}

// MySQL's utf32 is fixed-width big-endian UTF-32.
func TestRowToMapDecodesUTF32(t *testing.T) {
	tbl := &schema.Table{Columns: []schema.TableColumn{
		{Name: "v", Type: schema.TYPE_STRING, Collation: "utf32_general_ci"},
	}}
	if got := rowToMap(tbl, []any{[]byte{0x00, 0x00, 0x4e, 0x2d}})["v"]; got != "中" {
		t.Errorf("utf32 = %q, want %q", got, "中")
	}
}

// A stalled consumer (channel full, nobody reading) must not prevent the
// reader from shutting down. emit is OnRow's send path, and OnRow runs on
// canal's own event-loop goroutine — a bare channel send there would block
// that goroutine indefinitely, which stops canal reading further binlog
// events and makes Close()/context cancellation unable to break out (see
// issue #114). This test itself hangs against the old bare `r.out <- c`,
// so its own timeout guard is what turns that into a reported failure
// instead of a wedged test run.
func TestEmitUnblocksOnStop(t *testing.T) {
	out := make(chan rowchange.Change) // unbuffered: any send blocks until read
	r := newTestReader(out)

	errCh := make(chan error, 1)
	go func() { errCh <- r.emit(rowchange.Change{}) }()

	// Give emit a moment to actually reach the select and park on the send;
	// this is a best-effort scheduling nudge, not a correctness dependency
	// (stop() below is safe to call regardless of whether emit reached its
	// select yet, since done is closed once for the whole reader).
	r.stop()

	select {
	case err := <-errCh:
		if !errors.Is(err, errReaderStopped) {
			t.Fatalf("emit() = %v, want errReaderStopped", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("emit() did not return after stop() — a stalled consumer would hang OnRow, " +
			"and with it canal's event loop, forever")
	}
}

// A send that can complete must still complete normally — stop() being
// available must not turn every emit into a stop, and calling stop() must
// not race a concurrent successful send.
func TestEmitSucceedsWithoutStop(t *testing.T) {
	out := make(chan rowchange.Change, 1)
	r := newTestReader(out)

	if err := r.emit(rowchange.Change{Op: rowchange.OpInsert}); err != nil {
		t.Fatalf("emit() = %v, want nil", err)
	}
	select {
	case c := <-out:
		if c.Op != rowchange.OpInsert {
			t.Fatalf("got %+v", c)
		}
	default:
		t.Fatal("emit() returned nil but nothing was sent")
	}
}

// stop must be safe to call more than once — Close() and StartFromGTID's
// ctx.Done()/done-channel paths can both reach it during an ordinary
// shutdown race.
func TestStopIsIdempotent(t *testing.T) {
	r := newTestReader(nil)
	r.stop()
	r.stop() // must not panic (close of closed channel)
}
