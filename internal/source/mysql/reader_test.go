package mysql

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
)

const readerTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

var testCommitTS = time.Unix(1700000000, 0).UTC()

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

func ordersSchema() core.Schema {
	return core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "amount", Type: core.ColumnType{Kind: core.KindFloat64}},
		},
		PrimaryKey: []string{"id"},
	}
}

func newTestReader(out chan<- *dataplane.Batch) *Reader {
	return &Reader{
		batchOut:    out,
		bySrc:       map[string]TableRef{"shop.orders": ordersRef},
		done:        make(chan struct{}),
		encoders:    map[string]*tableEncoder{},
		projections: map[string]projection{"shop.orders": {}},
		cfg: Config{
			Schemas:       map[string]core.Schema{"raw.orders": ordersSchema()},
			Projections:   map[string]projection{"shop.orders": {}},
			UpsertTargets: map[string]bool{},
		},
	}
}

// rowToMap is a test shim: the live path no longer materializes a map, but the
// per-cell normalization the charset/ENUM tests pin is unchanged.
func rowToMap(tbl *schema.Table, row []any, loc *time.Location) map[string]any {
	out := make(map[string]any, len(tbl.Columns))
	for i, col := range tbl.Columns {
		if i >= len(row) {
			continue
		}
		out[col.Name] = normalizeCol(col, row[i], loc)
	}
	return out
}

// flushTxn closes the transaction being decoded, emitting its batches.
func flushTxn(t *testing.T, r *Reader) {
	t.Helper()
	if err := r.OnPosSynced(&replication.EventHeader{}, gomysql.Position{}, nil, false); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}
}

// drainChanges decodes and releases every pending batch into row changes.
func drainChanges(t *testing.T, out chan *dataplane.Batch) []rowchange.Change {
	t.Helper()
	var all []rowchange.Change
	for len(out) > 0 {
		b := <-out
		ch, err := transport.DecodeBatch(b.Record, b.Table, []string{"id"})
		b.Record.Release()
		if err != nil {
			t.Fatalf("decode batch: %v", err)
		}
		all = append(all, ch...)
	}
	return all
}

func TestDecodeInsert(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-3"
	r.curCommitTS = testCommitTS
	tbl := ordersTable()
	row := []any{int64(7), []byte("seven"), 1.5}

	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.InsertAction, Rows: [][]any{row}}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 1 {
		t.Fatalf("got %d changes, want 1", len(chs))
	}
	c := chs[0]
	if c.Op != rowchange.OpInsert || c.Position != "u:1-3" {
		t.Fatalf("change = %+v", c)
	}
	if c.After["v"] != "seven" {
		t.Fatalf("v = %v (%T), want normalized string", c.After["v"], c.After["v"])
	}
	if c.After["amount"] != 1.5 {
		t.Fatalf("amount = %v", c.After["amount"])
	}
}

func TestDecodeDeleteKeepsBeforeOnly(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-4"
	tbl := ordersTable()
	row := []any{int64(7), []byte("seven"), 1.5}

	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.DeleteAction, Rows: [][]any{row}}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 1 || chs[0].Op != rowchange.OpDelete {
		t.Fatalf("delete changes = %+v", chs)
	}
	if chs[0].After["v"] != "seven" {
		t.Fatalf("delete image v = %v", chs[0].After["v"])
	}
}

func TestDecodeUpdateCarriesBeforeAndAfter(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-5"
	tbl := ordersTable()
	before := []any{int64(7), []byte("old"), 1.0}
	after := []any{int64(7), []byte("new"), 2.0}

	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.UpdateAction, Rows: [][]any{before, after}}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 1 || chs[0].Op != rowchange.OpUpdate {
		t.Fatalf("update changes = %+v", chs)
	}
	if chs[0].After["v"] != "new" {
		t.Fatalf("update image v = %v", chs[0].After["v"])
	}
}

func TestOnRowRoutesAndPosition(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
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
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 2 {
		t.Fatalf("got %d changes, want 2", len(chs))
	}
	for i, wantID := range []int64{1, 2} {
		if chs[i].Op != rowchange.OpInsert || chs[i].Key[0] != wantID || chs[i].Position != "u:1-9" {
			t.Fatalf("row %d: %+v", i, chs[i])
		}
	}
}

func TestOnRowUpdatePairsAndUnregisteredTable(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
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
	flushTxn(t, r)
	chs := drainChanges(t, out)
	if len(chs) != 1 || chs[0].Op != rowchange.OpUpdate || chs[0].After["v"] != "new" {
		t.Fatalf("update change = %+v", chs)
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
	flushTxn(t, r)
	select {
	case b := <-out:
		b.Record.Release()
		t.Fatal("unregistered table leaked a batch")
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
	got := rowToMap(tbl, []any{int64(7), int64(2), int64(0b101)}, time.UTC)

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

	if got := rowToMap(tbl, []any{int64(1), int64(0), int64(0)}, time.UTC); got["status"] != "" {
		t.Errorf("ENUM index 0 = %#v, want \"\" (MySQL's marker for a rejected value)", got["status"])
	}
	if got := rowToMap(tbl, []any{int64(1), int64(99), int64(0)}, time.UTC); got["status"] != int64(99) {
		t.Errorf("out-of-range ENUM index = %#v, want the raw value kept rather than a "+
			"fabricated member", got["status"])
	}
	// Bit 3 has no member (only 3 declared); the members that do resolve stay.
	if got := rowToMap(tbl, []any{int64(1), int64(1), int64(0b1001)}, time.UTC); got["tags"] != "gift" {
		t.Errorf("SET with an unknown bit = %#v, want \"gift\"", got["tags"])
	}
	// An empty SET is the empty string, matching what SELECT returns.
	if got := rowToMap(tbl, []any{int64(1), int64(1), int64(0)}, time.UTC); got["tags"] != "" {
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
	if got := rowToMap(tbl, []any{int64(2)}, time.UTC); got["status"] != int64(2) {
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
			got := rowToMap(tbl, []any{tc.raw}, time.UTC)["v"]
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
	got, ok := rowToMap(tbl, []any{raw}, time.UTC)["b"].(string)
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
		got, _ := rowToMap(tbl, []any{raw}, time.UTC)["v"].(string)
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
			if got := rowToMap(tbl, []any{tc.raw}, time.UTC)["v"]; got != tc.want {
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
	got, _ := rowToMap(tbl, []any{raw}, time.UTC)["v"].(string)
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
			if got := rowToMap(tbl, []any{tc.raw}, time.UTC)["v"]; got != tc.want {
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
	if got := rowToMap(tbl, []any{[]byte{0x80}}, time.UTC)["v"]; got != "Ä" {
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
			if got := rowToMap(tbl, []any{[]byte{tc.raw}}, time.UTC)["v"]; got != tc.want {
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
	got, _ := rowToMap(tbl, []any{raw}, time.UTC)["v"].(string)
	if !bytes.Equal([]byte(got), raw) {
		t.Errorf("swe7 unassigned byte = %q, want the raw byte kept (not \"?\")", got)
	}
}

// MySQL's utf32 is fixed-width big-endian UTF-32.
func TestRowToMapDecodesUTF32(t *testing.T) {
	tbl := &schema.Table{Columns: []schema.TableColumn{
		{Name: "v", Type: schema.TYPE_STRING, Collation: "utf32_general_ci"},
	}}
	if got := rowToMap(tbl, []any{[]byte{0x00, 0x00, 0x4e, 0x2d}}, time.UTC)["v"]; got != "中" {
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
func TestSendBatchUnblocksOnStop(t *testing.T) {
	out := make(chan *dataplane.Batch) // unbuffered: any send blocks until read
	r := newTestReader(out)

	errCh := make(chan error, 1)
	go func() { errCh <- r.sendBatch(&dataplane.Batch{Table: "raw.orders"}) }()

	// Give sendBatch a moment to reach the select and park on the send; this
	// is a best-effort scheduling nudge, not a correctness dependency.
	r.stop()

	select {
	case err := <-errCh:
		if !errors.Is(err, errReaderStopped) {
			t.Fatalf("sendBatch() = %v, want errReaderStopped", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sendBatch() did not return after stop() — a stalled consumer would hang OnRow, " +
			"and with it canal's event loop, forever")
	}
}

// A send that can complete must still complete normally — stop() being
// available must not turn every send into a stop.
func TestSendBatchSucceedsWithoutStop(t *testing.T) {
	out := make(chan *dataplane.Batch, 1)
	r := newTestReader(out)
	b := &dataplane.Batch{Table: "raw.orders"}

	if err := r.sendBatch(b); err != nil {
		t.Fatalf("sendBatch() = %v, want nil", err)
	}
	select {
	case got := <-out:
		if got != b {
			t.Fatalf("got %+v, want the sent batch", got)
		}
	default:
		t.Fatal("sendBatch() returned nil but nothing was sent")
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

// OnGTID stamps the transaction's commit time from the GTID event (microsecond
// precision, MySQL 8.0.1+) with a fallback to the header's second-precision
// timestamp. Issue #137.
func TestOnGTIDCapturesCommitTime(t *testing.T) {
	sid := []byte{0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62}
	r := newTestReader(nil)

	micros := uint64(1700000000)*1_000_000 + 123456
	e := &replication.GTIDEvent{SID: sid, GNO: 3, OriginalCommitTimestamp: micros}
	if err := r.OnGTID(&replication.EventHeader{Timestamp: 1699999999}, e); err != nil {
		t.Fatalf("OnGTID: %v", err)
	}
	want := time.Unix(int64(micros/1_000_000), int64(micros%1_000_000)*1000).UTC()
	if !r.curCommitTS.Equal(want) {
		t.Fatalf("commit ts = %v, want %v", r.curCommitTS, want)
	}

	// No original commit timestamp (pre-8.0.1): fall back to the header.
	e2 := &replication.GTIDEvent{SID: sid, GNO: 4}
	if err := r.OnGTID(&replication.EventHeader{Timestamp: 1699999999}, e2); err != nil {
		t.Fatalf("OnGTID: %v", err)
	}
	if want := time.Unix(1699999999, 0).UTC(); !r.curCommitTS.Equal(want) {
		t.Fatalf("fallback commit ts = %v, want %v", r.curCommitTS, want)
	}
}

// canalConfig threads the TLS config into the replication connection. Issue
// #138.
func TestCanalConfigCarriesTLS(t *testing.T) {
	tc := &tls.Config{ServerName: "db.example.com"}
	cc := canalConfig(Config{Addr: "h:3306", TLSConfig: tc}, []string{`^db\.t$`})
	if cc.TLSConfig != tc {
		t.Fatal("canal config did not carry the TLS config")
	}
	if cc.Flavor != "mysql" || !cc.ParseTime {
		t.Fatalf("canal config = %+v", cc)
	}
}

// A CDC temporal value and the snapshot value for the same cell must normalize
// to the same time.Time in the operator's location (issue #139).
func TestNormalizeColTemporalParity(t *testing.T) {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}

	// DATETIME: same naive wall clock. Snapshot (go-sql-driver, loc=loc) is
	// time.Date(..., loc); CDC (go-mysql, ParseTime) tags the wall clock UTC.
	snapDT := time.Date(2023, 1, 8, 12, 30, 45, 0, loc)
	cdcDT := time.Date(2023, 1, 8, 12, 30, 45, 0, time.UTC)
	if got := normalizeCol(schema.TableColumn{Name: "d", Type: schema.TYPE_DATETIME}, cdcDT, loc).(time.Time); !got.Equal(snapDT) {
		t.Fatalf("datetime parity: got %v, want %v", got, snapDT)
	}

	// TIMESTAMP: same instant. go-mysql returns the instant (Local); the
	// snapshot returns it in loc.
	instant := time.Date(2023, 1, 8, 12, 30, 45, 0, time.UTC)
	snapTS := instant.In(loc)
	if got := normalizeCol(schema.TableColumn{Name: "t", Type: schema.TYPE_TIMESTAMP}, instant, loc).(time.Time); !got.Equal(snapTS) {
		t.Fatalf("timestamp parity: got %v, want %v", got, snapTS)
	}

	// DATE: go-mysql returns a string; the snapshot a midnight time.Time.
	wantDate := time.Date(2023, 1, 8, 0, 0, 0, 0, loc)
	if got := normalizeCol(schema.TableColumn{Name: "dt", Type: schema.TYPE_DATE}, "2023-01-08", loc).(time.Time); !got.Equal(wantDate) {
		t.Fatalf("date parity: got %v, want %v", got, wantDate)
	}
}

// FLOAT already agrees across paths and must keep agreeing (#181).
//
// Both readers deliver a MySQL FLOAT as float32 — go-sql-driver returns
// float32 for a 4-byte FLOAT (measured against MySQL 8.4, not inferred), and
// go-mysql's binlog decoder does the same. The codec widens float32 to
// float64 once, identically, for both. So 0.1 lands as 0.10000000149011612
// whichever path read it, which is the value MySQL actually stores.
//
// This test exists because the obvious "fix" — converting float32 to the
// shortest float64 that round-trips, so the column reads back 0.1 — would
// CREATE the divergence it appears to remove: the snapshot would then
// disagree with the CDC path, and both with the stored bits. Do not add a
// TYPE_FLOAT branch to normalizeCol without re-measuring both drivers.
func TestNormalizeColFloatParity(t *testing.T) {
	col := schema.TableColumn{Name: "f", Type: schema.TYPE_FLOAT, RawType: "float"}
	for _, v := range []float32{0.1, 1.1, 3.14159, 2.675, 0.7} {
		// Snapshot hands the driver's float32 to normalize; CDC hands the
		// binlog's float32 to normalizeCol. Same input, so the assertion is
		// that neither path rewrites it.
		snap := normalize(v)
		cdc := normalizeCol(col, v, time.UTC)
		if snap != cdc {
			t.Errorf("float %v: snapshot=%v (%T) cdc=%v (%T)", v, snap, snap, cdc, cdc)
		}
		if _, ok := cdc.(float32); !ok {
			t.Errorf("float %v: cdc widened to %T — the codec owns widening, not the source", v, cdc)
		}
	}

	// DOUBLE is float64 on both paths and must stay untouched too.
	d := schema.TableColumn{Name: "d", Type: schema.TYPE_FLOAT, RawType: "double"}
	if got := normalizeCol(d, float64(0.1), time.UTC); got != float64(0.1) {
		t.Errorf("double 0.1 = %v (%T), want 0.1 (float64)", got, got)
	}
}

// go-mysql returns MySQL's zero temporal as a string while the snapshot driver
// (parseTime) returns time.Time{}; normalizeCol must make them agree, or the
// same column has a different Go type by path.
func TestNormalizeColZeroTemporal(t *testing.T) {
	cols := []schema.TableColumn{
		{Name: "d", Type: schema.TYPE_DATETIME},
		{Name: "t", Type: schema.TYPE_TIMESTAMP},
		{Name: "dt", Type: schema.TYPE_DATE},
	}
	for _, col := range cols {
		for _, zero := range []string{"0000-00-00", "0000-00-00 00:00:00", "0000-00-00 00:00:00.000000"} {
			got := normalizeCol(col, zero, time.UTC)
			ts, ok := got.(time.Time)
			if !ok || !ts.IsZero() {
				t.Fatalf("%s zero %q = %v (%T), want time.Time{}", col.Name, zero, got, got)
			}
		}
	}
}

// Snapshot and CDC must agree even for a TIMESTAMP whose instant falls in a
// different DST period than "now": the snapshot does not freeze an offset (it
// re-tags/keeps the instant in the IANA location), so both paths preserve the
// instant. A fixed session offset (time.Now()) would shift it by the DST delta.
func TestTemporalParityAcrossDST(t *testing.T) {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	// 2018-12-15: São Paulo observed DST (-02:00).
	instant := time.Date(2018, 12, 15, 15, 0, 0, 0, time.UTC)

	snapTS := normalizeSnapshot(instant, "TIMESTAMP", loc).(time.Time)
	cdcTS := normalizeCol(schema.TableColumn{Name: "t", Type: schema.TYPE_TIMESTAMP}, instant, loc).(time.Time)
	if !snapTS.Equal(instant) || !cdcTS.Equal(instant) || !snapTS.Equal(cdcTS) {
		t.Fatalf("DST timestamp parity: snap=%v cdc=%v instant=%v", snapTS, cdcTS, instant)
	}

	wall := time.Date(2018, 12, 15, 12, 0, 0, 0, time.UTC) // naive DATETIME, parsed in UTC
	snapDT := normalizeSnapshot(wall, "DATETIME", loc).(time.Time)
	cdcDT := normalizeCol(schema.TableColumn{Name: "d", Type: schema.TYPE_DATETIME}, wall, loc).(time.Time)
	if !snapDT.Equal(cdcDT) {
		t.Fatalf("DST datetime parity: snap=%v cdc=%v", snapDT, cdcDT)
	}
}

// Issue #456: the reader emits a transaction's rows only when it ends, and a
// synced position with no row since the last end (a DDL, a transaction on an
// excluded table) emits nothing.
func TestTransactionEndFollowsItsRows(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-9"
	flushTxn(t, r) // nothing buffered: no batch

	e := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.InsertAction,
		Rows:   [][]any{{int64(1), []byte("a"), 1.0}, {int64(2), []byte("b"), 2.0}},
	}
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 2 {
		t.Fatalf("emitted %d changes, want 2", len(chs))
	}
	for _, c := range chs {
		if c.Op != rowchange.OpInsert || c.Position != "u:1-9" {
			t.Fatalf("change = %+v", c)
		}
	}
}

// Canal.Close calls OnPosSynced with a nil header, outside the event
// goroutine and possibly mid-transaction. That call ends no transaction: a
// transaction end there would release part of one to a batch (#456).
func TestShutdownSyncEndsNoTransaction(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-9"
	e := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.InsertAction,
		Rows:   [][]any{{int64(1), []byte("a"), 1.0}},
	}
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if err := r.OnPosSynced(nil, gomysql.Position{}, nil, true); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}
	select {
	case b := <-out:
		b.Record.Release()
		t.Fatal("the shutdown sync must not end the transaction")
	default:
	}
	if !r.txnRows {
		t.Fatal("the row must still be buffered for its real transaction end")
	}
}

// A split transaction's earlier record carries the SAFE position, and only
// its final record carries the transaction position, so an ack cannot advance
// the checkpoint past rows a later piece still owes (#456).
func TestSplitTransactionKeepsSafePosition(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.safePos = "u:1-0"
	r.curGTID = "u:1-9"
	rows := make([][]any, maxDirectRows+1)
	for i := range rows {
		rows[i] = []any{int64(i), []byte("x"), 1.0}
	}
	if err := r.OnRow(&canal.RowsEvent{Table: ordersTable(), Action: canal.InsertAction, Rows: rows}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	first := <-out
	defer first.Record.Release()
	second := <-out
	defer second.Record.Release()
	if got := first.Record.NumRows(); got != maxDirectRows {
		t.Fatalf("first piece rows = %d, want %d", got, maxDirectRows)
	}
	if got := second.Record.NumRows(); got != 1 {
		t.Fatalf("second piece rows = %d, want 1", got)
	}
	br, err := transport.NewBatchReader(first.Record, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if pos := br.Position(br.NumRows() - 1); pos != "u:1-0" {
		t.Fatalf("first piece position = %q, want the safe position", pos)
	}
	br2, err := transport.NewBatchReader(second.Record, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if pos := br2.Position(br2.NumRows() - 1); pos != "u:1-9" {
		t.Fatalf("last piece position = %q, want the transaction position", pos)
	}
}

// An upsert UPDATE that changes the primary key must delete the old key; the
// update's own delete is built from the new key, so the old row would survive.
func TestDecodeUpdateKeyChangeDeletesOldKey(t *testing.T) {
	out := make(chan *dataplane.Batch, 4)
	r := newTestReader(out)
	r.cfg.UpsertTargets = map[string]bool{"raw.orders": true}
	r.curGTID = "u:1-5"
	tbl := ordersTable()
	before := []any{int64(7), []byte("old"), 1.0}
	after := []any{int64(8), []byte("new"), 2.0}

	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.UpdateAction, Rows: [][]any{before, after}}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 2 {
		t.Fatalf("got %d changes, want a delete of the old key then the update", len(chs))
	}
	if chs[0].Op != rowchange.OpDelete || chs[0].Key[0] != int64(7) {
		t.Fatalf("synthetic change = %+v, want a delete of the old key 7", chs[0])
	}
	if chs[1].Op != rowchange.OpUpdate || chs[1].Key[0] != int64(8) {
		t.Fatalf("update = %+v, want the new key 8", chs[1])
	}
}

// An append target keeps the old row by design; the key change must not emit a
// delete.
func TestDecodeUpdateKeyChangeAppendKeepsOldKey(t *testing.T) {
	out := make(chan *dataplane.Batch, 4)
	r := newTestReader(out)
	r.curGTID = "u:1-5"
	tbl := ordersTable()
	before := []any{int64(7), []byte("old"), 1.0}
	after := []any{int64(8), []byte("new"), 2.0}

	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.UpdateAction, Rows: [][]any{before, after}}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 1 {
		t.Fatalf("append target emitted %d changes, want only the update", len(chs))
	}
	if chs[0].Key[0] != int64(8) {
		t.Fatalf("update key = %v, want 8", chs[0].Key)
	}
}
