package mysql

import (
	"fmt"
	"strings"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
)

// Character-set decoding for binlog string values.
//
// The binlog carries a string column's bytes in the column's own character
// set, with no conversion. The backfill does not: its SELECT runs over a
// connection the driver negotiates as utf8mb4, so the server converts every
// string to UTF-8 before it reaches us. Both paths write the same target
// column, so a non-UTF-8 source column lands as mojibake from CDC and as
// correct text from the snapshot — the same split ENUM/SET had.
//
// utf8, utf8mb3, utf8mb4 and ascii need no work: their bytes already are
// UTF-8 (ascii being a subset). Anything else needs a real decode, and an
// unrecognized character set is passed through unchanged rather than
// mangled — a wrong guess would corrupt data that the raw bytes at least
// preserve.

// decodeString converts raw binlog bytes to UTF-8 using the column's
// collation (e.g. "latin1_swedish_ci"). An empty collation, a UTF-8-family
// character set, or one with no decoder returns the bytes as a plain string.
func decodeString(b []byte, collation string) string {
	if len(b) == 0 {
		return ""
	}
	dec, ok := decoderFor(collation)
	if !ok {
		return string(b)
	}
	s, err := dec(b)
	if err != nil {
		// A decode failure means the bytes do not match the declared
		// character set. Keeping them is strictly better than substituting
		// replacement characters: the raw form is still recoverable.
		return string(b)
	}

	return s
}

// decoderFor resolves a collation to its byte decoder. The character set is
// the collation name up to the first underscore ("latin1_swedish_ci" →
// "latin1"); MySQL's own naming guarantees that split.
func decoderFor(collation string) (func([]byte) (string, error), bool) {
	if collation == "" {
		return nil, false
	}
	cs := collation
	if i := strings.IndexByte(cs, '_'); i >= 0 {
		cs = cs[:i]
	}
	dec, ok := charsetDecoders[strings.ToLower(cs)]

	return dec, ok
}

// charsetDecoders maps a MySQL character set to a decoder producing UTF-8.
// Only sets whose bytes are NOT already UTF-8 appear here; everything else
// (utf8/utf8mb3/utf8mb4/ascii, and anything unlisted) passes through.
//
// The list covers every MySQL character set x/text models faithfully (the
// single-byte sets, the Unicode family, the multi-byte East Asian sets), plus
// 7 single-byte sets x/text has no decoder for at all — armscii8, dec8,
// geostd8, hp8, keybcs2, swe7, macce — decoded via generatedCharsetTables
// (charset_tables.go), extracted directly from a real MySQL 8.4 server
// rather than borrowed from a lookalike standard. See
// internal/source/mysql/charsetgen's doc comment for the extraction method.
//
// macce deserves a specific note: it is NOT charmap.Macintosh (Mac Roman).
// Byte 0x8E decodes to "é" under Mac Roman and to a different character
// under Mac Central Europe — confirmed against the real server — so folding
// it into the x/text charmap would have silently corrupted exactly the
// accented characters the charset exists to carry. The generated table
// avoids that by construction.
//
// What remains unlisted, and why: eucjpms is MySQL's Microsoft-flavored
// EUC-JP. It is multi-byte, so the same-server extraction this package uses
// for the 7 single-byte sets does not directly apply (enumerating byte pairs
// found real vendor-row divergences from plain EUC-JP, but also inconsistent
// error-vs-"?" behavior from MySQL's CONVERT() that was not fully resolved).
// Tracked separately rather than shipped uncertain — see issue referenced in
// eucjpms's passthrough test.
var charsetDecoders = map[string]func([]byte) (string, error){
	"latin1":   decodeWith(charmap.Windows1252), // MySQL's latin1 IS cp1252, not ISO-8859-1
	"latin2":   decodeWith(charmap.ISO8859_2),
	"latin5":   decodeWith(charmap.ISO8859_9),
	"latin7":   decodeWith(charmap.ISO8859_13),
	"cp1250":   decodeWith(charmap.Windows1250),
	"cp1251":   decodeWith(charmap.Windows1251),
	"cp1256":   decodeWith(charmap.Windows1256),
	"cp1257":   decodeWith(charmap.Windows1257),
	"cp850":    decodeWith(charmap.CodePage850),
	"cp852":    decodeWith(charmap.CodePage852),
	"cp866":    decodeWith(charmap.CodePage866),
	"koi8r":    decodeWith(charmap.KOI8R),
	"koi8u":    decodeWith(charmap.KOI8U),
	"greek":    decodeWith(charmap.ISO8859_7),
	"hebrew":   decodeWith(charmap.ISO8859_8),
	"macroman": decodeWith(charmap.Macintosh),
	"ucs2":     decodeUTF16BE, // BMP-only subset of UTF-16, big endian
	"utf16":    decodeUTF16BE,
	"utf16le":  decodeUTF16LE,
	"utf32":    decodeUTF32BE, // MySQL's utf32 is big endian, fixed 4 bytes

	// Multi-byte East Asian sets. x/text's decoders are the WHATWG/Unicode
	// mappings, which match MySQL's tables for the overwhelming majority of
	// code points but are not guaranteed identical in every corner — MySQL
	// maintains its own tables. Two known shapes are handled explicitly:
	//
	//   - sjis vs cp932: MySQL ships both. cp932 is Microsoft's superset of
	//     Shift-JIS (extra NEC/IBM vendor rows), and x/text's ShiftJIS
	//     decoder follows the WHATWG index, which already includes those
	//     rows — so it serves both, and plain sjis input is unaffected by
	//     the extra mappings.
	//   - gbk vs gb18030: gb18030 is a strict superset of gbk, but decoding
	//     gbk bytes WITH the gb18030 decoder is not safe in general (the
	//     4-byte forms differ), so each maps to its own decoder.
	//
	// ujis is MySQL's name for EUC-JP. eucjpms is MySQL's Microsoft-flavored
	// EUC-JP variant, deliberately absent: it differs from plain EUC-JP in
	// exactly the vendor rows x/text does not model, so it passes through
	// rather than decoding a handful of characters wrongly.
	"sjis":    decodeWith(japanese.ShiftJIS),
	"cp932":   decodeWith(japanese.ShiftJIS),
	"ujis":    decodeWith(japanese.EUCJP),
	"gbk":     decodeWith(simplifiedchinese.GBK),
	"gb18030": decodeWith(simplifiedchinese.GB18030),
	"big5":    decodeWith(traditionalchinese.Big5),
	"euckr":   decodeWith(korean.EUCKR),

	// gb2312 is the subset GBK extends, so the GBK decoder covers it: every
	// gb2312 byte sequence is a valid GBK one with the same meaning.
	"gb2312": decodeWith(simplifiedchinese.GBK),
	// MySQL's tis620 is the Thai set Windows-874 encodes (cp874 is the
	// Microsoft name for the same code page).
	"tis620": decodeWith(charmap.Windows874),

	// Generated from a real MySQL server — see this map's doc comment.
	"armscii8": decodeGenerated("armscii8"),
	"dec8":     decodeGenerated("dec8"),
	"geostd8":  decodeGenerated("geostd8"),
	"hp8":      decodeGenerated("hp8"),
	"keybcs2":  decodeGenerated("keybcs2"),
	"swe7":     decodeGenerated("swe7"),
	"macce":    decodeGenerated("macce"),
}

// decodeGenerated returns a decoder backed by one of generatedCharsetTables'
// per-byte maps (charset_tables.go). A byte absent from the table is
// unassigned in the source charset — MySQL itself would convert it to "?",
// but this package's policy (see decodeString) is that a byte the decoder
// cannot place is kept raw rather than replaced, so it is reported as a
// decode error and the caller falls back to the original bytes.
func decodeGenerated(charset string) func([]byte) (string, error) {
	table := generatedCharsetTables[charset] // panics at init if charsetgen and this map drift — caught by TestGeneratedCharsetsRegistered
	return func(b []byte) (string, error) {
		out := make([]rune, len(b))
		for i, bb := range b {
			r, ok := table[bb]
			if !ok {
				return "", fmt.Errorf("mysql: byte 0x%02X is unassigned in charset %q", bb, charset)
			}
			out[i] = r
		}
		return string(out), nil
	}
}

// decodeWith adapts an x/text encoding to the decoder signature.
func decodeWith(enc encoding.Encoding) func([]byte) (string, error) {
	return func(b []byte) (string, error) {
		out, err := enc.NewDecoder().Bytes(b)

		return string(out), err
	}
}

func decodeUTF16BE(b []byte) (string, error) {
	out, err := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder().Bytes(b)

	return string(out), err
}

func decodeUTF16LE(b []byte) (string, error) {
	out, err := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder().Bytes(b)

	return string(out), err
}

func decodeUTF32BE(b []byte) (string, error) {
	out, err := utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM).NewDecoder().Bytes(b)

	return string(out), err
}
