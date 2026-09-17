package mysql

import (
	"strings"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
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
// The list covers the single-byte sets MySQL ships that have a Go decoder,
// plus the UCS-2/UTF-16 family. binary is deliberately absent: a binary
// column carries bytes, not text, and must not be reinterpreted.
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
}

func decodeWith(cm *charmap.Charmap) func([]byte) (string, error) {
	return func(b []byte) (string, error) {
		out, err := cm.NewDecoder().Bytes(b)

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
