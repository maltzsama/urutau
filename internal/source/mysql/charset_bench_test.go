package mysql

import "testing"

// benchSink prevents the compiler from eliding the decode calls.
var benchSink string

// latin1Input is "olá, mundo — ação, coração" in MySQL's latin1 (cp1252)
// bytes: 0xE1=á, 0xE7=ç, 0xE3=ã, 0xF4=ô.
var latin1Input = []byte{0x6f, 0x6c, 0xe1, 0x2c, 0x20, 0x6d, 0x75, 0x6e, 0x64, 0x6f, 0x20, 0x61, 0xe7, 0xe3, 0x6f}

// sjisInput is "日本語" in Shift-JIS (multi-byte, so it exercises the x/text
// transform rather than a byte table).
var sjisInput = []byte{0x93, 0xfa, 0x96, 0x7b, 0x8c, 0xea}

func BenchmarkDecodeStringLatin1(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = decodeString(latin1Input, "latin1_swedish_ci")
	}
}

func BenchmarkDecodeStringSjis(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = decodeString(sjisInput, "sjis_japanese_ci")
	}
}

func BenchmarkDecodeStringGenerated(b *testing.B) {
	in := []byte("abcdefgh")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = decodeString(in, "macce_general_ci")
	}
}
