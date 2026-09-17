// Command charsetgen turns the .tsv tables in this directory into
// charset_tables.go, the single-byte decoding tables for the 7 character
// sets that have no decoder in golang.org/x/text (see issue #113).
//
// Each .tsv was extracted from a REAL MySQL 8.4 server (the one
// test/e2e/docker-compose.yml already starts for the driver's own e2e
// suite), not hand-transcribed from another standard or a lookalike table.
// That distinction matters: two charsets originally considered for this same
// treatment (eucjpms, macce) have x/text lookalikes (EUC-JP, Mac Roman) that
// produce a DIFFERENT mapping in exactly their vendor rows — decoding
// through the lookalike would have silently corrupted those rows (macce
// byte 0x8E: "é" under Mac Roman, a different character under Mac Central
// Europe). Regenerating straight from the server sidesteps that risk: the
// table is whatever MySQL itself says the charset means, not a guess.
//
// Extraction query, run against the byte range 0-255 for each set:
//
//	SELECT HEX(CONVERT(CHAR(<n> USING <charset>) USING utf8mb4))
//
// CHAR(n USING cs) treats n as an ordinal IN THAT CHARSET (confirmed:
// CHAR(0xE9 USING latin1) and CHAR(233 USING latin1) both convert to "é"),
// not as a Unicode code point — which is what makes this a legitimate
// charset-to-UTF-8 mapping rather than an accidental Unicode passthrough.
//
// A byte with no assigned character in the source charset converts to "?"
// (0x3F) without error, rather than failing. Confirmed against two charsets
// where it is unambiguous: swe7 (a strict 7-bit set — every byte 0x80-0xFF
// has no assignment and converts to "?") and geostd8 (several unassigned
// 8-bit positions do the same, verified individually against the server).
// Byte 0x3F ITSELF legitimately decodes to "?" in every table (it is the
// ASCII question mark), which looks identical to the no-mapping case by
// output alone but is distinguishable by input: the generator drops any
// entry whose byte is NOT 0x3F but whose output IS "?", and keeps the
// 0x3F->"?" entry as the one legitimate case. buildTable's counts are
// printed on every run so this can be audited without re-querying MySQL.
//
// Regenerate with (from this directory): go run .
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// charsets is processed in this order for reproducible output.
var charsets = []string{"armscii8", "dec8", "geostd8", "hp8", "keybcs2", "swe7", "macce"}

type entry struct {
	b  int    // 0-255
	cp string // UTF-8 bytes, hex-encoded, as MySQL's CONVERT/HEX produced them
}

func main() {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	names := make([]string, len(charsets))
	copy(names, charsets)
	sort.Strings(names) // deterministic output order

	tables := make(map[string]map[int]rune, len(names))
	for _, cs := range names {
		entries, err := readTSV(filepath.Join(dir, cs+".tsv"))
		if err != nil {
			log.Fatalf("%s: %v", cs, err)
		}
		table, dropped := buildTable(cs, entries)
		fmt.Printf("%-10s %3d mapped, %3d unassigned (dropped, byte != 0x3F but decoded to \"?\")\n",
			cs, len(table), dropped)
		tables[cs] = table
	}

	out := generate(names, tables)
	dest := filepath.Join(dir, "..", "charset_tables.go")
	if err := os.WriteFile(dest, []byte(out), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Println("wrote", dest)
}

func readTSV(path string) ([]entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []entry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed line %q", line)
		}
		b, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("bad byte %q: %w", parts[0], err)
		}
		out = append(out, entry{b: b, cp: strings.ToUpper(parts[1])})
	}
	return out, sc.Err()
}

// buildTable drops every entry that is genuinely unassigned in the source
// charset (decodes to the literal "?" from a byte that is not itself the
// ASCII "?"), and fails loudly if the same byte appears twice with
// different results — a signal the .tsv was corrupted or hand-edited, not
// a case to paper over silently.
func buildTable(cs string, entries []entry) (map[int]rune, int) {
	table := make(map[int]rune, len(entries))
	dropped := 0
	for _, e := range entries {
		if e.cp == "3F" && e.b != 0x3F {
			dropped++
			continue
		}
		r := hexUTF8ToRune(e.cp)
		if prev, ok := table[e.b]; ok && prev != r {
			log.Fatalf("%s: byte %d has conflicting entries %q and %q", cs, e.b, prev, r)
		}
		table[e.b] = r
	}
	return table, dropped
}

// hexUTF8ToRune decodes a hex-encoded UTF-8 byte string (as MySQL's HEX()
// produced it) back to a single rune.
func hexUTF8ToRune(hexStr string) rune {
	if len(hexStr)%2 != 0 {
		log.Fatalf("odd-length hex %q", hexStr)
	}
	raw := make([]byte, len(hexStr)/2)
	for i := range raw {
		v, err := strconv.ParseUint(hexStr[i*2:i*2+2], 16, 8)
		if err != nil {
			log.Fatalf("bad hex %q: %v", hexStr, err)
		}
		raw[i] = byte(v)
	}
	runes := []rune(string(raw))
	if len(runes) != 1 {
		log.Fatalf("hex %q decoded to %d runes, want 1", hexStr, len(runes))
	}
	return runes[0]
}

func generate(names []string, tables map[string]map[int]rune) string {
	var b strings.Builder
	b.WriteString("// Code generated by internal/source/mysql/charsetgen; DO NOT EDIT.\n")
	b.WriteString("// Regenerate: cd internal/source/mysql/charsetgen && go run .\n")
	b.WriteString("//\n")
	b.WriteString("// Source: a real MySQL 8.4 server (test/e2e/docker-compose.yml's mysql\n")
	b.WriteString("// service), queried byte-by-byte via CONVERT(CHAR(n USING <charset>) USING\n")
	b.WriteString("// utf8mb4). See charsetgen/main.go's doc comment for the full method and\n")
	b.WriteString("// why a lookalike table from another standard was not used instead.\n\n")
	b.WriteString("package mysql\n\n")

	b.WriteString("// generatedCharsetTables maps a MySQL character set to its 256-entry\n")
	b.WriteString("// byte -> rune table. A missing entry at index n means byte n is\n")
	b.WriteString("// unassigned in that charset's source table (MySQL itself converts it to\n")
	b.WriteString("// \"?\" without error; decodeGenerated treats a missing entry as a decode\n")
	b.WriteString("// failure instead, keeping the raw byte per this package's own policy of\n")
	b.WriteString("// preferring recoverable raw bytes over a lossy substitute).\n")
	b.WriteString("var generatedCharsetTables = map[string]map[byte]rune{\n")
	for _, name := range names {
		b.WriteString(fmt.Sprintf("\t%q: %sTable,\n", name, name))
	}
	b.WriteString("}\n\n")

	for _, name := range names {
		tbl := tables[name]
		keys := make([]int, 0, len(tbl))
		for k := range tbl {
			keys = append(keys, k)
		}
		sort.Ints(keys)

		b.WriteString(fmt.Sprintf("// %sTable is the byte -> rune mapping for MySQL's %q charset,\n", name, name))
		b.WriteString(fmt.Sprintf("// %d of 256 byte values assigned; the rest are unassigned in the\n", len(tbl)))
		b.WriteString("// source charset.\n")
		b.WriteString(fmt.Sprintf("var %sTable = map[byte]rune{\n", name))
		for _, k := range keys {
			b.WriteString(fmt.Sprintf("\t0x%02X: %d, // %s\n", k, tbl[k], quoteRune(tbl[k])))
		}
		b.WriteString("}\n\n")
	}

	return b.String()
}

// quoteRune renders a rune as a Go rune literal comment for readability in
// the generated file; falls back to a numeric note for non-printable runes.
func quoteRune(r rune) string {
	if r < 0x20 || r == 0x7F {
		return "control"
	}
	return fmt.Sprintf("%q", r)
}
