package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/maltzsama/urutau/source"
)

// Partitions divides this table's single-column primary key into n
// contiguous, ordered ranges — the same range-partitioning approach
// range-based CDC tools (e.g. Olake) use for parallel backfill, adapted
// here to also govern live-stream routing: a worker owns exactly the
// rows whose PK falls in its range, in both the DBLog snapshot and the
// live binlog stream, so a key can never switch partition ownership
// between the two phases (no hash function is involved, so there is
// nothing for the two phases to disagree about).
//
// Only a single-column primary key is supported today — composite keys
// return an error rather than silently picking one column or refusing
// partitioning quietly. n <= 1 returns a single unbounded range
// (today's unpartitioned behavior).
func (c *Chunker) Partitions(ctx context.Context, n int) ([]source.Chunk, error) {
	if n <= 1 {
		return []source.Chunk{{}}, nil
	}
	if len(c.pk) != 1 {
		return nil, fmt.Errorf("mysql: partition: table %s.%s has a %d-column primary key — range partitioning supports exactly one column today", c.schema, c.table, len(c.pk))
	}
	pk := c.pk[0]

	dataType, maxLen, err := c.columnStats(ctx, pk)
	if err != nil {
		return nil, fmt.Errorf("mysql: partition: %s.%s.%s: %w", c.schema, c.table, pk, err)
	}

	minVal, maxVal, err := c.minMax(ctx, pk)
	if err != nil {
		return nil, fmt.Errorf("mysql: partition: %s.%s.%s: %w", c.schema, c.table, pk, err)
	}
	if minVal == nil {
		// Empty table: nothing to split — one range covers it (and the
		// worker that owns it will simply see zero rows).
		return []source.Chunk{{}}, nil
	}

	if isIntegerType(dataType) {
		return c.partitionNumeric(minVal, maxVal, n)
	}
	if isStringType(dataType) {
		return c.partitionString(ctx, pk, minVal, maxVal, maxLen, n)
	}
	return nil, fmt.Errorf("mysql: partition: %s.%s.%s: type %q is not a supported partition key (want an integer or char/varchar column)", c.schema, c.table, pk, dataType)
}

func isIntegerType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return true
	}
	return false
}

func isStringType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "char", "varchar":
		return true
	}
	return false
}

func (c *Chunker) columnStats(ctx context.Context, col string) (dataType string, maxLen sql.NullInt64, err error) {
	const q = `
		SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
		LIMIT 1`
	err = c.db.QueryRowContext(ctx, q, c.schema, c.table, col).Scan(&dataType, &maxLen)
	return dataType, maxLen, err
}

func (c *Chunker) minMax(ctx context.Context, col string) (minVal, maxVal any, err error) {
	query := fmt.Sprintf(
		"SELECT (SELECT `%s` FROM `%s`.`%s` ORDER BY `%s` ASC LIMIT 1), "+
			"(SELECT `%s` FROM `%s`.`%s` ORDER BY `%s` DESC LIMIT 1)",
		col, c.schema, c.table, col,
		col, c.schema, c.table, col,
	)
	err = c.db.QueryRowContext(ctx, query).Scan(&minVal, &maxVal)
	return minVal, maxVal, err
}

// partitionNumeric splits [min, max] into n arithmetically even ranges.
// Adapted from Olake's splitEvenlyForInt
// (drivers/mysql/internal/backfill.go) — same idea (an arithmetic
// progression across the key domain), rebuilt against source.Chunk
// instead of Olake's own types.Chunk.
func (c *Chunker) partitionNumeric(minVal, maxVal any, n int) ([]source.Chunk, error) {
	minI, err := toInt64(minVal)
	if err != nil {
		return nil, fmt.Errorf("min value: %w", err)
	}
	maxI, err := toInt64(maxVal)
	if err != nil {
		return nil, fmt.Errorf("max value: %w", err)
	}
	if maxI < minI {
		return nil, fmt.Errorf("max %d < min %d", maxI, minI)
	}

	span := maxI - minI + 1
	step := span / int64(n)
	if step < 1 {
		step = 1
	}

	var out []source.Chunk
	low := minI
	for i := 0; i < n; i++ {
		var lowVal, highVal any
		if i > 0 {
			lowVal = low
		}
		high := low + step
		if i == n-1 || high > maxI {
			highVal = nil // last partition is open-ended, covers any late-arriving max
		} else {
			highVal = high
		}
		out = append(out, source.Chunk{
			Low:  boundTuple(lowVal),
			High: boundTuple(highVal),
		})
		low = high
	}
	return out, nil
}

func boundTuple(v any) []any {
	if v == nil {
		return nil
	}
	return []any{v}
}

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int32:
		return int64(t), nil
	case int:
		return int64(t), nil
	case []byte:
		var out int64
		_, err := fmt.Sscanf(string(t), "%d", &out)
		return out, err
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", v)
	}
}

// partitionString splits a char/varchar primary key into n ranges by
// mapping the key domain into a big.Int space (each character maps to
// its position in a fixed charset), splitting that numeric space evenly,
// then aligning each candidate boundary to the nearest ACTUAL value
// present in the table (a candidate boundary is very unlikely to be a
// real key). Adapted from Olake's splitEvenlyForString/
// encodeCharsetStringToBigInt/decodeBigIntToCharsetString
// (drivers/mysql/internal/backfill.go) — same charset-encoding trick,
// simplified to fixed-count partitioning (Olake solves the harder
// "target roughly K rows per file" sizing problem; partitioning only
// needs N roughly-even ranges, so this skips the adaptive
// stepShrinkFactor retry loop and takes the arithmetic split directly).
func (c *Chunker) partitionString(ctx context.Context, col string, minVal, maxVal any, maxLen sql.NullInt64, n int) ([]source.Chunk, error) {
	if !maxLen.Valid {
		return nil, fmt.Errorf("column %q has no CHARACTER_MAXIMUM_LENGTH (not a fixed char/varchar?)", col)
	}
	minS, err := toString(minVal)
	if err != nil {
		return nil, fmt.Errorf("min value: %w", err)
	}
	maxS, err := toString(maxVal)
	if err != nil {
		return nil, fmt.Errorf("max value: %w", err)
	}
	width := int(maxLen.Int64)
	minPadded := padRight(minS, width)
	maxPadded := padRight(maxS, width)

	minBig, err := encodeCharsetString(minPadded)
	if err != nil {
		return nil, fmt.Errorf("encode min: %w", err)
	}
	maxBig, err := encodeCharsetString(maxPadded)
	if err != nil {
		return nil, fmt.Errorf("encode max: %w", err)
	}
	if maxBig.Cmp(minBig) < 0 {
		return nil, fmt.Errorf("encoded max < encoded min for column %q", col)
	}

	// n-1 interior candidate boundaries, evenly spaced across the encoded
	// domain.
	span := new(big.Int).Sub(maxBig, minBig)
	step := new(big.Int).Div(span, big.NewInt(int64(n)))
	if step.Sign() == 0 {
		step = big.NewInt(1)
	}
	candidates := make([]string, 0, n-1)
	cur := new(big.Int).Set(minBig)
	for i := 1; i < n; i++ {
		cur.Add(cur, step)
		if cur.Cmp(maxBig) >= 0 {
			break
		}
		candidates = append(candidates, decodeCharsetString(cur))
	}

	aligned, err := c.alignBoundaries(ctx, col, candidates)
	if err != nil {
		return nil, fmt.Errorf("align boundaries: %w", err)
	}

	var out []source.Chunk
	var low any
	for _, v := range aligned {
		out = append(out, source.Chunk{Low: boundTuple(low), High: boundTuple(v)})
		low = v
	}
	out = append(out, source.Chunk{Low: boundTuple(low), High: nil})
	// Fewer distinct aligned boundaries than requested (low cardinality)
	// collapses naturally into fewer, still-correct partitions — every
	// row is still covered by exactly one range.
	return out, nil
}

// alignBoundaries maps each candidate value to the first actual key at or
// after it — a candidate is a point in the encoded numeric space, almost
// never an actual row value.
func (c *Chunker) alignBoundaries(ctx context.Context, col string, candidates []string) ([]string, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	quoted := fmt.Sprintf("`%s`", col)
	table := fmt.Sprintf("`%s`.`%s`", c.schema, c.table)
	var out []string
	seen := map[string]bool{}
	for _, v := range candidates {
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT 1", quoted, table, quoted, quoted)
		var actual sql.NullString
		if err := c.db.QueryRowContext(ctx, query, v).Scan(&actual); err != nil {
			if err == sql.ErrNoRows {
				continue // candidate is past the max — no boundary here
			}
			return nil, err
		}
		if actual.Valid && !seen[actual.String] {
			seen[actual.String] = true
			out = append(out, actual.String)
		}
	}
	sort.Strings(out)
	return out, nil
}

func toString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	default:
		return "", fmt.Errorf("unsupported string type %T", v)
	}
}

func padRight(s string, width int) string {
	n := utf8.RuneCountInString(s)
	if n >= width {
		return s
	}
	return s + strings.Repeat("\x00", width-n)
}

// charset is the fixed alphabet partitionString maps characters into a
// big.Int over — same 95-character set Olake uses (digits, upper/lower
// case, common symbols), 1-indexed so a zero big.Int decodes to the
// empty string unambiguously.
var (
	charset                    = []rune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz[\\]^_`{|}~!\"#$%&'()*+,-./:;<=>?@ ")
	charsetIndex, indexCharset = buildCharsetTables()
	charsetBase                = big.NewInt(int64(len(charset)))
)

func buildCharsetTables() (map[rune]int64, map[int64]rune) {
	toIdx := make(map[rune]int64, len(charset))
	toChar := make(map[int64]rune, len(charset))
	for i, ch := range charset {
		idx := int64(i + 1)
		toIdx[ch] = idx
		toChar[idx] = ch
	}
	return toIdx, toChar
}

func encodeCharsetString(s string) (*big.Int, error) {
	val := big.NewInt(0)
	for _, ch := range s {
		idx, ok := charsetIndex[ch]
		if !ok {
			if ch == 0 {
				idx = 0 // the padding character: treat as "before everything"
			} else {
				return nil, fmt.Errorf("character %q is outside the supported partition charset", ch)
			}
		}
		val.Mul(val, charsetBase)
		val.Add(val, big.NewInt(idx))
	}
	return val, nil
}

func decodeCharsetString(n *big.Int) string {
	if n.Sign() == 0 {
		return ""
	}
	x := new(big.Int).Set(n)
	var runes []rune
	for x.Sign() > 0 {
		rem := new(big.Int).Mod(x, charsetBase)
		r := rem.Int64()
		if r == 0 {
			runes = append(runes, indexCharset[charsetBase.Int64()])
			x.Sub(x, big.NewInt(1))
		} else {
			runes = append(runes, indexCharset[r])
		}
		x.Div(x, charsetBase)
	}
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

var _ source.PartitionSource = (*Chunker)(nil)
