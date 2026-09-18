package postgres

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
// contiguous, ordered ranges for worker partitioning — the same ranges govern
// the DBLog snapshot and the live-stream routing, so a key can never switch
// partition ownership between the two phases (no hash is involved, so the two
// phases cannot disagree).
//
// Only a single-column primary key is supported today; a composite key
// returns an error rather than silently refusing partitioning. Calling this
// with n > 1 also forces key-based chunking for this chunker: CTID ranges are
// physical and cannot be clipped to a PK range.
func (c *Chunker) Partitions(ctx context.Context, n int) ([]source.Chunk, error) {
	if n <= 1 {
		return []source.Chunk{{}}, nil
	}
	if len(c.pk) != 1 {
		return nil, fmt.Errorf("postgres: partition: %s has a %d-column primary key — range partitioning supports exactly one column today", c.qualifiedTable(), len(c.pk))
	}
	pk := c.pk[0]

	st, err := QueryTable(ctx, c.db, c.schema, c.table)
	if err != nil {
		return nil, fmt.Errorf("postgres: partition: introspect %s: %w", c.qualifiedTable(), err)
	}
	i := st.FindColumn(pk)
	if i < 0 {
		return nil, fmt.Errorf("postgres: partition: key %q not found", pk)
	}
	dataType := strings.ToLower(st.Columns[i].DataType)

	minVal, maxVal, err := c.minMax(ctx, pk)
	if err != nil {
		return nil, fmt.Errorf("postgres: partition: %s: %w", c.qualifiedTable(), err)
	}
	if minVal == nil {
		// Empty table: one range covers it.
		return []source.Chunk{{}}, nil
	}

	// This chunker now chunks by the key (the partitioned snapshot path).
	c.partitioned = true
	c.chunkColumn = pk

	switch {
	case isIntType(dataType):
		c.chunkColumnKind = kindInt
		c.strategy = strategyBatch
		return c.partitionNumeric(minVal, maxVal, n)
	case isFloatType(dataType):
		c.chunkColumnKind = kindFloat
		c.strategy = strategyBatch
		return c.partitionNumeric(minVal, maxVal, n)
	case isStringType(dataType):
		c.strategy = strategyNext
		width, werr := c.maxStringLen(ctx, pk)
		if werr != nil {
			return nil, werr
		}
		return c.partitionString(ctx, pk, minVal, maxVal, width, n)
	default:
		return nil, fmt.Errorf("postgres: partition: %s.%s: column %q type %q is not a supported partition key (want an integer, float, or char/varchar column)", c.schema, c.table, pk, dataType)
	}
}

func isIntType(dataType string) bool {
	switch dataType {
	case "smallint", "integer", "bigint":
		return true
	}
	return false
}

func isFloatType(dataType string) bool {
	switch dataType {
	case "real", "double precision":
		return true
	}
	return false
}

func isStringType(dataType string) bool {
	switch dataType {
	case "character varying", "character", "text", "citext":
		return true
	}
	return false
}

func (c *Chunker) minMax(ctx context.Context, col string) (minVal, maxVal any, err error) {
	quoted := quoteIdent(col)
	query := psql.Select("MIN("+quoted+")", "MAX("+quoted+")").From(c.qualifiedTable())
	sqlStr, args, err := query.ToSql()
	if err != nil {
		return nil, nil, err
	}
	if err := c.db.QueryRowContext(ctx, sqlStr, args...).Scan(&minVal, &maxVal); err != nil {
		return nil, nil, err
	}
	return minVal, maxVal, nil
}

func (c *Chunker) maxStringLen(ctx context.Context, col string) (int, error) {
	query := psql.Select("MAX(length(" + quoteIdent(col) + "))").From(c.qualifiedTable())
	sqlStr, args, err := query.ToSql()
	if err != nil {
		return 0, err
	}
	var n sql.NullInt64
	if err := c.db.QueryRowContext(ctx, sqlStr, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("postgres: partition: max length: %w", err)
	}
	if !n.Valid || n.Int64 < 1 {
		return 1, nil
	}
	return int(n.Int64), nil
}

// partitionNumeric splits [min, max] into n arithmetically even ranges.
func (c *Chunker) partitionNumeric(minVal, maxVal any, n int) ([]source.Chunk, error) {
	minI, ok := toInt64(minVal)
	if !ok {
		f, fok := toFloat64(minVal)
		if !fok {
			return nil, fmt.Errorf("postgres: partition: min value %T is not numeric", minVal)
		}
		minI = int64(f)
	}
	maxI, ok := toInt64(maxVal)
	if !ok {
		f, fok := toFloat64(maxVal)
		if !fok {
			return nil, fmt.Errorf("postgres: partition: max value %T is not numeric", maxVal)
		}
		maxI = int64(f)
	}
	if maxI < minI {
		return nil, fmt.Errorf("postgres: partition: max %d < min %d", maxI, minI)
	}

	span := maxI - minI + 1
	step := span / int64(n)
	if step < 1 {
		step = 1
	}

	out := make([]source.Chunk, 0, n)
	low := minI
	for i := 0; i < n; i++ {
		var lowVal, highVal any
		if i > 0 {
			lowVal = low
		}
		high := low + step
		if i == n-1 || high > maxI {
			highVal = nil // last partition is open-ended
		} else {
			highVal = high
		}
		out = append(out, source.Chunk{Low: partTuple(lowVal), High: partTuple(highVal)})
		low = high
	}
	return out, nil
}

// partitionString splits a char/varchar/text key into n ranges by encoding the
// key domain into a big.Int (each character maps to its position in a fixed
// charset), splitting that space evenly, then aligning each candidate
// boundary to the nearest actual value present in the table.
func (c *Chunker) partitionString(ctx context.Context, col string, minVal, maxVal any, width, n int) ([]source.Chunk, error) {
	minS, err := toString(minVal)
	if err != nil {
		return nil, fmt.Errorf("postgres: partition: min value: %w", err)
	}
	maxS, err := toString(maxVal)
	if err != nil {
		return nil, fmt.Errorf("postgres: partition: max value: %w", err)
	}
	minBig, err := encodeCharsetString(padRight(minS, width))
	if err != nil {
		return nil, fmt.Errorf("postgres: partition: encode min: %w", err)
	}
	maxBig, err := encodeCharsetString(padRight(maxS, width))
	if err != nil {
		return nil, fmt.Errorf("postgres: partition: encode max: %w", err)
	}
	if maxBig.Cmp(minBig) < 0 {
		return nil, fmt.Errorf("postgres: partition: encoded max < encoded min for %q", col)
	}

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
		return nil, fmt.Errorf("postgres: partition: align boundaries: %w", err)
	}

	out := make([]source.Chunk, 0, len(aligned)+1)
	var low any
	for _, v := range aligned {
		out = append(out, source.Chunk{Low: partTuple(low), High: partTuple(v)})
		low = v
	}
	out = append(out, source.Chunk{Low: partTuple(low), High: nil})
	return out, nil
}

// alignBoundaries maps each candidate value to the first actual key at or
// after it — a candidate is a point in the encoded numeric space, almost never
// an actual row value.
func (c *Chunker) alignBoundaries(ctx context.Context, col string, candidates []string) ([]string, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	quoted := quoteIdent(col)
	var out []string
	seen := map[string]bool{}
	for _, v := range candidates {
		query := psql.Select(quoted).
			From(c.qualifiedTable()).
			Where(quoted+" >= ?", v).
			OrderBy(quoted).
			Limit(1)
		sqlStr, args, err := query.ToSql()
		if err != nil {
			return nil, err
		}
		var actual sql.NullString
		if err := c.db.QueryRowContext(ctx, sqlStr, args...).Scan(&actual); err != nil {
			if err == sql.ErrNoRows {
				continue
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

func partTuple(v any) []any {
	if v == nil {
		return nil
	}
	return []any{v}
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
// big.Int over (digits, upper/lower case, common symbols), 1-indexed so a zero
// big.Int decodes to the empty string unambiguously.
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
				idx = 0
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
