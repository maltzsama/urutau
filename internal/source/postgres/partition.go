package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/maltzsama/urutau/source"
)

// Partitions divides this table's single-column primary key into n
// contiguous, ordered ranges for worker partitioning — the same ranges govern
// the DBLog snapshot and the live-stream routing, so a key can never switch
// partition ownership between the two phases (no hash is involved, so the two
// phases cannot disagree).
//
// It always returns exactly n disjoint ranges (an empty or sparse table is
// padded with degenerate or type-domain ranges), and marks this chunker
// key-partitioned: CTID ranges are physical and cannot be clipped to a PK
// range, so a partitioned table must chunk by key.
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
	if !st.Columns[i].NotNull {
		// MIN/MAX and the >= / < range predicates all exclude NULL, so a
		// nullable key would silently drop every row with a NULL key.
		return nil, fmt.Errorf("postgres: partition: %s.%s: key %q is nullable — NULL keys cannot be range-partitioned (declare it NOT NULL)", c.schema, c.table, pk)
	}
	dataType := strings.ToLower(st.Columns[i].DataType)

	// Mark partitioned and resolve the strategy BEFORE any return, so an
	// empty table still chunks by key (never CTID).
	c.partitioned = true
	c.chunkColumn = pk
	switch {
	case isIntType(dataType):
		c.chunkColumnKind = kindInt
		c.strategy = strategyBatch
	case isFloatType(dataType):
		c.chunkColumnKind = kindFloat
		c.strategy = strategyBatch
	case isStringType(dataType):
		c.strategy = strategyNext
	default:
		return nil, fmt.Errorf("postgres: partition: %s.%s: column %q type %q is not a supported partition key (want an integer, float, or char/varchar column)", c.schema, c.table, pk, dataType)
	}

	minVal, maxVal, err := c.minMax(ctx, pk)
	if err != nil {
		return nil, fmt.Errorf("postgres: partition: %s: %w", c.qualifiedTable(), err)
	}
	if minVal == nil {
		// Empty table: no data to sample. Split the key type's full domain
		// into n disjoint ranges so the ranges never overlap (which would
		// let several workers snapshot the same rows if data arrives).
		return typeDomainRanges(dataType, n), nil
	}

	if isIntType(dataType) || isFloatType(dataType) {
		return c.partitionNumeric(minVal, maxVal, n)
	}
	return c.partitionString(ctx, pk, n)
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

// typeDomainRanges splits a key type's full domain into n disjoint ranges,
// used when the table is empty and there is no data to sample.
func typeDomainRanges(dataType string, n int) []source.Chunk {
	if isStringType(dataType) {
		return domainRanges(n, func(i int) any {
			return string([]byte{byte(256 * i / n)})
		})
	}
	// Integer and float keys: split the int64 domain (a float key casts).
	min := big.NewInt(math.MinInt64)
	span := new(big.Int).Sub(big.NewInt(math.MaxInt64), min)
	step := new(big.Int).Div(span, big.NewInt(int64(n)))
	cur := new(big.Int).Set(min)
	boundaries := make([]any, 0, n-1)
	for i := 1; i < n; i++ {
		cur.Add(cur, step)
		boundaries = append(boundaries, cur.Int64())
	}
	return rangesFromBoundaries(boundaries)
}

// domainRanges builds n contiguous half-open ranges from n-1 boundary values.
func domainRanges(n int, boundary func(i int) any) []source.Chunk {
	out := make([]source.Chunk, 0, n)
	var lowVal any
	for i := 0; i < n; i++ {
		var highVal any
		if i == n-1 {
			highVal = nil
		} else {
			highVal = boundary(i + 1)
		}
		out = append(out, source.Chunk{Low: partTuple(lowVal), High: partTuple(highVal)})
		lowVal = highVal
	}
	return out
}

// rangesFromBoundaries builds the half-open ranges [nil,b1), [b1,b2), …,
// [b_{n-1},nil) from n-1 ordered boundary values.
func rangesFromBoundaries(boundaries []any) []source.Chunk {
	out := make([]source.Chunk, 0, len(boundaries)+1)
	var lowVal any
	for _, v := range boundaries {
		out = append(out, source.Chunk{Low: partTuple(lowVal), High: partTuple(v)})
		lowVal = v
	}
	out = append(out, source.Chunk{Low: partTuple(lowVal), High: nil})
	return out
}

// partitionNumeric splits [min, max] into n arithmetically even ranges. Only
// the final range is open-ended; if the domain is too small to give every
// worker a full step, the extra partitions are empty [max,max) ranges (they
// own nothing, and a key above max routes to the final open range).
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
	if span < 1 {
		// Overflowed (a domain wider than int64): an even split that still
		// advances.
		span = maxI/int64(n) - minI/int64(n)
		if span < 1 {
			span = 1
		}
	}
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
		switch {
		case i == n-1:
			highVal = nil // only the final partition is open-ended
		case low >= maxI || step > maxI-low:
			// No room for a full step: an empty [max,max) range, so keys
			// above max still route to the final open partition.
			lowVal = maxI
			highVal = maxI
		default:
			low += step
			highVal = low
		}
		out = append(out, source.Chunk{Low: partTuple(lowVal), High: partTuple(highVal)})
	}
	return out, nil
}

// partitionString splits a text key into n ranges by sampling n-1 boundaries
// at row offsets. The count and every offset are read inside ONE REPEATABLE
// READ transaction, so concurrent writes cannot make a later offset return a
// key below an earlier boundary (overlapping ranges). The DB's own ORDER BY
// decides the order, so the ranges use the column's collation and accept any
// character set. A sparse table (fewer distinct keys than workers) is padded
// with degenerate empty ranges so the count stays n.
func (c *Chunker) partitionString(ctx context.Context, col string, n int) ([]source.Chunk, error) {
	quoted := quoteIdent(col)
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("postgres: partition: tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	countQ := psql.Select("COUNT(*)").From(c.qualifiedTable())
	sqlStr, args, err := countQ.ToSql()
	if err != nil {
		return nil, err
	}
	var total int64
	if err := tx.QueryRowContext(ctx, sqlStr, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("postgres: partition: count: %w", err)
	}
	if total == 0 {
		return typeDomainRanges("text", n), nil
	}

	var boundaries []any
	for i := 1; i < n; i++ {
		off := int64(i) * total / int64(n)
		q := psql.Select(quoted).
			From(c.qualifiedTable()).
			OrderBy(quoted).
			Limit(1).
			Offset(uint64(off))
		s, a, err := q.ToSql()
		if err != nil {
			return nil, err
		}
		var v any
		if err := tx.QueryRowContext(ctx, s, a...).Scan(&v); err != nil {
			return nil, fmt.Errorf("postgres: partition: boundary: %w", err)
		}
		if v == nil {
			continue
		}
		if len(boundaries) > 0 && fmt.Sprintf("%v", boundaries[len(boundaries)-1]) == fmt.Sprintf("%v", v) {
			continue // duplicate boundary (low cardinality)
		}
		boundaries = append(boundaries, v)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres: partition: commit: %w", err)
	}

	if len(boundaries) == 0 {
		// No distinct boundary (should not happen for a non-empty table, but
		// be safe): fall back to the type domain so the ranges stay disjoint.
		return typeDomainRanges("text", n), nil
	}

	out := rangesFromBoundaries(boundaries)
	// A sparse table yields fewer distinct boundaries than workers. Pad with
	// degenerate [low,low) ranges anchored at the last real boundary (they
	// own nothing; clipChunksToRange excludes them from snapshot work), so
	// the count is exactly n and the final range stays the only open one.
	low := boundaries[len(boundaries)-1]
	for len(out) < n {
		out = append(out, source.Chunk{Low: partTuple(low), High: partTuple(low)})
	}
	return out, nil
}

func partTuple(v any) []any {
	if v == nil {
		return nil
	}
	return []any{v}
}

var _ source.PartitionSource = (*Chunker)(nil)
