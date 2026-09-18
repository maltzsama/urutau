package postgres

import (
	"context"
	"fmt"
	"strings"

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
		// Empty table: no domain to split. Return n unbounded ranges so the
		// coordinator's n worker groups each get one (the snapshot reads no
		// rows; live events all route to the first owner until the table
		// grows).
		return make([]source.Chunk, n), nil
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
		return c.partitionString(ctx, pk, n)
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
	if span < 1 {
		// Overflowed (a domain wider than int64): fall back to an even split
		// that still advances.
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

// partitionString splits a text key into n ranges by sampling n-1 boundaries
// at row offsets. The DB's own ORDER BY decides the order, so the ranges use
// the column's collation and accept any character set — matching the snapshot
// predicates and the live routing exactly.
func (c *Chunker) partitionString(ctx context.Context, col string, n int) ([]source.Chunk, error) {
	quoted := quoteIdent(col)
	countQ := psql.Select("COUNT(*)").From(c.qualifiedTable())
	sqlStr, args, err := countQ.ToSql()
	if err != nil {
		return nil, err
	}
	var total int64
	if err := c.db.QueryRowContext(ctx, sqlStr, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("postgres: partition: count: %w", err)
	}
	if total == 0 {
		return make([]source.Chunk, n), nil
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
		if err := c.db.QueryRowContext(ctx, s, a...).Scan(&v); err != nil {
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

	out := make([]source.Chunk, 0, len(boundaries)+1)
	var low any
	for _, v := range boundaries {
		out = append(out, source.Chunk{Low: partTuple(low), High: partTuple(v)})
		low = v
	}
	out = append(out, source.Chunk{Low: partTuple(low), High: nil})
	return out, nil
}

func partTuple(v any) []any {
	if v == nil {
		return nil
	}
	return []any{v}
}

var _ source.PartitionSource = (*Chunker)(nil)
