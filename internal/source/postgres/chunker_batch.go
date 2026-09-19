package postgres

import (
	"context"
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

// maxChunkBounds caps the chunk count a value-range split may produce. A
// sparse wide domain (a bigint PK with a huge span and a small step) would
// otherwise allocate millions of empty chunks; the cap fails loud instead.
const maxChunkBounds = 1 << 20

// batchBounds splits an integer/float chunk column by value range: MIN..MAX
// stepped by chunkSize. A dense key yields ~chunkSize rows per chunk.
func (c *Chunker) batchBounds(ctx context.Context) ([][]any, error) {
	col := quoteIdent(c.chunkColumn)
	query, args, err := psql.
		Select("MIN("+col+")", "MAX("+col+")").
		From(c.qualifiedTable()).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("postgres: chunker: min/max sql: %w", err)
	}
	var minV, maxV any
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&minV, &maxV); err != nil {
		return nil, fmt.Errorf("postgres: chunker: min/max: %w", err)
	}
	if minV == nil {
		// Empty table: one open chunk covers nothing.
		return [][]any{{nil}}, nil
	}

	if c.chunkColumnKind == kindInt {
		min, okMin := toInt64(minV)
		max, okMax := toInt64(maxV)
		if !okMin || !okMax {
			return c.nextBounds(ctx)
		}
		step := int64(c.chunkSize)
		if step < 1 {
			step = 1
		}
		var bounds [][]any
		for v := min; v <= max; {
			bounds = append(bounds, []any{v})
			if len(bounds) >= maxChunkBounds {
				return nil, fmt.Errorf("postgres: chunker: %s: value-range split exceeds %d chunks (widen the key or set chunkColumn)", c.qualifiedTable(), maxChunkBounds)
			}
			// Stop before the next add would exceed max or overflow int64.
			if step > max-v {
				break
			}
			v += step
		}
		if len(bounds) == 0 {
			bounds = append(bounds, []any{min})
		}
		return bounds, nil
	}

	min, okMin := toFloat64(minV)
	max, okMax := toFloat64(maxV)
	if !okMin || !okMax {
		return c.nextBounds(ctx)
	}
	step := c.numericStep()
	var bounds [][]any
	for v := min; v <= max; {
		bounds = append(bounds, []any{v})
		if len(bounds) >= maxChunkBounds {
			return nil, fmt.Errorf("postgres: chunker: %s: value-range split exceeds %d chunks (widen the key or set chunkColumn)", c.qualifiedTable(), maxChunkBounds)
		}
		next := v + step
		if next == v {
			break // step too small to advance a large float
		}
		v = next
	}
	if len(bounds) == 0 {
		bounds = append(bounds, []any{min})
	}
	return bounds, nil
}

// nextBounds steps a non-numeric chunk column by cursor: the next boundary is
// the maximum of the first chunkSize values past the previous boundary.
func (c *Chunker) nextBounds(ctx context.Context) ([][]any, error) {
	col := quoteIdent(c.chunkColumn)
	query, args, err := psql.Select("MIN(" + col + ")").From(c.qualifiedTable()).ToSql()
	if err != nil {
		return nil, fmt.Errorf("postgres: chunker: min sql: %w", err)
	}
	var minV any
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&minV); err != nil {
		return nil, fmt.Errorf("postgres: chunker: min: %w", err)
	}
	if minV == nil {
		return [][]any{{nil}}, nil
	}

	bounds := [][]any{{minV}}
	prev := minV
	for {
		next, err := c.nextChunkEnd(ctx, prev)
		if err != nil {
			return nil, err
		}
		if next == nil || fmt.Sprintf("%v", next) == fmt.Sprintf("%v", prev) {
			break
		}
		bounds = append(bounds, []any{next})
		prev = next
	}
	return bounds, nil
}

// nextChunkEnd returns MAX(col) of the first chunkSize rows past prev.
func (c *Chunker) nextChunkEnd(ctx context.Context, prev any) (any, error) {
	col := quoteIdent(c.chunkColumn)
	sub := psql.Select(col).
		From(c.qualifiedTable()).
		Where(sq.Gt{col: prev}).
		OrderBy(col).
		Limit(uint64(c.chunkSize))
	query, args, err := psql.Select("MAX("+col+")").FromSelect(sub, "t").ToSql()
	if err != nil {
		return nil, fmt.Errorf("postgres: chunker: next sql: %w", err)
	}
	var next any
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&next); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres: chunker: next end: %w", err)
	}
	return next, nil
}

func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	default:
		return 0, false
	}
}

func toFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
}
