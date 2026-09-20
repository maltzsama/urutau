package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	sq "github.com/Masterminds/squirrel"
)

// ctidCompare builds a physical block-range predicate, e.g. ctid >= $1::tid.
// The explicit ::tid cast is required because the bound travels as a text
// parameter, which Postgres will not coerce to tid implicitly.
func ctidCompare(op string, val any) sq.Sqlizer {
	return sq.Expr("ctid "+op+" ?::tid", val)
}

// ctidString renders a block's first tuple as a tid literal.
func ctidString(block int64) string {
	return fmt.Sprintf("(%d,0)", block)
}

// ctidBounds splits the table by physical block range (the default strategy):
// pagesPerChunk pages per chunk, so no primary key is required and the chunks
// are uniform regardless of key skew. A chunk's size is the pipeline's target
// file size divided by the server block size — one chunk lands roughly one
// target file.
func (c *Chunker) ctidBounds(ctx context.Context) ([][]any, error) {
	blockSize, err := c.blockSize(ctx)
	if err != nil {
		return nil, err
	}
	pagesPerChunk := int64(math.Ceil(float64(c.targetBytes) / float64(blockSize)))
	if pagesPerChunk < 1 {
		pagesPerChunk = 1
	}

	// A partitioned parent has no storage of its own (relpages = 0), and its
	// CTID predicate is applied to every leaf independently — each leaf's ctid
	// starts at (0,0). So the block range that bounds the parent is the
	// LARGEST leaf's page count, not a sum.
	relpages, err := c.relPages(ctx)
	if err != nil {
		return nil, err
	}
	if partitioned, err := c.isPartitioned(ctx); err != nil {
		return nil, err
	} else if partitioned {
		if relpages, err = c.maxLeafPages(ctx); err != nil {
			return nil, err
		}
	}
	return ctidRanges(relpages, pagesPerChunk), nil
}

// ctidRanges generates the chunk starts [ (0,0), (N,0), (2N,0), … ]; the
// orchestrator's Chunks() makes the last one open-high.
func ctidRanges(relpages, pagesPerChunk int64) [][]any {
	if relpages <= 0 {
		relpages = 1
	}
	var bounds [][]any
	for start := int64(0); start < relpages; start += pagesPerChunk {
		bounds = append(bounds, []any{ctidString(start)})
	}
	if len(bounds) == 0 {
		bounds = append(bounds, []any{ctidString(0)})
	}
	return bounds
}

// blockSize reads the server's page size.
func (c *Chunker) blockSize(ctx context.Context) (int64, error) {
	var size int64
	if err := c.db.QueryRowContext(ctx, `SHOW block_size`).Scan(&size); err != nil {
		return 0, fmt.Errorf("postgres: chunker: block_size: %w", err)
	}
	if size <= 0 {
		size = 8192
	}
	return size, nil
}

// relPages reads the table's physical page count.
func (c *Chunker) relPages(ctx context.Context) (int64, error) {
	var pages sql.NullInt64
	err := c.db.QueryRowContext(ctx, `
		SELECT c.relpages
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, c.schema, c.table).Scan(&pages)
	if err != nil {
		return 0, fmt.Errorf("postgres: chunker: relpages: %w", err)
	}
	if !pages.Valid {
		return 0, nil
	}
	return pages.Int64, nil
}

// isPartitioned reports whether the table has any partitions.
func (c *Chunker) isPartitioned(ctx context.Context) (bool, error) {
	var count int64
	err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(i.inhrelid)
		FROM pg_catalog.pg_inherits i
		JOIN pg_catalog.pg_class c ON c.oid = i.inhparent
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, c.schema, c.table).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("postgres: chunker: partitioned check: %w", err)
	}
	return count > 0, nil
}

// maxLeafPages returns the largest leaf partition's page count, the block
// range that bounds a partitioned parent's chunking. pg_partition_tree()
// (PG 12+) is preferred; older servers use a recursive CTE over pg_inherits.
func (c *Chunker) maxLeafPages(ctx context.Context) (int64, error) {
	var version int
	if err := c.db.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return 0, fmt.Errorf("postgres: chunker: server version: %w", err)
	}
	query := maxLeafPagesCTE
	if version >= 120000 {
		query = maxLeafPagesTree
	}
	var pages sql.NullInt64
	if err := c.db.QueryRowContext(ctx, query, c.schema, c.table).Scan(&pages); err != nil {
		return 0, fmt.Errorf("postgres: chunker: partition pages: %w", err)
	}
	if !pages.Valid {
		return 0, nil
	}
	return pages.Int64, nil
}

// maxLeafPagesTree returns the largest leaf's page count on PG 12+.
const maxLeafPagesTree = `
	SELECT MAX(pg_relation_size(pt.relid::oid) / current_setting('block_size')::int)::bigint
	FROM pg_partition_tree(format('%I.%I', $1::text, $2::text)::regclass) pt
	WHERE pt.isleaf`

// maxLeafPagesCTE returns the largest leaf's page count on PG < 12 via
// pg_inherits (pg_partition_tree does not exist there).
const maxLeafPagesCTE = `
	WITH RECURSIVE partition_tree AS (
		SELECT c.oid
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
		UNION ALL
		SELECT child.oid
		FROM pg_inherits i
		JOIN pg_class child ON child.oid = i.inhrelid
		JOIN partition_tree pt ON pt.oid = i.inhparent
	)
	SELECT MAX(pg_relation_size(oid) / current_setting('block_size')::int)::bigint
	FROM partition_tree
	WHERE NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = partition_tree.oid)`
