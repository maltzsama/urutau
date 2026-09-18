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
// are uniform regardless of key skew. A partitioned parent has no storage of
// its own, so its leaf partitions' pages drive a proportional split.
func (c *Chunker) ctidBounds(ctx context.Context) ([][]any, error) {
	blockSize, err := c.blockSize(ctx)
	if err != nil {
		return nil, err
	}
	pagesPerChunk := int64(math.Ceil(float64(c.targetBytes) / float64(blockSize)))
	if pagesPerChunk < 1 {
		pagesPerChunk = 1
	}

	partitioned, err := c.isPartitioned(ctx)
	if err != nil {
		return nil, err
	}
	if !partitioned {
		relpages, err := c.relPages(ctx)
		if err != nil {
			return nil, err
		}
		return ctidRanges(relpages, pagesPerChunk), nil
	}
	return c.ctidPartitionedBounds(ctx, pagesPerChunk)
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

// ctidPartitionedBounds distributes the page budget proportionally across the
// leaf partitions: a partition with more pages gets proportionally more
// chunks, so the total chunk count stays near targetBytes/blockSize.
func (c *Chunker) ctidPartitionedBounds(ctx context.Context, pagesPerChunk int64) ([][]any, error) {
	pages, maxPages, err := c.loadPartitionPages(ctx)
	if err != nil {
		return nil, err
	}
	return ctidPartitionedBoundsPages(pages, maxPages, pagesPerChunk), nil
}

// ctidPartitionedBoundsPages distributes the page budget proportionally
// across the leaf partitions: a partition with more pages gets proportionally
// more chunks, so the total chunk count stays near targetBytes/blockSize.
func ctidPartitionedBoundsPages(pages []int64, maxPages, pagesPerChunk int64) [][]any {
	if maxPages <= 0 {
		// Every partition is empty: a single open chunk covers nothing.
		return [][]any{{ctidString(0)}}
	}
	var bounds [][]any
	for start := int64(0); start < maxPages; {
		bounds = append(bounds, []any{ctidString(start)})
		remaining := int64(0)
		for _, p := range pages {
			if p > start {
				remaining++
			}
		}
		if remaining < 1 {
			remaining = 1
		}
		batch := int64(math.Ceil(float64(pagesPerChunk) / float64(remaining)))
		if batch < 1 {
			batch = 1
		}
		start += batch
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

// loadPartitionPages returns each leaf partition's page count and the maximum
// across them. pg_partition_tree() (PG 12+) is preferred; older servers use a
// recursive CTE over pg_inherits.
func (c *Chunker) loadPartitionPages(ctx context.Context) ([]int64, int64, error) {
	var version int
	if err := c.db.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return nil, 0, fmt.Errorf("postgres: chunker: server version: %w", err)
	}
	query := partitionPagesCTE
	if version >= 120000 {
		query = partitionPagesTree
	}
	rows, err := c.db.QueryContext(ctx, query, c.schema, c.table)
	if err != nil {
		return nil, 0, fmt.Errorf("postgres: chunker: partition pages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var pages []int64
	var maxPages int64
	for rows.Next() {
		var name string
		var p int64
		if err := rows.Scan(&name, &p); err != nil {
			return nil, 0, err
		}
		pages = append(pages, p)
		if p > maxPages {
			maxPages = p
		}
	}
	return pages, maxPages, rows.Err()
}

// partitionPagesTree counts leaf-partition pages on PG 12+.
const partitionPagesTree = `
	SELECT pt.relid::text,
	       CEIL(1.05 * (pg_relation_size(pt.relid::oid) / current_setting('block_size')::int))::bigint
	FROM pg_partition_tree($1 || '.' || $2) pt
	WHERE pt.isleaf = true
	ORDER BY 2 DESC`

// partitionPagesCTE counts leaf-partition pages on PG < 12 via pg_inherits.
const partitionPagesCTE = `
	WITH RECURSIVE partition_tree AS (
		SELECT c.oid, c.relname AS name,
		       CEIL(1.05 * (pg_relation_size(c.oid) / current_setting('block_size')::int))::bigint AS pages
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
		UNION ALL
		SELECT child.oid, child.relname,
		       CEIL(1.05 * (pg_relation_size(child.oid) / current_setting('block_size')::int))::bigint
		FROM pg_inherits i
		JOIN pg_class child ON child.oid = i.inhrelid
		JOIN partition_tree pt ON pt.oid = i.inhparent
	)
	SELECT name, pages
	FROM partition_tree
	WHERE NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = partition_tree.oid)
	ORDER BY pages DESC`
