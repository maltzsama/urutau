package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/maltzsama/urutau/source"
)

// Chunker splits a table by its primary key, using the chunk-skipping
// bounds trick shared with the MySQL source: pick every chunkSize-th key
// with LIMIT 1 OFFSET n, then read the slice between consecutive bounds.
// Bounds are only seeds for the half-open range, so the split stays
// correct under concurrent inserts.
type Chunker struct {
	db        *sql.DB
	schema    string
	table     string
	pk        []string
	chunkSize int

	// workers bounds the concurrent row-normalization pool (issue #161).
	// 1 means sequential. The adapter sets it from maxThreads (#165).
	workers int
	// retries is the transient-error retry budget for the chunk SELECT and
	// bounds queries (#166). 0 disables retry.
	retries int
}

// ChunkerOption tunes a Chunker.
type ChunkerOption func(*Chunker)

// WithWorkers sets the concurrent normalization pool size. n < 1 is clamped
// to 1 (sequential).
func WithWorkers(n int) ChunkerOption {
	return func(c *Chunker) {
		if n < 1 {
			n = 1
		}
		c.workers = n
	}
}

// WithRetries sets the transient-error retry budget for snapshot queries.
func WithRetries(n int) ChunkerOption {
	return func(c *Chunker) {
		if n < 0 {
			n = 0
		}
		c.retries = n
	}
}

// NewChunker builds a chunker for one source table.
func NewChunker(db *sql.DB, source, pk string, chunkSize int, opts ...ChunkerOption) (*Chunker, error) {
	schema, table, ok := strings.Cut(source, ".")
	if !ok {
		return nil, fmt.Errorf("postgres: chunker: source %q must be schema.table", source)
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("postgres: chunker: chunkSize must be positive")
	}
	// A spec may write the key list as "a, b"; the raw split would leave a
	// leading space on the second column and break every keyed comparison.
	var pks []string
	for _, c := range strings.Split(pk, ",") {
		if c = strings.TrimSpace(c); c != "" {
			pks = append(pks, c)
		}
	}
	c := &Chunker{
		db:        db,
		schema:    schema,
		table:     table,
		pk:        pks,
		chunkSize: chunkSize,
		workers:   runtime.NumCPU(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// PK returns the primary key columns the chunker splits by.
func (c *Chunker) PK() []string { return c.pk }

// Bounds returns the ordered list of chunk boundary keys: key[0] is the
// lowest PK, followed by every chunkSize-th key, then nil (the open high
// bound of the last chunk). The whole computation is retried on a transient
// error, so a failure while fetching a boundary row is covered, not just the
// initial QueryContext.
func (c *Chunker) Bounds(ctx context.Context) ([][]any, error) {
	cols := quotedList(c.pk)
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s ORDER BY %s LIMIT 1 OFFSET $1",
		cols, quoteIdent(c.schema), quoteIdent(c.table), cols,
	)

	var bounds [][]any
	err := retryTransientErr(ctx, c.retries, func() error {
		bounds = bounds[:0]
		for offset := 0; ; offset += c.chunkSize {
			rows, err := c.db.QueryContext(ctx, query, offset)
			if err != nil {
				return fmt.Errorf("postgres: chunker bounds: %w", err)
			}
			key, scanErr := scanRow(rows)
			closeErr := rows.Close()
			switch {
			case scanErr == sql.ErrNoRows:
				// Clean end of the key sequence: only here is an empty
				// result the terminator, and only if the close was clean.
				if closeErr != nil {
					return fmt.Errorf("postgres: chunker bounds close: %w", closeErr)
				}
				return nil
			case scanErr != nil:
				return scanErr
			case closeErr != nil:
				return fmt.Errorf("postgres: chunker bounds close: %w", closeErr)
			}
			bounds = append(bounds, key)
		}
	})
	if err != nil {
		return nil, err
	}
	return bounds, nil
}

// Scan executes the chunk SELECT and calls fn for every row, keyed by
// column name. Values decode through the same scalar mapping the pgoutput
// reader uses, so snapshot rows and stream rows land in Iceberg identically.
//
// The whole scan is retried on a transient error, covering a failure during
// rows.Next/Scan and not just the initial QueryContext. Rows are buffered
// until the scan completes, so a retry never re-delivers rows to fn — the
// DBLog snapshot collects a chunk before relaying it, so a full re-scan is
// safe and cannot duplicate.
//
// Within one scan, rows are normalized by a bounded pool of `workers`
// goroutines, but fn is always invoked from a single goroutine, so it need
// not be thread-safe. Row order is not preserved (snapshot rows are keyed
// inserts, order-independent).
func (c *Chunker) Scan(ctx context.Context, ch source.Chunk, fn func(row map[string]any) error) error {
	var buffered []map[string]any
	err := retryTransientErr(ctx, c.retries, func() error {
		buffered = buffered[:0]
		return c.scanOnce(ctx, ch, func(m map[string]any) error {
			buffered = append(buffered, m)
			return nil
		})
	})
	if err != nil {
		return err
	}
	for _, m := range buffered {
		if err := fn(m); err != nil {
			return err
		}
	}
	return nil
}

// scanOnce runs one chunk SELECT, streaming normalized rows to fn.
func (c *Chunker) scanOnce(ctx context.Context, ch source.Chunk, fn func(row map[string]any) error) error {
	// Row-constructor comparison keeps composite PKs lexicographic; the
	// placeholder index runs across clauses ($1..$k, then $k+1..).
	cond := make([]string, 0, 2)
	args := make([]any, 0, 2*len(c.pk))
	cols := quotedList(c.pk)
	argIdx := 1

	if ch.Low != nil {
		cond = append(cond, fmt.Sprintf("(%s) >= (%s)", cols, placeholders(argIdx, len(c.pk))))
		args = append(args, ch.Low...)
		argIdx += len(c.pk)
	}
	if ch.High != nil {
		cond = append(cond, fmt.Sprintf("(%s) < (%s)", cols, placeholders(argIdx, len(c.pk))))
		args = append(args, ch.High...)
	}
	where := ""
	if len(cond) > 0 {
		where = " WHERE " + strings.Join(cond, " AND ")
	}

	// Without a row filter the SELECT reads the whole row; select * keeps
	// it simple and correct.
	query := fmt.Sprintf("SELECT * FROM %s.%s%s ORDER BY %s",
		quoteIdent(c.schema), quoteIdent(c.table), where, cols)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("postgres: chunk scan: %w", err)
	}
	// Close is captured, not merely deferred: a close error is a real
	// transient failure and must be retried like any other read error.
	closed := false
	closeRows := func() error {
		if closed {
			return nil
		}
		closed = true
		return rows.Close()
	}
	defer func() { _ = closeRows() }()

	colsMeta, err := rows.Columns()
	if err != nil {
		return err
	}

	produce := func() ([]any, bool, error) {
		if !rows.Next() {
			return nil, false, rows.Err()
		}
		vals := make([]any, len(colsMeta))
		ptrs := make([]any, len(colsMeta))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, false, fmt.Errorf("postgres: chunk scan row: %w", err)
		}
		return vals, true, nil
	}
	err = scanPipeline(ctx, colsMeta, c.workers, produce, fn)
	if cerr := closeRows(); err == nil && cerr != nil {
		return fmt.Errorf("postgres: chunk scan close: %w", cerr)
	}
	return err
}

// scanBatchSize is how many raw rows a producer hands to a worker at once.
// Batching amortizes the channel hand-off: one send per batch instead of one
// per row, which is what makes the concurrent path worthwhile for cheap rows.
const scanBatchSize = 256

// scanPipeline pulls raw rows from produce and calls fn with the normalized
// map. With workers <= 1 it is a straight loop; otherwise a producer feeds a
// bounded pool of workers that normalize whole batches in parallel, and a
// single collector calls fn — so fn never runs concurrently and no row
// escapes by racing a channel pull. Row order is not preserved: snapshot rows
// are keyed inserts, order-independent.
func scanPipeline(
	ctx context.Context,
	cols []string,
	workers int,
	produce func() ([]any, bool, error),
	fn func(map[string]any) error,
) error {
	if workers <= 1 {
		for {
			vals, ok, err := produce()
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if err := fn(normalizeRow(cols, vals)); err != nil {
				return err
			}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Buffered to workers batches so the producer and the workers overlap
	// without unbounded buffering (channel-based backpressure).
	jobs := make(chan [][]any, workers)
	results := make(chan []map[string]any, workers)

	g, gctx := errgroup.WithContext(ctx)

	// Producer: the only goroutine that touches the *sql.Rows. Rows are
	// accumulated into batches before the hand-off.
	g.Go(func() error {
		defer close(jobs)
		buf := make([][]any, 0, scanBatchSize)
		flush := func() error {
			if len(buf) == 0 {
				return nil
			}
			b := buf
			buf = make([][]any, 0, scanBatchSize)
			select {
			case jobs <- b:
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		for {
			vals, ok, err := produce()
			if err != nil {
				return err
			}
			if !ok {
				return flush()
			}
			buf = append(buf, vals)
			if len(buf) == scanBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	})

	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for b := range jobs {
				out := make([]map[string]any, len(b))
				for j := range b {
					out[j] = normalizeRow(cols, b[j])
				}
				select {
				case results <- out:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	}

	// Close results once every worker has drained jobs, so the collector's
	// range terminates. On error the errgroup's context cancels the
	// producer/workers and this still runs.
	go func() {
		_ = g.Wait()
		close(results)
	}()

	var fnErr error
	for b := range results {
		for _, m := range b {
			if err := fn(m); err != nil {
				fnErr = err
				cancel()
				break
			}
		}
		if fnErr != nil {
			break
		}
	}
	// Drain whatever is still buffered/in flight so a cancelled worker
	// blocked on results can observe gctx and exit.
	for range results {
	}
	if fnErr != nil {
		return fnErr
	}
	return g.Wait()
}

// normalizeRow maps one raw row to a column-keyed map through the shared
// scalar mapping.
func normalizeRow(cols []string, vals []any) map[string]any {
	m := make(map[string]any, len(cols))
	for i, name := range cols {
		m[name] = normalize(vals[i])
	}
	return m
}

func scanRow(rows *sql.Rows) ([]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		// A false Next is a clean end only if the iteration itself did not
		// fail; otherwise the transient error must surface (and be retried)
		// instead of being reported as an empty result.
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	// Same scalar mapping as Scan, so bounds and snapshot rows agree: a
	// []byte cell here would otherwise flow raw into the persisted bounds
	// and be bound back into SQL as the wrong type.
	for i := range vals {
		vals[i] = normalize(vals[i])
	}
	return vals, nil
}

// placeholders renders $from..$from+n-1.
func placeholders(from, n int) string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("$%d", from+i)
	}
	return strings.Join(out, ", ")
}

// quoteIdent quotes one identifier, doubling embedded quotes.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quotedList(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = quoteIdent(strings.TrimSpace(c))
	}
	return strings.Join(out, ", ")
}

// normalize maps driver values into the scalar subset shared with the
// stream decoder.
func normalize(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return v
	}
}
