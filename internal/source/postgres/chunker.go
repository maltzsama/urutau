package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"golang.org/x/sync/errgroup"

	"github.com/maltzsama/urutau/source"
)

// psql builds queries with Postgres $N placeholders.
var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// defaultChunkTargetBytes is the CTID strategy's target bytes per chunk when
// the pipeline leaves sink.defaults.targetFileSize unset. It mirrors the sink's
// own 512Mi default (and OLake's EffectiveParquetSize): pagesPerChunk =
// ceil(targetBytes / blockSize), so one chunk lands roughly one target file.
const defaultChunkTargetBytes = 512 << 20

// chunkStrategy selects how a table's snapshot is split into chunks.
type chunkStrategy int

const (
	// strategyCTID splits by physical block ranges (the default): no primary
	// key required, uniform chunks regardless of key skew.
	strategyCTID chunkStrategy = iota
	// strategyBatch splits an integer/float chunk column by value range.
	strategyBatch
	// strategyNext steps a non-numeric chunk column by cursor.
	strategyNext
)

// chunkColumnKind is the chunk column's numeric shape, deciding batch vs next.
type chunkColumnKind int

const (
	kindOther chunkColumnKind = iota
	kindInt
	kindFloat
)

// Chunker splits a table into snapshot chunks. The strategy is CTID by
// default; a configured chunk column switches to a key-based strategy
// (batch-size for integer/float, next-query otherwise). A partitioned table
// (workers > 1) always uses a key-based strategy, because the chunk range
// must stay routable to the same worker as the live stream.
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
	// columns is the projection (#162): the source columns to read. Empty
	// means all.
	columns []string
	// filter is the compiled structured filter (#163), composed with the
	// chunk bounds. Nil means no filter.
	filter sq.Sqlizer

	// strategy is the resolved chunking strategy.
	strategy chunkStrategy
	// chunkColumn is the configured chunk column (empty for CTID).
	chunkColumn string
	// chunkColumnKind is the chunk column's numeric shape (batch vs next).
	chunkColumnKind chunkColumnKind
	// targetBytes is the CTID strategy's target bytes per chunk.
	targetBytes int64
	// partitioned forces a key-based strategy: set by Partitions(n>1) so a
	// partitioned table never chunks by CTID (CTID is not routable).
	partitioned bool
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

// WithColumns sets the column projection (#162). Empty means all columns.
func WithColumns(cols []string) ChunkerOption {
	return func(c *Chunker) { c.columns = cols }
}

// WithFilter sets the compiled structured filter (#163). Nil means none.
func WithFilter(f sq.Sqlizer) ChunkerOption {
	return func(c *Chunker) { c.filter = f }
}

// WithChunkColumn selects the chunk column (#151). Empty keeps the CTID
// default.
func WithChunkColumn(col string) ChunkerOption {
	return func(c *Chunker) { c.chunkColumn = strings.TrimSpace(col) }
}

// WithTargetBytes sets the CTID strategy's target bytes per chunk (from
// sink.defaults.targetFileSize). Non-positive keeps the default.
func WithTargetBytes(n int64) ChunkerOption {
	return func(c *Chunker) {
		if n > 0 {
			c.targetBytes = n
		}
	}
}

// NewChunker builds a chunker for one source table.
func NewChunker(ctx context.Context, db *sql.DB, source, pk string, chunkSize int, opts ...ChunkerOption) (*Chunker, error) {
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
		db:          db,
		schema:      schema,
		table:       table,
		pk:          pks,
		chunkSize:   chunkSize,
		workers:     runtime.NumCPU(),
		targetBytes: defaultChunkTargetBytes,
	}
	for _, opt := range opts {
		opt(c)
	}
	if err := c.resolveStrategy(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// resolveStrategy picks the chunking strategy: CTID when no chunk column is
// configured, otherwise batch-size (integer/float) or next-query.
func (c *Chunker) resolveStrategy(ctx context.Context) error {
	if c.chunkColumn == "" {
		c.strategy = strategyCTID
		return nil
	}
	st, err := QueryTable(ctx, c.db, c.schema, c.table)
	if err != nil {
		return fmt.Errorf("postgres: chunker: introspect %s: %w", c.qualifiedTable(), err)
	}
	i := st.FindColumn(c.chunkColumn)
	if i < 0 {
		return fmt.Errorf("postgres: chunker: chunkColumn %q not found in %s", c.chunkColumn, c.qualifiedTable())
	}
	switch strings.ToLower(st.Columns[i].DataType) {
	case "smallint", "integer", "bigint":
		c.chunkColumnKind = kindInt
		c.strategy = strategyBatch
	case "real", "double precision":
		c.chunkColumnKind = kindFloat
		c.strategy = strategyBatch
	default:
		c.strategy = strategyNext
	}
	return nil
}

// PK returns the primary key columns.
func (c *Chunker) PK() []string { return c.pk }

// Bounds returns the ordered chunk boundary keys for the resolved strategy.
// The whole computation is retried on a transient error.
func (c *Chunker) Bounds(ctx context.Context) ([][]any, error) {
	var bounds [][]any
	err := retryTransientErr(ctx, c.retries, func() error {
		var err error
		bounds, err = c.bounds(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return bounds, nil
}

func (c *Chunker) bounds(ctx context.Context) ([][]any, error) {
	if c.strategy == strategyCTID && !c.partitioned {
		return c.ctidBounds(ctx)
	}
	if c.partitioned {
		// A partitioned table must chunk by key so the coordinator can clip
		// each chunk to its worker's PK range.
		return c.keyBounds(ctx)
	}
	switch c.strategy {
	case strategyBatch:
		return c.batchBounds(ctx)
	default:
		return c.nextBounds(ctx)
	}
}

// keyBounds is the key-based bounds for a partitioned table: batch when the
// single-column key is numeric, next-query otherwise.
func (c *Chunker) keyBounds(ctx context.Context) ([][]any, error) {
	if len(c.pk) != 1 {
		return nil, fmt.Errorf("postgres: chunker: %s: partitioned chunking requires a single-column primary key", c.qualifiedTable())
	}
	st, err := QueryTable(ctx, c.db, c.schema, c.table)
	if err != nil {
		return nil, err
	}
	i := st.FindColumn(c.pk[0])
	if i < 0 {
		return nil, fmt.Errorf("postgres: chunker: key %q not found", c.pk[0])
	}
	c.chunkColumn = c.pk[0]
	switch strings.ToLower(st.Columns[i].DataType) {
	case "smallint", "integer", "bigint":
		c.chunkColumnKind = kindInt
		c.strategy = strategyBatch
	case "real", "double precision":
		c.chunkColumnKind = kindFloat
		c.strategy = strategyBatch
	default:
		c.strategy = strategyNext
	}
	if c.strategy == strategyBatch {
		return c.batchBounds(ctx)
	}
	return c.nextBounds(ctx)
}

// qualifiedTable is the quoted "schema"."table".
func (c *Chunker) qualifiedTable() string {
	return quoteIdent(c.schema) + "." + quoteIdent(c.table)
}

// selectList is the SELECT column list: the projection, or * for all.
func (c *Chunker) selectList() []string {
	if len(c.columns) == 0 {
		return []string{"*"}
	}
	return quotedIdents(c.columns)
}

// Scan executes the chunk SELECT and calls fn for every row, keyed by column
// name. Values decode through the same scalar mapping the pgoutput reader
// uses, so snapshot rows and stream rows land in Iceberg identically.
//
// The whole scan is retried on a transient error, covering a failure during
// rows.Next/Scan and not just the initial QueryContext. Rows are buffered
// until the scan completes, so a retry never re-delivers rows to fn.
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

// chunkQuery builds the chunk SELECT: the projection, the strategy's range
// predicate, and the structured filter, all ANDed.
func (c *Chunker) chunkQuery(ch source.Chunk) (sq.SelectBuilder, error) {
	q := psql.Select(c.selectList()...).From(c.qualifiedTable())
	switch {
	case c.strategy == strategyCTID && !c.partitioned:
		if ch.Low != nil {
			q = q.Where(ctidCompare(">=", ch.Low[0]))
		}
		if ch.High != nil {
			q = q.Where(ctidCompare("<", ch.High[0]))
		}
	default:
		col := quoteIdent(c.chunkColumn)
		if ch.Low != nil {
			q = q.Where(sq.GtOrEq{col: ch.Low[0]})
		}
		if ch.High != nil {
			q = q.Where(sq.Lt{col: ch.High[0]})
		}
		q = q.OrderBy(col)
	}
	if c.filter != nil {
		q = q.Where(c.filter)
	}
	return q, nil
}

// scanOnce runs one chunk SELECT, streaming normalized rows to fn.
func (c *Chunker) scanOnce(ctx context.Context, ch source.Chunk, fn func(row map[string]any) error) error {
	q, err := c.chunkQuery(ch)
	if err != nil {
		return err
	}
	query, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("postgres: chunk scan sql: %w", err)
	}

	// One REPEATABLE READ, READ ONLY transaction per chunk: the chunk sees a
	// single consistent snapshot even under concurrent writes, and the
	// read-only marker forbids any accidental write. The transaction is
	// rolled back on every error path and committed only after the rows are
	// fully read, so a retry re-runs the whole chunk on a fresh snapshot.
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("postgres: chunk scan tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, query, args...)
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
	if err != nil {
		return err
	}
	if cerr := tx.Commit(); cerr != nil {
		return fmt.Errorf("postgres: chunk scan commit: %w", cerr)
	}
	return nil
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

// quoteIdent quotes one identifier, doubling embedded quotes.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quotedIdents quotes each identifier.
func quotedIdents(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = quoteIdent(strings.TrimSpace(c))
	}
	return out
}

// quotedList joins the quoted identifiers with ", ".
func quotedList(cols []string) string {
	return strings.Join(quotedIdents(cols), ", ")
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

// numericStep is the batch strategy's value step for a chunk: the configured
// chunk size (rows) used as a value range. At least 1.
func (c *Chunker) numericStep() float64 {
	step := float64(c.chunkSize)
	if step < 1 {
		step = 1
	}
	return step
}
