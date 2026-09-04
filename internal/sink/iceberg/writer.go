package iceberg

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
)

// ErrCommitExhausted marks a terminal commit failure: retries against the
// catalog are spent and the caller (the worker) must die rather than skip
// the batch — skipping would advance the position past uncommitted data.
var ErrCommitExhausted = errors.New("iceberg: commit retries exhausted")

// TableWriter owns the write path of a single target table.
//
// Commit applies a collapsed batch as delete-then-append — two snapshots —
// per the spike finding: in iceberg-go an append and an equality
// delete staged together produce two snapshots anyway, and the delete wins,
// so the delete must land BEFORE the fresh rows. The position is written
// ONLY on the final commit of the batch: append when upserts exist, delete
// when the batch is delete-only. This prevents advancing the position past
// uncommitted data on a crash between the two snapshots.
//
// A writer is NOT safe for concurrent use: commits must be serialized by the
// caller (the worker dedicates one committer goroutine per table, which is
// the design's serialization invariant).
type TableWriter struct {
	cat             *rest.Catalog
	ident           table.Identifier
	eqIDs           []int
	dataSchema      *arrow.Schema
	delSchema       *arrow.Schema
	delCols         []string
	maxTries        int
	backoff         time.Duration
	cast            core.CastPolicy
	metaByName      map[string]core.MetadataColumn
	sourceTable     string
	recordBatchSize int64
}

// NewTableWriter loads the table, resolves the equality-delete key (the
// primary key) and precomputes the arrow schemas for data and delete rows.
// The cast policy is applied to source column values during projection; the
// metadata columns are projected from the change header.
func NewTableWriter(ctx context.Context, cat *rest.Catalog, ident table.Identifier, primaryKey []string, cast core.CastPolicy, meta []core.MetadataColumn, sourceTable string) (*TableWriter, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("iceberg: load %v: %w", ident, err)
	}
	ischema := tbl.Schema()

	eqIDs := make([]int, 0, len(primaryKey))
	for _, name := range primaryKey {
		f, ok := ischema.FindFieldByName(name)
		if !ok {
			return nil, fmt.Errorf("iceberg: %v: primary key column %q not found", ident, name)
		}
		eqIDs = append(eqIDs, f.ID)
	}

	dataSchema, err := table.SchemaToArrowSchema(ischema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("iceberg: data schema: %w", err)
	}
	delISchema, err := ischema.Select(true, primaryKey...)
	if err != nil {
		return nil, fmt.Errorf("iceberg: delete schema: %w", err)
	}
	delSchema, err := table.SchemaToArrowSchema(delISchema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("iceberg: delete schema: %w", err)
	}

	metaByName := make(map[string]core.MetadataColumn, len(meta))
	for _, m := range meta {
		metaByName[m.As] = m
	}

	return &TableWriter{
		cat:             cat,
		ident:           ident,
		eqIDs:           eqIDs,
		dataSchema:      dataSchema,
		delSchema:       delSchema,
		delCols:         primaryKey,
		maxTries:        5,
		backoff:         200 * time.Millisecond,
		cast:            cast,
		metaByName:      metaByName,
		sourceTable:     sourceTable,
		recordBatchSize: 64 * 1024, // 64k rows per Arrow record batch
	}, nil
}

// Close releases writer resources. The Iceberg writer holds none per-call
// (it reloads metadata on every commit), so this is a no-op — present to
// satisfy sink.TableWriter.
func (w *TableWriter) Close() error { return nil }

// Commit applies the batch: snapshot one equality-deletes every key in it
// (upserts delete their older versions, deletes are the last word), snapshot
// two appends the surviving rows. The position is written only on the final
// commit the batch actually executes: when there are upserts, the append is
// last and carries the position; when the batch is delete-only, the delete
// commit carries it. This prevents advancing the position past uncommitted
// data on a crash between the two snapshots.
//
// In append mode the equality delete is skipped: every change becomes a row.
//
// CRASH SAFETY: a crash between commitDeletes and commitAppend leaves the
// batch temporarily absent (old rows deleted, new rows not yet written) but
// the position has not advanced. Resume reprocesses the batch: deletes are
// idempotent, appends rewrite. Converges without loss.
func (w *TableWriter) Commit(ctx context.Context, b change.Batch) error {
	hasUpserts := len(b.Upserts) > 0
	if b.Mode == change.UpsertMode {
		keys := append(collectKeys(b.Upserts), collectKeys(b.Deletes)...)
		if len(keys) > 0 {
			// Position goes on delete only when it IS the last commit.
			pos := ""
			if !hasUpserts {
				pos = b.Position
			}
			if err := w.commitDeletes(ctx, keys, pos); err != nil {
				return err
			}
		}
	}
	if hasUpserts {
		if err := w.commitAppend(ctx, b.Upserts, b.Position); err != nil {
			return err
		}
	}
	return nil
}

func (w *TableWriter) commitDeletes(ctx context.Context, keys [][]any, pos string) error {
	rec, err := w.deleteRecord(keys)
	if err != nil {
		return err
	}
	defer rec.Release()

	var lastErr error
	for attempt := 0; attempt < w.maxTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(w.backoff, attempt)); err != nil {
				return err
			}
		}
		tbl, err := w.cat.LoadTable(ctx, w.ident) // fresh metadata every attempt
		if err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		txn := tbl.NewTransaction()
		files, err := txn.WriteEqualityDeletes(ctx, w.eqIDs, oneBatch(rec))
		if err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		if len(files) == 0 {
			return nil
		}
		if err := txn.NewRowDelta(props(pos)).AddDeletes(files...).Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		if pos != "" {
			if err := txn.SetProperties(props(pos)); err != nil {
				return err
			}
		}
		if _, err := txn.Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("%w: delete commit on %v: %v", ErrCommitExhausted, w.ident, lastErr)
}

func (w *TableWriter) commitAppend(ctx context.Context, upserts []change.Change, pos string) error {
	rec, err := w.dataRecord(upserts)
	if err != nil {
		return err
	}
	defer rec.Release()
	at := array.NewTableFromRecords(rec.Schema(), []arrow.RecordBatch{rec})
	defer at.Release()

	var lastErr error
	for attempt := 0; attempt < w.maxTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(w.backoff, attempt)); err != nil {
				return err
			}
		}
		tbl, err := w.cat.LoadTable(ctx, w.ident)
		if err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		txn := tbl.NewTransaction()
		if err := txn.AppendTable(ctx, at, w.recordBatchSize, props(pos)); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		if err := txn.SetProperties(props(pos)); err != nil {
			return err
		}
		if _, err := txn.Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("%w: append commit on %v: %v", ErrCommitExhausted, w.ident, lastErr)
}

// CommittedPosition reads the committed cdc.position of a table. The fast
// path is the table property written atomically with every commit; when a
// third-party maintenance job replaced the current snapshot (compaction
// produces a replace without the property), the walk-back over snapshot
// summaries is the fallback — defense, not coupling (design §2.2). Empty
// string means the table has never committed (snapshot needed).
func CommittedPosition(ctx context.Context, cat *rest.Catalog, ident table.Identifier) (string, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return "", fmt.Errorf("iceberg: load %v: %w", ident, err)
	}
	return committedPosition(tbl), nil
}

// committedPosition implements the property-first, walk-back fallback over
// a loaded table's metadata. Split out for unit testing without a live
// catalog.
func committedPosition(tbl *table.Table) string {
	return walkBackPosition(tbl.Properties(), tbl.Metadata().Snapshots())
}

// walkBackPosition finds cdc.position in the table properties, else walks
// the snapshot summaries newest-first (Snapshots() is newest-first). Pure,
// for tests.
func walkBackPosition(props iceberg.Properties, snaps []table.Snapshot) string {
	if pos := props["cdc.position"]; pos != "" {
		return pos
	}
	for i := range snaps {
		if snaps[i].Summary == nil {
			continue
		}
		if pos := snaps[i].Summary.Properties["cdc.position"]; pos != "" {
			return pos
		}
	}
	return ""
}

// EnsureTable creates the target table if absent. When the table already
// exists, every column touched by a cast rule must match the resolved type:
// if the existing column's Iceberg type disagrees, a hard error prevents
// silent data corruption. The partition spec is applied on create and
// verified for divergence on existing tables.
func EnsureTable(ctx context.Context, cat *rest.Catalog, ident table.Identifier, schema *iceberg.Schema, partitionBy []string, cast core.CastPolicy) error {
	existing, err := cat.LoadTable(ctx, ident)
	if err != nil {
		// Table does not exist — create.
		opts := []catalog.CreateTableOpt{
			catalog.WithProperties(iceberg.Properties{"format-version": "2"}),
		}
		if len(partitionBy) > 0 {
			spec, err := buildPartitionSpec(schema, partitionBy)
			if err != nil {
				return fmt.Errorf("iceberg: %v: partition spec: %w", ident, err)
			}
			opts = append(opts, catalog.WithPartitionSpec(&spec))
		}
		_, err = cat.CreateTable(ctx, ident, schema, opts...)
		if err != nil {
			return fmt.Errorf("iceberg: create %v: %w", ident, err)
		}
		return nil
	}
	// Table exists — verify cast divergence and partition spec divergence.
	existSchema := existing.Schema()
	for colName, target := range cast.Columns {
		newField, ok := schema.FindFieldByName(colName)
		if !ok {
			continue // column not in resolved schema — introspection will catch
		}
		existField, ok := existSchema.FindFieldByName(colName)
		if !ok {
			return fmt.Errorf("iceberg: %v: cast column %q not in existing table", ident, colName)
		}
		if !newField.Type.Equals(existField.Type) {
			return fmt.Errorf("iceberg: %v: cast column %q type divergence: spec wants %s, table has %s",
				ident, colName, newField.Type, existField.Type)
		}
		_ = target // used for future PK-cast advisory
	}
	// Partition spec divergence: if partitionBy is specified, it must
	// match the existing table's partition spec (single-partition-field
	// identity/transform match).
	if len(partitionBy) > 0 {
		wantSpec, err := buildPartitionSpec(schema, partitionBy)
		if err != nil {
			return fmt.Errorf("iceberg: %v: partition spec: %w", ident, err)
		}
		existSpec := existing.Spec()
		if !wantSpec.CompatibleWith(&existSpec) {
			return fmt.Errorf("iceberg: %v: partition spec divergence: spec wants %v, table has %v — partition evolution is a deliberate maintenance operation",
				ident, wantSpec, existSpec)
		}
	}
	return nil
}

func props(pos string) iceberg.Properties {
	if pos == "" {
		return iceberg.Properties{}
	}
	return iceberg.Properties{"cdc.position": pos}
}

func collectKeys(cs []change.Change) [][]any {
	keys := make([][]any, 0, len(cs))
	for _, c := range cs {
		keys = append(keys, c.Key)
	}
	return keys
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// backoffDuration computes exponential backoff with jitter: base * 2^attempt
// clamped to 30s, plus 0-25% jitter. Jitter prevents synchronized retries
// across N workers hitting the same table.
func backoffDuration(base time.Duration, attempt int) time.Duration {
	d := base * (1 << min(attempt, 7)) // cap at 2^7 = 128x
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	// Add 0-25% jitter.
	jitter := time.Duration(rand.Int64N(int64(d) / 4))
	return d + jitter
}

// isRetryableError classifies an error as transient (retryable) or
// terminal. I/O and HTTP 5xx errors from the object store or catalog
// are transient; schema, type, and 4xx errors are terminal.
func isRetryableError(err error) bool {
	if errors.Is(err, table.ErrCommitFailed) {
		return true
	}
	s := err.Error()
	// HTTP 5xx or network errors from REST catalog or S3.
	if strings.Contains(s, "500") || strings.Contains(s, "502") ||
		strings.Contains(s, "503") || strings.Contains(s, "504") ||
		strings.Contains(s, "connection refused") || strings.Contains(s, "EOF") ||
		strings.Contains(s, "i/o timeout") || strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "reset by peer") {
		return true
	}
	return false
}

// deleteRecord builds one arrow record with the primary key tuples.
func (w *TableWriter) deleteRecord(keys [][]any) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, w.delSchema)
	defer b.Release()
	for i, col := range w.delCols {
		values := make([]any, len(keys))
		for j, k := range keys {
			if len(k) != len(w.delCols) {
				return nil, fmt.Errorf("iceberg: key arity %d != primary key arity %d", len(k), len(w.delCols))
			}
			values[j] = k[i]
		}
		if err := appendColumn(b.Field(i), col, values); err != nil {
			return nil, err
		}
	}
	return b.NewRecordBatch(), nil
}

// dataRecord builds one arrow record from the surviving rows.
func (w *TableWriter) dataRecord(upserts []change.Change) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, w.dataSchema)
	defer b.Release()
	for i, field := range w.dataSchema.Fields() {
		values := make([]any, len(upserts))
		for j, c := range upserts {
			proj, err := w.project(c)
			if err != nil {
				return nil, err
			}
			values[j] = proj[field.Name]
		}
		if err := appendColumn(b.Field(i), field.Name, values); err != nil {
			return nil, err
		}
	}
	return b.NewRecordBatch(), nil
}

// project resolves a change's source columns (applying casts) and metadata
// columns into a flat map matching the dataSchema field names.
func (w *TableWriter) project(c change.Change) (map[string]any, error) {
	out := make(map[string]any, len(w.dataSchema.Fields()))
	for _, f := range w.dataSchema.Fields() {
		if m, ok := w.metaByName[f.Name]; ok {
			v, err := metaValue(m.From, c, w.sourceTable)
			if err != nil {
				return nil, fmt.Errorf("iceberg: metadata %q: %w", f.Name, err)
			}
			out[f.Name] = v
			continue
		}
		v, ok := c.After[f.Name]
		if !ok {
			out[f.Name] = nil
			continue
		}
		if ct, ok := w.cast.Target(f.Name); ok {
			cv, err := ct.Convert(v)
			if err != nil {
				return nil, fmt.Errorf("iceberg: column %q: %w", f.Name, err)
			}
			v = cv
		}
		out[f.Name] = v
	}
	return out, nil
}

// metaValue resolves one metadata key to its concrete value for a change.
func metaValue(key core.MetadataKey, c change.Change, sourceTable string) (any, error) {
	switch key {
	case core.MetaOp:
		return c.Op.String(), nil
	case core.MetaCommitTS:
		if c.CommitTS.IsZero() {
			return nil, nil
		}
		return c.CommitTS, nil
	case core.MetaIngestTS:
		return c.IngestTS, nil
	case core.MetaPosition:
		if c.Position == "" {
			return nil, nil
		}
		return c.Position, nil
	case core.MetaSourceTable:
		return sourceTable, nil
	case core.MetaPhase:
		if c.Snapshot {
			return "snapshot", nil
		}
		return "stream", nil
	default:
		return nil, fmt.Errorf("unknown metadata key %q", key)
	}
}

// appendColumn appends values into a builder, tolerating a small, explicit
// set of scalar types. Numeric coercion is schema-directed, not silent: the
// Iceberg column type is authoritative, and JSON-based wire formats cannot
// distinguish whole floats from ints, so int64 may arrive for a double
// column and vice versa. Temporal, decimal, uuid and json values arrive as
// their canonical text and are parsed at the column boundary.
func appendColumn(builder array.Builder, name string, values []any) error {
	switch b := builder.(type) {
	case *array.Int64Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case int64:
				b.Append(t)
			case int:
				b.Append(int64(t))
			case int32:
				b.Append(int64(t))
			case float64:
				if t != float64(int64(t)) {
					return fmt.Errorf("iceberg: column %q: cannot append %v as int64", name, v)
				}
				b.Append(int64(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as int64", name, v)
			}
		}
	case *array.Int32Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case int32:
				b.Append(t)
			case int:
				b.Append(int32(t))
			case int64:
				b.Append(int32(t))
			case float64:
				if t != float64(int64(t)) {
					return fmt.Errorf("iceberg: column %q: cannot append %v as int32", name, v)
				}
				b.Append(int32(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as int32", name, v)
			}
		}
	case *array.Float32Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case float32:
				b.Append(t)
			case float64:
				b.Append(float32(t))
			case int64:
				b.Append(float32(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as float32", name, v)
			}
		}
	case *array.StringBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				b.Append(t)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as string", name, v)
			}
		}
	case *array.Float64Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case float64:
				b.Append(t)
			case float32:
				b.Append(float64(t))
			case int64:
				b.Append(float64(t))
			case int:
				b.Append(float64(t))
			case int32:
				b.Append(float64(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as double", name, v)
			}
		}
	case *array.BooleanBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case bool:
				b.Append(t)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as boolean", name, v)
			}
		}
	case *array.Decimal128Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				if err := b.AppendValueFromString(t); err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as decimal", name, v)
			}
		}
	case *array.Date32Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				days, err := dateToDays(t)
				if err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
				b.Append(arrow.Date32(days))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as date", name, v)
			}
		}
	case *array.Time64Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				micros, err := timeToMicros(t)
				if err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
				b.Append(arrow.Time64(micros))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as time", name, v)
			}
		}
	case *array.TimestampBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case time.Time:
				b.AppendTime(t)
			case string:
				tm, err := parseTimestampText(t)
				if err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
				b.AppendTime(tm)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as timestamp", name, v)
			}
		}
	case *array.FixedSizeBinaryBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				raw, err := uuidToBytes(t)
				if err != nil {
					return fmt.Errorf("iceberg: column %q: %w", name, err)
				}
				b.Append(raw)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as uuid", name, v)
			}
		}
	case *array.BinaryBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case []byte:
				b.Append(t)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as binary", name, v)
			}
		}
	default:
		return fmt.Errorf("iceberg: column %q: unsupported builder %T", name, builder)
	}
	return nil
}

// ── canonical text → arrow value parsing ─────────────────────────────

// dateToDays parses a "2006-01-02" date into days since the epoch.
func dateToDays(s string) (int32, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return 0, fmt.Errorf("date %q: %w", s, err)
	}
	return int32(t.Unix() / 86400), nil
}

var timeLayouts = []string{"15:04:05.999999999", "15:04:05.999999", "15:04:05"}

// timeToMicros parses a time-of-day text into microseconds since midnight.
func timeToMicros(s string) (int64, error) {
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return int64(t.Hour())*3600_000_000 + int64(t.Minute())*60_000_000 +
				int64(t.Second())*1_000_000 + int64(t.Nanosecond())/1000, nil
		}
	}
	return 0, fmt.Errorf("time %q: not a valid time of day", s)
}

var tsLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
}

// parseTimestampText parses a naive or RFC3339 timestamp. Naive values are
// anchored at UTC so the wall clock survives round-tripping.
func parseTimestampText(s string) (time.Time, error) {
	for _, layout := range tsLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("timestamp %q: not a valid timestamp", s)
}

// uuidToBytes parses a hyphenated uuid text into its 16 raw bytes.
func uuidToBytes(s string) ([]byte, error) {
	compact := strings.ReplaceAll(strings.ToLower(s), "-", "")
	raw, err := hex.DecodeString(compact)
	if err != nil || len(raw) != 16 {
		return nil, fmt.Errorf("uuid %q: not a valid uuid", s)
	}
	return raw, nil
}

// ── partition spec builder ──────────────────────────────────────────

// partitionExprRe matches the closed grammar of partition expressions:
//
//	transform(col)        — temporal transforms
//	bucket(N, col)        — hash bucketing
//	truncate(N, col)      — string truncation
//	identity(col)         — passthrough (explicit)
//
// Unknown transforms or malformed expressions are rejected at spec
// validation time, not in the writer.
var partitionExprRe = regexp.MustCompile(
	`^(?:(day|month|year|hour|identity)\((\w+)\)|(bucket|truncate)\((\d+),\s*(\w+)\))$`,
)

// buildPartitionSpec translates the closed catalog of partitionBy
// expressions into an Iceberg PartitionSpec validated against the schema.
// Only the transforms listed in the CR-016 closed catalog are accepted:
// day, month, year, hour, bucket, truncate, identity.  Arbitrary expressions
// remain out of scope.
func buildPartitionSpec(schema *iceberg.Schema, exprs []string) (iceberg.PartitionSpec, error) {
	if len(exprs) == 0 {
		return *iceberg.UnpartitionedSpec, nil
	}

	var fieldID int
	opts := make([]iceberg.PartitionOption, 0, len(exprs))
	for _, expr := range exprs {
		m := partitionExprRe.FindStringSubmatch(strings.TrimSpace(expr))
		if m == nil {
			return iceberg.PartitionSpec{}, fmt.Errorf("invalid partition expression %q: "+
				"want transform(col), bucket(N, col), or truncate(N, col)", expr)
		}

		var (
			transform iceberg.Transform
			col       string
			target    string
		)

		switch {
		case m[1] != "":
			// Temporal transform: day/month/year/hour(col)
			col = m[2]
			target = m[1] + "_" + col
			switch m[1] {
			case "day":
				transform = iceberg.DayTransform{}
			case "month":
				transform = iceberg.MonthTransform{}
			case "year":
				transform = iceberg.YearTransform{}
			case "hour":
				transform = iceberg.HourTransform{}
			case "identity":
				transform = iceberg.IdentityTransform{}
			}
		case m[3] != "":
			// bucket(N, col) or truncate(N, col)
			n, err := strconv.Atoi(m[4])
			if err != nil || n <= 0 {
				return iceberg.PartitionSpec{}, fmt.Errorf("partition %q: bucket/truncate size must be > 0", expr)
			}
			col = m[5]
			switch m[3] {
			case "bucket":
				target = fmt.Sprintf("bucket_%d_%s", n, col)
				transform = iceberg.BucketTransform{NumBuckets: n}
			case "truncate":
				target = fmt.Sprintf("trunc_%d_%s", n, col)
				transform = iceberg.TruncateTransform{Width: n}
			}
		}

		opts = append(opts, iceberg.AddPartitionFieldByName(col, target, transform, schema, &fieldID))
	}

	return iceberg.NewPartitionSpecOpts(opts...)
}
