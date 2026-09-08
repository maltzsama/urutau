package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/sink"
)

// SinkAdapter wraps a Flight client as a sink.Sink. It translates the
// internal change.Batch into Arrow records and writes them via DoPut,
// then calls Flush to guarantee durability.
type SinkAdapter struct {
	client *client.Client
	alloc  memory.Allocator
	logger *slog.Logger
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup // tracks in-flight DoPut operations
}

// NewSinkAdapter creates a sink adapter over a connected Flight client.
func NewSinkAdapter(c *client.Client, logger *slog.Logger) *SinkAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &SinkAdapter{
		client: c,
		alloc:  memory.NewGoAllocator(),
		logger: logger,
	}
}

// EnsureTable delegates to the plugin via Flight action. The plugin
// manages its own schema internally.
func (a *SinkAdapter) EnsureTable(ctx context.Context, ref core.TableRef, _ core.Schema, _ []string, _ core.CastPolicy, _ change.WriteMode) error {
	// No-op for now — the plugin handles its own schema.
	return nil
}

// Writer returns a table writer that streams records via DoPut.
func (a *SinkAdapter) Writer(_ context.Context, ref core.TableRef, _ core.CastPolicy, _ []core.MetadataColumn) (sink.TableWriter, error) {
	return &sinkWriter{
		client: a.client,
		alloc:  a.alloc,
		table:  ref.Target,
		logger: a.logger,
		wg:     &a.wg,
	}, nil
}

// Position returns empty — external plugins manage their own positions.
func (a *SinkAdapter) Position(_ context.Context, _ core.TableRef) (string, error) {
	return "", nil
}

// SetProperties is a no-op for external plugins.
func (a *SinkAdapter) SetProperties(_ context.Context, _ core.TableRef, _ map[string]string) error {
	return nil
}

// Properties returns an empty map for external plugins.
func (a *SinkAdapter) Properties(_ context.Context, _ core.TableRef) (map[string]string, error) {
	return map[string]string{}, nil
}

// Close waits for in-flight operations and releases the Flight client.
func (a *SinkAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	a.wg.Wait()
	return a.client.Close()
}

// sinkWriter implements sink.TableWriter over Flight DoPut.
type sinkWriter struct {
	client *client.Client
	alloc  memory.Allocator
	table  string
	logger *slog.Logger
	mu     sync.Mutex
	closed bool
	wg     *sync.WaitGroup
}

// Commit converts the change.Batch into Arrow records and sends them via
// DoPut, then calls Flush to guarantee durability (contract §10).
func (w *sinkWriter) Commit(ctx context.Context, b *dataplane.Batch) error {
	// Unpack: decode RecordBatch back to changes for the existing writer.
	// QUARANTINE: this bridge dies when the plugin sink consumes RecordBatch directly.
	cb, err := w.unpackBatch(b)
	if err != nil {
		return fmt.Errorf("plugin sink: unpack: %w", err)
	}
	records := batchToRecords(cb, w.alloc)
	if len(records) == 0 {
		return nil
	}

	schema := records[0].Schema()
	desc := contract.DoPutRequest{
		Mode:  "write",
		Table: w.table,
	}

	w.wg.Add(1)
	defer w.wg.Done()

	if err := w.client.DoPut(ctx, desc, schema, records); err != nil {
		for _, r := range records {
			r.Release()
		}
		return fmt.Errorf("doPut: %w", err)
	}

	// Flush to guarantee durability (contract §10).
	if err := w.client.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	for _, r := range records {
		r.Release()
	}
	return nil
}

// Close is a no-op; the writer has no state to release.
func (w *sinkWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

// batchToRecords converts a change.Batch into a single Arrow record batch
// with typed columns for efficiency.
func batchToRecords(b change.Batch, alloc memory.Allocator) []arrow.RecordBatch {
	total := len(b.Upserts) + len(b.Deletes)
	if total == 0 {
		return nil
	}

	colNames := collectColumns(b)
	if len(colNames) == 0 {
		return nil
	}

	// Build Arrow schema: op + columns + offset + ts_source.
	fields := make([]arrow.Field, 0, len(colNames)+3)
	fields = append(fields, arrow.Field{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false})
	for _, name := range colNames {
		fields = append(fields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true})
	}
	fields = append(fields, arrow.Field{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false})
	fields = append(fields, arrow.Field{Name: "ts_source", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true})
	arrowSchema := arrow.NewSchema(fields, nil)

	bb := array.NewRecordBuilder(alloc, arrowSchema)
	defer bb.Release()

	for _, u := range b.Upserts {
		appendChange(bb, "c", u, colNames, b.Position)
	}
	for _, d := range b.Deletes {
		appendChange(bb, "d", d, colNames, b.Position)
	}

	rec := bb.NewRecordBatch()
	return []arrow.RecordBatch{rec}
}

func appendChange(bb *array.RecordBuilder, op string, chg change.Change, colNames []string, position string) {
	bb.Field(0).(*array.StringBuilder).Append(op)
	for i, name := range colNames {
		val := resolveColumn(chg, name)
		if val == nil {
			bb.Field(1 + i).(*array.StringBuilder).AppendNull()
		} else {
			bb.Field(1 + i).(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
		}
	}
	bb.Field(len(colNames) + 1).(*array.BinaryBuilder).Append([]byte(position))
	bb.Field(len(colNames) + 2).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UTC().UnixMicro()))
}

func resolveColumn(chg change.Change, name string) any {
	if chg.After != nil {
		if v, ok := chg.After[name]; ok {
			return v
		}
	}
	if chg.Before != nil {
		if v, ok := chg.Before[name]; ok {
			return v
		}
	}
	return nil
}

func collectColumns(b change.Batch) []string {
	seen := make(map[string]bool)
	for _, u := range b.Upserts {
		for k := range u.After {
			seen[k] = true
		}
	}
	for _, d := range b.Deletes {
		for k := range d.Before {
			seen[k] = true
		}
	}
	cols := make([]string, 0, len(seen))
	for k := range seen {
		cols = append(cols, k)
	}
	return cols
}

// unpackBatch converts a columnar dataplane.Batch back to a row-oriented
// change.Batch. QUARANTINE: dies when the plugin sink consumes RecordBatch
// directly.
func (w *sinkWriter) unpackBatch(b *dataplane.Batch) (change.Batch, error) {
	if b.Record == nil || b.Record.NumRows() == 0 {
		return change.Batch{Table: b.Table, Position: string(b.Watermark)}, nil
	}
	rows, _, err := transport.DecodeBatch(b.Record, nil, nil)
	if err != nil {
		return change.Batch{}, err
	}
	var upserts, deletes []change.Change
	for _, r := range rows {
		switch r.Op {
		case change.OpDelete:
			deletes = append(deletes, r)
		default:
			upserts = append(upserts, r)
		}
	}
	return change.Batch{
		Table:    b.Table,
		Upserts:  upserts,
		Deletes:  deletes,
		Position: string(b.Watermark),
	}, nil
}
