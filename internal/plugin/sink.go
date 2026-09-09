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
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/sink"
)

// SinkAdapter wraps a Flight client as a sink.Sink. It translates the
// internal rowchange.Batch into Arrow records and writes them via DoPut,
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
func (a *SinkAdapter) EnsureTable(ctx context.Context, ref core.TableRef, _ core.Schema, _ []string, _ core.CastPolicy, _ dataplane.WriteMode) error {
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

// Commit re-projects the batch's RecordBatch into the plugin's record
// shape (op + stringified columns + offset + ts_source) and sends it via
// DoPut, then calls Flush to guarantee durability (contract §10).
// The record is consumed column-oriented (BatchReader): no rowchange
// intermediate, no per-row maps.
func (w *sinkWriter) Commit(ctx context.Context, b *dataplane.Batch) error {
	reader, err := transport.NewBatchReader(b.Record, nil)
	if err != nil {
		return fmt.Errorf("plugin sink: %w", err)
	}
	records := recordsFromReader(reader, w.alloc)
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

// recordsFromReader re-projects a wire record into the plugin's record
// shape: op ("c"/"d"), every data column stringified (the plugin sink
// carries opaque strings), offset (the row's source coordinate), and
// ts_source (write instant). Column order follows the record's own schema
// order — deterministic, no map iteration.
func recordsFromReader(r *transport.BatchReader, alloc memory.Allocator) []arrow.RecordBatch {
	n := r.NumRows()
	if n == 0 {
		return nil
	}
	colNames := r.DataColumns()

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

	for i := range n {
		op := "c"
		if r.Op(i) == rowchange.OpDelete {
			op = "d"
		}
		bb.Field(0).(*array.StringBuilder).Append(op)
		for ci, name := range colNames {
			v, ok := r.Value(name, i)
			if !ok || v == nil {
				bb.Field(1 + ci).(*array.StringBuilder).AppendNull()
			} else {
				bb.Field(1 + ci).(*array.StringBuilder).Append(fmt.Sprintf("%v", v))
			}
		}
		bb.Field(len(colNames) + 1).(*array.BinaryBuilder).Append([]byte(r.Position(i)))
		bb.Field(len(colNames) + 2).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UTC().UnixMicro()))
	}

	rec := bb.NewRecordBatch()
	return []arrow.RecordBatch{rec}
}
