package plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	clients ClientFunc
	alloc   memory.Allocator
	logger  *slog.Logger
	// server is the in-process Flight server this adapter owns (flightwrap);
	// nil for a subprocess plugin, whose supervisor owns the process.
	server ServerStopper
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup // tracks in-flight DoPut operations
}

// NewSinkAdapter creates a sink adapter over a connected Flight client. An
// optional ServerStopper is the in-process server flightwrap started, which
// Close stops (issue #496).
func NewSinkAdapter(clients ClientFunc, logger *slog.Logger, servers ...ServerStopper) *SinkAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	a := &SinkAdapter{
		clients: clients,
		alloc:   memory.NewGoAllocator(),
		logger:  logger,
	}
	if len(servers) > 0 {
		a.server = servers[0]
	}
	return a
}

// use resolves the adapter's current client, or nil when the plugin is down.
func (a *SinkAdapter) use() *client.Client {
	if a.clients == nil {
		return nil
	}
	return a.clients()
}

// EnsureTable hands the plugin sink the table's canonical schema, primary key
// and write mode, so it stores the real type shape and dedup key instead of
// re-inferring them from stringified rows (issue #569). A plugin that does not
// implement the action (an older binary) is left to manage its own schema.
func (a *SinkAdapter) EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, partitionBy []string, _ core.CastPolicy, mode dataplane.WriteMode) error {
	c := a.use()
	if c == nil {
		return errors.New("plugin sink: not connected")
	}
	modeStr := "upsert"
	if mode == dataplane.AppendMode {
		modeStr = "append"
	}
	err := c.EnsureTable(ctx, contract.EnsureTableRequest{
		Table:       ref.Target,
		Schema:      schema,
		PrimaryKey:  ref.PrimaryKey,
		PartitionBy: partitionBy,
		Mode:        modeStr,
	})
	if err != nil && status.Code(err) == codes.Unimplemented {
		a.logger.Warn("plugin sink does not implement urutau.ensure_table; schema not sent", "table", ref.Target)
		return nil
	}
	return err
}

// Writer returns a table writer that streams records via DoPut.
func (a *SinkAdapter) Writer(_ context.Context, ref core.TableRef, _ core.CastPolicy, _ []core.MetadataColumn) (sink.TableWriter, error) {
	return &sinkWriter{
		clients: a.clients,
		alloc:   a.alloc,
		table:   ref.Target,
		logger:  a.logger,
		wg:      &a.wg,
	}, nil
}

// Position reads the plugin sink's committed position for a table, so a
// restart resumes instead of re-reading from the beginning (issue #569). A
// plugin that does not implement the action, or one not connected yet, reads as
// empty.
func (a *SinkAdapter) Position(ctx context.Context, ref core.TableRef) (string, error) {
	c := a.use()
	if c == nil {
		return "", nil
	}
	pos, err := c.Position(ctx, ref.Target)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return "", nil
		}
		return "", err
	}
	return pos, nil
}

// SetProperties is a no-op for external plugins.
func (a *SinkAdapter) SetProperties(_ context.Context, _ core.TableRef, _ map[string]string) error {
	return nil
}

// Properties returns an empty map for external plugins.
func (a *SinkAdapter) Properties(_ context.Context, _ core.TableRef) (map[string]string, error) {
	return map[string]string{}, nil
}

// Close waits for in-flight operations, releases the Flight client, and stops
// the in-process server flightwrap started (issue #496).
func (a *SinkAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	a.wg.Wait()
	var err error
	if c := a.use(); c != nil {
		err = c.Close()
	}
	if a.server != nil {
		a.server.Stop()
	}
	return err
}

// sinkWriter implements sink.TableWriter over Flight DoPut.
type sinkWriter struct {
	clients ClientFunc
	alloc   memory.Allocator
	table   string
	logger  *slog.Logger
	mu      sync.Mutex
	closed  bool
	wg      *sync.WaitGroup
}

// use resolves the writer's current client (see SinkAdapter.use).
func (w *sinkWriter) use() *client.Client {
	if w.clients == nil {
		return nil
	}
	return w.clients()
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
	// Ref-counted Arrow buffers: every exit path — DoPut failure or Flush
	// failure included — must release them, or a slow sink leaks on the hot
	// write path (issue #497).
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	schema := records[0].Schema()
	desc := contract.DoPutRequest{
		Mode:  "write",
		Table: w.table,
	}

	c := w.use()
	if c == nil {
		return errors.New("plugin sink: not connected")
	}

	w.wg.Add(1)
	defer w.wg.Done()

	if err := c.DoPut(ctx, desc, schema, records); err != nil {
		return fmt.Errorf("doPut: %w", err)
	}

	// Flush to guarantee durability (contract §10).
	if err := c.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
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
		switch r.Op(i) {
		case rowchange.OpDelete:
			op = "d"
		case rowchange.OpUpdate:
			op = "u"
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
