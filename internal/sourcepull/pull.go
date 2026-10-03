// Package sourcepull adapts a push-based change channel into the pull-based
// source.Reader.Next surface, encoding buffered changes into wire batches.
//
// The built-in decoders emit rowchange.Change (binlog/JSON events are
// row-shaped — a private detail of the decoders); makeBatch buffers a few
// and encodes one wire RecordBatch via transport.RecordFromChanges. A
// source that can introspect its tables supplies the canonical schemas via
// SetSchemas so batches encode against a STABLE schema — never a per-batch
// inference that drifts when a drain happens to omit a sparse column.
// Without schemas, makeBatch falls back to inference for schema-less
// producers (their resolved schema is owned upstream).
package sourcepull

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

const batchTarget = 100

// maxBatchRows caps one emitted batch. A transaction-bounded decoder releases
// a whole transaction at once, so a multi-million-row transaction would
// otherwise become a single RecordBatch that exceeds the 128 MiB gRPC limit
// and loops forever. When a transaction is split, only its LAST piece carries
// the transaction's position; every earlier piece carries the last safe
// position, so acking it cannot free rows a later piece still owes.
const maxBatchRows = 2000

// Puller wraps a source's change channel and terminal-error channel into
// the pull-based Next surface. The concrete source calls Start to launch
// its decoder and wire the error channel.
type Puller struct {
	ch      <-chan rowchange.Change
	errCh   <-chan error
	buf     []rowchange.Change
	schemas map[string]core.Schema // target table -> canonical schema
	// index caches each target table's column-name -> position, so the drift
	// check reads a map instead of scanning the schema per row (#581).
	index map[string]map[string]int
	// txnBounded: the decoder ends each transaction with OpTxnEnd. buf
	// then holds only rows of ended transactions, and open the rows of
	// the one being decoded.
	txnBounded bool
	open       []rowchange.Change
	// safePos is the position of the last fully emitted transaction (or the
	// resume point before the first). A batch that splits a transaction is
	// stamped with it, so an ack of the partial piece does not advance the
	// durable position past rows still pending.
	safePos string
}

// New builds a puller over the decoder's change channel.
func New(ch <-chan rowchange.Change) *Puller {
	return &Puller{ch: ch}
}

// SetSchemas installs the canonical schema per target table so makeBatch
// encodes against a stable shape instead of per-drain inference. Call before
// Next.
func (p *Puller) SetSchemas(schemas map[string]core.Schema) {
	p.schemas = schemas
}

// SetSourceSchemas implements source.SchemaSetter for every stream that
// embeds a Puller: the engine hands over the resolved wire schema, and each
// column the source introspected as KindUnknown (an unsigned MySQL integer,
// say) takes its resolved kind, which only the declared cast knows. Without
// it such a column cannot be encoded ("unsupported canonical kind unknown").
// Every other column keeps the source's own type, and no column is added:
// the wire carries the source type, and the sink applies the cast.
func (p *Puller) SetSourceSchemas(resolved map[string]core.Schema) {
	for table, cs := range p.schemas {
		rs, ok := resolved[table]
		if !ok {
			continue
		}
		cols := make([]core.Column, len(cs.Columns))
		for i, c := range cs.Columns {
			if c.Type.Kind == core.KindUnknown {
				if rc, ok := rs.Column(c.Name); ok && rc.Type.Kind != core.KindUnknown {
					nullable := c.Type.Nullable
					c.Type = rc.Type
					c.Type.Nullable = nullable
				}
			}
			cols[i] = c
		}
		cs.Columns = cols
		p.schemas[table] = cs
	}
}

// BoundTransactions declares that the decoder ends every transaction with an
// OpTxnEnd change. Every row of a transaction carries its position, so the
// puller batches whole transactions and never splits one at a batch target it
// did not choose: a commit that recorded a transaction's position with part of
// its rows would let a resume skip the rest. A transaction larger than
// maxBatchRows is split anyway, and only its last piece then carries the
// transaction's position.
func (p *Puller) BoundTransactions() { p.txnBounded = true }

// SetResume records the position the stream resumes from, the safe position
// for a partial piece of the first transaction. Call before Next.
func (p *Puller) SetResume(pos string) { p.safePos = pos }

// take files one decoded change: a row joins the open transaction, and
// OpTxnEnd releases it to buf, grouped by table in order of first
// appearance so that no batch holds part of a table's rows of it.
func (p *Puller) take(c rowchange.Change) {
	if c.Op != rowchange.OpTxnEnd {
		p.open = append(p.open, c)
		return
	}
	for len(p.open) > 0 {
		table := p.open[0].Table
		rest := p.open[:0]
		for _, o := range p.open {
			if o.Table == table {
				p.buf = append(p.buf, o)
			} else {
				rest = append(rest, o)
			}
		}
		p.open = rest
	}
}

// nextTxn is Next for a transaction-bounded decoder. It batches only rows of
// ended transactions, blocking while none has ended, and closes a batch as
// Next does: at batchTarget rows, at a table change, or when nothing more is
// pending right now.
func (p *Puller) nextTxn(ctx context.Context) (*dataplane.Batch, error) {
	for {
		if len(p.buf) > 0 && (len(p.buf) >= batchTarget || p.buf[len(p.buf)-1].Table != p.buf[0].Table) {
			return p.makeBatch()
		}
		if len(p.buf) > 0 {
			select {
			case c, ok := <-p.ch:
				if !ok {
					return p.makeBatch()
				}
				p.take(c)
			case err := <-p.errCh:
				if err != nil {
					return nil, err
				}
				return p.makeBatch()
			default:
				return p.makeBatch()
			}
			continue
		}
		// A stream that ends inside a transaction leaves it unbatched:
		// it was never whole, and a resume re-reads it.
		select {
		case c, ok := <-p.ch:
			if !ok {
				return nil, nil
			}
			p.take(c)
		case err := <-p.errCh:
			if err != nil {
				return nil, err
			}
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// SetErr installs the decoder's terminal-error channel.
func (p *Puller) SetErr(errCh <-chan error) { p.errCh = errCh }

// Next reads accumulated changes and bridges them into one batch. Returns
// (nil, nil) at a clean stream end.
func (p *Puller) Next(ctx context.Context) (*dataplane.Batch, error) {
	if p.txnBounded {
		return p.nextTxn(ctx)
	}
	if len(p.buf) == 0 {
		select {
		case c, ok := <-p.ch:
			if !ok {
				return nil, nil
			}
			p.buf = append(p.buf, c)
		case err := <-p.errCh:
			if err != nil {
				return nil, err
			}
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Drain a few more non-blockingly to batch. A change of another table
	// ends the batch: it stays buffered and heads the next one.
	for len(p.buf) < batchTarget && p.buf[len(p.buf)-1].Table == p.buf[0].Table {
		select {
		case c, ok := <-p.ch:
			if !ok {
				return p.makeBatch()
			}
			p.buf = append(p.buf, c)
		case err := <-p.errCh:
			if err != nil {
				return nil, err
			}
			return p.makeBatch()
		default:
			return p.makeBatch()
		}
	}
	return p.makeBatch()
}

// Drain emits every batch of pending changes (buffered or still on the
// channel) without blocking for new data. Used by the runner's relay to
// flush decoded events ahead of a Closes marker.
func (p *Puller) Drain(ctx context.Context, emit func(*dataplane.Batch) error) error {
	for {
		b, ok, err := p.tryNext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := emit(b); err != nil {
			return err
		}
	}
}

// tryNext returns the next batch without blocking. ok=false means nothing
// buffered or pending right now.
func (p *Puller) tryNext(ctx context.Context) (*dataplane.Batch, bool, error) {
	if p.txnBounded {
		// Only ended transactions are pending; a transaction still open
		// waits for its end.
		for len(p.buf) == 0 {
			select {
			case c, ok := <-p.ch:
				if !ok {
					return nil, false, nil
				}
				p.take(c)
			case err := <-p.errCh:
				return nil, false, err
			default:
				return nil, false, nil
			}
		}
		b, err := p.makeBatch()
		return b, true, err
	}
	if len(p.buf) > 0 {
		b, err := p.makeBatch()
		return b, true, err
	}
	select {
	case c, ok := <-p.ch:
		if !ok {
			return nil, false, nil
		}
		p.buf = append(p.buf, c)
		b, err := p.makeBatch()
		return b, true, err
	case err := <-p.errCh:
		if err != nil {
			return nil, false, err
		}
		return nil, false, nil
	default:
		return nil, false, nil
	}
}

func (p *Puller) makeBatch() (*dataplane.Batch, error) {
	if len(p.buf) == 0 {
		return nil, nil
	}
	// A batch carries ONE table: encode only the leading run of buffered
	// changes of the same table and keep the rest for the next batch. A
	// backlog interleaves tables, and labelling the whole buffer with the
	// first change's table routed every other table's rows into it — silently
	// when the schemas match (issue #372).
	table := p.buf[0].Table
	n := 1
	for n < len(p.buf) && p.buf[n].Table == table {
		n++
	}
	if n > maxBatchRows {
		n = maxBatchRows
	}
	// The tail is advanced by a read index, not copied: only the run's own
	// elements are mutated (Position), and the tail is always p.buf[n:], so a
	// later append writes past the run's region.
	run, rest := p.buf[:n], p.buf[n:]
	p.buf = run
	defer func() { p.buf = rest }()

	// A split transaction: only its last piece may carry its commit position.
	// Every earlier piece carries the last safe position, so acking it cannot
	// free rows the remaining pieces still owe. A transaction's rows are
	// contiguous in the buffer (take groups each transaction at its end), so
	// the head of rest is enough to tell whether it continues.
	lastPos := run[len(run)-1].Position
	if len(rest) > 0 && rest[0].Position == lastPos {
		for i := range run {
			run[i].Position = p.safePos
		}
	} else if lastPos != "" {
		p.safePos = lastPos
	}

	// Drift check at the SOURCE boundary, where the native row shape
	// exists: encode against the canonical schema would silently DROP a
	// field the schema does not know (top-level OR nested inside a struct),
	// hiding source evolution from the downstream columnar worker. When the
	// source installed canonical schemas, compare each buffered row's shape
	// against it and fail loud — the operator declares the new column and
	// resumes, exactly like the old worker-side drift contract. The same
	// pass reports whether any row carries a key the schema lacks (including
	// a nil value, which MergeSchema would materialize): only then is the
	// merge — a whole extra pass over every row — needed (#581).
	cs, known := p.schemas[table]
	if known && len(cs.Columns) > 0 {
		index := p.colIndex(table, cs)
		merge := false
		for _, c := range p.buf {
			src := c.After
			if src == nil {
				src = c.Before
			}
			d, hit, unknown := driftAgainst(src, cs, index)
			if hit {
				return nil, fmt.Errorf("sourcepull: schema drift: column %q is not in the spec — declare it and resume", d)
			}
			if unknown {
				merge = true
			}
		}
		if merge {
			cs = transport.MergeSchema(p.buf, cs)
		}
	} else {
		// Known schema plus any column a change carries that the schema lacks
		// (schema-less producers, sparse rows). Empty cs → full inference.
		cs = transport.MergeSchema(p.buf, p.schemas[table])
	}

	// C-8: a delete with no PK becomes an orphaned NULL tuple in the sink.
	// The live CDC path carries deletes; the bridge used to guard this.
	if len(cs.PrimaryKey) == 0 {
		for _, c := range p.buf {
			if c.Op == rowchange.OpDelete {
				return nil, fmt.Errorf("sourcepull: batch %q carries a delete but the schema has no primary key — declare it and resume", table)
			}
		}
	}

	rec, err := transport.RecordFromChanges(p.buf, cs, nil)
	if err != nil {
		return nil, fmt.Errorf("sourcepull: encode batch: %w", err)
	}
	return &dataplane.Batch{Table: table, Record: rec, Mode: dataplane.UpsertMode}, nil
}

// colIndex returns (building it once) the column-name -> position index for a
// target table's canonical schema, so the drift check looks up a map instead
// of scanning the schema per row (#581).
func (p *Puller) colIndex(table string, cs core.Schema) map[string]int {
	if idx, ok := p.index[table]; ok {
		return idx
	}
	idx := make(map[string]int, len(cs.Columns))
	for i, c := range cs.Columns {
		idx[c.Name] = i
	}
	if p.index == nil {
		p.index = make(map[string]map[string]int)
	}
	p.index[table] = idx
	return idx
}

// driftAgainst reports the first column path a row carries that the schema
// lacks (descending into struct values so a field added inside a nested
// column is caught too), and whether the row carries any top-level key the
// schema lacks — a nil one included, since MergeSchema materializes it as a
// column even when its value is absent from this row.
func driftAgainst(row map[string]any, schema core.Schema, index map[string]int) (string, bool, bool) {
	unknown := false
	for name, v := range row {
		i, ok := index[name]
		if !ok {
			unknown = true
			if v == nil {
				continue
			}
			return name, true, unknown
		}
		if v == nil {
			continue
		}
		col := schema.Columns[i]
		if col.Type.Kind == core.KindStruct {
			if nested, isMap := v.(map[string]any); isMap {
				if path, hit := driftNested(name, nested, col.Type.Fields); hit {
					return path, true, unknown
				}
			}
		}
	}
	return "", false, unknown
}

func driftNested(path string, m map[string]any, fields []core.Column) (string, bool) {
	for name, v := range m {
		if v == nil {
			continue
		}
		full := path + "." + name
		var f *core.Column
		for i := range fields {
			if fields[i].Name == name {
				f = &fields[i]
				break
			}
		}
		if f == nil {
			return full, true
		}
		if f.Type.Kind == core.KindStruct {
			if nested, isMap := v.(map[string]any); isMap {
				if p2, hit := driftNested(full, nested, f.Type.Fields); hit {
					return p2, true
				}
			}
		}
	}
	return "", false
}
