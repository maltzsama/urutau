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

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// batchTarget is the soft row ceiling a batch drains toward under backlog.
// The channel emptying (or a table change) closes it early, so latency is
// unaffected; the target only bounds how many tiny batches a sustained
// stream is chopped into (#579).
const batchTarget = 2000

// batchTargetBytes is the byte ceiling for one drained batch: a run of large
// rows (a 256 KiB payload, say) closes early instead of buffering up to
// batchTarget rows in memory (#579).
const batchTargetBytes = 4 << 20

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
//
// A puller is EITHER row-oriented (ch) or column-oriented (batchCh). A
// column-oriented source decodes straight into Arrow (transport.RowEncoder)
// and emits ready wire batches; the puller then forwards them unchanged, so
// no map[string]any ever exists on the live path (#455). The reader owns
// transaction bounding, the safe-position split and drift, because it sees
// the native row shape and the transaction boundary.
type Puller struct {
	columnar bool
	batchCh  <-chan *dataplane.Batch
	// onSchemas, when set, receives the resolved canonical schemas
	// SetSourceSchemas assigns; a columnar source uses it to (re)build its
	// encoders against the cast-resolved types before it starts decoding.
	onSchemas func(map[string]core.Schema)
	ch        <-chan rowchange.Change
	errCh     <-chan error
	buf       []rowchange.Change
	// bufBytes is the estimated wire size of buf, so a byte ceiling can close
	// a batch of large rows before it buffers the whole row target (#579).
	bufBytes int
	schemas  map[string]core.Schema // target table -> canonical schema
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

// NewColumnar builds a puller over a column-oriented source's batch channel.
// The source emits wire-schema *dataplane.Batch values (one table each,
// transaction-bounded and position-stamped); the puller forwards them.
func NewColumnar(ch <-chan *dataplane.Batch) *Puller {
	return &Puller{columnar: true, batchCh: ch}
}

// SetOnSchemas installs a callback that receives the resolved canonical
// schemas SetSourceSchemas assigns. A columnar source uses it to rebuild its
// encoders against the cast-resolved column types before decoding.
func (p *Puller) SetOnSchemas(fn func(map[string]core.Schema)) { p.onSchemas = fn }

// push buffers one change and accounts for its estimated wire size.
func (p *Puller) push(c rowchange.Change) {
	p.buf = append(p.buf, c)
	p.bufBytes += changeBytes(c)
}

// changeBytes estimates a change's encoded size: the image RecordFromChanges
// would write (After, or Before for a delete) plus the key and position.
func changeBytes(c rowchange.Change) int {
	n := len(c.Position) + 16
	for _, k := range c.Key {
		n += valueBytes(k)
	}
	src := c.After
	if src == nil {
		src = c.Before
	}
	for name, v := range src {
		n += len(name) + valueBytes(v)
	}
	return n
}

// valueBytes estimates one cell's size: a string's or byte slice's length,
// the sum of a composite's elements, eight bytes for any other scalar. JSON
// objects and lists reach here as map[string]any / []any and can be large, so
// they must not be counted as a scalar (#632 review).
func valueBytes(v any) int {
	switch t := v.(type) {
	case string:
		return len(t)
	case []byte:
		return len(t)
	case map[string]any:
		n := 0
		for k, e := range t {
			n += len(k) + valueBytes(e)
		}
		return n
	case []any:
		n := 0
		for _, e := range t {
			n += valueBytes(e)
		}
		return n
	default:
		return 8
	}
}

// SetSchemas installs the canonical schema per target table so makeBatch
// encodes against a stable shape instead of per-drain inference. Call before
// Next.
func (p *Puller) SetSchemas(schemas map[string]core.Schema) {
	p.schemas = schemas
	// The cached column indexes belong to the replaced schemas; a later
	// schema with reordered or removed columns must not reuse them (#581).
	p.index = nil
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
	if p.onSchemas != nil {
		p.onSchemas(p.schemas)
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
				p.push(o)
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
		if len(p.buf) > 0 && (len(p.buf) >= batchTarget || p.bufBytes >= batchTargetBytes || p.buf[len(p.buf)-1].Table != p.buf[0].Table) {
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

// nextColumnar returns the next ready batch from a column-oriented source. It
// blocks for one; (nil, nil) is a clean stream end.
func (p *Puller) nextColumnar(ctx context.Context) (*dataplane.Batch, error) {
	for {
		// Prefer a ready batch: a clean stream end must not drop the batches
		// still buffered on the channel.
		select {
		case b, ok := <-p.batchCh:
			if !ok {
				return nil, nil
			}
			if b == nil {
				continue
			}
			return b, nil
		default:
		}
		select {
		case b, ok := <-p.batchCh:
			if !ok {
				return nil, nil
			}
			if b == nil {
				continue
			}
			return b, nil
		case err := <-p.errCh:
			if err != nil {
				return nil, err
			}
			// Terminal nil: hand back one last buffered batch, if any.
			select {
			case b, ok := <-p.batchCh:
				if ok && b != nil {
					return b, nil
				}
			default:
			}
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Next reads accumulated changes and bridges them into one batch. Returns
// (nil, nil) at a clean stream end.
func (p *Puller) Next(ctx context.Context) (*dataplane.Batch, error) {
	if p.columnar {
		return p.nextColumnar(ctx)
	}
	if p.txnBounded {
		return p.nextTxn(ctx)
	}
	if len(p.buf) == 0 {
		select {
		case c, ok := <-p.ch:
			if !ok {
				return nil, nil
			}
			p.push(c)
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
	for len(p.buf) < batchTarget && p.bufBytes < batchTargetBytes && p.buf[len(p.buf)-1].Table == p.buf[0].Table {
		select {
		case c, ok := <-p.ch:
			if !ok {
				return p.makeBatch()
			}
			p.push(c)
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
	if p.columnar {
		for {
			select {
			case b, ok := <-p.batchCh:
				if !ok {
					return nil
				}
				if b == nil {
					continue
				}
				if err := emit(b); err != nil {
					return err
				}
			case err := <-p.errCh:
				if err != nil {
					return err
				}
				return nil
			default:
				return nil
			}
		}
	}
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
		p.push(c)
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
