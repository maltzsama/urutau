// Package sourcepull adapts a push-based change channel into the pull-based
// source.Reader.Next surface, bridging changes into columnar batches.
//
// The decoders emit rowchange.Change (binlog/JSON events are row-shaped);
// Next bridges them into wire batches. A source that can introspect its
// tables supplies the canonical schemas via SetSchemas so batches encode
// against a STABLE schema — never a per-batch inference that drifts when a
// drain happens to omit a sparse column. Without schemas, makeBatch falls
// back to inference for schema-less producers (their resolved schema is
// owned upstream; see quarantine plan G1).
package sourcepull

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

const batchTarget = 100

// Puller wraps a source's change channel and terminal-error channel into
// the pull-based Next surface. The concrete source calls Start to launch
// its decoder and wire the error channel.
type Puller struct {
	ch      <-chan rowchange.Change
	errCh   <-chan error
	buf     []rowchange.Change
	schemas map[string]core.Schema // target table -> canonical schema
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

// SetErr installs the decoder's terminal-error channel.
func (p *Puller) SetErr(errCh <-chan error) { p.errCh = errCh }

// Next reads accumulated changes and bridges them into one batch. Returns
// (nil, nil) at a clean stream end.
func (p *Puller) Next(ctx context.Context) (*dataplane.Batch, error) {
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
	// Drain a few more non-blockingly to batch.
	for len(p.buf) < batchTarget {
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
	// Drift check at the SOURCE boundary, where the native row shape
	// exists: encode against the canonical schema would silently DROP a
	// field the schema does not know (top-level OR nested inside a struct),
	// hiding source evolution from the downstream columnar worker. When the
	// source installed canonical schemas, compare each buffered row's shape
	// against it and fail loud — the operator declares the new column and
	// resumes, exactly like the old worker-side drift contract.
	if cs, ok := p.schemas[p.buf[0].Table]; ok && len(cs.Columns) > 0 {
		for _, c := range p.buf {
			src := c.After
			if src == nil {
				src = c.Before
			}
			if d, hit := driftAgainst(src, cs); hit {
				return nil, fmt.Errorf("sourcepull: schema drift: column %q is not in the spec — declare it and resume", d)
			}
		}
	}
	cb := rowchange.Batch{Table: p.buf[0].Table, Changes: p.buf, Mode: rowchange.UpsertMode}
	cs := p.schemas[p.buf[0].Table]
	dpb, err := dpint.BatchFromChangeBatch(cb, cs)
	p.buf = nil
	if err != nil {
		return nil, err
	}
	return dpb, nil
}

// driftAgainst reports the first column path a row carries that the schema
// lacks, descending into struct values so a field added inside a nested
// column is caught too.
func driftAgainst(row map[string]any, schema core.Schema) (string, bool) {
	for name, v := range row {
		if v == nil {
			continue
		}
		col, ok := schema.Column(name)
		if !ok {
			return name, true
		}
		if col.Type.Kind == core.KindStruct {
			if nested, isMap := v.(map[string]any); isMap {
				if path, hit := driftNested(name, nested, col.Type.Fields); hit {
					return path, true
				}
			}
		}
	}
	return "", false
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
