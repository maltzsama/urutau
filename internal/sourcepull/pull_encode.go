package sourcepull

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

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
	runBytes := changeBytes(p.buf[0])
	for n < len(p.buf) && n < maxBatchRows && p.buf[n].Table == table {
		b := changeBytes(p.buf[n])
		if runBytes+b > batchTargetBytes {
			break
		}
		runBytes += b
		n++
	}
	// The tail is advanced by a read index, not copied: only the run's own
	// elements are mutated (Position), and the tail is always p.buf[n:], so a
	// later append writes past the run's region.
	run, rest := p.buf[:n], p.buf[n:]
	restBytes := p.bufBytes - runBytes
	if restBytes < 0 {
		restBytes = 0
	}
	p.buf = run
	p.bufBytes = runBytes
	defer func() {
		// Drop the run's references before restoring the tail: the tail keeps
		// the backing array alive, so without this its [0:n) elements would
		// retain the encoded batch's row maps until the array is replaced
		// (Sourcery finding).
		for i := range run {
			run[i] = rowchange.Change{}
		}
		p.buf = rest
		p.bufBytes = restBytes
	}()

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
	// exists (#581); the shared EncodeChanges runs it against the puller's
	// cached column index.
	cs := p.schemas[table]
	var index map[string]int
	if len(cs.Columns) > 0 {
		index = p.colIndex(table, cs)
	}
	rec, err := encodeChanges(p.buf, table, cs, index)
	if err != nil {
		return nil, err
	}
	return &dataplane.Batch{Table: table, Record: rec, Mode: dataplane.UpsertMode}, nil
}

// EncodeChanges encodes one table's changes into a wire record against its
// canonical schema, applying the SOURCE-boundary drift gate and the
// delete-with-no-PK guard exactly as Puller.makeBatch does. It is the shared
// encode stage: the puller passes its cached column index, a caller that holds
// ready changes (a columnar source re-encoding a drift record) passes nil and
// gets one built. A zero schema is schema-less: the shape is inferred/merged
// from the rows, as the puller's inference fallback does.
func EncodeChanges(changes []rowchange.Change, table string, schema core.Schema) (arrow.RecordBatch, error) {
	var index map[string]int
	if len(schema.Columns) > 0 {
		index = buildColIndex(schema)
	}
	return encodeChanges(changes, table, schema, index)
}

// encodeChanges is the shared body of EncodeChanges and Puller.makeBatch. The
// drift pass fails loud on a field the schema lacks (top-level OR nested
// inside a struct), hiding source evolution from the columnar worker;
// MergeSchema materializes an all-nil unknown column. C-8: a delete with no PK
// becomes an orphaned NULL tuple in the sink.
func encodeChanges(changes []rowchange.Change, table string, cs core.Schema, index map[string]int) (arrow.RecordBatch, error) {
	if len(cs.Columns) > 0 {
		merge := false
		for _, c := range changes {
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
			cs = transport.MergeSchema(changes, cs)
		}
	} else {
		// Known schema plus any column a change carries that the schema lacks
		// (schema-less producers, sparse rows). Empty cs → full inference.
		cs = transport.MergeSchema(changes, cs)
	}

	if len(cs.PrimaryKey) == 0 {
		for _, c := range changes {
			if c.Op == rowchange.OpDelete {
				return nil, fmt.Errorf("sourcepull: batch %q carries a delete but the schema has no primary key — declare it and resume", table)
			}
		}
	}

	rec, err := transport.RecordFromChanges(changes, cs, nil)
	if err != nil {
		return nil, fmt.Errorf("sourcepull: encode batch: %w", err)
	}
	return rec, nil
}

// buildColIndex builds the column-name -> position index for a canonical
// schema.
func buildColIndex(cs core.Schema) map[string]int {
	idx := make(map[string]int, len(cs.Columns))
	for i, c := range cs.Columns {
		idx[c.Name] = i
	}
	return idx
}

// colIndex returns (building it once) the column-name -> position index for a
// target table's canonical schema, so the drift check looks up a map instead
// of scanning the schema per row (#581).
func (p *Puller) colIndex(table string, cs core.Schema) map[string]int {
	if idx, ok := p.index[table]; ok {
		return idx
	}
	idx := buildColIndex(cs)
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
