// Direct-to-Arrow Kafka consumption (#733): the decoder writes each record's
// fields straight into a transport.RowEncoder built from the target table's
// canonical schema, so no per-record rowchange.Change is materialized and the
// source puller never re-encodes one. A record the direct path cannot encode
// without changing behavior falls back to the map path (Decode +
// sourcepull.EncodeChanges), which reproduces the row puller exactly.
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
	"github.com/maltzsama/urutau/internal/sourcepull"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
)

// Direct columnar size ceilings. Buffered rows are held as Arrow (not as
// rowchange.Change) until a batch crosses either ceiling: 2000 rows or 4 MiB,
// matching the row puller's batchTarget/batchTargetBytes.
const (
	maxDirectRows  = 2000
	maxDirectBytes = 4 << 20
)

// recordPosition renders a record's source coordinate — the Kafka offset map,
// identical to what the row path stamps on every change.
func recordPosition(rec *kgo.Record) string {
	return position.NewOffsets(rec.Topic, map[int32]int64{rec.Partition: rec.Offset}).String()
}

// consumeDirect decodes one record straight into its target table's Arrow
// encoder. A record the direct path declines (ErrShapeDrift) is re-encoded on
// the map path. Decode errors obey source.onDecodeError; an encode error (a
// value a declared column cannot hold) is always fatal, exactly as the row
// puller's RecordFromChanges fails the run.
func (r *Reader) consumeDirect(ctx context.Context, rec *kgo.Record) error {
	d, ok := r.dec.(decoder.ColumnarDecoder)
	if !ok {
		return fmt.Errorf("kafka: decoder %T is not columnar", r.dec)
	}
	position := recordPosition(rec)
	r.encTouched = ""
	n, err := d.DecodeInto(rec, r.encoderFor(rec.Topic), position)
	if err != nil {
		if errors.Is(err, decoder.ErrShapeDrift) {
			return r.emitFallback(ctx, rec, position)
		}
		if errors.Is(err, decoder.ErrEncode) {
			return fmt.Errorf("kafka: encode topic %s partition %d offset %d: %w",
				rec.Topic, rec.Partition, rec.Offset, err)
		}
		return r.decodeFailure(rec, err)
	}
	if n == 0 || r.encTouched == "" {
		return nil
	}
	return r.flushIfFull(ctx, r.encTouched)
}

// encoderFor resolves a decoded record's source key to its target table's
// encoder, building one lazily from the canonical schema installed by
// SetSourceSchemas. A key with no configured target reports mapped=false (the
// decoder appends nothing); a target with no canonical schema reports a nil
// encoder so the decoder falls back to the map path.
func (r *Reader) encoderFor(topic string) decoder.Encoder {
	return func(sourceKey string) (*transport.RowEncoder, bool, error) {
		ref, ok := r.resolveRef(sourceKey, topic)
		if !ok {
			return nil, false, nil
		}
		target := ref.Target
		if enc, ok := r.encoders[target]; ok {
			r.encTouched = target
			return enc, true, nil
		}
		cs, ok := r.schemas[target]
		if !ok || len(cs.Columns) == 0 {
			return nil, true, nil
		}
		enc, err := transport.NewRowEncoder(cs, nil)
		if err != nil {
			return nil, false, fmt.Errorf("kafka: %s: %w", target, err)
		}
		r.encoders[target] = enc
		r.encOrder = append(r.encOrder, target)
		r.encTouched = target
		return enc, true, nil
	}
}

// flushIfFull emits the target's batch when it crosses a size ceiling.
func (r *Reader) flushIfFull(ctx context.Context, target string) error {
	enc := r.encoders[target]
	if enc == nil {
		return nil
	}
	if enc.Rows() < maxDirectRows && enc.DataBytes() < maxDirectBytes {
		return nil
	}
	return r.flush(ctx, target)
}

// flush materializes the target's buffered rows into one wire batch and sends
// it. A table with nothing buffered is a no-op.
func (r *Reader) flush(ctx context.Context, target string) error {
	enc := r.encoders[target]
	if enc == nil || enc.Rows() == 0 {
		return nil
	}
	rec := enc.NewRecord()
	return r.sendBatch(ctx, &dataplane.Batch{Table: target, Record: rec, Mode: dataplane.UpsertMode})
}

// flushAll emits every table's buffered rows, in first-appearance order. Called
// at the end of every fetch so latency is bounded even below the size ceilings.
func (r *Reader) flushAll(ctx context.Context) error {
	for _, target := range r.encOrder {
		if err := r.flush(ctx, target); err != nil {
			return err
		}
	}
	return nil
}

// sendBatch hands one record to the puller, or unwinds when the stream is
// shutting down.
func (r *Reader) sendBatch(ctx context.Context, b *dataplane.Batch) error {
	select {
	case r.batchOut <- b:
		return nil
	case <-ctx.Done():
		b.Record.Release()
		return ctx.Err()
	}
}

// emitFallback re-encodes a record the direct path declined (ErrShapeDrift) on
// the map path. It applies the same per-change transformation the row path
// does (target resolution, position, transport envelope, key order) and encodes
// with sourcepull.EncodeChanges, which gates and merges schema drift and guards
// a delete with no primary key exactly as the puller.
func (r *Reader) emitFallback(ctx context.Context, rec *kgo.Record, position string) error {
	changes, err := r.dec.Decode(rec)
	if err != nil {
		return err
	}
	if len(changes) == 0 {
		return nil
	}
	byTable := make(map[string][]rowchange.Change, 1)
	var order []string
	for _, c := range changes {
		ref, ok := r.resolveRef(c.Table, rec.Topic)
		if !ok {
			r.logger.Warn("kafka: record for unmapped source", "topic", rec.Topic, "source", c.Table)
			continue
		}
		c.Table = ref.Target
		c.Position = position
		c.Transport = transportOf(rec)
		decoder.OrderKey(&c, ref.PrimaryKey)
		if _, seen := byTable[ref.Target]; !seen {
			order = append(order, ref.Target)
		}
		byTable[ref.Target] = append(byTable[ref.Target], c)
	}
	for _, target := range order {
		// Flush this target's live encoder first so the fallback record keeps
		// its arrival order relative to rows already buffered for the table.
		if err := r.flush(ctx, target); err != nil {
			return err
		}
		record, err := sourcepull.EncodeChanges(byTable[target], target, r.schemas[target])
		if err != nil {
			return err
		}
		if err := r.sendBatch(ctx, &dataplane.Batch{Table: target, Record: record, Mode: dataplane.UpsertMode}); err != nil {
			return err
		}
	}
	return nil
}
