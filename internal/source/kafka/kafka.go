// Package kafka implements the Kafka source adapter. It consumes CDC
// records from Kafka topics (produced by Debezium or compatible
// connectors) using franz-go with manual partition assignment. The
// source does not support DBLog snapshot: Kafka is an append-only log
// that the consumer reads from a chosen offset.
package kafka

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	"github.com/maltzsama/urutau/internal/position"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
	srctypes "github.com/maltzsama/urutau/internal/source/types"
	"github.com/maltzsama/urutau/internal/spec"
)

// Source implements adapter.Source for Kafka.
type Source struct {
	Spec *spec.Spec
	Rt   srctypes.Runtime
}

// Caps reports Kafka capabilities: streaming only, no DBLog snapshot.
func (s Source) Caps() srctypes.Capabilities {
	return srctypes.Capabilities{
		Snapshot:          false,
		ChunkQuery:        false,
		Stream:            true,
		MaxConnections:    0, // no query connections at all
		Modes:             []srctypes.Mode{srctypes.ModeCDC},
		BeforeImage:       false, // deletes are tombstones (null) — no image to record
		MonotonicSequence: true,  // (partition, offset) never reappears
	}
}

// OpenQuery returns nil — Kafka has no SQL query connection.
func (s Source) OpenQuery(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

// Introspect resolves one spec table into its ref and schema. Kafka has
// no SQL introspection; the schema comes from the spec's Columns map.
func (s Source) Introspect(_ context.Context, _ *sql.DB, t spec.Table) (core.TableRef, core.Schema, *iceberg.Schema, []core.Warning, error) {
	if len(t.Columns) == 0 {
		return core.TableRef{}, core.Schema{}, nil, nil,
			fmt.Errorf("kafka: table %q requires columns in spec", t.Source)
	}

	mode := t.WriteMode
	if mode == "" {
		mode = spec.WriteModeUpsert // upsert-first: reflecting state is the default
	}
	if len(t.PrimaryKey) == 0 && mode != spec.WriteModeAppend {
		return core.TableRef{}, core.Schema{}, nil, nil,
			fmt.Errorf("kafka: table %q requires primaryKey for writeMode=upsert", t.Source)
	}
	pk := t.PrimaryKey

	// The spec declares columns as a map, which carries no order — iterate
	// sorted so every boot resolves the same column order. Downstream
	// consumers are name-keyed, but a stable order keeps freshly created
	// targets and their schemas reproducible.
	names := make([]string, 0, len(t.Columns))
	for name := range t.Columns {
		names = append(names, name)
	}
	sort.Strings(names)

	cols := make([]core.Column, 0, len(t.Columns))
	for _, name := range names {
		typeStr := t.Columns[name]
		ct, err := core.ParseColumnType(typeStr)
		if err != nil {
			return core.TableRef{}, core.Schema{}, nil, nil,
				fmt.Errorf("kafka: table %q column %q: %w", t.Source, name, err)
		}
		cols = append(cols, core.Column{Name: name, Type: ct})
	}
	cs := core.Schema{Columns: cols}

	cast, err := core.ParseCastPolicy(t.Cast)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil,
			fmt.Errorf("kafka: table %q: %w", t.Source, err)
	}
	resolved, warns, err := core.ResolveSchema(cs, cast, t.Metadata)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil,
			fmt.Errorf("kafka: table %q: %w", t.Source, err)
	}

	is, err := icebergSchema(resolved)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil,
			fmt.Errorf("kafka: table %q: %w", t.Source, err)
	}

	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, resolved, is, warns, nil
}

// icebergSchema converts a resolved canonical schema into an Iceberg
// schema by mapping each column type.
func icebergSchema(cs core.Schema) (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, 0, len(cs.Columns))
	for i, col := range cs.Columns {
		itype, err := mapColumnType(col.Type)
		if err != nil {
			return nil, err
		}
		fields = append(fields, iceberg.NestedField{
			ID:       i + 1,
			Name:     col.Name,
			Type:     itype,
			Required: false,
		})
	}
	return iceberg.NewSchema(0, fields...), nil
}

func mapColumnType(ct core.ColumnType) (iceberg.Type, error) {
	switch ct.Kind {
	case core.KindBool:
		return iceberg.PrimitiveTypes.Bool, nil
	case core.KindInt32:
		return iceberg.PrimitiveTypes.Int32, nil
	case core.KindInt64:
		return iceberg.PrimitiveTypes.Int64, nil
	case core.KindFloat32:
		return iceberg.PrimitiveTypes.Float32, nil
	case core.KindFloat64:
		return iceberg.PrimitiveTypes.Float64, nil
	case core.KindString, core.KindJSON:
		return iceberg.PrimitiveTypes.String, nil
	case core.KindBinary:
		return iceberg.PrimitiveTypes.Binary, nil
	case core.KindDate:
		return iceberg.PrimitiveTypes.Date, nil
	case core.KindTime:
		return iceberg.PrimitiveTypes.Time, nil
	case core.KindTimestamp:
		return iceberg.PrimitiveTypes.Timestamp, nil
	case core.KindTimestampTZ:
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case core.KindUUID:
		return iceberg.PrimitiveTypes.UUID, nil
	default:
		return nil, fmt.Errorf("kafka: unsupported column type %s", ct.Kind)
	}
}

// NewChunker is not supported for Kafka.
func (s Source) NewChunker(_ *sql.DB, _, _ string, _ int) (snapshot.ChunkSource, error) {
	return nil, fmt.Errorf("kafka: chunking not supported")
}

// NewReader builds the Kafka consumer. It subscribes to the topics
// derived from the spec tables and produces changes to the output
// channel. No consumer group is used — the consumer manages its own
// offsets.
func (s Source) NewReader(ctx context.Context, _ *sql.DB, refs []snapshot.TableRef, out chan<- change.Change) (srctypes.StreamSource, error) {
	pkBySource := make(map[string][]string, len(refs))
	topics := make([]string, 0, len(refs))
	for _, ref := range refs {
		topics = append(topics, ref.Source)
		pkBySource[ref.Source] = ref.PrimaryKey
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(s.Spec.Source.URI),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(newKgoLogger(s.Rt.Logger)),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}

	dec := &decoder.DebeziumJSON{}

	r := &Reader{
		client:     client,
		dec:        dec,
		out:        out,
		logger:     s.Rt.Logger,
		pkBySource: pkBySource,
		synced:     &position.Offsets{},
	}
	return r, nil
}

// InitialPosition returns an empty offset — Kafka consumers start from
// the committed position or the beginning.
func (s Source) InitialPosition(_ context.Context, _ *sql.DB) (position.Position, error) {
	return &position.Offsets{}, nil
}

// ParsePosition decodes a stored cdc.position as Kafka offsets.
func (s Source) ParsePosition(pos string) (position.Position, error) {
	return position.ParseOffsets(pos)
}

// ── Reader ─────────────────────────────────────────────────────────

// Reader implements StreamSource for Kafka: it consumes records,
// decodes them, and feeds changes to the output channel.
type Reader struct {
	client *kgo.Client
	dec    decoder.Decoder
	out    chan<- change.Change
	logger *slog.Logger
	// pkBySource maps a source table to its declared primary-key columns.
	// The decoder's raw key tuple comes from a JSON object and has no
	// stable order; composite keys need the declared order.
	pkBySource map[string][]string

	mu     sync.Mutex
	synced *position.Offsets
}

// Synced returns the current consumer position.
func (r *Reader) Synced() position.Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced
}

// Master returns the high-watermark position (the latest offset across
// all partitions). For the caught-up proof, the consumer is caught up
// when its synced position contains the master.
func (r *Reader) Master(_ context.Context) (position.Position, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced, nil
}

// OpenWindow is a no-op for Kafka: there is no DBLog snapshot window.
func (r *Reader) OpenWindow(_ context.Context, _ uint32) {}

// ClearWindow is a no-op for Kafka.
func (r *Reader) ClearWindow() {}

// Start begins consuming from Kafka, blocking until ctx is cancelled
// or the client is closed.
func (r *Reader) Start(ctx context.Context, _ position.Position) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetches := r.client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("kafka: fetch", "err", err)
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			changes, err := r.dec.Decode(rec)
			if err != nil {
				r.logger.Error("kafka: decode", "topic", rec.Topic, "err", err)
				return
			}
			for _, c := range changes {
				c.Position = (&position.Offsets{
					Topic: rec.Topic,
					Parts: map[int32]int64{rec.Partition: rec.Offset},
				}).String()
				// The raw key tuple inherits JSON object disorder; rebuild
				// it in the declared primary-key order so every downstream
				// positional consumer (collapse, equality deletes) sees a
				// stable tuple.
				decoder.OrderKey(&c, r.pkBySource[c.Table])
				select {
				case r.out <- c:
				case <-ctx.Done():
					return
				}
			}

			r.mu.Lock()
			if r.synced.Parts == nil {
				r.synced.Parts = make(map[int32]int64)
			}
			r.synced.Topic = rec.Topic
			r.synced.Parts[rec.Partition] = rec.Offset + 1
			r.mu.Unlock()
		})
	}
}

// Close releases the Kafka client.
func (r *Reader) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// SetConfirmed is a no-op for Kafka: partition offsets are committed
// directly, not via a replication slot.
func (r *Reader) SetConfirmed(_ func() position.Position) {}

// kgoLogger adapts slog.Logger to the kgo.Logger interface.
type kgoLogger struct {
	l *slog.Logger
}

func newKgoLogger(l *slog.Logger) *kgoLogger {
	if l == nil {
		l = slog.Default()
	}
	return &kgoLogger{l: l}
}

func (l *kgoLogger) Level() kgo.LogLevel { return kgo.LogLevelInfo }
func (l *kgoLogger) Log(_ kgo.LogLevel, msg string, keyvals ...any) {
	l.l.Info(msg, keyvals...)
}
