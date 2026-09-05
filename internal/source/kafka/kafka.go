// Package kafka implements the Kafka source adapter. It consumes CDC
// records from Kafka topics (produced by Debezium or compatible
// connectors) using franz-go with manual partition assignment. The
// source does not support DBLog snapshot: Kafka is an append-only log
// that the consumer reads from a chosen offset.
package kafka

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Source implements source.Source for Kafka.
type Source struct {
	Spec *spec.Spec
	Rt   source.Runtime
}

func capabilities() source.Capabilities {
	return source.Capabilities{
		Snapshot:          false,
		ChunkQuery:        false,
		Stream:            true,
		MaxConnections:    0, // no query connections at all
		Modes:             []source.Mode{source.ModeCDC},
		BeforeImage:       false, // deletes are tombstones (null) — no image to record
		MonotonicSequence: true,  // (partition, offset) never reappears
	}
}

func init() {
	driver.RegisterSource("kafka", capabilities(), func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
		return Source{Spec: s, Rt: rt}, nil
	})
}

var _ source.Source = Source{}

// OpenQuery is intentionally absent: Kafka has no SQL query connection, and
// the Source interface no longer carries a SQL surface — that lives on
// QuerySource, which only relational sources implement.

// Introspect resolves one spec table into its ref and schema. Kafka has
// no SQL introspection; the schema comes from the spec's Columns map.
func (s Source) Introspect(_ context.Context, _ *sql.DB, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	if len(t.Columns) == 0 {
		return core.TableRef{}, core.Schema{}, nil,
			fmt.Errorf("kafka: table %q requires columns in spec", t.Source)
	}

	mode := t.WriteMode
	if mode == "" {
		mode = spec.WriteModeUpsert // upsert-first: reflecting state is the default
	}
	isAppend := mode == spec.WriteModeAppend || mode == spec.WriteModeAppendIdempotent
	if len(t.PrimaryKey) == 0 && !isAppend {
		return core.TableRef{}, core.Schema{}, nil,
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
			return core.TableRef{}, core.Schema{}, nil,
				fmt.Errorf("kafka: table %q column %q: %w", t.Source, name, err)
		}
		cols = append(cols, core.Column{Name: name, Type: ct})
	}
	cs := core.Schema{Columns: cols}

	cast, err := core.ParseCastPolicy(t.Cast)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil,
			fmt.Errorf("kafka: table %q: %w", t.Source, err)
	}
	resolved, warns, err := core.ResolveSchema(cs, cast, t.Metadata)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil,
			fmt.Errorf("kafka: table %q: %w", t.Source, err)
	}

	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, resolved, warns, nil
}

// NewReader builds the Kafka consumer. It subscribes to the topics
// derived from the spec tables and produces changes to the output
// channel. No consumer group is used — the consumer manages its own
// offsets.
func (s Source) NewReader(ctx context.Context, _ *sql.DB, refs []source.TableRef, out chan<- change.Change) (source.StreamSource, error) {
	refBySource := make(map[string]source.TableRef, len(refs))
	topics := make([]string, 0, len(refs))
	for _, ref := range refs {
		topics = append(topics, ref.Source)
		refBySource[ref.Source] = ref
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

	dec := decoder.Decoder(&decoder.DebeziumJSON{})
	switch s.Spec.Source.Format {
	case "raw":
		dec = &decoder.Raw{}
	case "avro":
		if s.Spec.Source.SchemaRegistry == "" {
			client.Close()
			return nil, fmt.Errorf("kafka: source.schemaRegistry required when format is avro")
		}
		dec = decoder.NewAvroDecoder(decoder.NewHTTPRegistry(s.Spec.Source.SchemaRegistry))
	}

	r := &Reader{
		client:      client,
		dec:         dec,
		out:         out,
		logger:      s.Rt.Logger,
		refBySource: refBySource,
		synced:      &position.Offsets{},
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
	// refBySource resolves a decoded source (envelope source or topic) to
	// its full table mapping, so the change is addressed by its TARGET —
	// the worker routes on target names.
	refBySource map[string]source.TableRef

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
				// Resolve the source (envelope source for debezium, topic for
				// raw) to the target the worker routes on, and attach the
				// message-queue envelope for transport metadata.
				src := c.Table
				if src == "" {
					src = rec.Topic
				}
				ref, ok := r.refBySource[src]
				if !ok {
					r.logger.Warn("kafka: record for unmapped source", "topic", rec.Topic, "source", src)
					return
				}
				c.Table = ref.Target
				c.Position = (&position.Offsets{
					Topic: rec.Topic,
					Parts: map[int32]int64{rec.Partition: rec.Offset},
				}).String()
				c.Transport = transportOf(rec)
				// The raw key tuple inherits JSON object disorder; rebuild
				// it in the declared primary-key order so every downstream
				// positional consumer (collapse, equality deletes) sees a
				// stable tuple.
				decoder.OrderKey(&c, ref.PrimaryKey)
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

// transportOf captures the message-queue envelope of a record for the
// transport metadata columns (stream, shard, sequence, msg_ts, msg_key,
// headers). Headers serialize to JSON because the canonical type system has
// no map yet.
func transportOf(rec *kgo.Record) *change.Transport {
	t := &change.Transport{
		Stream: rec.Topic,
		Shard:  strconv.Itoa(int(rec.Partition)),
		Seq:    strconv.FormatInt(rec.Offset, 10),
		MsgTS:  rec.Timestamp,
		MsgKey: string(rec.Key),
	}
	if len(rec.Headers) > 0 {
		h := make(map[string]string, len(rec.Headers))
		for _, hd := range rec.Headers {
			h[hd.Key] = string(hd.Value)
		}
		if b, err := json.Marshal(h); err == nil {
			t.Headers = string(b)
		}
	}
	return t
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
