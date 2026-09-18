// Package kafka implements the Kafka source adapter. It consumes CDC
// records from Kafka topics (produced by Debezium or compatible
// connectors) using franz-go with manual partition assignment. The
// source does not support DBLog snapshot: Kafka is an append-only log
// that the consumer reads from a chosen offset.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
	"github.com/maltzsama/urutau/internal/sourcepull"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Backoff bounds for transient fetch failures. A permanent error returns
// instead of retrying, so these only pace a broker that is coming back.
const (
	minFetchBackoff = 100 * time.Millisecond
	maxFetchBackoff = 30 * time.Second
)

// Source implements source.Source for Kafka.
type Source struct {
	Spec *spec.Spec
	Rt   source.Runtime
}

func capabilities() source.Capabilities {
	return source.Capabilities{
		Snapshot:            false,
		ChunkQuery:          false,
		Stream:              true,
		MaxConnections:      0, // no query connections at all
		Modes:               []source.Mode{source.ModeCDC},
		BeforeImage:         false, // deletes are tombstones (null) — no image to record
		MonotonicSequence:   true,  // (partition, offset) never reappears
		RecoverablePosition: true,  // offsets allow exact resume
	}
}

func init() {
	factory := func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
		return Source{Spec: s, Rt: rt}, nil
	}
	if err := driver.RegisterSource("kafka", capabilities(), factory); err != nil {
		panic(err)
	}
}

var _ source.Source = Source{}

// OpenQuery is intentionally absent: Kafka has no SQL query connection, and
// the Source interface no longer carries a SQL surface — that lives on
// QuerySource, which only relational sources implement.

// Introspect resolves one spec table into its ref and schema. Kafka has
// no SQL introspection; the schema comes from the spec's Columns map.
func (s Source) Introspect(_ context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
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
		ct, err := t.Columns[name].Resolve()
		if err != nil {
			return core.TableRef{}, core.Schema{}, nil,
				fmt.Errorf("kafka: table %q column %q: %w", t.Source, name, err)
		}
		cols = append(cols, core.Column{Name: name, Type: ct})
	}
	cs := core.Schema{Columns: cols}

	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, cs, nil, nil
}

// NewReader builds the Kafka consumer. It subscribes to the topics
// derived from the spec tables and produces changes to the output
// channel. No consumer group is used — the consumer manages its own
// offsets.
func (s Source) Open(ctx context.Context, refs []source.TableRef) (source.Reader, error) {
	refBySource := make(map[string]source.TableRef, len(refs))
	topics := make([]string, 0, len(refs))
	for _, ref := range refs {
		topics = append(topics, ref.Source)
		refBySource[ref.Source] = ref
	}

	// No ConsumeGroup: this is direct/manual partition consuming, so there
	// is no group-coordinated autocommit to disable — kgo.DisableAutoCommit
	// is only valid alongside a group, and errors client construction
	// otherwise (issue #17 e2e round trip surfaced this: no Kafka pipeline
	// had ever actually connected).
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.Spec.Source.URI),
		kgo.ConsumeTopics(topics...),
		kgo.WithLogger(newKgoLogger(s.Rt.Logger)),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}

	dec := decoder.Decoder(&decoder.DebeziumJSON{
		TopicToTable: topicToTarget(refs),
	})
	switch s.Spec.Source.Format {
	case "raw":
		dec = &decoder.Raw{
			ByTopic: extractionByTopic(s.Spec, "payload"),
			Miss:    s.missLogger(),
		}
	case "avro":
		if s.Spec.Source.SchemaRegistry == "" {
			client.Close()
			return nil, fmt.Errorf("kafka: source.schemaRegistry required when format is avro")
		}
		avroDec := decoder.NewAvroDecoder(decoder.NewHTTPRegistry(s.Spec.Source.SchemaRegistry))
		avroDec.ByTopic = extractionByTopic(s.Spec, "")
		avroDec.Miss = s.missLogger()
		dec = avroDec
	}

	r := &Reader{
		client:      client,
		dec:         dec,
		out:         make(chan rowchange.Change, 1024),
		logger:      s.Rt.Logger,
		refBySource: refBySource,
		synced:      &position.Offsets{},
	}
	// The puller wraps the reader's own out channel. (A nil puller would
	// panic in Start — the reader must not be handed out half-wired.)
	r.puller = sourcepull.New(r.out)
	return r, nil
}

// SetSourceSchemas installs the resolved canonical schema per target table
// so the source boundary gates on drift and encodes against a stable shape.
// The coordinator/runner holds the resolved schema (spec-declared for
// Kafka) and injects it after Open through this optional interface.
func (r *Reader) SetSourceSchemas(schemas map[string]core.Schema) {
	r.puller.SetSchemas(schemas)
}

// InitialPosition returns an empty offset — Kafka consumers start from
// the committed position or the beginning.
func (s Source) InitialPosition(_ context.Context) (position.Position, error) {
	return &position.Offsets{}, nil
}

// extractionByTopic builds the per-topic field-extraction declarations from
// the spec's tables, for the raw and avro decoders. payloadColumnName is the
// column that means "also keep the raw payload" (raw's "payload"; empty for
// avro, which has none — see spec.validateColumns).
//
// Introspect requires every Kafka table to declare Columns regardless of
// format (Columns is also how debezium tables assert their shape, since
// Kafka has no SQL introspection), so len(t.Columns) == 0 cannot happen for
// a resolved spec. It is still checked here defensively: this function reads
// the spec directly rather than through Introspect's validated path, and an
// empty Columns must mean "no extraction entry" (opaque), never a
// zero-field TopicExtraction that would extract nothing while still
// switching the topic out of passthrough mode.
func extractionByTopic(sp *spec.Spec, payloadColumnName string) map[string]decoder.TopicExtraction {
	byTopic := make(map[string]decoder.TopicExtraction, len(sp.Tables))
	for _, t := range sp.Tables {
		if len(t.Columns) == 0 {
			continue
		}
		te := decoder.TopicExtraction{Fields: make([]decoder.Field, 0, len(t.Columns))}
		for name, col := range t.Columns {
			if payloadColumnName != "" && name == payloadColumnName {
				te.KeepPayload = true
				continue
			}
			te.Fields = append(te.Fields, decoder.Field{Name: name, Path: col.From, Required: col.Required})
		}
		sort.Slice(te.Fields, func(i, j int) bool { return te.Fields[i].Name < te.Fields[j].Name })
		byTopic[t.Source] = te
	}
	return byTopic
}

// missLogger returns a decoder.MissFn that logs an extraction miss. Bronze
// should not go down over one malformed message, but a silent NULL hides a
// producer schema change — this is the visible middle ground until a metric
// is wired up (see issue #143's open question on making this an observable
// counter).
func (s Source) missLogger() decoder.MissFn {
	return func(column string) {
		s.Rt.Logger.Warn("kafka: declared field absent from payload", "column", column)
	}
}

// topicToTarget builds a topic → target table map from the table refs.
func topicToTarget(refs []source.TableRef) map[string]string {
	m := make(map[string]string, len(refs))
	for _, ref := range refs {
		m[ref.Source] = ref.Target
	}
	return m
}

// ParsePosition decodes a stored cdc.position as Kafka offsets.
func (s Source) ParsePosition(pos string) (position.Position, error) {
	return position.ParseOffsets(pos)
}

// ── Reader ─────────────────────────────────────────────────────────

// Reader implements source.Reader for Kafka: it consumes records,
// decodes them, and feeds changes to the output channel.
type Reader struct {
	client *kgo.Client
	dec    decoder.Decoder
	out    chan rowchange.Change
	puller *sourcepull.Puller
	logger *slog.Logger
	// refBySource resolves a decoded source (envelope source or topic) to
	// its full table mapping, so the change is addressed by its TARGET —
	// the worker routes on target names.
	refBySource map[string]source.TableRef

	mu     sync.Mutex
	synced *position.Offsets
}

// Synced returns the current consumer position. The copy is deliberate: the
// consume loop keeps mutating r.synced, so handing out the live maps would
// race with whoever holds the returned position.
func (r *Reader) Synced() position.Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced.Clone()
}

// Master returns the high-watermark position (the latest offset across
// all partitions). For the caught-up proof, the consumer is caught up
// when its synced position contains the master.
func (r *Reader) Master(_ context.Context) (position.Position, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced.Clone(), nil
}

// OpenWindow is a no-op for Kafka: there is no DBLog snapshot window.
func (r *Reader) OpenWindow(_ context.Context, _ uint32) {}

// ClearWindow is a no-op for Kafka.
func (r *Reader) ClearWindow() {}

// Start begins consuming from Kafka (the offset is carried in the reader's
// construction; Kafka has no single resume position like GTID/LSN).
func (r *Reader) Start(ctx context.Context, _ position.Position) error {
	errCh := make(chan error, 1)
	r.puller.SetErr(errCh)
	go func() { errCh <- r.consume(ctx) }()
	return nil
}

// Next returns the next columnar batch. The source boundary owns the
// row-to-wire encode (the puller batches the decoder's row-shaped output);
// the row universe ends at the CDC decoder, the worker is fully columnar.
func (r *Reader) Next(ctx context.Context) (*dataplane.Batch, error) {
	return r.puller.Next(ctx)
}

// consume is the blocking consume loop: it polls fetches and feeds decoded
// changes to r.out, returning the terminal error.
//
// Errors are classified rather than uniformly retried. A permanent condition
// (unknown topic, denied authorization, bad credentials) returns and fails
// the pipeline: retrying it spins at full speed forever while making no
// progress and reporting nothing upstream. A transient one backs off.
func (r *Reader) consume(ctx context.Context) error {
	backoff := minFetchBackoff
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
			switch classifyFetch(err) {
			case fetchPermanent:
				return fmt.Errorf("kafka: fetch failed permanently: %w", err)
			case fetchPositionLost:
				return &ErrPositionLost{Topic: firstFetchTopic(fetches), Err: err}
			}
			r.logger.Error("kafka: fetch, retrying", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxFetchBackoff)
			continue
		}
		backoff = minFetchBackoff

		var fatal error
		fetches.EachRecord(func(rec *kgo.Record) {
			if fatal != nil {
				return
			}
			// franz-go leaves Record.Context nil on the consume side (it is
			// only populated by an explicit hook, or on the produce side) —
			// a decoder that needs one (Avro's schema registry HTTP fetch)
			// would otherwise fail every record with "net/http: nil
			// Context". Give it this loop's ctx explicitly.
			rec.Context = ctx
			changes, err := r.dec.Decode(rec)
			if err != nil {
				if decodeIsFatal(err) {
					fatal = fmt.Errorf("kafka: decode topic %s partition %d offset %d: %w",
						rec.Topic, rec.Partition, rec.Offset, err)
					return
				}
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
				c.Position = position.NewOffsets(rec.Topic,
					map[int32]int64{rec.Partition: rec.Offset}).String()
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
			r.synced.Set(rec.Topic, rec.Partition, rec.Offset+1)
			r.mu.Unlock()
		})
		if fatal != nil {
			return fatal
		}
	}
}

// firstFetchTopic names a topic carried by the fetch, for the error message.
func firstFetchTopic(fetches kgo.Fetches) string {
	for _, e := range fetches.Errors() {
		if e.Topic != "" {
			return e.Topic
		}
	}
	return ""
}

// transportOf captures the message-queue envelope of a record for the
// transport metadata columns (stream, shard, sequence, msg_ts, msg_key,
// headers). Headers serialize to JSON because the canonical type system has
// no map yet.
func transportOf(rec *kgo.Record) *rowchange.Transport {
	t := &rowchange.Transport{
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
