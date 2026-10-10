package runner

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/sourcepull"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

const runnerTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// toRowMode translates the data-plane write mode to the row-layer enum the
// recorded rowchange.Batch carries. Test-only: production never bridges the
// two.
func toRowMode(m dataplane.WriteMode) rowchange.WriteMode {
	if m == dataplane.AppendMode {
		return rowchange.AppendMode
	}
	return rowchange.UpsertMode
}

// gateCommitter records committed batches.
type gateCommitter struct {
	mu      sync.Mutex
	batches []rowchange.Batch
}

func (c *gateCommitter) Close() error { return nil }
func (c *gateCommitter) Commit(_ context.Context, b *dataplane.Batch) error {
	// Decode to rowchange for row-shaped assertions — a test read path, not
	// a production bridge.
	if b.Record == nil || b.Record.NumRows() == 0 {
		c.mu.Lock()
		c.batches = append(c.batches, rowchange.Batch{Table: b.Table, Position: string(b.Watermark), Mode: toRowMode(b.Mode)})
		c.mu.Unlock()
		return nil
	}
	rows, _ := transport.DecodeBatch(b.Record, b.Table, []string{"id"})
	var upserts []rowchange.Change
	for _, r := range rows {
		if r.Op != rowchange.OpDelete {
			upserts = append(upserts, r)
		}
	}
	c.mu.Lock()
	c.batches = append(c.batches, rowchange.Batch{
		Table: b.Table, Changes: upserts, Position: string(b.Watermark), Mode: toRowMode(b.Mode),
	})
	c.mu.Unlock()
	return nil
}

// upsert returns the final committed value for a key, if any.
func (c *gateCommitter) upsert(id int64) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range c.batches {
		for _, u := range b.Changes {
			if u.Op == rowchange.OpDelete {
				continue
			}
			if len(u.Key) == 1 && u.Key[0] == id {
				return u.After["v"].(string), true
			}
		}
	}
	return "", false
}

// TestRelayGateLiveEventsAfterWindowRows proves the gate ordering (design
// §3.1): a live event decoded while a chunk's SELECT is in flight is buffered
// by the relay and released InWindow-tagged only after AddWindowRows has
// populated the window — so the live value deterministically wins over the
// stale snapshot row instead of racing ahead of an empty window.
func TestRelayGateLiveEventsAfterWindowRows(t *testing.T) {
	at := position.MustGTID(runnerTestUUID + ":1-9")
	committer := &gateCommitter{}
	w := worker.New(worker.Config{MaxRows: 100, MaxInterval: time.Hour})
	w.RegisterCommitter("raw.orders", committer, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	})

	ingest := make(chan worker.Ingest, 64)
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ingest) }()

	r := newRelay(ingest, w, nil)
	out := make(chan rowchange.Change, 64)
	pr := &pullTestReader{Puller: sourcepull.New(out)}
	relayDone := make(chan struct{})
	go func() {
		_ = r.run(context.Background(), pr)
		close(relayDone)
	}()

	// Chunk 0 SELECT in flight: the table's live events are gated.
	r.GateOn("raw.orders", 0)

	// A live UPDATE of id=1 decoded during the SELECT: the reader tags it
	// InWindow, but the relay must hold it until the window is populated.
	out <- rowchange.Change{
		Op:       rowchange.OpUpdate,
		Table:    "raw.orders",
		Key:      []any{int64(1)},
		After:    map[string]any{"id": int64(1), "v": "live"},
		Position: at.String(),
	}

	// The chunk SELECT lands: id=1 is stale (v=a), id=2 stable (v=x).
	cols := []rowchange.Change{
		{Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}, Position: at.String()},
		{Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "v": "x"}, Position: at.String()},
	}
	rec, err := transport.RecordFromChanges(cols, transport.MergeSchema(cols, w.KnownSchema("raw.orders")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddWindowRows("raw.orders", 0, &dataplane.Batch{Table: "raw.orders", Record: rec, Mode: dataplane.AppendMode}); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}

	// Release the gated live event, then close the chunk. The pull-based
	// relay bridges asynchronously; wait until the event is gated.
	deadline := time.Now().Add(2 * time.Second)
	for r.gatedCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.gatedCount() == 0 {
		t.Fatal("live event never reached the gate")
	}
	if err := r.GateFlush(context.Background()); err != nil {
		t.Fatalf("GateFlush: %v", err)
	}
	if err := r.Release(context.Background(), "raw.orders", 0, at); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Wait for the pump to fully exit (it drains the gate buffer first), so
	// closing ingest can never race an in-flight write.
	close(out)
	<-relayDone
	close(ingest)
	if err := <-done; err != nil {
		t.Fatalf("worker run: %v", err)
	}

	if v, ok := committer.upsert(1); !ok || v != "live" {
		t.Fatalf("id=1 must carry the live value, got %q (present=%v)", v, ok)
	}
	if v, ok := committer.upsert(2); !ok || v != "x" {
		t.Fatalf("id=2 must carry the snapshot value, got %q (present=%v)", v, ok)
	}
	// The synchronous GateFlush guarantees the gated live event was
	// deduplicated against the populated window before Closes flushed it —
	// so the droppedByWindow evidence is deterministic, not a race.
	if n := w.DroppedByWindow("raw.orders"); n != 1 {
		t.Fatalf("droppedByWindow = %d, want 1 (deterministic dedup)", n)
	}
}

// The confirmed position must use the position's own ordering, not string
// comparison: "0/10" sorts before "0/2" lexicographically while 16 follows 2
// numerically — a string min would advance the Postgres slot past data still
// in flight.
func TestConfirmedPositionUsesPositionOrdering(t *testing.T) {
	r := &Runner{committedPositions: make(map[string]position.Position)}

	r.updateCommitted("raw.a", position.MustLSN("0/10"))
	r.updateCommitted("raw.b", position.MustLSN("0/2"))

	got := r.confirmedPosition()
	want := position.MustLSN("0/2")
	if got == nil || got.String() != want.String() {
		t.Fatalf("confirmed = %v, want %v — the true minimum", got, want)
	}

	// A later, larger commit moves the floor to the other table's position.
	r.updateCommitted("raw.b", position.MustLSN("0/40"))
	if got := r.confirmedPosition().String(); got != "0/10" {
		t.Fatalf("confirmed = %q after raw.b advanced, want 0/10 (raw.a's position)", got)
	}
}

// Nothing durably committed means nil: the reader must not advance the slot.
func TestConfirmedPositionEmptyIsNil(t *testing.T) {
	r := &Runner{committedPositions: make(map[string]position.Position)}
	if r.confirmedPosition() != nil {
		t.Fatal("confirmed = non-nil with no commits, want nil")
	}
}

// pullTestReader adapts a change channel to the pull-based Reader contract
// for relay tests.
type pullTestReader struct {
	*sourcepull.Puller
}

func (p *pullTestReader) Start(context.Context, position.Position) error    { return nil }
func (p *pullTestReader) Synced() position.Position                         { return nil }
func (p *pullTestReader) Master(context.Context) (position.Position, error) { return nil, nil }
func (p *pullTestReader) OpenWindow(context.Context, uint32)                {}
func (p *pullTestReader) ClearWindow()                                      {}
func (p *pullTestReader) Close()                                            {}
func (p *pullTestReader) SetConfirmed(func() position.Position)             {}

// bufferedReader models a reader whose decoder has produced a batch but whose
// Next has not pulled it: the batch sits in the reader's own buffer and is
// only reachable through Drain (the source.Drainer surface).
type bufferedReader struct {
	mu  sync.Mutex
	buf []*dataplane.Batch
	ch  chan struct{}
}

func (b *bufferedReader) push(batch *dataplane.Batch) {
	b.mu.Lock()
	b.buf = append(b.buf, batch)
	b.mu.Unlock()
	select {
	case b.ch <- struct{}{}:
	default:
	}
}

func (b *bufferedReader) Drain(_ context.Context, emit func(*dataplane.Batch) error) error {
	b.mu.Lock()
	buf := b.buf
	b.buf = nil
	b.mu.Unlock()
	for _, batch := range buf {
		if err := emit(batch); err != nil {
			return err
		}
	}
	return nil
}

func (b *bufferedReader) Next(ctx context.Context) (*dataplane.Batch, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (b *bufferedReader) Start(context.Context, position.Position) error    { return nil }
func (b *bufferedReader) Synced() position.Position                         { return nil }
func (b *bufferedReader) Master(context.Context) (position.Position, error) { return nil, nil }
func (b *bufferedReader) OpenWindow(context.Context, uint32)                {}
func (b *bufferedReader) ClearWindow()                                      {}
func (b *bufferedReader) Close()                                            {}
func (b *bufferedReader) SetConfirmed(func() position.Position)             {}

// TestFlushDecodedDrainsReaderBuffer guards #488: a live event decoded by the
// reader but not yet pulled must be flushed ahead of the Closes marker, so it
// is never lost across the marker.
func TestFlushDecodedDrainsReaderBuffer(t *testing.T) {
	ingest := make(chan worker.Ingest, 16)
	r := newRelay(ingest, nil, nil)

	rdr := &bufferedReader{ch: make(chan struct{}, 1)}
	cols := []rowchange.Change{
		{Op: rowchange.OpUpdate, Table: "raw.orders", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "live"}, Position: "0/1"},
	}
	rec, err := transport.RecordFromChanges(cols, transport.MergeSchema(cols, core.Schema{}), nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	rdr.push(&dataplane.Batch{Table: "raw.orders", Record: rec, Mode: dataplane.UpsertMode})

	drainReq := make(chan *drainRequest, 1)
	batchCh := make(chan *dataplane.Batch, 16)

	// Stand in for the puller goroutine: service a drain by flushing the
	// reader's buffer into batchCh.
	go func() {
		for req := range drainReq {
			close(req.accepted)
			err := rdr.Drain(context.Background(), func(b *dataplane.Batch) error {
				batchCh <- b
				return nil
			})
			req.err <- err
		}
	}()

	if err := r.flushDecoded(context.Background(), rdr, drainReq, batchCh); err != nil {
		t.Fatalf("flushDecoded: %v", err)
	}
	close(drainReq) // let the stand-in puller goroutine exit

	select {
	case in := <-ingest:
		if in.Batch == nil {
			t.Fatal("the drained batch was not flushed into ingest")
		}
	default:
		t.Fatal("the decoded-but-not-pulled batch was lost across the drain")
	}
}

// A drain error must propagate to the caller, so Release never emits the
// Closes marker past a failed flush (Sourcery #488 finding).
func TestFlushDecodedPropagatesDrainError(t *testing.T) {
	ingest := make(chan worker.Ingest, 16)
	r := newRelay(ingest, nil, nil)
	rdr := &bufferedReader{ch: make(chan struct{}, 1)}
	rdr.push(&dataplane.Batch{Table: "raw.orders", Record: nil, Mode: dataplane.UpsertMode})

	drainReq := make(chan *drainRequest, 1)
	batchCh := make(chan *dataplane.Batch, 16)

	boom := errors.New("decode failed")
	go func() {
		for req := range drainReq {
			close(req.accepted)
			req.err <- boom
		}
	}()

	if err := r.flushDecoded(context.Background(), rdr, drainReq, batchCh); !errors.Is(err, boom) {
		t.Fatalf("flushDecoded = %v, want the drain error %v", err, boom)
	}
	close(drainReq) // let the stand-in puller goroutine exit
}

// A reader whose puller is blocked on Next (empty buffer) never accepts the
// drain request; flushDecoded must give up after the acceptance deadline, not
// wait forever (Sourcery #488 finding).
func TestFlushDecodedEmptyReaderDeadline(t *testing.T) {
	ingest := make(chan worker.Ingest, 16)
	r := newRelay(ingest, nil, nil)
	rdr := &bufferedReader{ch: make(chan struct{}, 1)} // empty buffer

	drainReq := make(chan *drainRequest, 1)
	batchCh := make(chan *dataplane.Batch, 16)
	// No goroutine services drainReq: the puller is blocked on Next.

	start := time.Now()
	if err := r.flushDecoded(context.Background(), rdr, drainReq, batchCh); err != nil {
		t.Fatalf("flushDecoded = %v, want nil (empty reader)", err)
	}
	if elapsed := time.Since(start); elapsed < drainWaitTimeout {
		t.Fatalf("flushDecoded returned after %v, before the acceptance deadline %v", elapsed, drainWaitTimeout)
	}
}

// P3 / retention: updateCommitted recomputes the confirmed point the Postgres
// slot advances to. An incomparable pair must set it to nil (hold the slot
// back), never an arbitrary minimum.
func TestUpdateCommittedIncomparableHolds(t *testing.T) {
	r := &Runner{
		log:                slog.New(slog.DiscardHandler),
		committedPositions: map[string]position.Position{},
	}
	r.updateCommitted("a", opaqueTestPos("x"))
	r.updateCommitted("b", opaqueTestPos("y"))
	if r.minConfirmed != nil {
		t.Fatalf("incomparable positions must hold the confirmed point (nil), got %s", r.minConfirmed)
	}

	lo := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3")
	hi := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	r2 := &Runner{log: slog.New(slog.DiscardHandler), committedPositions: map[string]position.Position{}}
	r2.updateCommitted("a", lo)
	r2.updateCommitted("b", hi)
	if r2.minConfirmed == nil || r2.minConfirmed.String() != lo.String() {
		t.Fatalf("comparable fold = %v, want %s", r2.minConfirmed, lo)
	}
}

// opaqueTestPos is identity-only: different values are Incomparable.
type opaqueTestPos string

func (o opaqueTestPos) String() string { return string(o) }
func (o opaqueTestPos) Compare(other position.Position) int {
	p, ok := other.(opaqueTestPos)
	if ok && o == p {
		return 0
	}
	return position.Incomparable
}
func (o opaqueTestPos) Contains(other position.Position) bool {
	p, ok := other.(opaqueTestPos)
	return ok && o == p
}

// introspectSource is the minimal source.Source for the introspectAll test:
// only Introspect is exercised.
type introspectSource struct {
	schema core.Schema
}

func (f *introspectSource) Open(context.Context, []source.TableRef) (source.Reader, error) {
	return nil, errors.New("not used")
}
func (f *introspectSource) InitialPosition(context.Context) (position.Position, error) {
	return nil, nil
}
func (f *introspectSource) ParsePosition(string) (position.Position, error) { return nil, nil }
func (f *introspectSource) Introspect(context.Context, spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	return core.TableRef{Source: "db.users", Target: "raw.users", PrimaryKey: []string{"id"}}, f.schema, nil, nil
}

// FT-1: introspectAll extends BOTH the wire and the resolved shape with the
// reference destinations (explicit selects, final names), so EnsureTable
// creates the column and the drift check sees one stable shape from batch 1.
func TestIntrospectAllExtendsBothShapesForEnrich(t *testing.T) {
	src := &introspectSource{schema: core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}}}
	s := &spec.Spec{Tables: []spec.Table{{
		Source: "db.users", Target: "raw.users",
		Enrich: []spec.Enrich{{
			Table:  "users",
			Select: []string{"name"},
			On:     map[string]string{"user_ref": "id"},
			As:     map[string]string{"users.name": "user_name"},
		}},
	}}}
	_, resolved, wire, sourceSchemas, _, err := introspectAll(context.Background(), src, s, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("introspectAll: %v", err)
	}
	for name, m := range map[string]map[string]core.Schema{"resolved": resolved, "wire": wire} {
		col, ok := m["db.users"].Column("user_name")
		if !ok || col.Type.Kind != core.KindString || !col.Type.Nullable {
			t.Fatalf("%s shape lacks the reference column as nullable string: %+v", name, col.Type)
		}
	}
	// The event schema handed to enrich.New is the SOURCE view: the
	// reference destination is not an event column.
	if _, ok := sourceSchemas["db.users"].Column("user_name"); ok {
		t.Fatal("sourceSchemas must not contain the reference destination")
	}
}

// wildcardLoader satisfies enrich.Loader without a database: two columns
// unknown to the config (id, tier) — the shape a select:["*"] reference
// only discovers by actually running the query.
type wildcardLoader struct{}

func (l *wildcardLoader) Load(context.Context) (arrow.RecordBatch, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "tier", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StringBuilder).Append("gold")
	return b.NewRecordBatch(), nil
}
func (l *wildcardLoader) Close() error { return nil }

// #56(a): a wildcard reference's real destination columns must land in
// BOTH wire and resolved before EnsureTable runs, not only after the
// first async refresh. This exercises the exact sequence newRunner now
// runs — introspectAll, then enrich.New + LoadWildcards + AddColumns —
// with a fake loader standing in for the reference query, since
// LoadWildcards's whole point is to run that query synchronously right
// here, before any sink DDL.
func TestWildcardReferenceColumnsLandBeforeEnsureTable(t *testing.T) {
	src := &introspectSource{schema: core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64}},
	}}}
	cfg := spec.Enrich{
		Table:    "users",
		Source:   spec.EnrichSource{URI: "mysql://refdb/internal", Query: "SELECT id, tier FROM users"},
		On:       map[string]string{"user_ref": "id"},
		Select:   []string{"*"},
		JoinType: "left",
	}
	s := &spec.Spec{Tables: []spec.Table{{
		Source: "db.users", Target: "raw.users", Enrich: []spec.Enrich{cfg},
	}}}
	_, resolved, wire, sourceSchemas, _, err := introspectAll(context.Background(), src, s, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("introspectAll: %v", err)
	}
	// Before the fix's loop runs, a wildcard reference contributes nothing
	// — this is the bug #56(a) describes.
	if _, ok := wire["db.users"].Column("users.id"); ok {
		t.Fatal("wildcard columns present before LoadWildcards ran — test setup is wrong")
	}

	// The sequence newRunner's boot now runs, before EnsureTable.
	st, err := enrich.New([]spec.Enrich{cfg}, sourceSchemas["db.users"], slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("enrich.New: %v", err)
	}
	if err := st.UseLoader("users", &wildcardLoader{}); err != nil {
		t.Fatalf("UseLoader: %v", err)
	}
	if err := st.LoadWildcards(context.Background()); err != nil {
		t.Fatalf("LoadWildcards: %v", err)
	}
	dests := st.RefColumns()
	wire["db.users"] = enrich.AddColumns(wire["db.users"], dests)
	resolved["db.users"] = enrich.AddColumns(resolved["db.users"], dests)

	for name, m := range map[string]core.Schema{"resolved": resolved["db.users"], "wire": wire["db.users"]} {
		for _, want := range []string{"users.id", "users.tier"} {
			if _, ok := m.Column(want); !ok {
				t.Fatalf("%s shape missing wildcard-discovered column %q after LoadWildcards", name, want)
			}
		}
	}
}

// A wildcard reference whose first load fails must fail boot loudly,
// naming the reference — never a silent fallback to an empty schema.
func TestWildcardReferenceLoadFailureFailsLoudly(t *testing.T) {
	cfg := spec.Enrich{
		Table:    "users",
		Source:   spec.EnrichSource{URI: "mysql://refdb/internal", Query: "SELECT id, tier FROM users"},
		On:       map[string]string{"user_ref": "id"},
		Select:   []string{"*"},
		JoinType: "left",
	}
	sourceSchema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64}},
	}}
	st, err := enrich.New([]spec.Enrich{cfg}, sourceSchema, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("enrich.New: %v", err)
	}
	if err := st.UseLoader("users", &failingLoader{err: errors.New("connection refused")}); err != nil {
		t.Fatalf("UseLoader: %v", err)
	}
	err = st.LoadWildcards(context.Background())
	if err == nil {
		t.Fatal("LoadWildcards: want error for an unreachable reference, got nil")
	}
	got := err.Error()
	if !strings.Contains(got, "users") || !strings.Contains(got, "connection refused") {
		t.Fatalf("LoadWildcards error %q does not name the reference and the cause", got)
	}
}

type failingLoader struct{ err error }

func (l *failingLoader) Load(context.Context) (arrow.RecordBatch, error) { return nil, l.err }
func (l *failingLoader) Close() error                                    { return nil }

// FT-5: the golden-path integration — a spec with enrich, an empty→hot
// loader, and a runner with fake source/sink producing one batch per phase,
// asserting the schema of the batch WRITTEN to the sink — is not covered
// here. The runner has no fake source/sink harness: newRunner needs a
// streaming source.Reader (Start/Next/Close) and a full sink.Sink, plus the
// snapshot/caught-up machinery; the existing gateCommitter drives a worker
// directly, not newRunner. That is a new harness, not an adaptation.
// Coverage stands on the two halves instead: the wire extension
// (TestIntrospectAllExtendsBothShapesForEnrich) and the seam schema equality
// (enrich.TestEnrichBatchSameSchemaAcrossEmptyAndHotReference).
func TestGoldenPathEnrichRunnerIntegration(t *testing.T) {
	t.Skip("needs a fake streaming source.Reader + sink.Sink harness; tracked in the PR body")
}

// C0 (WK-001): the collapsed runner has a single in-process worker per
// table; workers>1 is a distributed-only contract and must fail at boot
// instead of silently running one worker.
func TestCollapsedRejectsPartitionedTable(t *testing.T) {
	s := &spec.Spec{Tables: []spec.Table{
		{Target: "raw.orders", Workers: &spec.WorkerSpec{Number: 3}},
	}}
	_, err := newRunner(context.Background(), s, Config{}, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "distributed") {
		t.Fatalf("collapsed run with workers>1 must fail citing distributed mode, got: %v", err)
	}
}

// gatedCount reports how many batches the gate currently buffers. Test-only:
// it moved here from runner.go, whose production build has no caller for it.
func (r *relay) gatedCount() int {
	r.gateMu.Lock()
	tgt := r.gateTgt
	r.gateMu.Unlock()
	return r.gate.Len(tgt, 0)
}

// failingReader yields nBatches nil-free batches and then fails, modelling a
// source that breaks mid-stream (a dropped plugin connection, a replication
// slot error) rather than reaching a clean end of stream.
type failingReader struct {
	remaining int
	err       error
}

func (f *failingReader) Next(context.Context) (*dataplane.Batch, error) {
	if f.remaining > 0 {
		f.remaining--

		return nil, nil // no batch, no error: relay treats nil as end-of-stream
	}

	return nil, f.err
}

func (f *failingReader) Start(context.Context, position.Position) error    { return nil }
func (f *failingReader) Synced() position.Position                         { return nil }
func (f *failingReader) Master(context.Context) (position.Position, error) { return nil, nil }
func (f *failingReader) OpenWindow(context.Context, uint32)                {}
func (f *failingReader) ClearWindow()                                      {}
func (f *failingReader) Close()                                            {}
func (f *failingReader) SetConfirmed(func() position.Position)             {}

// A source failure must reach the Runner, not be swallowed as a clean end of
// stream. relay.run pulls batches in a goroutine and signals completion by
// closing the channel, which cannot by itself distinguish "the source ended"
// from "the source broke" — so a discarded Next error would end the pipeline
// as a successful run with silently truncated data.
//
// This is the same failure class as the plugin reader's io.EOF fix one layer
// up: propagating the error there accomplishes nothing if the relay drops it
// here.
func TestRelayPropagatesSourceError(t *testing.T) {
	ingest := make(chan worker.Ingest, 8)
	w := worker.New(worker.Config{MaxRows: 100, MaxInterval: time.Hour})
	r := newRelay(ingest, w, nil)

	wantErr := errors.New("source connection lost mid-stream")
	err := r.run(context.Background(), &failingReader{err: wantErr})

	if err == nil {
		t.Fatal("relay.run = nil on a source failure — a broken stream must not look like a clean end of stream")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("relay.run = %v, want it to wrap %v", err, wantErr)
	}
}

// The clean case must stay clean: a reader that ends by returning a nil
// batch (no error) is a successful end of stream, not a failure.
func TestRelayCleanEndOfStreamIsNotAnError(t *testing.T) {
	ingest := make(chan worker.Ingest, 8)
	w := worker.New(worker.Config{MaxRows: 100, MaxInterval: time.Hour})
	r := newRelay(ingest, w, nil)

	if err := r.run(context.Background(), &failingReader{err: nil}); err != nil {
		t.Fatalf("relay.run = %v on a clean end of stream, want nil", err)
	}
}

// fakeSink records Close for release tests.
type fakeSink struct{ closed bool }

func (f *fakeSink) EnsureTable(context.Context, core.TableRef, core.Schema, []string, core.CastPolicy, dataplane.WriteMode) error {
	return nil
}

func (f *fakeSink) Writer(context.Context, core.TableRef, core.CastPolicy, []core.MetadataColumn) (sink.TableWriter, error) {
	return nil, nil
}

func (f *fakeSink) Position(context.Context, core.TableRef) (string, error) { return "", nil }
func (f *fakeSink) SetProperties(context.Context, core.TableRef, map[string]string) error {
	return nil
}

func (f *fakeSink) Properties(context.Context, core.TableRef) (map[string]string, error) {
	return nil, nil
}

func (f *fakeSink) Close() error { f.closed = true; return nil }

// The collapsed runner owns the sink and must close it on the run path; only
// closing it on startup failure leaks a ClickHouse/Couchbase/plugin
// connection on every run (issue #487).
func TestRunnerReleaseClosesSink(t *testing.T) {
	fs := &fakeSink{}
	r := &Runner{snk: fs, closeQuery: func() {}}
	r.release()
	if !fs.closed {
		t.Fatal("release must close the sink")
	}
}

// a table with decoded-but-uncommitted rows must hold the confirmed
// point back. Before the fix, the table was absent from committedPositions,
// so the minimum advanced past its pending row.
func TestConfirmedPositionHoldsForInFlightTable(t *testing.T) {
	newRunner := func() *Runner {
		return &Runner{
			log:                slog.New(slog.DiscardHandler),
			committedPositions: map[string]position.Position{},
			delivered:          map[string]position.Position{},
		}
	}

	// B delivered a row but never committed: hold at nil, not A's 0/100.
	r := newRunner()
	r.noteDelivered("raw.a", position.MustLSN("0/100"))
	r.updateCommitted("raw.a", position.MustLSN("0/100"))
	r.noteDelivered("raw.b", position.MustLSN("0/90"))
	if got := r.confirmedPosition(); got != nil {
		t.Fatalf("confirmed = %v, want nil: B has an in-flight row and no committed baseline", got)
	}

	// B has an old committed baseline (0/50) and a pending row at 0/90: the
	// confirmed holds at B's committed, not A's 0/100.
	r2 := newRunner()
	r2.noteDelivered("raw.a", position.MustLSN("0/100"))
	r2.updateCommitted("raw.a", position.MustLSN("0/100"))
	r2.updateCommitted("raw.b", position.MustLSN("0/50"))
	r2.noteDelivered("raw.b", position.MustLSN("0/90"))
	if got := r2.confirmedPosition(); got == nil || got.String() != "0/50" {
		t.Fatalf("confirmed = %v, want 0/50 (held by B's pending row)", got)
	}

	// B commits 0/90: everything dispatched is committed, so advance to the
	// last dispatched position.
	r2.updateCommitted("raw.b", position.MustLSN("0/90"))
	if got := r2.confirmedPosition(); got == nil || got.String() != "0/100" {
		t.Fatalf("confirmed = %v, want 0/100 (last dispatched)", got)
	}
}

// a table with nothing in flight must not pin the confirmed point.
func TestConfirmedPositionIdleTableDoesNotPin(t *testing.T) {
	r := &Runner{
		log:                slog.New(slog.DiscardHandler),
		committedPositions: map[string]position.Position{},
		delivered:          map[string]position.Position{},
	}
	r.noteDelivered("busy", position.MustLSN("0/100"))
	r.updateCommitted("busy", position.MustLSN("0/100"))
	r.noteDelivered("idle", position.MustLSN("0/10"))
	r.updateCommitted("idle", position.MustLSN("0/10"))
	if got := r.confirmedPosition(); got == nil || got.String() != "0/100" {
		t.Fatalf("confirmed = %v, want 0/100 (the idle table must not pin it)", got)
	}
}
