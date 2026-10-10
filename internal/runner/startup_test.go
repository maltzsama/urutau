package runner

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// assertNoGoroutineLeak waits for the goroutine count to fall back to the
// pre-boot baseline, failing if setup left a worker, relay, maintenance or
// watch goroutine running.
func assertNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		after := runtime.NumGoroutine()
		if after <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines leaked: %d before newRunner, %d after", before, after)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startupSource is a fake source.Source + QuerySource driving newRunner to a
// chosen failure: it hands out reader, resolves initialPosition, and records
// CloseQuery calls.
type startupSource struct {
	reader     source.Reader
	initPos    position.Position
	initErr    error
	ref        core.TableRef
	schema     core.Schema
	queryClose atomic.Int32
}

func (s *startupSource) Open(context.Context, []source.TableRef) (source.Reader, error) {
	return s.reader, nil
}

func (s *startupSource) InitialPosition(context.Context) (position.Position, error) {
	return s.initPos, s.initErr
}

func (s *startupSource) ParsePosition(string) (position.Position, error) {
	return position.MustLSN("0/1"), nil
}

func (s *startupSource) Introspect(context.Context, spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	return s.ref, s.schema, nil, nil
}

func (s *startupSource) NewChunker(string, string, int) (source.ChunkSource, error) {
	return nil, errors.New("startup test: no chunker")
}

func (s *startupSource) CloseQuery() error {
	s.queryClose.Add(1)
	return nil
}

// startupReader is a fake source.Reader recording Close; Start fails when
// startErr is set so newRunner's Start path is exercised.
type startupReader struct {
	startErr error
	closes   atomic.Int32
}

func (r *startupReader) Start(context.Context, position.Position) error { return r.startErr }
func (r *startupReader) Next(ctx context.Context) (*dataplane.Batch, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (r *startupReader) Close()                                            { r.closes.Add(1) }
func (r *startupReader) Synced() position.Position                         { return nil }
func (r *startupReader) Master(context.Context) (position.Position, error) { return nil, nil }
func (r *startupReader) OpenWindow(context.Context, uint32)                {}
func (r *startupReader) ClearWindow()                                      {}
func (r *startupReader) SetConfirmed(func() position.Position)             {}

// startupSink is a sink.Sink recording EnsureTable and Close. It deliberately
// does NOT implement sink.Maintainable, so a spec with maintenance enabled
// fails the capability gate.
type startupSink struct {
	mu     sync.Mutex
	ensur  int
	closed int
}

func (s *startupSink) EnsureTable(context.Context, core.TableRef, core.Schema, []string, core.CastPolicy, dataplane.WriteMode) error {
	s.mu.Lock()
	s.ensur++
	s.mu.Unlock()
	return nil
}

func (s *startupSink) Writer(context.Context, core.TableRef, core.CastPolicy, []core.MetadataColumn) (sink.TableWriter, error) {
	return nil, nil
}

func (s *startupSink) Position(context.Context, core.TableRef) (string, error) { return "", nil }
func (s *startupSink) SetProperties(context.Context, core.TableRef, map[string]string) error {
	return nil
}
func (s *startupSink) Properties(context.Context, core.TableRef) (map[string]string, error) {
	return nil, nil
}
func (s *startupSink) Close() error {
	s.mu.Lock()
	s.closed++
	s.mu.Unlock()
	return nil
}
func (s *startupSink) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func startupTestSpec() *spec.Spec {
	return &spec.Spec{Tables: []spec.Table{{Source: "db.users", Target: "raw.users"}}}
}

func startupTestSource(rdr source.Reader, initErr error) *startupSource {
	return &startupSource{
		reader:  rdr,
		initPos: position.MustLSN("0/1"),
		initErr: initErr,
		ref:     core.TableRef{Source: "db.users", Target: "raw.users", PrimaryKey: []string{"id"}},
		schema: core.Schema{Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		}},
	}
}

// A failure resolving the initial position must release the sink, the query
// connection, the reader and stop the worker goroutine — the old newRunner
// returned without any of that, leaking all four.
func TestStartupReleasesResourcesWhenInitialPositionFails(t *testing.T) {
	before := runtime.NumGoroutine()
	rdr := &startupReader{}
	src := startupTestSource(rdr, errors.New("no slot"))
	snk := &startupSink{}

	_, err := newRunner(context.Background(), startupTestSpec(), Config{MaxRows: 100, MaxInterval: time.Hour}, src, snk, nil)
	if err == nil || !strings.Contains(err.Error(), "initial position") {
		t.Fatalf("newRunner = %v, want initial-position error", err)
	}
	if got := snk.closeCount(); got != 1 {
		t.Fatalf("sink Close called %d times, want 1", got)
	}
	if got := src.queryClose.Load(); got != 1 {
		t.Fatalf("CloseQuery called %d times, want 1", got)
	}
	if got := rdr.closes.Load(); got != 1 {
		t.Fatalf("reader Close called %d times, want 1", got)
	}
	assertNoGoroutineLeak(t, before)
}

// A failure starting the stream must release the same resources; the relay is
// never launched, but the worker and reader are already live.
func TestStartupReleasesResourcesWhenReaderStartFails(t *testing.T) {
	before := runtime.NumGoroutine()
	rdr := &startupReader{startErr: errors.New("slot in use")}
	src := startupTestSource(rdr, nil)
	snk := &startupSink{}

	_, err := newRunner(context.Background(), startupTestSpec(), Config{MaxRows: 100, MaxInterval: time.Hour}, src, snk, nil)
	if err == nil || !strings.Contains(err.Error(), "start stream") {
		t.Fatalf("newRunner = %v, want start-stream error", err)
	}
	if got := snk.closeCount(); got != 1 {
		t.Fatalf("sink Close called %d times, want 1", got)
	}
	if got := src.queryClose.Load(); got != 1 {
		t.Fatalf("CloseQuery called %d times, want 1", got)
	}
	if got := rdr.closes.Load(); got != 1 {
		t.Fatalf("reader Close called %d times, want 1", got)
	}
	assertNoGoroutineLeak(t, before)
}

// maintenance enabled on a sink without the capability fails the boot; the
// sink and query connection acquired before the gate must still be released.
func TestStartupReleasesResourcesWhenMaintenanceUnsupported(t *testing.T) {
	before := runtime.NumGoroutine()
	s := startupTestSpec()
	s.Sink.Type = "clickhouse"
	s.Sink.Maintenance = &spec.Maintenance{Enabled: true}
	src := startupTestSource(&startupReader{}, nil)
	snk := &startupSink{}

	_, err := newRunner(context.Background(), s, Config{MaxRows: 100, MaxInterval: time.Hour}, src, snk, nil)
	if err == nil || !strings.Contains(err.Error(), "maintenance") {
		t.Fatalf("newRunner = %v, want maintenance-capability error", err)
	}
	if got := snk.closeCount(); got != 1 {
		t.Fatalf("sink Close called %d times, want 1", got)
	}
	if got := src.queryClose.Load(); got != 1 {
		t.Fatalf("CloseQuery called %d times, want 1", got)
	}
	assertNoGoroutineLeak(t, before)
}

// The startup stack releases in reverse acquisition order, so a resource is
// never torn down before a later-acquired one that may depend on it.
func TestStartupRunsClosersInReverse(t *testing.T) {
	var order []string
	st := &startup{}
	st.add(func() { order = append(order, "a") })
	st.add(func() { order = append(order, "b") })
	st.add(func() { order = append(order, "c") })
	st.run()
	if got := strings.Join(order, ","); got != "c,b,a" {
		t.Fatalf("closers ran %q, want c,b,a", got)
	}
}
