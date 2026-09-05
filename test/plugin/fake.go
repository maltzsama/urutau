// Package plugin is a proof that the door is open: it implements a source
// and a sink entirely outside internal/, importing only the public
// contracts (source, sink, driver, core, change, position, spec) and
// registering them with the driver registry. The fake_test drives the
// collapsed runner end-to-end against these two drivers.
package plugin

import (
	"context"
	"strconv"
	"sync"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// fakePos is a trivial monotonic position, enough to satisfy the contract.
type fakePos uint64

func (p fakePos) String() string { return strconv.FormatUint(uint64(p), 10) }

func (p fakePos) Compare(o position.Position) int {
	q, ok := o.(fakePos)
	if !ok {
		panic("fake: incompatible position")
	}
	switch {
	case p < q:
		return -1
	case p > q:
		return 1
	default:
		return 0
	}
}

func (p fakePos) Contains(o position.Position) bool {
	q, ok := o.(fakePos)
	if !ok {
		panic("fake: incompatible position")
	}
	return p >= q
}

// records captures every committed batch. The mutex makes the test's reads
// race-free against the worker's commits.
type records struct {
	mu      sync.Mutex
	upserts map[string][]change.Change
	deletes map[string][]change.Change
}

func newRecords() *records {
	return &records{
		upserts: map[string][]change.Change{},
		deletes: map[string][]change.Change{},
	}
}

func (r *records) commit(b change.Batch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upserts[b.Table] = append(r.upserts[b.Table], b.Upserts...)
	r.deletes[b.Table] = append(r.deletes[b.Table], b.Deletes...)
}

func (r *records) rows(target string) []change.Change {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]change.Change, 0, len(r.upserts[target])+len(r.deletes[target]))
	out = append(out, r.upserts[target]...)
	out = append(out, r.deletes[target]...)
	return out
}

// seedRows are the changes the fake source streams; set before a run.
var seedRows []change.Change

// committed is the sink singleton the runner writes into and the test reads.
var committed = newRecords()

func init() {
	driver.RegisterSource("fake", source.Capabilities{Stream: true}, func(*spec.Spec, source.Runtime) (source.Source, error) {
		return Source{}, nil
	})
	driver.RegisterSink("fake", func(context.Context, sink.Config) (sink.Sink, error) {
		return &Sink{rec: committed}, nil
	})
}

// Source is a stream-only fake: it emits seedRows and holds the stream open
// until the pipeline cancels it.
type Source struct{}

var _ source.Source = Source{}

func (Source) Introspect(_ context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: t.PrimaryKey,
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: t.PrimaryKey}, cs, nil, nil
}

func (Source) InitialPosition(context.Context) (position.Position, error) { return fakePos(0), nil }

func (Source) ParsePosition(s string) (position.Position, error) {
	if s == "" {
		return fakePos(0), nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, err
	}
	return fakePos(n), nil
}

func (Source) Open(_ context.Context, _ []source.TableRef) (source.Reader, error) {
	return reader{out: make(chan change.Change, len(seedRows)+1)}, nil
}

type reader struct {
	out chan change.Change
}

var _ source.Reader = reader{}

// Stream emits the seeded rows, then holds open until ctx is cancelled —
// the same shape a real source's stream has.
func (r reader) Stream(ctx context.Context, _ position.Position) (<-chan change.Change, <-chan error) {
	errCh := make(chan error, 1)
	go func() {
		for _, c := range seedRows {
			select {
			case r.out <- c:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
		<-ctx.Done()
		errCh <- ctx.Err()
	}()
	return r.out, errCh
}

func (r reader) Synced() position.Position                         { return fakePos(0) }
func (r reader) Master(context.Context) (position.Position, error) { return fakePos(0), nil }
func (r reader) OpenWindow(context.Context, uint32)                {}
func (r reader) ClearWindow()                                      {}
func (r reader) Close()                                            {}
func (r reader) SetConfirmed(func() position.Position)             {}

// Sink records committed batches in the shared records singleton.
type Sink struct {
	rec *records
}

var _ sink.Sink = &Sink{}

func (s *Sink) EnsureTable(context.Context, core.TableRef, core.Schema, []string, core.CastPolicy) error {
	return nil
}

func (s *Sink) Writer(context.Context, core.TableRef, core.CastPolicy, []core.MetadataColumn) (sink.TableWriter, error) {
	return writer{s: s}, nil
}

func (s *Sink) Position(context.Context, core.TableRef) (string, error) { return "", nil }
func (s *Sink) SetProperties(context.Context, core.TableRef, map[string]string) error {
	return nil
}
func (s *Sink) Properties(context.Context, core.TableRef) (map[string]string, error) {
	return nil, nil
}
func (s *Sink) Close() error { return nil }

type writer struct {
	s *Sink
}

var _ sink.TableWriter = writer{}

func (w writer) Commit(_ context.Context, b change.Batch) error {
	w.s.rec.commit(b)
	return nil
}

func (w writer) Close() error { return nil }
