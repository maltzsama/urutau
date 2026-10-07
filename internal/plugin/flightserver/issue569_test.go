package flightserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

type resumePos struct{ offset string }

func (p resumePos) String() string { return p.offset }
func (p resumePos) Compare(other position.Position) int {
	if o, ok := other.(resumePos); ok && o.offset == p.offset {
		return 0
	}
	return position.Incomparable
}
func (p resumePos) Contains(other position.Position) bool {
	o, ok := other.(resumePos)
	return ok && o.offset == p.offset
}

// resumeSource records the position its reader was started from.
type resumeSource struct {
	ref     core.TableRef
	schema  core.Schema
	started position.Position
}

func (s *resumeSource) Open(context.Context, []core.TableRef) (source.Reader, error) {
	return &resumeReader{src: s}, nil
}
func (s *resumeSource) InitialPosition(context.Context) (position.Position, error) { return nil, nil }
func (s *resumeSource) ParsePosition(v string) (position.Position, error) {
	return resumePos{offset: v}, nil
}
func (s *resumeSource) Introspect(context.Context, spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	return s.ref, s.schema, nil, nil
}

type resumeReader struct{ src *resumeSource }

func (r *resumeReader) Start(_ context.Context, from position.Position) error {
	r.src.started = from
	return nil
}
func (r *resumeReader) Next(context.Context) (*dataplane.Batch, error)    { return nil, nil }
func (r *resumeReader) Close()                                            {}
func (r *resumeReader) SetConfirmed(func() position.Position)             {}
func (r *resumeReader) Synced() position.Position                         { return nil }
func (r *resumeReader) Master(context.Context) (position.Position, error) { return nil, nil }
func (r *resumeReader) OpenWindow(context.Context, uint32)                {}
func (r *resumeReader) ClearWindow()                                      {}

type fakeDoGet struct {
	grpc.ServerStream
	ctx  context.Context
	sent int
}

func (f *fakeDoGet) Context() context.Context      { return f.ctx }
func (f *fakeDoGet) Send(*flight.FlightData) error { f.sent++; return nil }

// GetFlightInfo must carry FromOffset into the ticket, and DoGet must resume
// the wrapped source from it (issue #569).
func TestDoGetResumesFromTicketOffset(t *testing.T) {
	cs := core.Schema{Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}}}
	src := &resumeSource{ref: core.TableRef{Source: "s.t", Target: "d.t"}, schema: cs}
	srv := NewSourceServer(src, &spec.Spec{Tables: []spec.Table{{Source: "s.t"}}}, source.Runtime{})

	desc, _ := json.Marshal(contract.GetFlightInfoRequest{Table: "s.t", Mode: "changes", FromOffset: "off-9"})
	info, err := srv.GetFlightInfo(context.Background(), &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: desc})
	if err != nil {
		t.Fatalf("GetFlightInfo: %v", err)
	}
	var tp ticketPayload
	if err := json.Unmarshal(info.Endpoint[0].Ticket.Ticket, &tp); err != nil {
		t.Fatalf("ticket: %v", err)
	}
	if tp.FromOffset != "off-9" {
		t.Fatalf("ticket FromOffset = %q, want off-9", tp.FromOffset)
	}

	stream := &fakeDoGet{ctx: context.Background()}
	if err := srv.DoGet(info.Endpoint[0].Ticket, stream); err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	if src.started == nil || src.started.String() != "off-9" {
		t.Fatalf("reader Start from = %v, want off-9", src.started)
	}
}

// fakeSink records the schema/key EnsureTable received.
type fakeSink struct {
	ensured core.TableRef
	schema  core.Schema
	pos     string
}

func (s *fakeSink) EnsureTable(_ context.Context, ref core.TableRef, schema core.Schema, _ []string, _ core.CastPolicy, _ dataplane.WriteMode) error {
	s.ensured, s.schema = ref, schema
	return nil
}
func (s *fakeSink) Writer(context.Context, core.TableRef, core.CastPolicy, []core.MetadataColumn) (sink.TableWriter, error) {
	return nil, errors.New("unused")
}
func (s *fakeSink) Position(context.Context, core.TableRef) (string, error) { return s.pos, nil }
func (s *fakeSink) SetProperties(context.Context, core.TableRef, map[string]string) error {
	return nil
}
func (s *fakeSink) Properties(context.Context, core.TableRef) (map[string]string, error) {
	return nil, nil
}
func (s *fakeSink) Close() error { return nil }

type fakeDoAction struct {
	grpc.ServerStream
	ctx     context.Context
	results []*flight.Result
}

func (f *fakeDoAction) Context() context.Context    { return f.ctx }
func (f *fakeDoAction) Send(r *flight.Result) error { f.results = append(f.results, r); return nil }

// The sink server must accept the announced schema/key and answer the position
// action (issue #569).
func TestSinkServerEnsureTableAndPosition(t *testing.T) {
	fs := &fakeSink{pos: "off-7"}
	srv := NewSinkServer(fs)
	cs := core.Schema{Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}}}

	body, _ := json.Marshal(contract.EnsureTableRequest{Table: "d.t", Schema: cs, PrimaryKey: []string{"id"}, Mode: "upsert"})
	if err := srv.DoAction(&flight.Action{Type: "urutau.ensure_table", Body: body}, &fakeDoAction{ctx: context.Background()}); err != nil {
		t.Fatalf("ensure_table: %v", err)
	}
	if fs.ensured.Target != "d.t" || len(fs.ensured.PrimaryKey) != 1 || fs.ensured.PrimaryKey[0] != "id" {
		t.Fatalf("ensure ref = %+v", fs.ensured)
	}
	if len(fs.schema.Columns) != 1 {
		t.Fatalf("schema not delivered: %+v", fs.schema)
	}
	if got := srv.refFor("d.t"); len(got.PrimaryKey) != 1 {
		t.Fatalf("the announced key was not retained for the writer: %+v", got)
	}

	pbody, _ := json.Marshal(contract.PositionRequest{Table: "d.t"})
	out := &fakeDoAction{ctx: context.Background()}
	if err := srv.DoAction(&flight.Action{Type: "urutau.position", Body: pbody}, out); err != nil {
		t.Fatalf("position: %v", err)
	}
	var resp contract.PositionResponse
	if err := json.Unmarshal(out.results[0].Body, &resp); err != nil {
		t.Fatalf("position response: %v", err)
	}
	if resp.Position != "off-7" {
		t.Fatalf("position = %q, want off-7", resp.Position)
	}
}
