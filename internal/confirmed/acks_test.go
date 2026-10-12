package confirmed

import (
	"errors"
	"reflect"
	"testing"

	"github.com/maltzsama/urutau/position"
)

// fakeAckHost records what an ack released.
type fakeAckHost struct {
	staged     map[string]bool
	epoch      map[string]uint64
	noWindow   map[string]bool
	released   []string
	markers    []uint64
	parseFails bool
}

func (h *fakeAckHost) ParsePosition(s string) (position.Position, error) {
	if h.parseFails {
		return nil, errors.New("bad position")
	}
	return position.MustLSN(s), nil
}
func (h *fakeAckHost) Staged(table string) bool { return h.staged[table] }
func (h *fakeAckHost) Superseded(worker string, epoch uint64) bool {
	return h.epoch[worker] != epoch
}
func (h *fakeAckHost) ReleaseMarker(_ string, id uint64) { h.markers = append(h.markers, id) }
func (h *fakeAckHost) Release(worker, table string, pos position.Position) bool {
	if h.noWindow[worker] {
		return false
	}
	h.released = append(h.released, worker+" "+table+" "+pos.String())
	return true
}

// On a table the worker commits itself an ack releases the batches and
// records the position, whatever generation sent it.
func TestOnAckRecordsADirectCommit(t *testing.T) {
	h := &fakeAckHost{epoch: map[string]uint64{"w0": 3}}
	var p Positions
	out, err := p.OnAck(h, "w0", Ack{Table: "raw.a", Position: "0/30", Epoch: 1})
	if out != AckCommitted || err != nil {
		t.Fatalf("outcome = %v, %v; want committed", out, err)
	}
	if !reflect.DeepEqual(h.released, []string{"w0 raw.a 0/30"}) {
		t.Fatalf("released = %v", h.released)
	}
	if got := p.Of("w0"); got == nil || got.String() != "0/30" {
		t.Fatalf("confirmed = %v, want 0/30", got)
	}
}

// On a staged table the ack releases the batches but records nothing: the
// coordinator records the position once the cycle is durable.
func TestOnAckOfAStagedTableRecordsNoPosition(t *testing.T) {
	h := &fakeAckHost{staged: map[string]bool{"raw.a": true}, epoch: map[string]uint64{"w0": 1}}
	var p Positions
	out, err := p.OnAck(h, "w0", Ack{Table: "raw.a", Position: "0/30", Epoch: 1})
	if out != AckCommitted || err != nil || len(h.released) != 1 {
		t.Fatalf("outcome = %v, %v, released %v; want committed and released", out, err, h.released)
	}
	if p.Has("w0") {
		t.Fatalf("a staged table's ack recorded %v: its commit is not durable yet", p.Of("w0"))
	}
}

// A superseded generation's ack on a staged table is dropped, position or
// marker: its staged delivery was dropped too, and what it would release
// must be redelivered (#855).
func TestOnAckDropsAStaleGenerationOnAStagedTable(t *testing.T) {
	h := &fakeAckHost{staged: map[string]bool{"raw.a": true}, epoch: map[string]uint64{"w0": 2}}
	var p Positions
	if out, _ := p.OnAck(h, "w0", Ack{Table: "raw.a", Position: "0/30", Epoch: 1}); out != AckStale {
		t.Fatalf("position ack: outcome = %v, want stale", out)
	}
	if out, _ := p.OnAck(h, "w0", Ack{Table: "raw.a", MarkerID: 191, Epoch: 1}); out != AckStaleMarker {
		t.Fatalf("marker ack: outcome = %v, want stale marker", out)
	}
	if len(h.released) != 0 || len(h.markers) != 0 {
		t.Fatalf("a stale ack released %v and markers %v", h.released, h.markers)
	}
	if out, _ := p.OnAck(h, "w0", Ack{Table: "raw.a", MarkerID: 191, Epoch: 2}); out != AckMarker || !reflect.DeepEqual(h.markers, []uint64{191}) {
		t.Fatalf("current generation's marker ack: outcome = %v, markers %v", out, h.markers)
	}
	// A direct table's marker is released whatever the generation.
	if out, _ := p.OnAck(h, "w0", Ack{Table: "raw.direct", MarkerID: 7, Epoch: 1}); out != AckMarker {
		t.Fatalf("direct table's marker ack: outcome = %v, want released", out)
	}
}

func TestOnAckUnparsableAndWindowless(t *testing.T) {
	var p Positions
	out, err := p.OnAck(&fakeAckHost{parseFails: true}, "w0", Ack{Table: "raw.a", Position: "junk"})
	if out != AckUnparsable || err == nil {
		t.Fatalf("outcome = %v, %v; want unparsable with an error", out, err)
	}
	h := &fakeAckHost{noWindow: map[string]bool{"w0": true}, epoch: map[string]uint64{"w0": 0}}
	if out, _ := p.OnAck(h, "w0", Ack{Table: "raw.a", Position: "0/30"}); out != AckNoWindow || p.Has("w0") {
		t.Fatalf("outcome = %v, recorded %v; want no window and nothing recorded", out, p.Has("w0"))
	}
}

// A worker owes work with a batch queued, one in flight, or a table buffered
// for it before the send; in-flight is not read for a worker with a queue.
func TestOwing(t *testing.T) {
	var asked []string
	inFlight := func(w string) int {
		asked = append(asked, w)
		if w == "flying" {
			return 2
		}
		return 0
	}
	got := Owing([]Load{{Name: "queued", Queued: true}, {Name: "flying"}, {Name: "idle"}, {Name: "gated"}}, inFlight, []string{"gated"})
	want := map[string]bool{"queued": true, "flying": true, "gated": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("owing = %v, want %v", got, want)
	}
	for _, w := range asked {
		if w == "queued" {
			t.Fatal("in-flight was read for a worker that already owes its queue")
		}
	}
}
