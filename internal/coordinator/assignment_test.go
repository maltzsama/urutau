package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// fakeDoGetStream implements the Flight server stream for DoGet tests.
type fakeDoGetStream struct {
	grpc.ServerStream
	ctx    context.Context
	sent   [][]byte
	sentCh chan []byte
	fail   bool // fail every Send
}

func (s *fakeDoGetStream) Send(d *flight.FlightData) error {
	if s.fail {
		return errors.New("send failed")
	}
	body := append([]byte(nil), d.GetDataBody()...)
	s.sent = append(s.sent, body)
	if s.sentCh != nil {
		s.sentCh <- body
	}
	return nil
}

func (s *fakeDoGetStream) Context() context.Context { return s.ctx }

// TestDoGetRedeliversSentOnSendFailure covers audit #2 and issue #235: a
// batch popped from the queue must survive a failed Flight Send — it stays on
// the sent list, and the next DoGet redelivers it BEFORE draining the queue,
// so FIFO order is preserved.
func TestDoGetRedeliversSentOnSendFailure(t *testing.T) {
	c := &Coordinator{byTicket: map[string]*workerState{}}
	w := &workerState{name: "w", queue: make(chan queuedBatch, 2)}
	w.queue <- queuedBatch{id: 1, body: []byte("b1")}
	w.queue <- queuedBatch{id: 2, body: []byte("b2")}
	c.byTicket["t"] = w
	srv := &flightServer{c: c}
	ticket := &flight.Ticket{Ticket: []byte("t")}

	// First DoGet: the first Send fails; the popped batch must move to the
	// sent list, never vanish.
	s1 := &fakeDoGetStream{ctx: context.Background(), fail: true}
	if err := srv.DoGet(ticket, s1); err == nil {
		t.Fatal("first DoGet should fail on the failed Send")
	}
	if len(s1.sent) != 0 {
		t.Fatalf("first DoGet sent %d batches, want 0", len(s1.sent))
	}
	w.sentMu.Lock()
	got := append([]queuedBatch(nil), w.sent...)
	w.sentMu.Unlock()
	if len(got) != 1 || string(got[0].body) != "b1" {
		t.Fatalf("sent list = %v, want [b1]", got)
	}

	// Second DoGet: redelivers the sent list before the queue, then b2, in
	// order.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s2 := &fakeDoGetStream{ctx: ctx, sentCh: make(chan []byte, 4)}
	done := make(chan error, 1)
	go func() { done <- srv.DoGet(ticket, s2) }()

	var bodies []string
	for i := 0; i < 2; i++ {
		select {
		case b := <-s2.sentCh:
			bodies = append(bodies, string(b))
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for batch %d", i)
		}
	}
	if bodies[0] != "b1" || bodies[1] != "b2" {
		t.Fatalf("second DoGet delivered %v, want [b1 b2]", bodies)
	}
	cancel()
	<-done

	// Both stay on the sent list until an ack covers them (dropSent).
	w.sentMu.Lock()
	n := len(w.sent)
	w.sentMu.Unlock()
	if n != 2 {
		t.Fatalf("sent list has %d entries, want 2 (cleared only on ack)", n)
	}
}

// TestDropSentPrunesAcked covers issue #235: the ack that pops an index batch
// removes exactly that batch from the sent list, so an acked batch is never
// redelivered.
func TestDropSentPrunesAcked(t *testing.T) {
	w := &workerState{sent: []queuedBatch{{id: 1}, {id: 2}, {id: 3}}}
	w.dropSent([]uint64{1, 3})
	if len(w.sent) != 1 || w.sent[0].id != 2 {
		t.Fatalf("sent = %+v, want only id 2", w.sent)
	}
}

// TestWaitWorkersRequiresAllAttached covers audit #4: ready signals alone
// must not satisfy the wait — a flapping worker that attaches, dies, and
// reattaches inside the window counts as one worker, and the wait still
// fails when another group never connects.
func TestWaitWorkersRequiresAllAttached(t *testing.T) {
	c := &Coordinator{
		workers: map[string]*workerState{
			"a": {name: "a"},
			"b": {name: "b"},
		},
		ready: make(chan struct{}, 8),
	}
	done := make(chan error, 1)
	go func() { done <- c.waitWorkers(context.Background(), 50*time.Millisecond) }()

	// Flapper "a" attaches (twice) — two ready signals — while "b" never
	// connects. The wait must still time out.
	c.mu.Lock()
	c.workers["a"].attached = true
	c.mu.Unlock()
	c.ready <- struct{}{}
	c.ready <- struct{}{}

	select {
	case <-time.After(2 * time.Second):
		t.Fatal("waitWorkers should have returned")
	case err := <-done:
		if err == nil {
			t.Fatal("expected a timeout, got nil")
		}
	}

	// Now "b" attaches too: the wait succeeds.
	c.mu.Lock()
	c.workers["b"].attached = true
	c.mu.Unlock()
	done2 := make(chan error, 1)
	go func() { done2 <- c.waitWorkers(context.Background(), 200*time.Millisecond) }()
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("waitWorkers should have returned once all attached")
	case err := <-done2:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	}
}

// TestAssignmentEpochMatchesWorker covers audit #6: the assignment must
// carry the worker's CURRENT epoch (not a hardcoded 1), or every ready
// Hello's committed map is silently dropped.
func TestAssignmentEpochMatchesWorker(t *testing.T) {
	c := &Coordinator{
		cfg: Config{Spec: &spec.Spec{Source: spec.Source{Kind: "mysql"}}},
	}
	w := &workerState{name: "w", epoch: 2}
	msg, err := c.assignmentFor(w)
	if err != nil {
		t.Fatalf("assignmentFor: %v", err)
	}
	if got := msg.GetAssign().Epoch; got != 2 {
		t.Fatalf("assignment epoch = %d, want 2", got)
	}
}

// TestAssignmentCarriesCastAndMode covers audits #8/#10: the per-table write
// mode and the cast policy must travel with the assignment so the worker's
// collapse/DDL agrees with the coordinator's.
func TestAssignmentCarriesCastAndMode(t *testing.T) {
	c := &Coordinator{
		cfg: Config{Spec: &spec.Spec{
			Source: spec.Source{Kind: "mysql"},
			Tables: []spec.Table{{
				Source:    "shop.a",
				Target:    "lake.a",
				WriteMode: spec.WriteModeAppend,
				Cast:      map[string]string{"geom": "string"},
			}},
		}},
		canonical: map[string]core.Schema{
			"shop.a": {
				Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
			},
		},
	}
	w := &workerState{
		name: "w",
		refs: []source.TableRef{{Source: "shop.a", Target: "lake.a"}},
	}
	msg, err := c.assignmentFor(w)
	if err != nil {
		t.Fatalf("assignmentFor: %v", err)
	}
	ta := msg.GetAssign().Tables[0]
	if ta.WriteMode != pbWriteModeAppend {
		t.Fatalf("write mode = %v, want append", ta.WriteMode)
	}
	if len(ta.CastPolicy) == 0 {
		t.Fatal("cast policy missing from assignment")
	}
	var cast core.CastPolicy
	if err := json.Unmarshal(ta.CastPolicy, &cast); err != nil {
		t.Fatalf("unmarshal cast: %v", err)
	}
	if _, ok := cast.Columns["geom"]; !ok {
		t.Fatalf("cast policy columns = %v, want geom", cast.Columns)
	}
}

// pbWriteModeAppend is the wire enum append value, kept local to avoid
// importing the pb package in this test file.
const pbWriteModeAppend = 2

// TestAssignmentCarriesSourceProjection covers #162/#163: the source column
// projection and the structured filter must travel with the assignment so the
// worker's snapshot chunk SELECT matches the coordinator's.
func TestAssignmentCarriesSourceProjection(t *testing.T) {
	c := &Coordinator{
		cfg: Config{Spec: &spec.Spec{
			Source: spec.Source{Kind: "postgres"},
			Tables: []spec.Table{{
				Source:       "public.orders",
				Target:       "lake.orders",
				PrimaryKey:   []string{"id"},
				ColumnFilter: []string{"id", "v"},
				Filter:       &spec.Filter{Predicate: &spec.Predicate{Column: "active", Op: spec.OpEq, Value: true}},
			}},
		}},
		canonical: map[string]core.Schema{
			"public.orders": {
				Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
			},
		},
	}
	w := &workerState{
		name: "w",
		refs: []source.TableRef{{Source: "public.orders", Target: "lake.orders", PrimaryKey: []string{"id"}}},
	}
	msg, err := c.assignmentFor(w)
	if err != nil {
		t.Fatalf("assignmentFor: %v", err)
	}
	ta := msg.GetAssign().Tables[0]
	if len(ta.ColumnFilter) != 2 || ta.ColumnFilter[0] != "id" {
		t.Fatalf("column filter = %v, want [id v]", ta.ColumnFilter)
	}
	var f spec.Filter
	if err := json.Unmarshal(ta.Filter, &f); err != nil {
		t.Fatalf("unmarshal filter: %v", err)
	}
	if f.Predicate == nil || f.Predicate.Column != "active" {
		t.Fatalf("filter = %+v, want a predicate on active", f)
	}
}

// TestAssignmentTickLockOrderNoDeadlock is a smoke test that the supervisor
// tick and a concurrent Session attach do not deadlock (audit #3): attach
// takes c.mu then supervisor.mu; tick takes c.mu, releases, then takes
// supervisor.mu. Exercising both concurrently under -race would catch an
// inversion.
func TestAssignmentTickLockOrderNoDeadlock(t *testing.T) {
	c := &Coordinator{
		workers: map[string]*workerState{"w": {name: "w"}},
		log:     slog.New(slog.DiscardHandler),
	}
	s := newSupervisor(c)
	c.supervisor = s // resetWorker reaches it via c.supervisor
	// The worker has already acked, so the race between attached=true and
	// noteAttach never reads it as stale — without this, the tight tick loop
	// can drive resetWorker on an uninitialized Coordinator and panic.
	s.noteAck("w", time.Now())
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				if err := s.tick(time.Now(), SupervisorConfig{AckTimeout: time.Hour}); err != nil {
					t.Errorf("tick: %v", err)
				}
			}
		}
	}()
	// Concurrent noteAttach: takes c.mu then supervisor.mu.
	for i := 0; i < 100; i++ {
		c.mu.Lock()
		c.workers["w"].attached = true
		c.mu.Unlock()
		s.noteAttach("w")
	}
	close(stop)
	<-done
}

// TestAssignmentTicketRandom covers audit #3: tickets must not be the
// deterministic "urutau/<name>" guess.
func TestAssignmentTicketRandom(t *testing.T) {
	t1 := randTicket()
	t2 := randTicket()
	if len(t1) != 32 || len(t2) != 32 {
		t.Fatalf("ticket lengths = %d/%d, want 32", len(t1), len(t2))
	}
	if string(t1) == string(t2) {
		t.Fatal("two tickets must differ")
	}
	if string(t1) == "urutau/" {
		t.Fatal("ticket must not be deterministic")
	}
}

// TestDoGetRedeliversUnackedAfterSuccess covers issue #235: a batch whose Send
// succeeded but whose ack was lost (session died) is redelivered to the next
// session, not dropped.
func TestDoGetRedeliversUnackedAfterSuccess(t *testing.T) {
	c := &Coordinator{byTicket: map[string]*workerState{}}
	w := &workerState{name: "w", queue: make(chan queuedBatch, 1)}
	w.queue <- queuedBatch{id: 7, body: []byte("b1")}
	c.byTicket["t"] = w
	srv := &flightServer{c: c}
	ticket := &flight.Ticket{Ticket: []byte("t")}

	ctx1, cancel1 := context.WithCancel(context.Background())
	s1 := &fakeDoGetStream{ctx: ctx1, sentCh: make(chan []byte, 4)}
	done1 := make(chan error, 1)
	go func() { done1 <- srv.DoGet(ticket, s1) }()
	select {
	case <-s1.sentCh:
	case <-time.After(2 * time.Second):
		t.Fatal("first DoGet did not deliver b1")
	}
	cancel1()
	<-done1

	// New session: b1 is redelivered even though its Send succeeded, because
	// no ack covered it.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	s2 := &fakeDoGetStream{ctx: ctx2, sentCh: make(chan []byte, 4)}
	done2 := make(chan error, 1)
	go func() { done2 <- srv.DoGet(ticket, s2) }()
	select {
	case b := <-s2.sentCh:
		if string(b) != "b1" {
			t.Fatalf("redelivered %q, want b1", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an unacked batch was not redelivered on reconnect")
	}
	cancel2()
	<-done2
}
