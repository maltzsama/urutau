package coordinator

// Coordinator runtime correctness guards: session end, snapshot wait, supervisor reset, flow and retention invariants.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// A stalled worker with unacked batches is reset (issue #461): the batches
// are redelivered on reconnect (issue #235), and on an upsert table the
// worker skips what it committed and re-applies the rest idempotently.
func TestSupervisorResetsAStalledWorkerWithInFlight(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	w := workers["w1"]
	cancelled := false
	w.cancel = func() { cancelled = true }
	c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	c.index["w1"].add(inflightBatch{id: 1, table: "t"})
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	if err := s.tick(time.Now(), SupervisorConfig{AckTimeout: 30 * time.Second}); err != nil {
		t.Fatalf("tick: %v; a stalled upsert worker must be reset", err)
	}
	if !cancelled || w.epoch != 1 {
		t.Fatalf("cancelled=%v epoch=%d; want the worker reset", cancelled, w.epoch)
	}
}

// On an append table a redelivered batch that was committed before its ack
// was lost would be appended twice (issue #235): a stalled append worker with
// unacked batches ends the run instead.
func TestSupervisorEndsTheRunForAStalledAppendWorkerWithInFlight(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	workers["w1"].refs = []source.TableRef{{Source: "shop.t", Target: "raw.t"}}
	c.cfg.Spec = &spec.Spec{Tables: []spec.Table{{Source: "shop.t", Target: "raw.t", WriteMode: spec.WriteModeAppend}}}
	c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	c.index["w1"].add(inflightBatch{id: 1, table: "raw.t"})
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	err := s.tick(time.Now(), SupervisorConfig{AckTimeout: 30 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "append") {
		t.Fatalf("err = %v, want the run ended for a stalled append worker", err)
	}
}

// The control: a stale worker with NOTHING owed resets normally (no data to
// lose), so the supervisor's recovery path still works.
func TestSupervisorResetWithoutInFlightRecovers(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	w := workers["w1"]
	cancelled := false
	w.cancel = func() { cancelled = true }
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	if err := s.tick(time.Now(), SupervisorConfig{
		AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute,
	}); err != nil {
		t.Fatalf("tick: %v (a reset with nothing owed must not terminate)", err)
	}
	if !cancelled {
		t.Fatal("a safe reset must still cancel the session")
	}
}

// CD-T4: a marker batch for a table with no canonical schema (not in refs)
// must error in the coordinator, not encode a zero-column record that the
// worker rejects with a misleading column error. Markers go through enqueueTo
// (issue #214).
func TestEnqueueToMarkerUnknownTableErrors(t *testing.T) {
	c, w := coordHarness()
	c.refs = nil
	c.canonical = map[string]core.Schema{}
	err := c.enqueueTo(context.Background(), w, nil, &pb.BatchMeta{Table: "raw.orders"})
	if err == nil || !strings.Contains(err.Error(), "canonical schema") {
		t.Fatalf("err = %v, want a marker-schema error", err)
	}
}

// CD-T4 negative: a marker with an empty table is rejected up front.
func TestEnqueueBatchMarkerEmptyTableErrors(t *testing.T) {
	c := &Coordinator{canonical: map[string]core.Schema{}}
	c.publishRouting(&routing{owners: map[string][]*workerState{}, ranges: map[string][]source.Chunk{}})
	err := c.enqueueBatch(context.Background(), nil, &pb.BatchMeta{})
	if err == nil || !strings.Contains(err.Error(), "requires a table") {
		t.Fatalf("err = %v, want a marker-table error", err)
	}
}

// A worker lost mid-snapshot does not end the run (issue #461): its chunk
// window died with it, so the snapshot loop of its partition is told, and
// redoes the chunks the lost window held once the worker is back. A stale
// ChunkReady from the lost session cannot satisfy the wait: the loss bumps
// the epoch.
func TestSignalSessionEndDuringSnapshotSignalsTheSnapshot(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	c.snapshotActive.Store(true)
	lost := c.lostSignal(workers["w1"])

	c.signalSessionEnd("w1", context.Canceled)

	select {
	case err := <-c.sessionErrs:
		t.Fatalf("a worker lost mid-snapshot must be recovered, not end the run: %v", err)
	default:
	}
	select {
	case <-lost:
	default:
		t.Fatal("the snapshot loop must be told its worker was lost")
	}
	if workers["w1"].epoch != 1 {
		t.Fatalf("epoch = %d, want 1: a reply from the lost session must be stale", workers["w1"].epoch)
	}
}

// The control: the SAME reset without a snapshot active is not a run error
// (the supervisor recovers).
func TestSignalSessionEndResetWithoutSnapshotIsSilent(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	c.snapshotActive.Store(false)

	c.signalSessionEnd("w1", errSessionReset)

	select {
	case err := <-c.sessionErrs:
		t.Fatalf("a reset outside a snapshot must not fail the run, got %v", err)
	default:
	}
}

// A supervisor reset (pending) must NOT discard the worker's open staged
// cycles: the reconnecting session redelivers them, and a discarded cycle
// would drop those redeliveries (its new-epoch sequences are unknown to
// deliver). The run stays up so the reconnect can complete them (issue #372).
func TestSignalSessionEndPendingDoesNotDiscard(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	c.snapshotActive.Store(false)
	s.pendingSet("w1")
	c.staged = newStagedCycles()
	c.staged.expect(core.TableRef{Target: "raw.t"}, 7, []string{"w1", "w2"})

	c.signalSessionEnd("w1", context.Canceled)

	select {
	case err := <-c.sessionErrs:
		t.Fatalf("a pending reset must not fail the run, got %v", err)
	default:
	}
	if c.staged.isGapped("raw.t") {
		t.Fatal("a pending reset must not discard the worker's cycles")
	}
}

// A stream that dies (network error, Pod killed) is a hiccup: recovered.
func TestSignalSessionEndStreamErrorRecovers(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)

	c.signalSessionEnd("w1", errWorkerDead)

	select {
	case err := <-c.sessionErrs:
		t.Fatalf("a dead stream must be recovered, not end the run: %v", err)
	default:
	}
	if workers["w1"].epoch != 1 || !s.isPending("w1") {
		t.Fatal("the worker must be awaited under a new epoch")
	}
}

// Issue #461: a worker lost while it owes batches is recovered, not a reason
// to end the run. Its queued and sent-but-unacked batches stay with it and
// are redelivered when its Pod reconnects (issue #235), under a new epoch, so
// a reply from the lost session is ignored. The run ending instead put the
// coordinator into CrashLoopBackOff under sustained worker kills, and no
// table advanced.
func TestSignalSessionEndOwingWorkRecovers(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	// supervisorHarness seeds w1 with one undelivered queued batch.
	if len(workers["w1"].queue) == 0 {
		t.Fatal("precondition: w1 must owe a queued batch")
	}

	c.signalSessionEnd("w1", context.Canceled)

	select {
	case err := <-c.sessionErrs:
		t.Fatalf("a worker lost owing work must be recovered, not end the run: %v", err)
	default:
	}
	if len(workers["w1"].queue) != 1 {
		t.Fatalf("w1's queue holds %d batches, want its 1 owed batch kept for redelivery", len(workers["w1"].queue))
	}
	if workers["w1"].epoch != 1 || !s.isPending("w1") {
		t.Fatalf("epoch=%d pending=%v; want a new epoch and the worker awaited like a reset", workers["w1"].epoch, s.isPending("w1"))
	}
}

// The control: a session that ends with nothing owed (a clean scale-in retire,
// or a drained worker) must not fail the run.
func TestSignalSessionEndNothingOwedIsSilent(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	<-workers["w1"].queue // drain the seeded batch: nothing owed

	c.signalSessionEnd("w1", context.Canceled)

	select {
	case err := <-c.sessionErrs:
		t.Fatalf("a session ending with nothing owed must not fail the run, got %v", err)
	default:
	}
}

// A lost worker's open staged cycles are kept: its reconnected session
// redelivers the batches and stages them again, and the cycles complete.
// Discarding them left a gap only a run restart recovered (issue #461).
func TestSignalSessionEndKeepsOpenCycles(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	<-workers["w1"].queue // nothing owed: the loss is purely open cycles

	c.staged = newStagedCycles()
	c.staged.expect(core.TableRef{Target: "raw.t"}, 7, []string{"w1"})

	c.signalSessionEnd("w1", context.Canceled)

	select {
	case err := <-c.sessionErrs:
		t.Fatalf("a worker lost with open cycles must be recovered, not end the run: %v", err)
	default:
	}
	if c.staged.isGapped("raw.t") || c.staged.openFor(core.TableRef{Target: "raw.t"}) != 1 {
		t.Fatal("the lost worker's open cycle must stay open for its redelivery")
	}
}

// A worker that reports an error itself (schema drift, a failed commit) is
// not a hiccup: its session ends with that error, and the run ends.
func TestSignalSessionEndWorkerReportedErrorIsFatal(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)

	c.signalSessionEnd("w1", &workerReportedError{detail: "schema drift on raw.t"})

	select {
	case err := <-c.sessionErrs:
		if err == nil || !strings.Contains(err.Error(), "schema drift") {
			t.Fatalf("err = %v, want the worker's own error", err)
		}
	default:
		t.Fatal("an error the worker reported must end the run")
	}
}

// Losses are bounded: the same worker lost maxLossesWithoutProgress times in
// a row with no committed progress in between is a crash loop (a batch that
// OOM-kills it every time), and the run ends with what it knows.
func TestLossesWithoutProgressEndTheRun(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	c.confirmed = map[string]position.Position{"w1": position.MustLSN("0/10")}

	for i := 1; i <= defaultMaxLossesWithoutProgress; i++ {
		s.noteAttach("w1") // it reconnected since its previous loss
		c.signalSessionEnd("w1", context.Canceled)
		select {
		case err := <-c.sessionErrs:
			if i < defaultMaxLossesWithoutProgress {
				t.Fatalf("loss %d ended the run: %v", i, err)
			}
			if !strings.Contains(err.Error(), "without progress") || !strings.Contains(err.Error(), "0/10") {
				t.Fatalf("err = %v, want the loss count and the stuck position", err)
			}
		default:
			if i == defaultMaxLossesWithoutProgress {
				t.Fatalf("%d losses without progress must end the run", i)
			}
		}
	}
}

// Progress between losses restarts the count: a worker killed now and then
// under chaos, committing in between, is never a crash loop.
func TestLossesWithProgressDoNotEndTheRun(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 8)
	c.confirmed = map[string]position.Position{"w1": position.MustLSN("0/10")}

	for i := 0; i < 2*defaultMaxLossesWithoutProgress; i++ {
		s.noteAttach("w1") // it reconnected since its previous loss
		c.signalSessionEnd("w1", context.Canceled)
		c.recordConfirmed("w1", position.MustLSN(fmt.Sprintf("0/%X", 0x20+i)))
	}
	select {
	case err := <-c.sessionErrs:
		t.Fatalf("losses with progress in between ended the run: %v", err)
	default:
	}
}

var errWorkerDead = errors.New("stream died")

// opaquePos is an identity-only position (plugin offset cookie): different
// values are Incomparable.
type opaquePos string

func (o opaquePos) String() string { return string(o) }
func (o opaquePos) Compare(other position.Position) int {
	p, ok := other.(opaquePos)
	if ok && o == p {
		return 0
	}
	return position.Incomparable
}
func (o opaquePos) Contains(other position.Position) bool {
	p, ok := other.(opaquePos)
	return ok && o == p
}

// P2: an incomparable ack must NOT truncate the in-flight index — the safe
// direction is "don't truncate" (the head batch stays queued until coverage
// is certain). Truncating on an undefined order could free budget for
// batches that were never committed.
func TestPositionIndexIncomparableAckDoesNotTruncate(t *testing.T) {
	// A different opaque cookie cannot prove coverage: the batch stays
	// queued.
	p := newPositionIndex("run-p2a")
	p.add(inflightBatch{table: "t", high: opaquePos("cookie-1"), bytes: 5})
	if freed, _, _ := p.truncate("t", opaquePos("cookie-2")); freed != 0 {
		t.Fatalf("incomparable ack freed %d, want 0 (must not truncate)", freed)
	}
	if p.InFlight() != 1 {
		t.Fatalf("in-flight = %d, want 1", p.InFlight())
	}

	// Identity DOES cover it (same cookie).
	p2 := newPositionIndex("run-p2b")
	p2.add(inflightBatch{table: "t", high: opaquePos("cookie-1"), bytes: 5})
	if freed, _, _ := p2.truncate("t", opaquePos("cookie-1")); freed != 5 {
		t.Fatalf("identical ack freed %d, want 5", freed)
	}
}

// DP1 / D-CD1: the worker's Assignment carries the scoped read-only
// SnapshotURI when set, never the replication URI.
func TestSnapshotDSNPrefersScopedURI(t *testing.T) {
	c := &Coordinator{cfg: Config{Spec: &spec.Spec{Source: spec.Source{
		URI:         "mysql://repl:secret@db/repl",
		SnapshotURI: "mysql://readonly@db/ro",
	}}}}
	got, err := c.snapshotDSN()
	if err != nil {
		t.Fatalf("snapshotDSN: %v", err)
	}
	if got != "mysql://readonly@db/ro" {
		t.Fatalf("snapshotDSN = %q, want the scoped read-only URI", got)
	}
	// Fallback when unset: the pre-scoping behavior.
	c2 := &Coordinator{cfg: Config{Spec: &spec.Spec{Source: spec.Source{URI: "mysql://repl@db/repl"}}}}
	got, err = c2.snapshotDSN()
	if err != nil {
		t.Fatalf("snapshotDSN: %v", err)
	}
	if got != "mysql://repl@db/repl" {
		t.Fatalf("snapshotDSN fallback = %q, want the source URI", got)
	}
}

// A structured postgres source renders its block to a DSN for the worker.
// An SSH-tunneled one renders the same DSN (the block itself travels
// separately in the Assignment, #170); snapshotDSN no longer rejects it.
func TestSnapshotDSNPostgres(t *testing.T) {
	c := &Coordinator{cfg: Config{Spec: &spec.Spec{Source: spec.Source{
		Kind:     "postgres",
		Postgres: &spec.PostgresSource{Host: "db.internal", Database: "shop"},
	}}}}
	got, err := c.snapshotDSN()
	if err != nil {
		t.Fatalf("snapshotDSN: %v", err)
	}
	if !strings.Contains(got, "host=db.internal") || !strings.Contains(got, "dbname=shop") {
		t.Fatalf("snapshotDSN = %q, want the rendered postgres DSN", got)
	}

	c2 := &Coordinator{cfg: Config{Spec: &spec.Spec{Source: spec.Source{
		Kind:     "postgres",
		Postgres: &spec.PostgresSource{Host: "db.internal", Database: "shop", SSH: &spec.SSHConfig{Host: "bastion", Username: "u", Password: "p"}},
	}}}}
	got, err = c2.snapshotDSN()
	if err != nil {
		t.Fatalf("snapshotDSN with SSH must not error (#170 ships the block instead): %v", err)
	}
	if !strings.Contains(got, "host=db.internal") {
		t.Fatalf("snapshotDSN with SSH = %q, want the rendered DSN fallback", got)
	}
}

// P3 / retention: confirmedPosition feeds the source's retention. When the
// committed positions are not mutually comparable there is no safe minimum —
// it must return nil (hold retention), never an arbitrary pick.
func TestConfirmedPositionIncomparableHoldsRetention(t *testing.T) {
	c := &Coordinator{
		confirmed: map[string]position.Position{"a": opaquePos("x"), "b": opaquePos("y")},
		log:       slog.New(slog.DiscardHandler),
	}
	if got := c.confirmedPosition(); got != nil {
		t.Fatalf("incomparable committed positions must hold retention (nil), got %s", got)
	}

	// Comparable positions still fold to the minimum.
	lo := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:1")
	hi := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:5")
	c2 := &Coordinator{
		confirmed: map[string]position.Position{"a": lo, "b": hi},
		log:       slog.New(slog.DiscardHandler),
	}
	if got := c2.confirmedPosition(); got == nil || got.String() != lo.String() {
		t.Fatalf("comparable fold = %v, want %s", got, lo)
	}
}

// ChunkReady carries the Assignment epoch: a reply from a superseded
// generation (same table+chunkID, old epoch) must be ignored, never satisfy
// the wait against a dead window.
func TestWaitChunkReadyIgnoresStaleEpoch(t *testing.T) {
	c := &Coordinator{chunkReady: make(chan *pb.ChunkReady, 4), log: slog.New(slog.DiscardHandler)}
	c.chunkReady <- &pb.ChunkReady{Table: "t", ChunkId: 0, Epoch: 1} // stale
	c.chunkReady <- &pb.ChunkReady{Table: "t", ChunkId: 0, Epoch: 2} // current
	if err := c.waitChunkReady(context.Background(), "t", 0, 2); err != nil {
		t.Fatalf("waitChunkReady: %v", err)
	}
	if len(c.chunkReady) != 0 {
		t.Fatalf("both replies should have been consumed, %d left", len(c.chunkReady))
	}
}

// A stale reply alone must NOT satisfy the wait.
func TestWaitChunkReadyStaleOnlyTimesOut(t *testing.T) {
	c := &Coordinator{chunkReady: make(chan *pb.ChunkReady, 1), log: slog.New(slog.DiscardHandler)}
	c.chunkReady <- &pb.ChunkReady{Table: "t", ChunkId: 0, Epoch: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.waitChunkReady(ctx, "t", 0, 2); err == nil {
		t.Fatal("a stale-epoch reply must not satisfy the wait")
	}
}

// A worker holds two streams, Session and Control, and both end when it is
// lost: that is one loss, not two. Counting both ended a run after two real
// losses (issue #461, full chaos run).
func TestALossIsCountedOncePerEpoch(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	c.confirmed = map[string]position.Position{"w1": position.MustLSN("0/10")}

	c.signalSessionEnd("w1", context.Canceled) // Session stream
	c.signalSessionEnd("w1", context.Canceled) // Control stream, same loss
	if got := s.losses["w1"].count; got != 1 {
		t.Fatalf("losses = %d after one loss seen on both streams, want 1", got)
	}
}
