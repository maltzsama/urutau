package confirmed

import (
	"fmt"

	"github.com/maltzsama/urutau/internal/faultinject"
	"github.com/maltzsama/urutau/position"
)

// Ack is what a worker's ack says: the commit of table through Position, or,
// with a MarkerID and no position, the commit of a snapshot window's rows.
type Ack struct {
	Table    string
	Position string
	MarkerID uint64
	Epoch    uint64
}

// AckOutcome is what an ack did.
type AckOutcome int

const (
	// AckCommitted: the batches the commit covers left the worker's in-flight
	// window, and the position was recorded unless the table is staged.
	AckCommitted AckOutcome = iota
	// AckMarker: a snapshot window's Closes marker was released.
	AckMarker
	// AckStale: dropped, a superseded generation's ack on a staged table.
	AckStale
	// AckStaleMarker: dropped, a superseded generation's marker ack on a
	// staged table.
	AckStaleMarker
	// AckUnparsable: the position did not parse; the error says so.
	AckUnparsable
	// AckNoWindow: the worker has no in-flight window left to release from.
	AckNoWindow
)

// AckHost is the coordinator as an ack sees it.
type AckHost interface {
	// ParsePosition parses a source position.
	ParsePosition(s string) (position.Position, error)
	// Staged reports whether table's commits are the coordinator's staged
	// cycles: a worker's ack then precedes the durable commit.
	Staged(table string) bool
	// Superseded reports whether epoch is not worker's current generation.
	Superseded(worker string, epoch uint64) bool
	// ReleaseMarker takes a Closes marker out of worker's in-flight window.
	ReleaseMarker(worker string, id uint64)
	// Release takes out of worker's in-flight window every batch of table
	// the commit at pos covers. It reports false when the worker has no
	// window: unregistered on a rolled-back scale, it holds nothing.
	Release(worker, table string, pos position.Position) bool
}

// OnAck handles a worker's ack and reports what it did.
//
// On a table the worker commits itself the ack is evidence of a durable
// commit: the covered batches are released and the position recorded, keyed
// by worker, so a partitioned table's partitions each hold their own and the
// minimum is the lagging one (WK-001 §2.2).
//
// On a staged table the ack is evidence of nothing durable: the cycle is
// committed from the worker's staged delivery, after which the coordinator
// records the position itself, so source retention never passes data the
// cycle still owes (WK-001 §2.2/F2). The ack still releases the batches. A
// delivery from a superseded generation is dropped, and its ack is dropped
// with it: accepting it would take the batch, or the window's marker, out of
// the in-flight window, so it would never be redelivered, and the cycle would
// wait forever for that owner's share with every later cycle of the table
// queued behind it (#855).
func (p *Positions) OnAck(h AckHost, worker string, ack Ack) (AckOutcome, error) {
	if ack.MarkerID != 0 && ack.Position == "" {
		if h.Staged(ack.Table) && h.Superseded(worker, ack.Epoch) {
			return AckStaleMarker, nil
		}
		h.ReleaseMarker(worker, ack.MarkerID)
		return AckMarker, nil
	}
	pos, err := h.ParsePosition(ack.Position)
	if err != nil {
		// The position format is a shared contract; a worker that cannot
		// produce a valid one is broken, and the ack's batch would sit at the
		// head of the window forever, leaking its budget charge with no
		// visible error (issue #210).
		return AckUnparsable, fmt.Errorf("worker %s: unparsable ack position %q: %w", worker, ack.Position, err)
	}
	staged := h.Staged(ack.Table)
	if staged && h.Superseded(worker, ack.Epoch) {
		return AckStale, nil
	}
	faultinject.At(faultinject.CoordinatorAckBeforeRecord,
		"table", ack.Table, "worker", worker, "position", ack.Position)
	if !h.Release(worker, ack.Table, pos) {
		return AckNoWindow, nil
	}
	if !staged {
		p.Record(worker, pos)
	}
	return AckCommitted, nil
}

// Load is one worker as Owing sees it.
type Load struct {
	Name string
	// Queued: it has a batch queued.
	Queued bool
}

// Owing reports which workers hold data not yet durable: a non-empty queue,
// an in-flight (delivered-but-unacked) batch, or a table buffered before it
// was sent — in the snapshot gate, the re-slice pause or the staged
// accumulator — whose owners will receive it. A worker that owes nothing must
// not constrain the confirmed position, or an idle table pins the source's
// retention forever. inFlight is asked only for a worker with nothing queued.
func Owing(workers []Load, inFlight func(worker string) int, bufferedOwners []string) map[string]bool {
	owing := make(map[string]bool, len(workers))
	for _, w := range workers {
		if w.Queued || inFlight(w.Name) > 0 {
			owing[w.Name] = true
		}
	}
	for _, name := range bufferedOwners {
		owing[name] = true
	}
	return owing
}
