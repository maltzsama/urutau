package coordinator

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/maltzsama/urutau/source"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DoGet streams the worker's queued batches; the ticket (from its
// Assignment) selects which queue. Each FlightData carries one complete IPC
// stream in DataBody and a BatchMeta proto in AppMetadata — both produced
// at enqueue time, so the server only moves bytes.
//
// A batch that has left the queue is kept on the worker's sent list until an
// Ack covers it. On session loss the next DoGet redelivers the whole sent list
// before draining the queue, so a delivered-but-unacked batch is replayed
// rather than dropped — at-least-once (issue #235). FIFO is preserved: the
// sent list is in send order, ahead of the queue's head, and the budget charge
// is released only by the Ack that truncates past the batch.
func (s *flightServer) DoGet(req *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	w, ok := s.c.workerByTicket(string(req.Ticket))
	if !ok {
		return fmt.Errorf("coordinator: unknown flight ticket %q", string(req.Ticket))
	}
	if !w.activeGet.CompareAndSwap(false, true) {
		return status.Error(codes.ResourceExhausted, "coordinator: a DoGet stream is already active for this worker")
	}
	defer w.activeGet.Store(false)

	// Redeliver everything a previous session delivered but never had acked.
	// The list is pruned by the ack, so what remains here is exactly the
	// in-flight window of a lost session.
	w.sentMu.Lock()
	redeliver := append([]queuedBatch(nil), w.sent...)
	w.sentMu.Unlock()
	for _, qb := range redeliver {
		if err := stream.Send(&flight.FlightData{
			DataHeader:  []byte("urutau-batch"),
			DataBody:    qb.body,
			AppMetadata: qb.meta,
		}); err != nil {
			return err // still on the sent list; the next session retries
		}
	}

	for {
		select {
		case qb := <-w.queue:
			// Record before sending: a batch that has left the queue must be
			// redeliverable until its ack arrives.
			w.sentMu.Lock()
			w.sent = append(w.sent, qb)
			w.sentMu.Unlock()
			if err := stream.Send(&flight.FlightData{
				DataHeader:  []byte("urutau-batch"),
				DataBody:    qb.body,
				AppMetadata: qb.meta,
			}); err != nil {
				return err // still on the sent list; the next session retries
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// dropSent removes the batches an ack covered from the worker's sent list.
func (w *workerState) dropSent(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	// ids is almost always 1-2 (an ack pops a couple of batches), so a nested
	// scan beats allocating a map per ack (issue #605).
	w.sentMu.Lock()
	kept := w.sent[:0]
	for _, qb := range w.sent {
		dropped := false
		for _, id := range ids {
			if qb.id == id {
				dropped = true
				break
			}
		}
		if !dropped {
			kept = append(kept, qb)
		}
	}
	// Zero the removed tail: it shares the backing array and each queuedBatch
	// holds a multi-MiB body, so leaving it referenced leaks the batch
	// (issue #559).
	clear(w.sent[len(kept):])
	w.sent = kept
	w.sentMu.Unlock()
}

// randSuffix returns n hex chars of crypto randomness (run-id suffix).
func randSuffix(n int) string {
	b := make([]byte, n/2+1)
	if _, err := rand.Read(b); err != nil {
		// Same rule as randTicket: a deterministic fallback would make
		// runIDs collide across restarts and overwrite a prior run's S3
		// manifests. crypto/rand failure is fatal, not degraded.
		panic(fmt.Sprintf("coordinator: crypto/rand: %v", err))
	}
	return hex.EncodeToString(b)[:n]
}

// randTicket is the worker's Flight DoGet ticket: 128 random bits so a
// worker's stream cannot be opened by guessing "urutau/<name>" (audit #3).
// crypto/rand failure is fatal — a deterministic fallback would restore the
// guessable-ticket hole the randomness exists to close.
func randTicket() []byte {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("coordinator: crypto/rand: %v", err))
	}
	return []byte(hex.EncodeToString(b))
}

// tableNames renders a group's source tables for the audit trail.
func tableNames(refs []source.TableRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Source
	}
	return out
}

type flightServer struct {
	flight.BaseFlightServer
	c *Coordinator
}
