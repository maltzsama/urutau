package coordinator

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ── Flight data plane ────────────────────────────────────────────────

type flightServer struct {
	flight.BaseFlightServer
	c *Coordinator
}

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
	drop := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		drop[id] = struct{}{}
	}
	w.sentMu.Lock()
	kept := w.sent[:0]
	for _, qb := range w.sent {
		if _, ok := drop[qb.id]; !ok {
			kept = append(kept, qb)
		}
	}
	w.sent = kept
	w.sentMu.Unlock()
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
