package worker

import (
	"github.com/maltzsama/urutau/dataplane"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// installAcks installs the worker's two acks to the coordinator: a commit's
// position, and a Closes marker's id once its window's rows are committed.
func installAcks(w *Worker, sender *sessionSender, epoch uint64, cfg RemoteConfig) {
	// A Closes marker is acked by its own id once its window's rows are
	// committed; the coordinator releases the marker on nothing else (#468).
	w.OnMarkerCommitted(func(table string, id uint64) {
		if cfg.FaultAckGate != nil && cfg.FaultAckGate.Load() {
			return
		}
		_ = sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Ack{Ack: &pb.Ack{
			Table: table, Epoch: epoch, BatchId: id,
		}}})
	})
	w.OnCommit(func(b *dataplane.Batch, rows int) {
		if cfg.FaultAckGate != nil && cfg.FaultAckGate.Load() {
			return
		}
		// A commit of snapshot window rows alone carries no position (#468):
		// its marker is acked by id instead (OnMarkerCommitted).
		if len(b.Watermark) == 0 {
			return
		}
		// Carry the equality-delete count so the coordinator's dashboard
		// metrics (deletes, collapse ratio) are not always zero in distributed
		// mode (issue #263).
		_, deletes := CountOps(b)
		_ = sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Ack{Ack: &pb.Ack{
			Table:    b.Table,
			Epoch:    epoch,
			Position: string(b.Watermark),
			Rows:     uint64(rows),
			Deletes:  uint64(deletes),
		}}})
	})
}
