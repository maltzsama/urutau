package supervisor

import (
	"sync"
	"time"
)

// A worker that acks nothing past the ack timeout may be wedged, or writing
// to storage slow enough that one commit outlasts the timeout. Terminating
// the second replays the same load against the same storage: the matrix run
// restarted 12 times against a struggling S3 and never converged (#422).
// The worker reports its network output (WorkerMetricsReport.net_tx_bytes);
// output that keeps growing marks it busy, not stalled.
const (
	// ProgressMinBytes is the growth that counts as progress: well above
	// the control stream's own reports and logs.
	ProgressMinBytes = 1 << 20
	// ProgressAckCap bounds how long progress stands in for acks, in ack
	// timeouts: a worker that keeps sending but never acks (retrying one
	// failing upload, say) is ended all the same.
	ProgressAckCap = 10
	// progressKeep bounds the samples kept per worker: enough to span any
	// sane ack timeout at the worker's 5 s report cadence.
	progressKeep = 128
)

// progressSample is one reported network output and when it arrived.
type progressSample struct {
	at    time.Time
	bytes int64
}

// Progress is each worker's recent network output reports: a worker whose
// output grows is busy on slow storage, not stalled (#422). The zero value is
// ready to use.
type Progress struct {
	mu       sync.Mutex
	byWorker map[string][]progressSample
}

// Note records a worker's cumulative network output. A counter lower than the
// last one belongs to a new process: its samples start over.
func (p *Progress) Note(worker string, txBytes int64, at time.Time) {
	if txBytes <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.byWorker[worker]
	if n := len(h); n > 0 && txBytes < h[n-1].bytes {
		h = h[:0]
	}
	h = append(h, progressSample{at: at, bytes: txBytes})
	if len(h) > progressKeep {
		h = append(h[:0], h[len(h)-progressKeep:]...)
	}
	if p.byWorker == nil {
		p.byWorker = map[string][]progressSample{}
	}
	p.byWorker[worker] = h
}

// Busy reports whether a worker past its ack timeout is making progress: its
// output grew by ProgressMinBytes over the last ack timeout (from the newest
// sample at or before its start), and its last ack is within ProgressAckCap
// timeouts.
func (p *Progress) Busy(worker string, now, lastAck time.Time, ack time.Duration) bool {
	if lastAck.IsZero() || now.Sub(lastAck) > ProgressAckCap*ack {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.byWorker[worker]
	if len(h) < 2 || now.Sub(h[len(h)-1].at) > ack {
		return false
	}
	from := h[0]
	for _, s := range h {
		if s.at.After(now.Add(-ack)) {
			break
		}
		from = s
	}
	return h[len(h)-1].bytes-from.bytes >= ProgressMinBytes
}

// Tracks reports whether worker has a sample on record.
func (p *Progress) Tracks(worker string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byWorker[worker]) > 0
}

// Forget drops a worker's samples.
func (p *Progress) Forget(worker string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.byWorker, worker)
}
