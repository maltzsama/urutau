package eventlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// enqueue marshals the batch (assigning sequence numbers under mu, so the
// queue order matches the sequence order) and hands each line to the flusher,
// dropping and counting when the queue is full.
func (r *Run) enqueue(kind string, batch []map[string]any) error {
	r.mu.Lock()
	if r.closed || r.closing {
		r.mu.Unlock()
		return fmt.Errorf("eventlog: run %s is closed", r.id)
	}
	lines, seq, err := r.marshalLocked(kind, batch)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.emitted = seq
	var dropped int64
	for _, line := range lines {
		entry := append(line, '\n')
		select {
		case r.ch <- entry:
		default:
			dropped++
		}
	}
	r.mu.Unlock()

	if dropped > 0 {
		n := r.dropped.Add(dropped)
		r.log.Warn("eventlog: queue full; events dropped",
			"run", r.id, "dropped", n, "queued", len(r.ch))
	}
	return nil
}

// flushLoop is the single goroutine that owns the buffer and the putter. It
// batches queued lines and uploads on the size cap, the interval, an explicit
// Flush, or shutdown — in that order, so a burst flushes before the timer.
func (r *Run) flushLoop() {
	defer close(r.done)
	ticker := time.NewTicker(r.flushEvery)
	defer ticker.Stop()
	for {
		select {
		case line := <-r.ch:
			r.buf = append(r.buf, line...)
			r.flushIfFull()
		case <-ticker.C:
			_ = r.flush()
		case errc := <-r.flushReq:
			r.drain()
			errc <- r.flush()
		case <-r.stop:
			r.drain()
			r.seal()
			return
		}
	}
}

// drain pulls every currently-queued line into the buffer without blocking.
func (r *Run) drain() {
	for {
		select {
		case line := <-r.ch:
			r.buf = append(r.buf, line...)
		default:
			return
		}
	}
}

// flushIfFull uploads early once the buffer reaches the object cap, so a burst
// between ticks does not grow one object without limit.
func (r *Run) flushIfFull() {
	if int64(len(r.buf)) >= r.maxObjectBytes {
		_ = r.flush()
	}
}

// flush uploads the current buffer as one immutable object and, on success,
// advances to the next key. On failure the buffer is retained (bounded) and
// the upload is retried on the next flush.
func (r *Run) flush() error {
	if len(r.buf) == 0 {
		return nil
	}
	key := r.key // flusher-owned: no other goroutine writes it
	putCtx, cancel := context.WithTimeout(context.Background(), putTimeout)
	defer cancel()
	if err := r.putter.Put(putCtx, r.bucket, key, r.buf); err != nil {
		r.boundBuffer()
		r.log.Warn("eventlog: put", "key", key, "err", err)
		return fmt.Errorf("eventlog: put %s/%s: %w", r.bucket, key, err)
	}
	r.buf = nil
	r.advance()
	return nil
}

// advance publishes the next object key after a successful upload.
func (r *Run) advance() {
	r.mu.Lock()
	r.seq++
	r.key = r.baseKey + fmt.Sprintf("events-%06d.jsonl", r.seq)
	r.mu.Unlock()
}

// boundBuffer trims the buffer to the last MaxObjectBytes bytes, aligned to a
// line boundary, when a failing upload let it grow past the hard cap. The
// lines it replaces are gone, so it leaves an events_dropped marker: without
// it the trail reads as if the pipeline simply did nothing during the gap
// (issues #230, #333).
func (r *Run) boundBuffer() {
	if int64(len(r.buf)) <= r.maxBufferBytes {
		return
	}
	target := len(r.buf) - int(r.maxObjectBytes)
	if target <= 0 {
		return
	}
	if i := bytes.IndexByte(r.buf[target:], '\n'); i >= 0 {
		target += i + 1
	} else {
		target = len(r.buf)
	}
	dropped := target
	rest := r.buf[target:]
	next := make([]byte, 0, len(rest)+128)
	if marker, err := json.Marshal(map[string]any{
		"ts":     time.Now().UTC().Format(time.RFC3339Nano),
		"run_id": r.id,
		"kind":   KindEventsDropped,
		"bytes":  dropped,
	}); err == nil {
		next = append(next, marker...)
		next = append(next, '\n')
	}
	next = append(next, rest...)
	r.buf = next
	r.log.Warn("eventlog: dropped buffered events under a failing upload",
		"run", r.id, "bytes", dropped)
}

// seal appends the run_sealed terminal marker and uploads the final object.
func (r *Run) seal() {
	r.mu.Lock()
	emitted := r.emitted
	r.mu.Unlock()
	// The terminal marker records the final emitted count, so a reader can
	// verify it read every event and can tell a sealed run from an abandoned
	// one (issue #333). It is written even for a run that emitted nothing.
	if marker, err := json.Marshal(map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"run_id":  r.id,
		"kind":    KindRunSealed,
		"emitted": emitted,
	}); err == nil {
		r.buf = append(r.buf, marker...)
		r.buf = append(r.buf, '\n')
	}
	r.closeErr = r.flush()
}

// Flush uploads everything currently queued and returns the upload error, if
// any. Callers on a lifecycle path use it to make an event durable before they
// continue. A closed run yields an error.
func (r *Run) Flush(ctx context.Context) error {
	errc := make(chan error, 1)
	select {
	case r.flushReq <- errc:
	case <-r.done:
		return fmt.Errorf("eventlog: run %s is closed", r.id)
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-errc:
		return err
	case <-r.done:
		return fmt.Errorf("eventlog: run %s is closed", r.id)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close seals the run: no further emits are accepted, the queue is drained,
// and the final buffer — plus the run_sealed marker — is uploaded. The
// returned error surfaces a best-effort PUT failure, so a graceful shutdown
// that lost the trail's tail is not silent (issue #231). Idempotent.
func (r *Run) Close() error {
	r.mu.Lock()
	if r.closed || r.closing {
		closing := r.closing
		r.mu.Unlock()
		if closing {
			<-r.done
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.closeErr
	}
	r.closing = true // no new Emit may start
	r.mu.Unlock()

	close(r.stop)
	<-r.done

	r.mu.Lock()
	r.closed = true
	err := r.closeErr
	r.mu.Unlock()
	return err
}

// marshalLocked renders batch as JSONL lines carrying consecutive seqs after
// the events already appended. It reads only r.emitted and r.id, so it runs
// with mu held; on failure nothing has been mutated and the caller leaves the
// trail untouched. ts defaults to now, but a caller-supplied one wins, so a
// batched log record keeps its own timestamp instead of the flush time.
func (r *Run) marshalLocked(kind string, batch []map[string]any) ([][]byte, int, error) {
	lines := make([][]byte, 0, len(batch))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	seq := r.emitted
	for _, fields := range batch {
		seq++
		ev := make(map[string]any, len(fields)+4)
		for k, v := range fields {
			ev[k] = v
		}
		if _, ok := ev["ts"]; !ok {
			ev["ts"] = now
		}
		ev["run_id"] = r.id
		ev["kind"] = kind
		ev["seq"] = seq
		line, err := json.Marshal(ev)
		if err != nil {
			return nil, 0, fmt.Errorf("eventlog: marshal %s: %w", kind, err)
		}
		lines = append(lines, line)
	}
	return lines, seq, nil
}
