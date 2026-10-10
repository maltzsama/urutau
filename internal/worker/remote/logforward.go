package remote

import (
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltzsama/urutau/internal/logging"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

const workerLogQueue = 1024

func normalizeRemoteConfig(cfg *RemoteConfig) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 1000
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = 2 * time.Second
	}
}

func startWorkerLogForwarder(buf *logging.Buffer, sender *sessionSender, epoch uint64) func() {
	if buf == nil {
		return func() {}
	}
	forwarder := newWorkerLogForwarder(buf, sender, epoch)
	return forwarder.stop
}

// workerLogForwarder keeps logging off the gRPC send path. A worker can log
// while a stream send is flow-controlled; the bounded queue protects the
// processing path, and the drop marker makes overflow visible remotely.
type workerLogForwarder struct {
	buf         *logging.Buffer
	sender      *sessionSender
	epoch       uint64
	ch          chan logging.Record
	stopCh      chan struct{}
	wg          sync.WaitGroup
	once        sync.Once
	pendingDrop atomic.Int64
}

func newWorkerLogForwarder(buf *logging.Buffer, sender *sessionSender, epoch uint64) *workerLogForwarder {
	f := &workerLogForwarder{
		buf:    buf,
		sender: sender,
		epoch:  epoch,
		ch:     make(chan logging.Record, workerLogQueue),
		stopCh: make(chan struct{}),
	}
	buf.SetSink(f.sink)
	f.wg.Add(1)
	go f.loop()
	return f
}

func (f *workerLogForwarder) sink(r logging.Record) {
	select {
	case f.ch <- r:
	default:
		f.pendingDrop.Add(1)
	}
}

func (f *workerLogForwarder) loop() {
	defer f.wg.Done()
	for {
		select {
		case r := <-f.ch:
			if !f.sendDropMarker() || !f.send(r) {
				return
			}
		case <-f.stopCh:
			for {
				select {
				case r := <-f.ch:
					if !f.sendDropMarker() || !f.send(r) {
						return
					}
				default:
					_ = f.sendDropMarker()
					return
				}
			}
		}
	}
}

func (f *workerLogForwarder) sendDropMarker() bool {
	dropped := f.pendingDrop.Swap(0)
	if dropped == 0 {
		return true
	}
	return f.send(logging.Record{
		Time:    time.Now().UTC(),
		Level:   slog.LevelWarn,
		Message: "worker: log queue full; records dropped",
		Attrs: map[string]any{
			"dropped": dropped,
			"reason":  "worker_log_queue_full",
		},
	})
}

func (f *workerLogForwarder) send(r logging.Record) bool {
	attrs, err := json.Marshal(r.Attrs)
	if err != nil {
		attrs = nil
	}
	return f.sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Log{Log: &pb.WorkerLog{
		Ts:        r.Time.UTC().Format(time.RFC3339Nano),
		Level:     r.Level.String(),
		Msg:       r.Message,
		AttrsJson: attrs,
		Epoch:     f.epoch,
	}}}) == nil
}

func (f *workerLogForwarder) stop() {
	f.once.Do(func() {
		f.buf.SetSink(nil)
		close(f.stopCh)
		f.wg.Wait()
	})
}
