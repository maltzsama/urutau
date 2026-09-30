package coordinator

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/maltzsama/urutau/internal/logging"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// onWorkerLog turns a wire log back into the coordinator's shared record
// path. That keeps worker logs in the live dashboard buffer and the same
// durable event trail as coordinator logs, while the worker identity makes a
// multi-worker postmortem searchable.
func (c *Coordinator) onWorkerLog(worker string, msg *pb.WorkerLog) {
	if msg == nil || c.cfg.LogBuffer == nil {
		return
	}
	c.mu.Lock()
	w := c.workers[worker]
	validEpoch := (w != nil && msg.Epoch == w.epoch) ||
		(w == nil && c.maint != nil)
	c.mu.Unlock()
	if !validEpoch {
		return
	}

	level := slog.LevelInfo
	if err := level.UnmarshalText([]byte(msg.Level)); err != nil {
		level = slog.LevelInfo
	}
	attrs := map[string]any{}
	if len(msg.AttrsJson) > 0 {
		var wireAttrs map[string]any
		if err := json.Unmarshal(msg.AttrsJson, &wireAttrs); err == nil {
			for key, value := range wireAttrs {
				attrs[key] = value
			}
		} else {
			attrs["attrs_json"] = string(msg.AttrsJson)
		}
	}
	attrs["worker"] = worker
	at, err := time.Parse(time.RFC3339Nano, msg.Ts)
	if err != nil {
		at = time.Now().UTC()
	}
	c.cfg.LogBuffer.Append(logging.Record{
		Time:    at,
		Level:   level,
		Message: msg.Msg,
		Attrs:   attrs,
	})
	// A maintenance worker's Pod is deleted after its one pass, taking its
	// own log with it: its lines also go to the coordinator's log, which
	// outlives the pass (issue #457). A data worker keeps its own Pod log.
	if w == nil && c.log != nil {
		args := make([]any, 0, 2*len(attrs))
		for k, v := range attrs {
			args = append(args, k, v)
		}
		c.log.Log(context.Background(), level, msg.Msg, args...)
	}
}
