package dashboard

import (
	"encoding/json"
	"sync"
)

// streamMsg is one Server-Sent Event: a named event and its pre-marshaled JSON
// payload.
type streamMsg struct {
	event string
	data  []byte
}

// Hub fans state changes out to the connected SSE subscribers. Publishing
// never blocks the coordinator: a subscriber whose buffer is full drops the
// message (a monitoring feed prefers freshness over completeness).
type Hub struct {
	mu   sync.Mutex
	subs map[chan streamMsg]struct{}
}

// NewHub returns an empty hub.
func NewHub() *Hub {
	return &Hub{subs: map[chan streamMsg]struct{}{}}
}

// Subscribe returns a channel of messages and a cancel func that unsubscribes
// and releases the channel.
func (h *Hub) Subscribe() (<-chan streamMsg, func()) {
	ch := make(chan streamMsg, 64)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
	}
}

// Publish fans data out to every subscriber under the named event. data is a
// thunk, called only when at least one subscriber is connected: a high-rate
// caller (the per-ack state snapshot) must not build a payload nobody will
// read (issue #338). A marshal failure or a full subscriber buffer drops the
// message.
func (h *Hub) Publish(event string, data func() any) {
	h.mu.Lock()
	if len(h.subs) == 0 {
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()
	// Build outside the lock: data() reads coordinator state under its own
	// locks, and holding h.mu across it would both block Subscribe behind the
	// build and add a hub→coordinator lock order to reason about.
	payload := data()

	h.mu.Lock()
	defer h.mu.Unlock()
	// The last subscriber may have left while the payload was built; skip the
	// marshal and the fan-out then. One wasted build on that rare race is the
	// price of not holding h.mu across data() above.
	if len(h.subs) == 0 {
		return
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	msg := streamMsg{event: event, data: b}
	for ch := range h.subs {
		select {
		case ch <- msg:
		default: // subscriber is behind; drop rather than block the caller
		}
	}
}
