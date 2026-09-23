package dashboard

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHubPublishesToSubscribers(t *testing.T) {
	h := NewHub()
	a, cancelA := h.Subscribe()
	defer cancelA()
	b, cancelB := h.Subscribe()
	defer cancelB()

	h.Publish("event", func() any { return Event{Type: "commit"} })

	for i, ch := range []<-chan streamMsg{a, b} {
		select {
		case msg := <-ch:
			if msg.event != "event" {
				t.Errorf("sub %d: event = %q, want event", i, msg.event)
			}
			var ev Event
			if err := json.Unmarshal(msg.data, &ev); err != nil || ev.Type != "commit" {
				t.Errorf("sub %d: payload = %s (err %v)", i, msg.data, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("sub %d: no message delivered", i)
		}
	}
}

func TestHubUnsubscribeStopsDelivery(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe()
	cancel()
	h.Publish("event", func() any { return Event{Type: "commit"} })
	select {
	case msg := <-ch:
		t.Fatalf("received %q after unsubscribe", msg.event)
	case <-time.After(50 * time.Millisecond):
	}
}

// The payload thunk must not run when no subscriber is connected (issue
// #338): a per-ack state snapshot is not worth materializing for nobody.
func TestHubSkipsPayloadWithNoSubscribers(t *testing.T) {
	h := NewHub()
	called := false
	h.Publish("state", func() any { called = true; return map[string]any{"x": 1} })
	if called {
		t.Fatal("payload thunk ran with zero subscribers")
	}

	// With a subscriber, it runs exactly once.
	_, cancel := h.Subscribe()
	defer cancel()
	h.Publish("state", func() any { called = true; return map[string]any{"x": 1} })
	if !called {
		t.Fatal("payload thunk did not run with a subscriber connected")
	}
}
