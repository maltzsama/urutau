package coordinator

import (
	"context"
	"errors"
	"testing"
)

func errChan(err error) <-chan error {
	ch := make(chan error, 1)
	ch <- err
	return ch
}

// A clean snapshot completion (nil on extra) lets the run continue.
func TestAwaitTerminalSnapshotCleanCompletes(t *testing.T) {
	c := &Coordinator{}
	extra := make(chan error, 1)
	extra <- nil
	done, err := c.awaitTerminal(context.Background(), extra)
	if !done || err != nil {
		t.Fatalf("awaitTerminal clean snapshot = (%v, %v), want (true, nil)", done, err)
	}
}

// A snapshot failure (non-nil on extra) ends the run with that error.
func TestAwaitTerminalSnapshotFailureEndsRun(t *testing.T) {
	c := &Coordinator{}
	want := errors.New("boom")
	extra := make(chan error, 1)
	extra <- want
	done, err := c.awaitTerminal(context.Background(), extra)
	if done || !errors.Is(err, want) {
		t.Fatalf("awaitTerminal snapshot failure = (%v, %v), want (false, %v)", done, err, want)
	}
}

// A cancelled run reports ctx.Err(), never a wrapped session/stream error, even
// when one is already pending. The ctx.Err() guard now applies during the
// snapshot phase too — the same wait is used in both places (audit #11).
func TestAwaitTerminalCancelledRunPrefersContext(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(c *Coordinator)
	}{
		{"stream error pending", func(c *Coordinator) { c.streamErrs = errChan(errors.New("stream boom")) }},
		{"session error pending", func(c *Coordinator) { c.sessionErrs <- errors.New("session boom") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Coordinator{sessionErrs: make(chan error, 1)}
			tc.prepare(c)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			done, err := c.awaitTerminal(ctx, nil)
			if done {
				t.Fatal("done = true, want false")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("awaitTerminal = %v, want context.Canceled", err)
			}
		})
	}
}
