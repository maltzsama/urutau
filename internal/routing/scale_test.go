package routing

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/source"
)

// fakeHost is a pipeline whose owners are their names. It records every step
// a re-slice takes, in order.
type fakeHost struct {
	layout   *Layout[string]
	known    map[string]bool // owners already registered
	steps    []string
	admit    error
	ranges   []source.Chunk
	rangeErr error
	drainErr error
	stranded map[string]bool
	failed   error
}

func newFakeHost(owners ...string) *fakeHost {
	known := map[string]bool{}
	for _, o := range owners {
		known[o] = true
	}
	return &fakeHost{
		layout: NewLayout(map[string][]string{"raw.a": owners}, map[string][]source.Chunk{"raw.a": {{}}}),
		known:  known,
		ranges: []source.Chunk{{}},
	}
}

func (h *fakeHost) step(format string, a ...any) {
	h.steps = append(h.steps, fmt.Sprintf(format, a...))
}

func (h *fakeHost) Layout() *Layout[string]            { return h.layout }
func (h *fakeHost) Publish(l *Layout[string])          { h.layout = l; h.step("publish") }
func (h *fakeHost) Admit(string, int) error            { return h.admit }
func (h *fakeHost) Name(o string) string               { return o }
func (h *fakeHost) Stranded(o string) bool             { return h.stranded[o] }
func (h *fakeHost) Forget(o string)                    { delete(h.known, o); h.step("forget %s", o) }
func (h *fakeHost) Seed(created []string)              { h.step("seed %v", created) }
func (h *fakeHost) PauseInput(string)                  { h.step("pause") }
func (h *fakeHost) ResumeInput(string)                 { h.step("resume") }
func (h *fakeHost) Retire(_ context.Context, o string) { h.step("retire %s", o) }
func (h *fakeHost) Fail(err error)                     { h.failed = err }
func (h *fakeHost) Scaled(_ string, from, to int)      { h.step("scaled %d->%d", from, to) }

func (h *fakeHost) Ranges(context.Context, string, int) ([]source.Chunk, error) {
	return h.ranges, h.rangeErr
}

func (h *fakeHost) Owner(_ string, p int) (string, bool, error) {
	name := fmt.Sprintf("a-%d", p)
	created := !h.known[name]
	h.known[name] = true
	return name, created, nil
}

func (h *fakeHost) Drain(context.Context, string, []string) error {
	h.step("drain")
	return h.drainErr
}

func (h *fakeHost) owners() []string {
	o, _ := h.layout.OwnersOf("raw.a")
	return o
}

// The input is paused and drained on the OLD owners before the layout
// flips, and a removed owner is retired only after it: retiring it first
// would race the reader still routing to it.
func TestScaleInFlipsAfterTheDrainAndRetiresAfterTheFlip(t *testing.T) {
	h := newFakeHost("a-0", "a-1", "a-2")
	var s Scaler[string]
	changed, err := s.Scale(context.Background(), h, "raw.a", 1)
	if err != nil || !changed {
		t.Fatalf("Scale = %v, %v", changed, err)
	}
	want := []string{"seed []", "pause", "drain", "publish", "resume", "scaled 3->1", "retire a-1", "retire a-2"}
	if !reflect.DeepEqual(h.steps, want) {
		t.Fatalf("steps = %v\nwant    %v", h.steps, want)
	}
	if !reflect.DeepEqual(h.owners(), []string{"a-0"}) {
		t.Fatalf("owners = %v, want [a-0]", h.owners())
	}
}

func TestScaleOutSeedsOnlyTheOwnersItCreated(t *testing.T) {
	h := newFakeHost("a-0")
	h.known["a-1"] = true // a pod restart left it registered: reused, not created
	var s Scaler[string]
	if _, err := s.Scale(context.Background(), h, "raw.a", 3); err != nil {
		t.Fatal(err)
	}
	want := []string{"seed [a-2]", "pause", "drain", "publish", "resume", "scaled 1->3"}
	if !reflect.DeepEqual(h.steps, want) {
		t.Fatalf("steps = %v\nwant    %v", h.steps, want)
	}
	if !reflect.DeepEqual(h.owners(), []string{"a-0", "a-1", "a-2"}) {
		t.Fatalf("owners = %v", h.owners())
	}
}

// A drain that does not converge leaves the old layout, resumes the input
// and rolls back exactly the owners this call created.
func TestAFailedDrainLeavesTheOldLayoutAndForgetsTheNewOwners(t *testing.T) {
	h := newFakeHost("a-0")
	h.known["a-1"] = true
	h.drainErr = errors.New("3 batch(es) still pending")
	var s Scaler[string]
	changed, err := s.Scale(context.Background(), h, "raw.a", 3)
	if changed || err == nil || !strings.Contains(err.Error(), "scale raw.a: drain: 3 batch(es) still pending") {
		t.Fatalf("Scale = %v, %v", changed, err)
	}
	want := []string{"seed [a-2]", "pause", "drain", "resume", "forget a-2"}
	if !reflect.DeepEqual(h.steps, want) {
		t.Fatalf("steps = %v\nwant    %v", h.steps, want)
	}
	if !reflect.DeepEqual(h.owners(), []string{"a-0"}) {
		t.Fatalf("owners = %v, want the old layout", h.owners())
	}
}

// A removed owner that is not attached yet owes work can never drain: the
// run ends for a clean replay instead of timing out on every retry.
func TestAStrandedRemovedOwnerEndsTheRun(t *testing.T) {
	h := newFakeHost("a-0", "a-1")
	h.stranded = map[string]bool{"a-1": true}
	var s Scaler[string]
	changed, err := s.Scale(context.Background(), h, "raw.a", 1)
	if changed || err == nil || h.failed == nil || !errors.Is(h.failed, err) {
		t.Fatalf("Scale = %v, %v; failed = %v", changed, err, h.failed)
	}
	if len(h.steps) != 0 {
		t.Fatalf("steps = %v, want none: nothing may be paused or published", h.steps)
	}
}

func TestScaleRefusals(t *testing.T) {
	cases := []struct {
		name   string
		target string
		n      int
		setup  func(*fakeHost)
		want   string
	}{
		{"zero owners", "raw.a", 0, nil, "worker count must be >= 1"},
		{"unknown table", "raw.x", 2, nil, "no such routed table"},
		{"not admitted", "raw.a", 2, func(h *fakeHost) { h.admit = errors.New("snapshot in progress") }, "scale raw.a: snapshot in progress"},
		{"ranges fail", "raw.a", 2, func(h *fakeHost) { h.rangeErr = errors.New("chunker: boom") }, "scale raw.a: chunker: boom"},
		{"no ranges", "raw.a", 2, func(h *fakeHost) { h.ranges = nil }, "no partition range resolved"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newFakeHost("a-0")
			if tc.setup != nil {
				tc.setup(h)
			}
			var s Scaler[string]
			changed, err := s.Scale(context.Background(), h, tc.target, tc.n)
			if changed || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Scale = %v, %v; want an error containing %q", changed, err, tc.want)
			}
			if len(h.steps) != 0 {
				t.Fatalf("steps = %v, want none", h.steps)
			}
		})
	}
}

func TestScaleToTheCurrentCountChangesNothing(t *testing.T) {
	h := newFakeHost("a-0", "a-1")
	h.admit = errors.New("must not be asked")
	var s Scaler[string]
	if changed, err := s.Scale(context.Background(), h, "raw.a", 2); changed || err != nil {
		t.Fatalf("Scale = %v, %v", changed, err)
	}
	if len(h.steps) != 0 {
		t.Fatalf("steps = %v, want none", h.steps)
	}
}
