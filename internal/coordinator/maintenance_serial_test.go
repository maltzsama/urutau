package coordinator

import (
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Every table of a pipeline comes due together, and their maintenance passes
// used to run side by side: on 1M-row tables each held 4-5 GiB and the three
// of them exhausted the node (#831). While one table's worker is in flight,
// no other table's is provisioned.
func TestTickProvisionsNoOtherTableWhileAWorkerIsInFlight(t *testing.T) {
	t.Run("a session in progress", func(t *testing.T) {
		m, cs := testScheduler(t)
		m.register("w-a", "raw.a")
		m.register("w-b", "raw.b")
		m.mu.Lock()
		m.connected["w-a"] = true
		m.mu.Unlock()
		m.tick(time.Now())
		if got := podsLookedUp(cs); len(got) != 0 {
			t.Fatalf("tick went on to provision %v while raw.a's pass is running", got)
		}
	})
	t.Run("a pod still starting", func(t *testing.T) {
		m, cs := testScheduler(t)
		m.register("w-a", "raw.a")
		m.register("w-b", "raw.b")
		m.register("w-c", "raw.c")
		seedPodWithStatus(t, cs, "w-a", corev1.PodStatus{Phase: corev1.PodPending})
		m.tick(time.Now())
		if got, want := podsLookedUp(cs), []string{"w-a"}; !slices.Equal(got, want) {
			t.Fatalf("tick looked at %v, want only %v: raw.a's worker is on its way", got, want)
		}
	})
}

// The tables take turns: one that used its turn goes after the others, so a
// table that is always due, or whose worker keeps failing, does not keep the
// rest from ever being maintained.
func TestTickServesTheTableWhoseTurnIsOldestFirst(t *testing.T) {
	m, cs := testScheduler(t)
	m.register("w-a", "raw.a")
	m.register("w-b", "raw.b")
	m.register("w-c", "raw.c")
	// raw.a's worker died without connecting: tick removes it, and that was
	// raw.a's turn.
	seedPodWithStatus(t, cs, "w-a", corev1.PodStatus{Phase: corev1.PodFailed})
	m.tick(time.Now())
	cs.ClearActions()

	m.tick(time.Now())
	if got, want := podsLookedUp(cs), []string{"w-b", "w-c", "w-a"}; !slices.Equal(got, want) {
		t.Fatalf("second tick took the tables in order %v, want %v", got, want)
	}
}
