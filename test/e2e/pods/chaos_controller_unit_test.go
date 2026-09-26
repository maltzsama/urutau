package pods

// Cluster-free tests of the chaos controller's planner and manifests.

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func draws(seed uint64, n int) []chaosDecision {
	pl := newChaosPlanner(seed, smokeChaos)
	out := make([]chaosDecision, n)
	for i := range out {
		out[i] = pl.next()
	}
	return out
}

// Same seed, same fault stream; another seed, another stream.
func TestChaosPlannerIsDeterministicPerSeed(t *testing.T) {
	if !reflect.DeepEqual(draws(7, 50), draws(7, 50)) {
		t.Fatal("same seed produced different decisions")
	}
	if reflect.DeepEqual(draws(7, 50), draws(8, 50)) {
		t.Fatal("different seeds produced identical decisions")
	}
}

// Over enough draws every weighted kind comes up, with its scope, durations
// in range and delays capped.
func TestChaosPlannerCoversKindsAndBounds(t *testing.T) {
	seen := map[chaosKind]int{}
	overlap := 0
	for _, d := range draws(42, 600) {
		seen[d.Kind]++
		if d.Overlap {
			overlap++
		}
		if d.Duration < smokeChaos.MinDuration || d.Duration > smokeChaos.MaxDuration {
			t.Fatalf("%s duration %s out of [%s, %s]", d.Kind, d.Duration, smokeChaos.MinDuration, smokeChaos.MaxDuration)
		}
		if d.Delay < 0 || d.Delay > 4*smokeChaos.MeanGap {
			t.Fatalf("delay %s out of [0, %s]", d.Delay, 4*smokeChaos.MeanGap)
		}
		switch d.Kind {
		case chaosCoordinatorKill, chaosCoordFailure:
			if d.Scope != scopeCoordinator {
				t.Fatalf("%s scoped %s", d.Kind, d.Scope)
			}
		case chaosWorkerKill, chaosWorkerFailure:
			if d.Scope != scopeOnePod {
				t.Fatalf("%s scoped %s", d.Kind, d.Scope)
			}
		case chaosNetworkPartition, chaosNetworkLoss, chaosNetworkDelay:
			if d.Scope != scopeTableGroup && d.Scope != scopeAllWorkers {
				t.Fatalf("%s scoped %s", d.Kind, d.Scope)
			}
		}
	}
	for _, k := range allChaosKinds {
		if seen[k] == 0 {
			t.Errorf("kind %s never drawn in 600 decisions", k)
		}
	}
	if overlap == 0 || overlap == 600 {
		t.Errorf("overlap drawn %d/600 times; want a mix", overlap)
	}
}

var testPods = []podInfo{
	{name: "p-coordinator-0", app: "urutau-coordinator", phase: "Running"},
	{name: "p-raw-a-0", app: "urutau-worker", group: "p-raw-a", phase: "Running"},
	{name: "p-raw-a-1", app: "urutau-worker", group: "p-raw-a", phase: "Running"},
	{name: "p-raw-b-0", app: "urutau-worker", group: "p-raw-b", phase: "Pending"},
}

// Every kind renders a well-formed Chaos Mesh resource aimed at a running
// Pod of the right role, in the test namespace.
func TestRenderChaosTargetsAndParses(t *testing.T) {
	for i, k := range allChaosKinds {
		d := newChaosPlanner(uint64(i), smokeChaos).next()
		d.Kind = k
		switch k {
		case chaosCoordinatorKill, chaosCoordFailure:
			d.Scope = scopeCoordinator
		case chaosNetworkPartition, chaosNetworkLoss, chaosNetworkDelay:
			d.Scope = scopeTableGroup
		default:
			d.Scope = scopeOnePod
		}
		m, resource, target, _, err := renderChaos("pod-e2e", "p", d, "x", testPods)
		if err != nil {
			t.Fatalf("%s: %v", k, err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(m), &doc); err != nil {
			t.Fatalf("%s: manifest does not parse: %v\n%s", k, err, m)
		}
		md := doc["metadata"].(map[string]any)
		if md["namespace"] != "pod-e2e" || md["name"] != "x" {
			t.Fatalf("%s: metadata %v", k, md)
		}
		if strings.Contains(target, "p-raw-b-0") {
			t.Fatalf("%s targeted a Pod that is not running: %s", k, target)
		}
		switch k {
		case chaosCoordinatorKill, chaosCoordFailure:
			if resource != "podchaos" || target != "p-coordinator-0" {
				t.Fatalf("%s: %s → %s, want podchaos → the coordinator", k, resource, target)
			}
		case chaosWorkerKill, chaosWorkerFailure:
			if resource != "podchaos" || !strings.HasPrefix(target, "p-raw-a-") {
				t.Fatalf("%s: %s → %s, want podchaos → a running worker", k, resource, target)
			}
		case chaosNetworkPartition, chaosNetworkLoss, chaosNetworkDelay:
			if resource != "networkchaos" || !strings.HasSuffix(target, "<-> coordinator") {
				t.Fatalf("%s: %s → %s", k, resource, target)
			}
			if !strings.Contains(m, `app: "urutau-coordinator"`) {
				t.Fatalf("%s: the target side is not the coordinator:\n%s", k, m)
			}
		case chaosCPUStress, chaosMemoryStress:
			if resource != "stresschaos" {
				t.Fatalf("%s: resource %s", k, resource)
			}
		}
		// pod-kill is continuous: only the duration-bound kinds carry one.
		hasDuration := strings.Contains(m, "duration:")
		if (k == chaosWorkerKill || k == chaosCoordinatorKill) == hasDuration {
			t.Fatalf("%s: duration present=%v\n%s", k, hasDuration, m)
		}
	}
}

// With no running Pod of the needed role the injection is refused, never
// redirected elsewhere.
func TestRenderChaosRefusesWithoutTarget(t *testing.T) {
	d := chaosDecision{Kind: chaosCoordinatorKill, Scope: scopeCoordinator}
	if _, _, _, _, err := renderChaos("pod-e2e", "p", d, "x", testPods[1:]); err == nil {
		t.Fatal("a coordinator fault with no coordinator Pod must fail")
	}
}

// Maintenance Pods live for seconds, so no fault aims at them, and a
// network fault names exactly the running Pods of its worker side.
func TestRenderChaosSkipsMaintenanceAndListsRunningPods(t *testing.T) {
	pods := append([]podInfo{
		{name: "p-raw-a-maint", app: "urutau-worker", group: "p-raw-a-maint", phase: "Running"},
	}, testPods...)
	for i := range 40 {
		for _, k := range []chaosKind{chaosWorkerKill, chaosNetworkPartition} {
			d := newChaosPlanner(uint64(i), smokeChaos).next()
			d.Kind, d.Scope = k, scopeOnePod
			if k == chaosNetworkPartition {
				d.Scope = scopeAllWorkers
			}
			m, _, target, _, err := renderChaos("pod-e2e", "p", d, "x", pods)
			if err != nil {
				t.Fatalf("%s: %v", k, err)
			}
			if strings.Contains(m, "maint") || strings.Contains(target, "maint") {
				t.Fatalf("%s aimed at a maintenance Pod:\n%s", k, m)
			}
			if k == chaosNetworkPartition && (!strings.Contains(m, "- p-raw-a-0") || strings.Contains(m, "p-raw-b-0")) {
				t.Fatalf("network fault must list the running workers only:\n%s", m)
			}
		}
	}
}

// A reserved slot counts toward the concurrency limit before its resource
// exists: once MaxConcurrent slots are taken, the loop's check refuses the
// next draw.
func TestChaosReservationCountsTowardTheLimit(t *testing.T) {
	c := newChaosController("pod-e2e", "p", 1, smokeChaos, nil)
	for seq := 1; seq <= smokeChaos.MaxConcurrent; seq++ {
		c.active[c.name(seq)] = reserved
	}
	if n := len(c.active); n < c.profile.MaxConcurrent {
		t.Fatalf("%d reserved slots, want %d", n, c.profile.MaxConcurrent)
	}
	if c.name(1) == c.name(2) {
		t.Fatal("experiment names must differ per seq")
	}
}

// A reactive injection that arrives once stop has begun must not start: it
// would join the wait group while stop waits on it, and outlive cleanup.
func TestInjectNowAfterStopIsANoop(t *testing.T) {
	c := newChaosController("pod-e2e", "p", 1, smokeChaos, nil)
	c.stopCh = make(chan struct{})
	c.stop()
	c.injectNow(context.Background(), chaosWorkerKill, "late")
	c.wg.Wait()
	if n := len(c.report().Events); n != 0 {
		t.Fatalf("%d experiment(s) started after stop", n)
	}
}
