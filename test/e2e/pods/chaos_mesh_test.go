package pods

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// chaosMeshNS is the namespace Chaos Mesh's own controller/daemon run in
// (test/e2e/pods/k8s/chaos-mesh/manifest.yaml). Experiment CRs are created in
// testNS instead, alongside the CDCPipeline they target — Chaos Mesh CRs are
// namespaced but select pods across namespaces via the selector, so locality
// with the pipeline under test matters more than locality with the
// controller.
const chaosMeshNS = "chaos-mesh"

// verifyChaosMeshReady asserts the Chaos Mesh controller manager and daemon
// are Ready before a scenario starts. `make e2e-pods-up` already waits for
// this at cluster bring-up; this is a second, in-test check so a scenario
// fails loudly — instead of silently running without real chaos — if Chaos
// Mesh was torn down, crashed, or was never installed.
func verifyChaosMeshReady(t *testing.T) {
	t.Helper()
	out := kubectl(t, "-n", chaosMeshNS, "get", "deployment", "chaos-controller-manager", "-o",
		"jsonpath={.status.readyReplicas}")
	if strings.TrimSpace(out) == "" || out == "0" {
		t.Fatalf("chaos-controller-manager has no ready replicas (got %q) — is Chaos Mesh installed? run `make e2e-pods-up`", out)
	}
	out = kubectl(t, "-n", chaosMeshNS, "get", "daemonset", "chaos-daemon", "-o",
		"jsonpath={.status.numberReady}")
	if strings.TrimSpace(out) == "" || out == "0" {
		t.Fatalf("chaos-daemon has no ready pods (got %q) — is Chaos Mesh installed? run `make e2e-pods-up`", out)
	}
}

// labelSelectorYAML renders a label map as Chaos Mesh's selector.labelSelectors
// block.
func labelSelectorYAML(selector map[string]string, indent string) string {
	var b strings.Builder
	for k, v := range selector {
		fmt.Fprintf(&b, "%s%s: %q\n", indent, k, v)
	}
	return b.String()
}

// applyPodChaos creates a PodChaos experiment named name in ns, targeting
// pods matching selector. action is "pod-kill" or "pod-failure"; duration is
// required for pod-failure (how long the pod stays unavailable) and ignored
// for pod-kill (a one-shot kill). mode "one" picks a single random pod from
// the selector match, mirroring how deletePod targets exactly one Pod.
func applyPodChaos(t *testing.T, ns, name string, selector map[string]string, action, duration string) {
	t.Helper()
	var durationLine string
	if duration != "" {
		durationLine = "  duration: " + duration + "\n"
	}
	manifest := fmt.Sprintf(`apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: %s
  namespace: %s
spec:
  action: %s
  mode: one
%s  selector:
    namespaces:
      - %s
    labelSelectors:
%s`, name, ns, action, durationLine, ns, labelSelectorYAML(selector, "      "))
	applyCR(t, manifest)
}

// applyNetworkChaos creates a NetworkChaos experiment named name in ns,
// targeting pods matching selector. action is one of Chaos Mesh's network
// fault kinds ("partition", "loss", "delay", ...); target additionally scopes
// which peer pods the fault applies between (e.g. the coordinator's
// selector, to fault only worker<->coordinator traffic).
func applyNetworkChaos(t *testing.T, ns, name string, selector, target map[string]string, action, duration string) {
	t.Helper()
	manifest := fmt.Sprintf(`apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: %s
  namespace: %s
spec:
  action: %s
  mode: all
  duration: %s
  selector:
    namespaces:
      - %s
    labelSelectors:
%s  direction: both
  target:
    mode: all
    selector:
      namespaces:
        - %s
      labelSelectors:
%s`, name, ns, action, duration, ns, labelSelectorYAML(selector, "      "), ns, labelSelectorYAML(target, "        "))
	applyCR(t, manifest)
}

// applyStressChaos creates a StressChaos experiment named name in ns,
// targeting pods matching selector. kind is "cpu" or "memory"; workers is the
// stressor worker-process count (stress-ng --cpu/--vm N); size is the memory
// size per worker (e.g. "256MB"), ignored for kind "cpu".
func applyStressChaos(t *testing.T, ns, name string, selector map[string]string, kind string, workers int, size, duration string) {
	t.Helper()
	var stressorBlock string
	switch kind {
	case "cpu":
		stressorBlock = fmt.Sprintf("    cpu:\n      workers: %d\n", workers)
	case "memory":
		stressorBlock = fmt.Sprintf("    memory:\n      workers: %d\n      size: %q\n", workers, size)
	default:
		t.Fatalf("applyStressChaos: unknown kind %q, want cpu or memory", kind)
	}
	manifest := fmt.Sprintf(`apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: %s
  namespace: %s
spec:
  mode: all
  duration: %s
  selector:
    namespaces:
      - %s
    labelSelectors:
%s  stressors:
%s`, name, ns, duration, ns, labelSelectorYAML(selector, "      "), stressorBlock)
	applyCR(t, manifest)
}

// waitChaosExperimentInjected polls a Chaos Mesh experiment until its
// AllInjected condition is True — the fault has actually been applied to
// every selected pod. This is the only reliable "the fault took effect"
// signal for every chaos kind: .status.experiment.desiredPhase is not it —
// its CRD schema allows exactly two values, "Run" and "Stop", never
// "Finished" — and for a continuous action like pod-kill it never leaves
// "Run" at all (Chaos Mesh reapplies the kill to any new pod matching the
// selector until the CR is deleted), so it cannot signal completion for
// every kind the way AllInjected does.
func waitChaosExperimentInjected(t *testing.T, ns, kind, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out := kubectl(t, "-n", ns, "get", kind, name, "-o",
			`jsonpath={.status.conditions[?(@.type=="AllInjected")].status}`)
		if strings.TrimSpace(out) == "True" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s/%s did not reach AllInjected within %s (last status: %q)", kind, name, timeout, out)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// waitChaosExperimentStopped polls a Chaos Mesh experiment until its
// .status.experiment.desiredPhase reports Stop — Chaos Mesh's own terminal
// value once a duration-bound experiment's duration elapses. Only
// duration-bound actions (pod-failure, NetworkChaos, StressChaos) reach this
// on their own; a continuous action like pod-kill stays at "Run" forever —
// use waitChaosExperimentInjected for that instead.
func waitChaosExperimentStopped(t *testing.T, ns, kind, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out := kubectl(t, "-n", ns, "get", kind, name, "-o",
			"jsonpath={.status.experiment.desiredPhase}")
		if strings.TrimSpace(out) == "Stop" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s/%s did not reach Stop within %s (last phase: %q)", kind, name, timeout, out)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// deleteChaosExperiment removes a Chaos Mesh experiment CR, best-effort — safe
// to call from t.Cleanup after a test already deleted it explicitly.
func deleteChaosExperiment(t *testing.T, ns, kind, name string) {
	t.Helper()
	kubectl(t, "-n", ns, "delete", kind, name, "--ignore-not-found", "--wait=true")
}
