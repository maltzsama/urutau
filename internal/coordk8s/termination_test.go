package coordk8s

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A worker that exits because it lost the coordinator, or exits cleanly, did
// not crash: counting it would end the run on a network fault.
func TestCrashTellsAWorkerFailureFromALostCoordinator(t *testing.T) {
	cases := []struct {
		name  string
		term  Termination
		crash bool
	}{
		{"oom", Termination{Reason: "OOMKilled", ExitCode: 137}, true},
		{"panic", Termination{ExitCode: 2, Message: "panic: runtime error"}, true},
		{"error", Termination{ExitCode: 1, Message: "worker: table t: commit: boom"}, true},
		{"network", Termination{ExitCode: 1, Message: NetworkExitPrefix + "worker: channel lost"}, false},
		{"oom while the network was down", Termination{Reason: "OOMKilled", ExitCode: 137, Message: NetworkExitPrefix + "x"}, true},
		{"clean exit", Termination{ExitCode: 0}, false},
	}
	for _, tc := range cases {
		if got, _ := tc.term.Crash(); got != tc.crash {
			t.Errorf("%s: Crash = %v, want %v", tc.name, got, tc.crash)
		}
	}
	if _, desc := (Termination{Reason: "Error", ExitCode: 2, Message: "panic: nil map\ngoroutine 1"}).Crash(); desc != "last terminated Error, exit code 2: panic: nil map" {
		t.Fatalf("description = %q, want the first line of the message only", desc)
	}
}

// The worker's own container is read, by name: a sidecar's status sorts
// first and is not the worker's.
func TestTerminationOfReadsTheWorkerContainer(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{UID: "uid-1"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "istio-proxy", RestartCount: 9, LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}}},
			{Name: WorkerContainerName, RestartCount: 2, LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, Message: "m"}}},
		}},
	}
	got, ok := TerminationOf(pod)
	want := Termination{PodUID: "uid-1", Restarts: 2, Reason: "OOMKilled", ExitCode: 137, Message: "m"}
	if !ok || got != want {
		t.Fatalf("TerminationOf = %+v, %v; want %+v", got, ok, want)
	}
	if _, ok := TerminationOf(&corev1.Pod{}); ok {
		t.Fatal("a Pod with no worker container status reported a termination")
	}
}
