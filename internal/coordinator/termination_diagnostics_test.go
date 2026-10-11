package coordinator

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
)

// The kubelet lists container statuses by name, so a sidecar can come before
// the worker. Its state is not why the worker did not come back.
func TestDescribeTerminationReadsTheWorkerContainerNotASidecar(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{
			{Name: "istio-proxy", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
			{Name: workerContainerName, LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137}}},
		},
	}}
	got := describeTermination("w0", pod)
	if !strings.Contains(got, "OOMKilled") || !strings.Contains(got, "137") {
		t.Errorf("message %q does not carry the worker's own termination", got)
	}
	if strings.Contains(got, "CrashLoopBackOff") {
		t.Errorf("message %q reports the sidecar's state as the worker's", got)
	}
}

// With nothing to say about the worker container, the message falls back to
// the Pod's phase, sidecar or not.
func TestDescribeTerminationFallsBackToThePhase(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{
			{Name: "istio-proxy", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
			{Name: workerContainerName},
		},
	}}
	if got, want := describeTermination("w0", pod), "; pod w0 is Running"; got != want {
		t.Errorf("message = %q, want %q", got, want)
	}
}

// A maintenance session ends in a Recv error every time: the worker closes
// the stream after its pass. Only an end that is neither that nor a
// cancellation is worth a warning.
func TestUnexpectedSessionEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"worker closed the stream", io.EOF, false},
		{"context cancelled", context.Canceled, false},
		{"grpc cancelled", status.Error(codes.Canceled, "context canceled"), false},
		{"transport failure", status.Error(codes.Unavailable, "transport is closing"), true},
		{"plain error", errors.New("connection reset by peer"), true},
	} {
		if got := unexpectedSessionEnd(tc.err); got != tc.want {
			t.Errorf("%s: unexpectedSessionEnd = %v, want %v", tc.name, got, tc.want)
		}
	}
}
