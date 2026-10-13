package coordk8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Termination is a worker container's last termination, as its Pod status
// reports it, with the Pod's identity and restart count to tell a fresh
// termination from one already seen.
type Termination struct {
	PodUID   string
	Restarts int32
	Reason   string
	ExitCode int32
	Message  string
}

// NetworkExitPrefix starts the termination message of a worker that exited
// because it lost the coordinator (cmd/worker writes it): not a crash.
const NetworkExitPrefix = "network: "

// Crash reports whether the termination was the worker failing on its own,
// and describes it.
func (t Termination) Crash() (bool, string) {
	desc := fmt.Sprintf("last terminated %s, exit code %d", t.Reason, t.ExitCode)
	if msg := strings.TrimSpace(t.Message); msg != "" {
		desc += ": " + strings.SplitN(msg, "\n", 2)[0]
	}
	switch {
	case t.Reason == "OOMKilled":
		return true, desc
	case strings.HasPrefix(t.Message, NetworkExitPrefix):
		return false, desc
	case t.ExitCode == 0:
		return false, desc
	default:
		return true, desc
	}
}

// TerminationOf reads the worker container's last termination from its Pod.
// It reports false for a Pod with no worker container status yet.
func TerminationOf(pod *corev1.Pod) (Termination, bool) {
	var st *corev1.ContainerStatus
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == WorkerContainerName {
			st = &pod.Status.ContainerStatuses[i]
			break
		}
	}
	if st == nil {
		return Termination{}, false
	}
	t := Termination{PodUID: string(pod.UID), Restarts: st.RestartCount}
	if last := st.LastTerminationState.Terminated; last != nil {
		t.Reason, t.ExitCode, t.Message = last.Reason, last.ExitCode, last.Message
	}
	return t, true
}

// DescribeTermination renders, for an error message, why a worker's Pod last
// stopped or is not running: its last termination reason and exit code, a
// waiting reason, or an unschedulable condition. It reads the worker's own
// container, selected by name: the kubelet sorts container statuses by name,
// so an injected sidecar (istio-proxy, linkerd-proxy) can come first, and a
// sidecar stuck in CrashLoopBackOff would otherwise be reported as the reason
// the worker did not come back.
func DescribeTermination(worker string, pod *corev1.Pod) string {
	var parts []string
	for _, st := range pod.Status.ContainerStatuses {
		if st.Name != WorkerContainerName {
			continue
		}
		if t := st.LastTerminationState.Terminated; t != nil {
			parts = append(parts, fmt.Sprintf("last terminated %s, exit code %d", t.Reason, t.ExitCode))
		}
		if w := st.State.Waiting; w != nil && w.Reason != "" {
			parts = append(parts, "waiting: "+w.Reason)
		}
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status != corev1.ConditionTrue && cond.Message != "" {
			parts = append(parts, "not scheduled: "+cond.Message)
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("; pod %s is %s", worker, pod.Status.Phase)
	}
	return fmt.Sprintf("; pod %s: %s", worker, strings.Join(parts, "; "))
}

// podReadTimeout bounds one read of a worker's Pod.
const podReadTimeout = 5 * time.Second

// ReadTermination reads worker's last container termination from its Pod.
// It reports false when there is no cluster (cs is nil) or the Pod cannot be
// read: no loss is then taken for a crash.
func ReadTermination(cs kubernetes.Interface, ns, worker string) (Termination, bool) {
	if cs == nil {
		return Termination{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), podReadTimeout)
	defer cancel()
	pod, err := cs.CoreV1().Pods(ns).Get(ctx, worker, metav1.GetOptions{})
	if err != nil {
		return Termination{}, false
	}
	return TerminationOf(pod)
}

// ReadDescription describes, for an error message, why worker's Pod last
// stopped or is not running (DescribeTermination). Best effort: empty
// without a cluster or when the Pod cannot be read.
func ReadDescription(cs kubernetes.Interface, ns, worker string) string {
	if cs == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), podReadTimeout)
	defer cancel()
	pod, err := cs.CoreV1().Pods(ns).Get(ctx, worker, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return DescribeTermination(worker, pod)
}
