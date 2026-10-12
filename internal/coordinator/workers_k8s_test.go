package coordinator

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/maltzsama/urutau/spec"

	"github.com/maltzsama/urutau/internal/coordk8s"
)

func sampleTemplate() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "urutau-worker"}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "worker",
				Image: "urutau:v1",
				Args:  []string{"run", "--coordinator", "orders-coordinator:50051"},
			}},
		},
	}
}

// provisionWorkers's disabled path: no worker pod template file on disk
// (the normal case for a pipeline that never sets spec.image) means zero
// Kubernetes API calls and no error — proven here by never constructing a
// client at all, since Coordinator.log is nil in this harness and any
// attempted API call would panic on the log line before erroring.
func TestProvisionWorkersNoTemplateIsANoop(t *testing.T) {
	c := &Coordinator{}
	if err := c.provisionWorkers(context.Background(), map[string]string{"w0": "raw.nonexistent-table-xyz"}); err != nil {
		t.Fatalf("provisionWorkers with no template file: %v", err)
	}
}

func TestProvisionWorkersEmptyMapIsANoop(t *testing.T) {
	c := &Coordinator{}
	if err := c.provisionWorkers(context.Background(), nil); err != nil {
		t.Fatalf("provisionWorkers with no workers: %v", err)
	}
}

// scaleReconcileLoop is a no-op when the operator rendered no worker pod
// templates: the coordinator makes zero Kubernetes API calls (c.log is nil
// here, so any attempted call would panic first).
func TestScaleReconcileLoopDisabledIsANoop(t *testing.T) {
	c := &Coordinator{}
	c.scaleReconcileLoop(context.Background())
}

// reconcileReplicas leaves a table alone when no StatefulSet exists for it,
// and re-slices when the replica count diverges. Exercised through the
// in-memory path: a fake clientset injected into the cluster handle.
func TestReconcileReplicasNoStatefulSetIsANoop(t *testing.T) {
	c, _ := scaleHarness(t)
	c.workerK8s = true
	c.k8s.Use(fake.NewSimpleClientset(), "ns")

	c.reconcileReplicas(context.Background())

	if got, _ := c.loadRouting().OwnersOf("raw.orders"); len(got) != 1 {
		t.Fatalf("owners = %d, want the untouched 1", len(got))
	}
}

func TestReconcileReplicasFollowsStatefulSet(t *testing.T) {
	c, _ := scaleHarness(t)
	c.workerK8s = true
	owner := metav1.OwnerReference{Kind: "Pod", Name: "c", UID: types.UID("x")}
	name := spec.WorkerGroupPrefix("p", "raw.orders")
	sts := coordk8s.WorkerStatefulSet(name, "ns", owner, sampleTemplate(), 3)
	c.k8s.Use(fake.NewSimpleClientset(sts), "ns")

	c.reconcileReplicas(context.Background())

	got, _ := c.loadRouting().OwnersOf("raw.orders")
	if len(got) != 3 {
		t.Fatalf("owners = %d, want 3 (routing follows the StatefulSet)", len(got))
	}
}

// A failed scale suppresses the reconcile until the cooldown elapses, so the
// table's input is not held paused on every tick (issue #298).
func TestReconcileReplicasBacksOffAfterFailure(t *testing.T) {
	c, _ := scaleHarness(t)
	c.workerK8s = true
	owner := metav1.OwnerReference{Kind: "Pod", Name: "c", UID: types.UID("x")}
	sts := coordk8s.WorkerStatefulSet(spec.WorkerGroupPrefix("p", "raw.orders"), "ns", owner, sampleTemplate(), 3)
	c.k8s.Use(fake.NewSimpleClientset(sts), "ns")
	c.autoscaler().Hold("raw.orders", time.Now().Add(time.Minute))

	c.reconcileReplicas(context.Background())

	if got, _ := c.loadRouting().OwnersOf("raw.orders"); len(got) != 1 {
		t.Fatalf("owners = %d, want the untouched 1 (reconcile suppressed)", len(got))
	}
}

// The kubelet sorts containerStatuses by name, so an injected sidecar can
// precede the worker; crash detection must read the worker's status by name,
// not by index (issue #553).
func TestK8sTerminationReadsWorkerContainerNotSidecar(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns", UID: types.UID("uid-1")},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "istio-proxy",
				RestartCount: 7,
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					Reason: "Completed", ExitCode: 0,
				}},
			},
			{
				Name:         "worker",
				RestartCount: 2,
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					Reason: "OOMKilled", ExitCode: 137,
				}},
			},
		}},
	})
	c := &Coordinator{}
	c.k8s.Use(cs, "ns")

	term, ok := c.k8sTermination("w")
	if !ok {
		t.Fatal("termination not read")
	}
	if term.restarts != 2 || term.reason != "OOMKilled" || term.exitCode != 137 {
		t.Fatalf("read the wrong container: %+v", term)
	}
	if term.podUID != "uid-1" {
		t.Fatalf("podUID = %q, want uid-1", term.podUID)
	}
}

// The maintenance flags belong to the worker container only; an injected
// sidecar must not receive them (issue #553).
func TestMaintenanceWorkerPodFlagsOnlyWorkerContainer(t *testing.T) {
	tmpl := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "istio-proxy", Image: "proxy:v1", Args: []string{"proxy", "sidecar"}},
				{Name: "worker", Image: "urutau:test", Command: []string{"urutau-worker"}},
			},
		},
	}
	pod := coordk8s.MaintenanceWorkerPod("maint", "ns", metav1.OwnerReference{Name: "c", Kind: "Pod"}, tmpl)

	var worker, sidecar *corev1.Container
	for i := range pod.Spec.Containers {
		switch pod.Spec.Containers[i].Name {
		case "worker":
			worker = &pod.Spec.Containers[i]
		case "istio-proxy":
			sidecar = &pod.Spec.Containers[i]
		}
	}
	if worker == nil || sidecar == nil {
		t.Fatalf("containers = %+v", pod.Spec.Containers)
	}
	if !hasArg(worker.Args, "--maintenance") || !hasArgPair(worker.Args, "--name", "maint") {
		t.Fatalf("worker args = %v, want --maintenance --name maint", worker.Args)
	}
	for _, a := range sidecar.Args {
		if a == "--maintenance" || a == "--name" {
			t.Fatalf("sidecar args = %v, must not carry worker flags", sidecar.Args)
		}
	}
}

// A StatefulSet that cannot be read is not a StatefulSet that does not exist.
// Not found is silent; any other error is logged, because a reconcile that
// cannot see the replica count never re-slices and nothing else would say so.
func TestReconcileReplicasReportsAnUnreadableStatefulSet(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		wantLog bool
	}{
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "x", errors.New("rbac")), true},
		{"not found", apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "x"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			c, _ := scaleHarness(t)
			c.log = slog.New(slog.NewTextHandler(&out, nil))
			c.workerK8s = true
			cs := fake.NewSimpleClientset()
			cs.PrependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, tc.err
			})
			c.k8s.Use(cs, "ns")

			c.reconcileReplicas(context.Background())

			if got, _ := c.loadRouting().OwnersOf("raw.orders"); len(got) != 1 {
				t.Fatalf("owners = %d, want the untouched 1", len(got))
			}
			if logged := strings.Contains(out.String(), "cannot read the worker statefulset"); logged != tc.wantLog {
				t.Fatalf("logged = %v, want %v; log: %q", logged, tc.wantLog, out.String())
			}
		})
	}
}

// A cooldown is written when a scale fails. When its table leaves the
// pipeline the entry goes with it; the cooldown of a table still there stays.
func TestReconcileReplicasPrunesTheCooldownOfATableThatLeft(t *testing.T) {
	c, _ := scaleHarness(t)
	c.workerK8s = true
	c.k8s.Use(fake.NewSimpleClientset(), "ns")
	later := time.Now().Add(time.Minute)
	c.autoscaler().Hold("raw.orders", later)
	c.autoscaler().Hold("raw.gone", later)

	c.reconcileReplicas(context.Background())

	if _, ok := c.autoscaler().HeldUntil("raw.gone"); ok {
		t.Error("the cooldown of a table no longer in the pipeline was kept")
	}
	if got, ok := c.autoscaler().HeldUntil("raw.orders"); !ok || !got.Equal(later) {
		t.Errorf("the cooldown of a table still in the pipeline was dropped or changed: %v, %v", got, ok)
	}
}
