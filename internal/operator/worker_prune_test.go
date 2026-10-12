package operator

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// workerWorkload is a worker StatefulSet and its headless Service as the
// coordinator creates them: labelled, and owned by the coordinator's
// StatefulSet.
func workerWorkload(name, ns string, owner types.UID) []client.Object {
	meta := metav1.ObjectMeta{
		Name:      name,
		Namespace: ns,
		Labels:    map[string]string{workerWorkloadLabel: name},
		OwnerReferences: []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "owner", UID: owner},
		},
	}
	return []client.Object{&appsv1.StatefulSet{ObjectMeta: meta}, &corev1.Service{ObjectMeta: meta}}
}

// workloadExists reports whether the worker's StatefulSet and Service exist.
func workloadExists(t *testing.T, c client.Client, name string) (sts, svc bool) {
	t.Helper()
	key := types.NamespacedName{Name: name, Namespace: "ns"}
	err := c.Get(context.Background(), key, &appsv1.StatefulSet{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get statefulset %s: %v", name, err)
	}
	sts = err == nil
	err = c.Get(context.Background(), key, &corev1.Service{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get service %s: %v", name, err)
	}
	return sts, err == nil
}

// The coordinator only creates and updates its worker StatefulSets. A table
// removed from the pipeline left its workers behind: the pods crash-looped
// against a coordinator that no longer planned their table and held the CPU
// and memory they requested (#835). Another pipeline's workers, in the same
// namespace under the same label, are not this pipeline's to delete.
func TestPruneWorkerWorkloadsDeletesTheWorkersOfATableThatLeftThePipeline(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = urutauv1alpha1.AddToScheme(scheme)

	cr := pipelineCR("orders", "ns")
	cr.Spec.Definition.Inline["tables"] = []any{
		map[string]any{"source": "shop.orders", "target": "raw.orders", "primaryKey": []any{"id"}},
	}
	coord := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: "ns", UID: "coord"}}

	objs := []client.Object{coord}
	objs = append(objs, workerWorkload("e2e-raw-orders", "ns", "coord")...)
	objs = append(objs, workerWorkload("e2e-raw-gone", "ns", "coord")...)
	objs = append(objs, workerWorkload("other-raw-gone", "ns", "another-coordinator")...)

	r := &CoordinatorReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()}
	if err := r.pruneWorkerWorkloads(context.Background(), cr); err != nil {
		t.Fatalf("pruneWorkerWorkloads: %v", err)
	}

	if sts, svc := workloadExists(t, r.Client, "e2e-raw-gone"); sts || svc {
		t.Fatalf("removed table: statefulset=%v service=%v, want both deleted", sts, svc)
	}
	if sts, svc := workloadExists(t, r.Client, "e2e-raw-orders"); !sts || !svc {
		t.Fatalf("live table: statefulset=%v service=%v, want both kept", sts, svc)
	}
	if sts, svc := workloadExists(t, r.Client, "other-raw-gone"); !sts || !svc {
		t.Fatalf("another pipeline's workers: statefulset=%v service=%v, want both kept", sts, svc)
	}
}

// A discovery pipeline's tables are unknown to the operator: with no list to
// compare against, nothing is deleted.
func TestPruneWorkerWorkloadsKeepsEverythingWhenTheTablesAreUnknown(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = urutauv1alpha1.AddToScheme(scheme)

	cr := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns"}}
	coord := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: "ns", UID: "coord"}}
	objs := append([]client.Object{coord}, workerWorkload("e2e-raw-orders", "ns", "coord")...)

	r := &CoordinatorReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()}
	if err := r.pruneWorkerWorkloads(context.Background(), cr); err != nil {
		t.Fatalf("pruneWorkerWorkloads: %v", err)
	}
	if sts, svc := workloadExists(t, r.Client, "e2e-raw-orders"); !sts || !svc {
		t.Fatalf("statefulset=%v service=%v, want both kept", sts, svc)
	}
}
