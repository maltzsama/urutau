package coordk8s

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestEnsureServiceCreatesThenUpdates(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ctx := context.Background()
	owner := metav1.OwnerReference{Kind: "Pod", Name: "c", UID: types.UID("x")}

	svc := workerHeadlessService("w", "ns", owner)
	if err := ensureService(ctx, cs, "ns", svc); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := cs.CoreV1().Services("ns").Get(ctx, "w", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after create: %v", err)
	}
	if got.Spec.ClusterIP != "None" {
		t.Fatalf("clusterIP = %q, want None (headless)", got.Spec.ClusterIP)
	}
	if err := ensureService(ctx, cs, "ns", workerHeadlessService("w", "ns", owner)); err != nil {
		t.Fatalf("update: %v", err)
	}
	list, err := cs.CoreV1().Services("ns").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly one service, got %d", len(list.Items))
	}
}

// #599: worker workloads are owned by the coordinator STATEFULSET (stable UID),
// not the coordinator Pod, so a Pod replacement never garbage-collects them.
func TestOwnerFromPodUsesTheControllingOwner(t *testing.T) {
	ctrl := true
	sts := metav1.OwnerReference{
		APIVersion: "apps/v1", Kind: "StatefulSet", Name: "orders-coordinator",
		UID: types.UID("sts-uid"), Controller: &ctrl,
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "orders-coordinator-0", UID: types.UID("pod-uid"),
		OwnerReferences: []metav1.OwnerReference{sts},
	}}
	got := ownerFromPod(pod)
	if got.Kind != "StatefulSet" || got.UID != "sts-uid" || got.Name != "orders-coordinator" {
		t.Fatalf("owner = %+v, want the controlling StatefulSet", got)
	}
	if got.Controller != nil || got.BlockOwnerDeletion != nil {
		t.Fatalf("owner must not set controller/blockOwnerDeletion: %+v", got)
	}

	// A bare Pod (no controlling owner) falls back to the Pod itself.
	bare := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "solo", UID: types.UID("solo-uid")}}
	if got := ownerFromPod(bare); got.Kind != "Pod" || got.UID != "solo-uid" {
		t.Fatalf("bare pod owner = %+v, want the Pod itself", got)
	}

	// A non-controller owner reference is ignored.
	cm := metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "cm", UID: types.UID("cm-uid")}
	pod2 := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", UID: types.UID("p-uid"), OwnerReferences: []metav1.OwnerReference{cm}}}
	if got := ownerFromPod(pod2); got.Kind != "Pod" {
		t.Fatalf("non-controller owner = %+v, want the Pod fallback", got)
	}
}

// workerStatefulSet clones the template rather than mutating the caller's
// copy, and carries NO --name: a StatefulSet pod's hostname is
// "<statefulset>-<ordinal>", which the worker already uses as its default
// name, and that string is exactly spec.Table.WorkerGroupNames[ordinal].
func TestWorkerStatefulSetDoesNotMutateSharedTemplate(t *testing.T) {
	tmpl := sampleTemplate()
	owner := metav1.OwnerReference{Kind: "Pod", Name: "orders-coordinator-0", UID: types.UID("abc")}

	s1 := WorkerStatefulSet("orders-raw.orders", "ns", owner, tmpl, 3)
	s2 := WorkerStatefulSet("orders-raw.orders", "ns", owner, tmpl, 3)

	if got := s1.Spec.Template.Spec.Containers[0].Args; len(got) != 3 {
		t.Fatalf("worker args = %v, want the template's 3 (no --name stamp)", got)
	}
	if got := s2.Spec.Template.Spec.Containers[0].Args; len(got) != 3 {
		t.Fatalf("second worker args = %v, template must not be shared/mutated", got)
	}
	// The original template argument passed in must be untouched.
	if len(tmpl.Spec.Containers[0].Args) != 3 {
		t.Fatalf("caller's template was mutated: %v", tmpl.Spec.Containers[0].Args)
	}
}

func TestWorkerStatefulSetCarriesOwnerReferenceAndName(t *testing.T) {
	tmpl := sampleTemplate()
	owner := metav1.OwnerReference{Kind: "Pod", Name: "orders-coordinator-0", UID: types.UID("abc-123")}
	s := WorkerStatefulSet("orders-raw.orders", "ns", owner, tmpl, 3)

	if s.Name != "orders-raw.orders" || s.Namespace != "ns" {
		t.Fatalf("statefulset identity = %s/%s", s.Namespace, s.Name)
	}
	if len(s.OwnerReferences) != 1 || s.OwnerReferences[0].UID != "abc-123" {
		t.Fatalf("ownerReferences = %+v", s.OwnerReferences)
	}
	if got := s.Spec.Template.Labels["urutau.io/worker"]; got != "orders-raw.orders" {
		t.Fatalf("worker label = %q", got)
	}
	if s.Spec.Selector.MatchLabels["urutau.io/worker"] != "orders-raw.orders" {
		t.Fatalf("selector = %+v, must match the stamped label", s.Spec.Selector)
	}
	// The governing Service is the same name, so pods get stable hostnames
	// "<pipeline>-<target>-<ordinal>".
	if s.Spec.ServiceName != "orders-raw.orders" {
		t.Fatalf("serviceName = %q", s.Spec.ServiceName)
	}
	if s.Spec.Replicas == nil || *s.Spec.Replicas != 3 {
		t.Fatalf("replicas = %v, want 3", s.Spec.Replicas)
	}
}

// ensureStatefulSet: absent -> create; present -> update, carrying the live
// ResourceVersion and immutable Selector/ServiceName forward.
func TestEnsureStatefulSetCreatesThenUpdates(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ctx := context.Background()
	owner := metav1.OwnerReference{Kind: "Pod", Name: "c", UID: types.UID("x")}

	if err := ensureStatefulSet(ctx, cs, "ns", WorkerStatefulSet("w", "ns", owner, sampleTemplate(), 1)); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := cs.AppsV1().StatefulSets("ns").Get(ctx, "w", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after create: %v", err)
	}
	if got.Spec.Template.Spec.Containers[0].Image != "urutau:v1" {
		t.Fatalf("created image = %q", got.Spec.Template.Spec.Containers[0].Image)
	}

	tmpl2 := sampleTemplate()
	tmpl2.Spec.Containers[0].Image = "urutau:v2"
	if err := ensureStatefulSet(ctx, cs, "ns", WorkerStatefulSet("w", "ns", owner, tmpl2, 1)); err != nil {
		t.Fatalf("update: %v", err)
	}
	got2, err := cs.AppsV1().StatefulSets("ns").Get(ctx, "w", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got2.Spec.Template.Spec.Containers[0].Image != "urutau:v2" {
		t.Fatalf("updated image = %q, want urutau:v2", got2.Spec.Template.Spec.Containers[0].Image)
	}

	list, err := cs.AppsV1().StatefulSets("ns").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly one statefulset, got %d", len(list.Items))
	}
}

// KEDA owns the replica count: a re-provision must adopt the live value, not
// reset it to the spec's partition count, or the coordinator would fight the
// autoscaler (issue #298).
func TestEnsureStatefulSetAdoptsLiveReplicas(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ctx := context.Background()
	owner := metav1.OwnerReference{Kind: "Pod", Name: "c", UID: types.UID("x")}

	// KEDA has scaled the table to 5 replicas.
	live := WorkerStatefulSet("w", "ns", owner, sampleTemplate(), 5)
	if _, err := cs.AppsV1().StatefulSets("ns").Create(ctx, live, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed live statefulset: %v", err)
	}

	// The coordinator re-provisions with its boot count of 1.
	if err := ensureStatefulSet(ctx, cs, "ns", WorkerStatefulSet("w", "ns", owner, sampleTemplate(), 1)); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	got, err := cs.AppsV1().StatefulSets("ns").Get(ctx, "w", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 5 {
		t.Fatalf("replicas = %v, want the live 5 adopted", got.Spec.Replicas)
	}
}
