package coordinator

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
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

// workerDeployment clones the template rather than mutating the caller's
// copy — provisioning a second worker from the same cached template must
// not leak the first worker's --name into it.
func TestWorkerDeploymentDoesNotMutateSharedTemplate(t *testing.T) {
	tmpl := sampleTemplate()
	owner := metav1.OwnerReference{Kind: "Pod", Name: "orders-coordinator-0", UID: types.UID("abc")}

	d1 := workerDeployment("orders-raw.orders-0", "ns", owner, tmpl)
	d2 := workerDeployment("orders-raw.orders-1", "ns", owner, tmpl)

	if got := d1.Spec.Template.Spec.Containers[0].Args; len(got) != 5 || got[3] != "--name" || got[4] != "orders-raw.orders-0" {
		t.Fatalf("worker 0 args = %v", got)
	}
	if got := d2.Spec.Template.Spec.Containers[0].Args; len(got) != 5 || got[4] != "orders-raw.orders-1" {
		t.Fatalf("worker 1 args = %v, want its own --name (template must not be shared/mutated)", got)
	}
	// The original template argument passed in must be untouched.
	if len(tmpl.Spec.Containers[0].Args) != 3 {
		t.Fatalf("caller's template was mutated: %v", tmpl.Spec.Containers[0].Args)
	}
}

func TestWorkerDeploymentCarriesOwnerReferenceAndName(t *testing.T) {
	tmpl := sampleTemplate()
	owner := metav1.OwnerReference{Kind: "Pod", Name: "orders-coordinator-0", UID: types.UID("abc-123")}
	d := workerDeployment("orders-raw.orders-0", "ns", owner, tmpl)

	if d.Name != "orders-raw.orders-0" || d.Namespace != "ns" {
		t.Fatalf("deployment identity = %s/%s", d.Namespace, d.Name)
	}
	if len(d.OwnerReferences) != 1 || d.OwnerReferences[0].UID != "abc-123" {
		t.Fatalf("ownerReferences = %+v", d.OwnerReferences)
	}
	if got := d.Spec.Template.Labels["urutau.io/worker"]; got != "orders-raw.orders-0" {
		t.Fatalf("worker label = %q", got)
	}
	if d.Spec.Selector.MatchLabels["urutau.io/worker"] != "orders-raw.orders-0" {
		t.Fatalf("selector = %+v, must match the stamped label", d.Spec.Selector)
	}
}

// ensureDeployment: absent -> create; present -> update, carrying the
// live ResourceVersion and immutable Selector forward (same pattern
// internal/operator's ensure() uses for the coordinator's own objects).
func TestEnsureDeploymentCreatesThenUpdates(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ctx := context.Background()
	tmpl := sampleTemplate()
	owner := metav1.OwnerReference{Kind: "Pod", Name: "c", UID: types.UID("x")}
	desired := workerDeployment("w0", "ns", owner, tmpl)

	if err := ensureDeployment(ctx, cs, "ns", desired); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := cs.AppsV1().Deployments("ns").Get(ctx, "w0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after create: %v", err)
	}
	if got.Spec.Template.Spec.Containers[0].Image != "urutau:v1" {
		t.Fatalf("created deployment image = %q", got.Spec.Template.Spec.Containers[0].Image)
	}

	// Second ensure with a changed image must update, not fail or
	// duplicate.
	tmpl2 := sampleTemplate()
	tmpl2.Spec.Containers[0].Image = "urutau:v2"
	desired2 := workerDeployment("w0", "ns", owner, tmpl2)
	if err := ensureDeployment(ctx, cs, "ns", desired2); err != nil {
		t.Fatalf("update: %v", err)
	}
	got2, err := cs.AppsV1().Deployments("ns").Get(ctx, "w0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got2.Spec.Template.Spec.Containers[0].Image != "urutau:v2" {
		t.Fatalf("updated deployment image = %q, want urutau:v2", got2.Spec.Template.Spec.Containers[0].Image)
	}

	list, err := cs.AppsV1().Deployments("ns").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly one deployment, got %d", len(list.Items))
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
