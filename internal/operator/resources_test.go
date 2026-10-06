package operator

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// A CR whose resource field is not a valid Kubernetes quantity must be
// rejected at validation, not panic resource.MustParse on every reconcile
// (issue #575).
func TestValidateResourceQuantities(t *testing.T) {
	ok := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"}}
	ok.Spec.Coordinator.CPU = "500m"
	ok.Spec.Coordinator.Memory = "2Gi"
	ok.Spec.Worker.MemoryOverhead = "512Mi"
	if err := validateResourceQuantities(ok); err != nil {
		t.Fatalf("valid quantities rejected: %v", err)
	}

	for _, bad := range []string{"2GB", "not-a-quantity", "1 Gi"} {
		cr := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"}}
		cr.Spec.Coordinator.Memory = bad
		if err := validateResourceQuantities(cr); err == nil {
			t.Fatalf("quantity %q was accepted (resource.MustParse would panic)", bad)
		}
	}
}

// validateSpec is the reconciler's terminal gate: it must reject a bad
// quantity before any workload is built.
func TestValidateSpecRejectsBadQuantity(t *testing.T) {
	r := &CoordinatorReconciler{}
	cr := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"}}
	cr.Spec.Definition.Inline = map[string]any{"pipeline": "x"}
	cr.Spec.Worker.Memory = "2GB"
	if err := r.validateSpec(cr); err == nil {
		t.Fatal("validateSpec accepted an invalid quantity")
	}
}
