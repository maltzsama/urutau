package operator

import (
	"testing"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateSpecRequiresInline(t *testing.T) {
	r := &CoordinatorReconciler{}
	cr := &urutauv1alpha1.CDCPipeline{
		Spec: urutauv1alpha1.CDCPipelineSpec{
			Definition: urutauv1alpha1.Definition{},
		},
	}
	if err := r.validateSpec(cr); err == nil {
		t.Error("empty inline: want error")
	}
}

func TestValidateSpecWithInline(t *testing.T) {
	r := &CoordinatorReconciler{}
	cr := &urutauv1alpha1.CDCPipeline{
		Spec: urutauv1alpha1.CDCPipelineSpec{
			Definition: urutauv1alpha1.Definition{
				Inline: map[string]any{"tables": []any{}},
			},
		},
	}
	if err := r.validateSpec(cr); err != nil {
		t.Errorf("valid spec: %v", err)
	}
}

func TestResolveImageFromCR(t *testing.T) {
	r := &CoordinatorReconciler{}
	cr := &urutauv1alpha1.CDCPipeline{
		Spec: urutauv1alpha1.CDCPipelineSpec{Image: "custom:v2"},
	}
	got := r.resolveImage(cr)
	if got != "custom:v2" {
		t.Errorf("resolveImage = %q, want custom:v2", got)
	}
}

func TestResolveImageFallbackToDefault(t *testing.T) {
	r := &CoordinatorReconciler{Image: "default:v1"}
	cr := &urutauv1alpha1.CDCPipeline{
		Spec: urutauv1alpha1.CDCPipelineSpec{},
	}
	got := r.resolveImage(cr)
	if got != "default:v1" {
		t.Errorf("resolveImage fallback = %q, want default:v1", got)
	}
}

func TestInt32Ptr(t *testing.T) {
	got := int32Ptr(42)
	if got == nil || *got != 42 {
		t.Errorf("int32Ptr(42) = %v, want &42", got)
	}
}

func TestResourceRequirements(t *testing.T) {
	got := resourceRequirements("2", "500m", "4Gi", "256Mi")
	// Request CPU = "2" (2000m)
	cpuReq := got.Requests["cpu"]
	if cpuReq.String() != "2" {
		t.Errorf("cpu request = %q, want 2", cpuReq.String())
	}
	// Limit CPU = 2000m + 500m = 2500m
	cpuLim := got.Limits["cpu"]
	if cpuLim.String() != "2500m" {
		t.Errorf("cpu limit = %q, want 2500m", cpuLim.String())
	}
}

func TestAddQuantity(t *testing.T) {
	got := addQuantity("1", "500m")
	if got.String() != "1500m" {
		t.Errorf("addQuantity(1, 500m) = %q, want 1500m", got.String())
	}
}

func TestCoordinatorName(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pipeline"},
	}
	got := coordinatorName(cr)
	if got != "my-pipeline-coordinator" {
		t.Errorf("coordinatorName = %q, want my-pipeline-coordinator", got)
	}
}

func TestCoordinatorSAName(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pipeline"},
	}
	got := coordinatorSAName(cr)
	want := coordinatorName(cr)
	if got != want {
		t.Errorf("coordinatorSAName = %q, want %q", got, want)
	}
}

func TestSelectorLabels(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pipeline"},
	}
	got := selectorLabels(cr)
	if got["app"] != "urutau-coordinator" {
		t.Errorf("app label = %q", got["app"])
	}
	if got["urutau.io/pipeline"] != "my-pipeline" {
		t.Errorf("pipeline label = %q", got["urutau.io/pipeline"])
	}
}

func TestCoordinatorClusterAddr(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "pipe", Namespace: "ns"},
	}
	got := coordinatorClusterAddr(cr)
	want := "pipe-coordinator.ns.svc.cluster.local:50051"
	if got != want {
		t.Errorf("coordinatorClusterAddr = %q, want %q", got, want)
	}
}

func TestCoordinatorCommand(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{}
	got := coordinatorCommand(cr)
	if len(got) == 0 {
		t.Error("coordinatorCommand returned empty")
	}
}

func TestWorkerPodTemplateKey(t *testing.T) {
	got := workerPodTemplateKey("orders")
	want := "worker-pod-template.orders.yaml"
	if got != want {
		t.Errorf("workerPodTemplateKey = %q, want %q", got, want)
	}
}
