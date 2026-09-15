package operator

import (
	"testing"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	urutauspec "github.com/maltzsama/urutau/spec"
)

// resourceRequirements: cpu/memory become the request; +overhead becomes
// the limit. No overhead means limit == request.
func TestResourceRequirementsAppliesOverheadOnlyToLimit(t *testing.T) {
	r := resourceRequirements("500m", "100m", "1Gi", "256Mi")
	if got := r.Requests.Cpu().String(); got != "500m" {
		t.Fatalf("cpu request = %q, want 500m", got)
	}
	if got := r.Limits.Cpu().String(); got != "600m" {
		t.Fatalf("cpu limit = %q, want 600m (500m+100m)", got)
	}
	if got := r.Requests.Memory().String(); got != "1Gi" {
		t.Fatalf("memory request = %q, want 1Gi", got)
	}
	if got := r.Limits.Memory().Value(); got != (1<<30)+(256<<20) {
		t.Fatalf("memory limit = %d bytes, want 1Gi+256Mi", got)
	}
}

func TestResourceRequirementsNoOverheadLimitEqualsRequest(t *testing.T) {
	r := resourceRequirements("1", "", "2Gi", "")
	if r.Requests.Cpu().String() != r.Limits.Cpu().String() {
		t.Fatalf("cpu request %v != limit %v with no overhead", r.Requests.Cpu(), r.Limits.Cpu())
	}
	if r.Requests.Memory().String() != r.Limits.Memory().String() {
		t.Fatalf("memory request %v != limit %v with no overhead", r.Requests.Memory(), r.Limits.Memory())
	}
}

func TestResourceRequirementsEmptyIsZeroValue(t *testing.T) {
	r := resourceRequirements("", "", "", "")
	if r.Requests != nil || r.Limits != nil {
		t.Fatalf("empty cpu/memory should produce a zero-value ResourceRequirements, got %+v", r)
	}
}

// A table's own Workers.CPU/Memory overrides the pipeline-wide default;
// the default's overhead still applies (a table override never sets its
// own overhead — spec.WorkerSpec has no overhead field).
func TestWorkerResourcesTableOverridesDefault(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{
		Spec: urutauv1alpha1.CDCPipelineSpec{
			Worker: urutauv1alpha1.WorkerDefaults{CPU: "500m", CPUOverhead: "100m", Memory: "1Gi", MemoryOverhead: "256Mi"},
		},
	}
	table := urutauspec.Table{Target: "raw.orders", Workers: &urutauspec.WorkerSpec{Number: 3, CPU: "2", Memory: "4Gi"}}
	r := workerResources(cr, table)
	if r.Requests.Cpu().String() != "2" {
		t.Fatalf("cpu request = %v, want table override 2", r.Requests.Cpu())
	}
	if r.Limits.Cpu().String() != "2100m" {
		t.Fatalf("cpu limit = %v, want override(2) + default overhead(100m)", r.Limits.Cpu())
	}
	if r.Requests.Memory().String() != "4Gi" {
		t.Fatalf("memory request = %v, want table override 4Gi", r.Requests.Memory())
	}
}

// A table with no Workers block falls back to the pipeline-wide default
// entirely.
func TestWorkerResourcesFallsBackToDefault(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{
		Spec: urutauv1alpha1.CDCPipelineSpec{
			Worker: urutauv1alpha1.WorkerDefaults{CPU: "500m", Memory: "1Gi"},
		},
	}
	r := workerResources(cr, urutauspec.Table{Target: "raw.customers"})
	if r.Requests.Cpu().String() != "500m" || r.Requests.Memory().String() != "1Gi" {
		t.Fatalf("resources = %+v, want the pipeline default (500m/1Gi)", r)
	}
}

// The ConfigMap carries no worker pod templates at all when the CR has no
// resolvable image — every pipeline that never sets spec.image today is
// completely unaffected (no Kubernetes API surface touched, no template
// rendered).
func TestCoordinatorConfigMapNoImageMeansNoWorkerTemplates(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cm, err := coordinatorConfigMap(cr, "")
	if err != nil {
		t.Fatalf("coordinatorConfigMap: %v", err)
	}
	for k := range cm.Data {
		if k != "pipeline.yaml" {
			t.Fatalf("unexpected key %q with no image set", k)
		}
	}
}

// With an image resolved, one worker pod template is rendered per table,
// keyed by target — and it carries no --name (the coordinator, not the
// operator, stamps that on per worker group).
func TestCoordinatorConfigMapRendersOneTemplatePerTable(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Definition.Inline["tables"] = []any{
		map[string]any{"source": "shop.orders", "target": "raw.orders", "primaryKey": []any{"id"}},
		map[string]any{"source": "shop.customers", "target": "raw.customers", "primaryKey": []any{"id"}},
	}
	cm, err := coordinatorConfigMap(cr, "urutau:v1")
	if err != nil {
		t.Fatalf("coordinatorConfigMap: %v", err)
	}
	for _, target := range []string{"raw.orders", "raw.customers"} {
		key := workerPodTemplateKey(target)
		body, ok := cm.Data[key]
		if !ok {
			t.Fatalf("missing worker pod template key %q, got keys %v", key, keysOf(cm.Data))
		}
		if containsAny(body, "--name") {
			t.Fatalf("template for %s must not carry --name (the coordinator stamps it): %s", target, body)
		}
	}
}

// The worker reads its catalog settings from the URUTAU_SINK_* env the
// operator mounts; the warehouse is the one that is NOT a Secret and so
// must be carried across from the inline spec.
func TestWorkerPodTemplateCarriesWarehouse(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Definition.Inline["sink"] = map[string]any{"namespace": "raw", "warehouse": "my_warehouse"}
	tmpl := workerPodTemplate(cr, "urutau:v1", urutauspec.Table{Source: "shop.orders", Target: "raw.orders"})
	got := ""
	for _, e := range tmpl.Spec.Containers[0].Env {
		if e.Name == "URUTAU_SINK_WAREHOUSE" {
			got = e.Value
		}
	}
	if got != "my_warehouse" {
		t.Fatalf("URUTAU_SINK_WAREHOUSE = %q, want my_warehouse", got)
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func containsAny(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// coordinatorStatefulSet applies CoordinatorSpec.CPU/Memory to the actual
// container — CoordinatorSpec.CPU/Memory used to be a dead CRD field
// (ResourceRequirements was declared but coordinatorStatefulSet never
// read it); this guards against that regression recurring.
func TestCoordinatorStatefulSetAppliesResources(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Coordinator.CPU = "1"
	cr.Spec.Coordinator.Memory = "2Gi"
	sts := coordinatorStatefulSet(cr, "urutau:v1")
	c := sts.Spec.Template.Spec.Containers[0]
	if c.Resources.Requests.Cpu().String() != "1" || c.Resources.Requests.Memory().String() != "2Gi" {
		t.Fatalf("coordinator resources = %+v, want cpu=1 memory=2Gi", c.Resources)
	}
}
