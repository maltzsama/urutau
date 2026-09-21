package operator

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	urutauspec "github.com/maltzsama/urutau/spec"
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
	got := urutauspec.WorkerPodTemplateKey("orders")
	want := "worker-pod-template.orders.yaml"
	if got != want {
		t.Errorf("WorkerPodTemplateKey = %q, want %q", got, want)
	}
}

// #250: validateSecrets must check the required keys, not just that the Secret
// object exists.
func TestValidateSecretsChecksKeys(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	mk := func(name string, data map[string][]byte) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}, Data: data}
	}
	cr := &urutauv1alpha1.CDCPipeline{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns"},
		Spec: urutauv1alpha1.CDCPipelineSpec{
			Secrets: urutauv1alpha1.Secrets{Source: "src", Catalog: "cat", SSH: "ssh"},
		},
	}
	with := func(objs ...client.Object) *CoordinatorReconciler {
		return &CoordinatorReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()}
	}

	// Source secret missing "uri".
	if err := with(
		mk("src", map[string][]byte{"nope": nil}),
		mk("cat", map[string][]byte{"uri": nil}),
		mk("ssh", map[string][]byte{"privateKey": nil}),
	).validateSecrets(context.Background(), cr); err == nil || !strings.Contains(err.Error(), "uri") {
		t.Fatalf("missing uri = %v, want an error naming uri", err)
	}
	// SSH secret missing "privateKey".
	if err := with(
		mk("src", map[string][]byte{"uri": nil}),
		mk("cat", map[string][]byte{"uri": nil}),
		mk("ssh", map[string][]byte{}),
	).validateSecrets(context.Background(), cr); err == nil || !strings.Contains(err.Error(), "privateKey") {
		t.Fatalf("missing privateKey = %v, want an error naming privateKey", err)
	}
	// All present.
	if err := with(
		mk("src", map[string][]byte{"uri": nil}),
		mk("cat", map[string][]byte{"uri": nil}),
		mk("ssh", map[string][]byte{"privateKey": nil}),
	).validateSecrets(context.Background(), cr); err != nil {
		t.Fatalf("valid secrets: %v", err)
	}
}

// #257: the status subresource must be its own rule without resourceNames —
// some authorizers ignore resourceNames on a subresource, so scoping it to the
// pipeline name would silently deny the coordinator its own status.
func TestCoordinatorRoleStatusSubresourceUnscoped(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "orders"}}
	role := coordinatorRole(cr)

	var statusRule *rbacv1.PolicyRule
	for i := range role.Rules {
		for _, res := range role.Rules[i].Resources {
			if res == "cdcpipelines/status" {
				statusRule = &role.Rules[i]
			}
		}
	}
	if statusRule == nil {
		t.Fatal("no rule grants cdcpipelines/status")
	}
	if len(statusRule.ResourceNames) != 0 {
		t.Fatalf("status rule is scoped by resourceNames: %v", statusRule.ResourceNames)
	}
	// The main resource stays scoped to this pipeline.
	if len(role.Rules[0].ResourceNames) != 1 || role.Rules[0].ResourceNames[0] != "orders" {
		t.Fatalf("cdcpipelines rule resourceNames = %v, want [orders]", role.Rules[0].ResourceNames)
	}
}

// #249: serverId uniqueness is enforced at admission, across CRs in the
// namespace.
func TestWebhookRejectsDuplicateServerID(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = urutauv1alpha1.AddToScheme(scheme)

	existing := pipelineCR("a", "ns")
	existing.Spec.Definition.Inline["source"].(map[string]any)["serverId"] = "77"
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	v := &pipelineValidator{client: cli}

	dup := pipelineCR("b", "ns")
	dup.Spec.Definition.Inline["source"].(map[string]any)["serverId"] = "77"
	if _, err := v.ValidateCreate(context.Background(), dup); err == nil {
		t.Fatal("webhook accepted a duplicate serverId")
	}

	distinct := pipelineCR("c", "ns")
	distinct.Spec.Definition.Inline["source"].(map[string]any)["serverId"] = "88"
	if _, err := v.ValidateCreate(context.Background(), distinct); err != nil {
		t.Fatalf("webhook rejected a distinct serverId: %v", err)
	}

	// Updating the same CR is not a collision with itself.
	if _, err := v.ValidateUpdate(context.Background(), existing, existing); err != nil {
		t.Fatalf("webhook rejected a self-update: %v", err)
	}
}

// The SSA field manager is configurable, not hardcoded.
func TestFieldManagerDefaultAndOverride(t *testing.T) {
	r := &CoordinatorReconciler{}
	if got := r.fieldManager(); got != DefaultFieldManager {
		t.Fatalf("default fieldManager = %q, want %q", got, DefaultFieldManager)
	}
	r.FieldManager = "custom-controller"
	if got := r.fieldManager(); got != "custom-controller" {
		t.Fatalf("fieldManager = %q, want custom-controller", got)
	}
}
