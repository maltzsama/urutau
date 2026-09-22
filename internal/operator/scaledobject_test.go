package operator

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	urutauspec "github.com/maltzsama/urutau/spec"
)

// scaledObjectScheme registers the unstructured ScaledObject GVK so the fake
// client can track it without a KEDA Go dependency.
func scaledObjectScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = urutauv1alpha1.AddToScheme(scheme)
	scheme.AddKnownTypeWithName(kedaGroupVersion, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(kedaGroupVersion.GroupVersion().WithKind("ScaledObjectList"), &unstructured.UnstructuredList{})
	return scheme
}

// The ScaledObject must target the worker StatefulSet the coordinator creates
// ("<pipeline>-<target>"), between the configured count and workers.max, on
// the per-table lag gauge (issue #298).
func TestScaledObjectTargetsWorkerStatefulSet(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns"}}
	tbl := urutauspec.Table{Target: "raw.orders", Workers: &urutauspec.WorkerSpec{Number: 2, Max: 8}}

	obj := scaledObject(cr, "shop", tbl, "http://prom:9090", "30")

	if obj.GetName() != "shop-raw.orders" || obj.GetNamespace() != "ns" {
		t.Fatalf("identity = %s/%s, want ns/shop-raw.orders", obj.GetNamespace(), obj.GetName())
	}
	if obj.GroupVersionKind() != kedaGroupVersion {
		t.Fatalf("gvk = %v, want %v", obj.GroupVersionKind(), kedaGroupVersion)
	}

	spec, _ := obj.Object["spec"].(map[string]any)
	target, _ := spec["scaleTargetRef"].(map[string]any)
	if target["kind"] != "StatefulSet" || target["name"] != "shop-raw.orders" || target["apiVersion"] != "apps/v1" {
		t.Fatalf("scaleTargetRef = %v", target)
	}
	if spec["minReplicaCount"] != int64(2) || spec["maxReplicaCount"] != int64(8) {
		t.Fatalf("replica bounds = %v..%v, want 2..8", spec["minReplicaCount"], spec["maxReplicaCount"])
	}

	triggers, _ := spec["triggers"].([]any)
	if len(triggers) != 1 {
		t.Fatalf("triggers = %v", triggers)
	}
	tr := triggers[0].(map[string]any)
	if tr["type"] != "prometheus" {
		t.Fatalf("trigger type = %v", tr["type"])
	}
	md := tr["metadata"].(map[string]any)
	if md["serverAddress"] != "http://prom:9090" || md["threshold"] != "30" {
		t.Fatalf("trigger metadata = %v", md)
	}
	if md["metricName"] != kedaMetricName {
		t.Fatalf("metricName = %v, want %v", md["metricName"], kedaMetricName)
	}
	if md["query"] != `urutau_coordinator_lag_seconds{table="raw.orders"}` {
		t.Fatalf("query = %v", md["query"])
	}
	if len(obj.GetOwnerReferences()) != 1 || obj.GetOwnerReferences()[0].Name != "orders" {
		t.Fatalf("ownerReferences = %v", obj.GetOwnerReferences())
	}
}

// A table with no partition cap is not autoscalable; scaledObject is only
// built for tables the caller already filtered, so this pins the filtering
// contract through the helper the caller uses.
func TestWorkerCountIsTheScaledObjectMinimum(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns"}}
	// Number<=1 means a single worker: the floor is 1, never 0 (routing
	// always has an owner).
	tbl := urutauspec.Table{Target: "raw.orders", Workers: &urutauspec.WorkerSpec{Max: 4}}
	obj := scaledObject(cr, "shop", tbl, "http://prom:9090", "30")
	spec := obj.Object["spec"].(map[string]any)
	if spec["minReplicaCount"] != int64(1) {
		t.Fatalf("minReplicaCount = %v, want 1", spec["minReplicaCount"])
	}
}

// With no Prometheus address, autoscaling is off: no ScaledObject is created,
// and a stale one from a previous run is pruned.
func TestEnsureScaledObjectsDisabledWithoutPrometheus(t *testing.T) {
	scheme := scaledObjectScheme()
	cr := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns"}}

	stale := &unstructured.Unstructured{}
	stale.SetGroupVersionKind(kedaGroupVersion)
	stale.SetName("shop-raw.orders")
	stale.SetNamespace("ns")
	stale.SetLabels(selectorLabels(cr))

	r := &CoordinatorReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(stale).Build()}
	if err := r.ensureScaledObjects(context.Background(), cr); err != nil {
		t.Fatalf("disabled ensureScaledObjects: %v", err)
	}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(kedaGroupVersion)
	err := r.Get(context.Background(), types.NamespacedName{Name: "shop-raw.orders", Namespace: "ns"}, got)
	if err == nil {
		t.Fatal("stale ScaledObject survived with autoscaling off")
	}
}

// With a Prometheus address, a table that declares workers.max gets a
// ScaledObject targeting its worker StatefulSet; a table without one does not.
func TestEnsureScaledObjectsCreatesForCappedTables(t *testing.T) {
	scheme := scaledObjectScheme()
	cr := pipelineCR("orders", "ns")
	// Two tables: raw.orders is autoscalable, raw.items is not.
	cr.Spec.Definition.Inline["tables"] = []any{
		map[string]any{"source": "shop.orders", "target": "raw.orders", "primaryKey": []any{"id"},
			"workers": map[string]any{"number": 2, "max": 8}},
		map[string]any{"source": "shop.items", "target": "raw.items", "primaryKey": []any{"id"}},
	}

	r := &CoordinatorReconciler{
		Client:                fake.NewClientBuilder().WithScheme(scheme).Build(),
		KEDAPrometheusAddress: "http://prom:9090",
	}
	if err := r.ensureScaledObjects(context.Background(), cr); err != nil {
		t.Fatalf("ensureScaledObjects: %v", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(kedaGroupVersion)
	if err := r.Get(context.Background(), types.NamespacedName{Name: "e2e-raw.orders", Namespace: "ns"}, got); err != nil {
		t.Fatalf("ScaledObject for the capped table: %v", err)
	}
	err := r.Get(context.Background(), types.NamespacedName{Name: "e2e-raw.items", Namespace: "ns"}, &unstructured.Unstructured{})
	if err == nil {
		t.Fatal("a table without workers.max must not get a ScaledObject")
	}
}

// The coordinator's Role must let it create the worker StatefulSets and their
// headless Services, and must no longer grant deployments (issue #298).
func TestCoordinatorRoleGrantsWorkerStatefulSetsAndServices(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{ObjectMeta: metav1.ObjectMeta{Name: "orders"}}
	role := coordinatorRole(cr)

	has := func(group, resource string) bool {
		for _, r := range role.Rules {
			inGroup, inResource := false, false
			for _, g := range r.APIGroups {
				if g == group {
					inGroup = true
				}
			}
			for _, res := range r.Resources {
				if res == resource {
					inResource = true
				}
			}
			if inGroup && inResource {
				return true
			}
		}
		return false
	}

	if !has("apps", "statefulsets") {
		t.Error("coordinator Role does not grant apps/statefulsets")
	}
	if !has("", "services") {
		t.Error("coordinator Role does not grant core/services")
	}
	if has("apps", "deployments") {
		t.Error("coordinator Role still grants apps/deployments; workers are StatefulSets now")
	}
}
