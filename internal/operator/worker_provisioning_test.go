package operator

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	"github.com/maltzsama/urutau/internal/maintenance"
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
		key := urutauspec.WorkerPodTemplateKey(target)
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

// #170: a configured SSH Secret is mounted as a read-only file volume into
// the worker Pod when the source tunnels without a scoped snapshotUri;
// without it, no volume is added.
func TestWorkerPodTemplateMountsSSHKey(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Definition.Inline["source"] = map[string]any{
		"kind":     "postgres",
		"slotName": "s",
		"postgres": map[string]any{
			"host": "db", "database": "shop",
			"ssh": map[string]any{"host": "bastion", "username": "u", "privateKey": sshMountPath + "/privateKey"},
		},
	}
	tbl := urutauspec.Table{Source: "shop.orders", Target: "raw.orders"}

	// No secret: no ssh volume/mount.
	tmpl := workerPodTemplate(cr, "urutau:v1", tbl)
	if len(tmpl.Spec.Volumes) != 0 || len(tmpl.Spec.Containers[0].VolumeMounts) != 0 {
		t.Fatalf("no SSH secret must add no volume, got %+v / %+v",
			tmpl.Spec.Volumes, tmpl.Spec.Containers[0].VolumeMounts)
	}

	// Secret set: one read-only mount at sshMountPath.
	cr.Spec.Secrets.SSH = "ssh-secret"
	tmpl = workerPodTemplate(cr, "urutau:v1", tbl)
	if len(tmpl.Spec.Volumes) != 1 || tmpl.Spec.Volumes[0].Secret == nil ||
		tmpl.Spec.Volumes[0].Secret.SecretName != "ssh-secret" {
		t.Fatalf("ssh volume = %+v, want a secret volume for ssh-secret", tmpl.Spec.Volumes)
	}
	mounts := tmpl.Spec.Containers[0].VolumeMounts
	if len(mounts) != 1 || mounts[0].MountPath != sshMountPath || !mounts[0].ReadOnly {
		t.Fatalf("ssh mount = %+v, want read-only at %s", mounts, sshMountPath)
	}
}

// A scoped snapshotUri makes the worker connect directly, so the SSH key must
// not be mounted into worker Pods (only the coordinator needs it).
func TestWorkerPodTemplateSuppressesSSHWithSnapshotURI(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Definition.Inline["source"] = map[string]any{
		"kind":        "postgres",
		"slotName":    "s",
		"snapshotUri": "postgres://readonly@db/shop",
		"postgres": map[string]any{
			"host": "db", "database": "shop",
			"ssh": map[string]any{"host": "bastion", "username": "u", "privateKey": sshMountPath + "/privateKey"},
		},
	}
	cr.Spec.Secrets.SSH = "ssh-secret"
	tmpl := workerPodTemplate(cr, "urutau:v1", urutauspec.Table{Source: "shop.orders", Target: "raw.orders"})
	if len(tmpl.Spec.Volumes) != 0 {
		t.Fatalf("a scoped snapshotUri must suppress the worker SSH mount, got %+v", tmpl.Spec.Volumes)
	}
}

// The coordinator opens the replication connection, so it needs the SSH key
// even when a scoped snapshotUri spares the workers.
func TestCoordinatorStatefulSetMountsSSHKey(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Definition.Inline["source"] = map[string]any{
		"kind":        "postgres",
		"slotName":    "s",
		"snapshotUri": "postgres://readonly@db/shop",
		"postgres": map[string]any{
			"host": "db", "database": "shop",
			"ssh": map[string]any{"host": "bastion", "username": "u", "privateKey": sshMountPath + "/privateKey"},
		},
	}
	cr.Spec.Secrets.SSH = "ssh-secret"
	ss := coordinatorStatefulSet(cr, "urutau:v1")
	var found bool
	for _, m := range ss.Spec.Template.Spec.Containers[0].VolumeMounts {
		if m.MountPath == sshMountPath {
			found = true
		}
	}
	if !found {
		t.Fatalf("coordinator must mount the SSH key, mounts = %+v", ss.Spec.Template.Spec.Containers[0].VolumeMounts)
	}
}

// A discovery pipeline lists no tables, so the operator renders one generic
// worker template the coordinator clones for every discovered target.
func TestCoordinatorConfigMapGenericTemplateForDiscovery(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Definition.Inline["source"] = map[string]any{
		"kind":     "postgres",
		"slotName": "s",
		"postgres": map[string]any{"host": "db", "database": "shop", "discover": true},
	}
	delete(cr.Spec.Definition.Inline, "tables")
	cm, err := coordinatorConfigMap(cr, "urutau:v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cm.Data[urutauspec.WorkerPodTemplateKey(urutauspec.DefaultWorkerTemplateTarget)]; !ok {
		t.Fatalf("discovery must render a generic worker template, got keys %v", keysOf(cm.Data))
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

// withMaintenance turns sink.maintenance on in a CR's inline spec.
func withMaintenance(cr *urutauv1alpha1.CDCPipeline) *urutauv1alpha1.CDCPipeline {
	sink := cr.Spec.Definition.Inline["sink"].(map[string]any)
	sink["maintenance"] = map[string]any{
		"enabled":    true,
		"compaction": map[string]any{"interval": "1h"},
	}
	return cr
}

// podRule returns the coordinator Role's rule carrying the given pods verb.
func podRule(t *testing.T, cr *urutauv1alpha1.CDCPipeline, verb string) *rbacv1.PolicyRule {
	t.Helper()
	for _, r := range coordinatorRole(cr).Rules {
		if len(r.Resources) != 1 || r.Resources[0] != "pods" {
			continue
		}
		for _, v := range r.Verbs {
			if v == verb {
				rule := r
				return &rule
			}
		}
	}
	return nil
}

// The coordinator deletes only the maintenance Pods it provisions, so the
// Role names them explicitly rather than granting namespace-wide delete.
func TestCoordinatorRoleScopesPodDeleteToMaintenanceWorkers(t *testing.T) {
	cr := withMaintenance(pipelineCR("orders", "ns"))

	del := podRule(t, cr, "delete")
	if del == nil {
		t.Fatal("no pods/delete rule; the coordinator cannot clean up its maintenance Pods")
	}
	want := maintenance.WorkerName("e2e", "raw.orders")
	if len(del.ResourceNames) != 1 || del.ResourceNames[0] != want {
		t.Fatalf("delete resourceNames = %v, want exactly [%s]", del.ResourceNames, want)
	}

	// create cannot be name-scoped (the Pod does not exist yet), but it must
	// not be the rule that also carries delete.
	create := podRule(t, cr, "create")
	if create == nil {
		t.Fatal("no pods/create rule")
	}
	for _, v := range create.Verbs {
		if v == "delete" && len(create.ResourceNames) == 0 {
			t.Fatal("create and delete share an unscoped rule, which grants namespace-wide pod deletion")
		}
	}
}

// A pipeline that never enables maintenance provisions no maintenance Pods,
// so it gets no Pod write permission at all.
func TestCoordinatorRoleWithoutMaintenanceGrantsNoPodWrites(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	for _, verb := range []string{"create", "delete"} {
		if r := podRule(t, cr, verb); r != nil {
			t.Errorf("pods/%s granted with maintenance off: %+v", verb, r)
		}
	}
	// Reading its own Pod is still required, for the ownerReference.
	if podRule(t, cr, "get") == nil {
		t.Error("pods/get must remain: the coordinator reads its own Pod for the ownerReference")
	}
}

// The Role names the Pods the coordinator actually creates. If these drift,
// the coordinator silently loses the ability to delete its own workers.
func TestMaintenanceWorkerNamesMatchWhatTheCoordinatorCreates(t *testing.T) {
	names := maintenanceWorkerNames(withMaintenance(pipelineCR("orders", "ns")))
	want := maintenance.WorkerName("e2e", "raw.orders")
	if len(names) != 1 || names[0] != want {
		t.Fatalf("maintenanceWorkerNames = %v, want [%s] (same derivation the coordinator uses)", names, want)
	}
}
