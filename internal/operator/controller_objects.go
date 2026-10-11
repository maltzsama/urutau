package operator

import (
	"fmt"
	"sort"
	"strings"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	"github.com/maltzsama/urutau/internal/maintenance"
	urutauspec "github.com/maltzsama/urutau/spec"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// coordinatorServiceAccount is the identity the coordinator pod runs as,
// bound (below) to a Role that lets it read and write its own CR status.
func coordinatorServiceAccount(cr *urutauv1alpha1.CDCPipeline) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorSAName(cr), Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
	}
}

// coordinatorRole grants the coordinator the least privilege it needs:
//
//   - the worker StatefulSets/Services it provisions (issue #298). create
//     cannot be name-scoped (RBAC matches resourceNames against an existing
//     object), so it stays namespace-wide; get/update are scoped to the worker
//     names when the operator can enumerate them (a declared table list). A
//     discovery pipeline's targets are unknown until boot, so its get/update
//     stay namespace-wide.
//   - reading its own Pod, to derive the ownerReference its worker workloads
//     carry.
//   - the ephemeral maintenance Pods it creates and deletes (issue #105),
//     name-scoped, only when maintenance is on.
//
// It grants nothing on the CDCPipeline CR: the coordinator does not write
// status (issue #595).
func coordinatorRole(cr *urutauv1alpha1.CDCPipeline) *rbacv1.Role {
	rules := []rbacv1.PolicyRule{
		// create cannot be name-scoped: RBAC matches resourceNames against an
		// existing object, and the worker does not exist yet.
		{APIGroups: []string{"apps"}, Resources: []string{"statefulsets"}, Verbs: []string{"create"}},
		{APIGroups: []string{""}, Resources: []string{"services"}, Verbs: []string{"create"}},
		// Reading its own Pod (by hostname) derives the ownerReference its
		// worker workloads carry (a stable StatefulSet, issue #599).
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get"}},
	}
	if names := workerWorkloadNames(cr); len(names) > 0 {
		// get/update scoped to THIS pipeline's worker names, so a compromised
		// coordinator cannot touch another pipeline's workloads.
		rules = append(rules,
			rbacv1.PolicyRule{APIGroups: []string{"apps"}, Resources: []string{"statefulsets"}, ResourceNames: names, Verbs: []string{"get", "update"}},
			rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"services"}, ResourceNames: names, Verbs: []string{"get", "update"}},
		)
	} else {
		rules = append(rules,
			rbacv1.PolicyRule{APIGroups: []string{"apps"}, Resources: []string{"statefulsets"}, Verbs: []string{"get", "update"}},
			rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"services"}, Verbs: []string{"get", "update"}},
		)
	}
	if names := maintenanceWorkerNames(cr); len(names) > 0 {
		rules = append(rules,
			rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"create"}},
			rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"pods"}, ResourceNames: names, Verbs: []string{"delete"}},
		)
	}
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
		Rules: rules,
	}
}

// maintenanceWorkerNames returns the Pod names this pipeline's coordinator
// may delete — one ephemeral maintenance worker per table — or nil when
// maintenance is off, so no Pod write permission is granted at all. The
// names must match coordinator.maintenanceWorkerName exactly; a spec that
// does not parse yields nil, and the coordinator fails on the same spec
// before it could provision anything.
func maintenanceWorkerNames(cr *urutauv1alpha1.CDCPipeline) []string {
	if len(cr.Spec.Definition.Inline) == 0 {
		return nil
	}
	payload, err := yaml.Marshal(cr.Spec.Definition.Inline)
	if err != nil {
		return nil
	}
	s, err := urutauspec.LoadYAML(strings.NewReader(string(payload)))
	if err != nil || !s.Sink.MaintenanceEnabled() {
		return nil
	}
	names := make([]string, 0, len(s.Tables))
	for _, t := range s.Tables {
		names = append(names, maintenance.WorkerName(s.Pipeline, t.Target))
	}
	sort.Strings(names) // stable Role content, so ensure() sees no spurious drift
	return names
}

func coordinatorRoleBinding(cr *urutauv1alpha1.CDCPipeline) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      coordinatorSAName(cr),
			Namespace: cr.Namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     coordinatorName(cr),
		},
	}
}

// coordinatorService is the headless service the StatefulSet's stable
// network identity hangs off.
func coordinatorService(cr *urutauv1alpha1.CDCPipeline) *corev1.Service {
	labels := selectorLabels(cr)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace,
			Labels: labels},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			// The workers dial <coordinator>.<ns>:50051 before the coordinator
			// is Ready (it only becomes Ready once the stream is up, which
			// needs the workers). A headless Service publishes an endpoint —
			// and DNS — only for Ready pods, so without this the coordinator's
			// readiness probe would deadlock boot: not Ready → no endpoint →
			// no worker → not Ready. Publish the not-ready address so the
			// StatefulSet can bootstrap (issue #601 follow-up).
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{{
				Name: "grpc",
				Port: 50051,
			}},
		},
	}
}

// coordinatorConfigMap holds the resolved spec the coordinator reads at
// boot. For an inline definition the spec is rendered verbatim (credentials
// left empty — they arrive as env from the mounted Secrets). A planner
// (image/s3 definitions) will render the same artifact.
//
// The same ConfigMap also carries one worker pod template per table —
// "worker-pod-template.<target>.yaml" — the coordinator reads at boot to
// provision that table's worker group(s) (spec.Table.WorkerGroupNames):
// the coordinator only ever copies this template and stamps a derived
// --name onto it, it never builds a PodSpec of its own (mirrors Spark:
// the driver clones the executor pod template, it doesn't assemble one
// field by field). Emitted only when cr.Spec.Image is resolvable —
// without an image there is nothing to provision, and every existing
// pipeline that never sets spec.image is completely unaffected.
func coordinatorConfigMap(cr *urutauv1alpha1.CDCPipeline, image string) (*corev1.ConfigMap, error) {
	name := coordinatorName(cr)
	// The caller has already run validateSpec (which requires inline), so this
	// does not repeat that check (issue #255).
	inline := cr.Spec.Definition.Inline
	payload, err := yaml.Marshal(inline)
	if err != nil {
		return nil, fmt.Errorf("render inline spec: %w", err)
	}
	data := map[string]string{"pipeline.yaml": string(payload)}

	if image != "" {
		s, err := urutauspec.LoadYAML(strings.NewReader(string(payload)))
		if err != nil {
			return nil, fmt.Errorf("parse inline spec for worker provisioning: %w", err)
		}
		for _, t := range s.Tables {
			tmpl := workerPodTemplate(cr, image, t)
			b, err := yaml.Marshal(tmpl)
			if err != nil {
				return nil, fmt.Errorf("render worker pod template for %s: %w", t.Target, err)
			}
			data[urutauspec.WorkerPodTemplateKey(t.Target)] = string(b)
		}
		// A discovery pipeline lists no tables at operator time — the source
		// enumerates them at boot — so no per-table template can be rendered.
		// Emit one generic template the coordinator clones for every
		// discovered target (#152).
		if s.Source.Postgres != nil && s.Source.Postgres.Discover {
			tmpl := workerPodTemplate(cr, image, urutauspec.Table{})
			b, err := yaml.Marshal(tmpl)
			if err != nil {
				return nil, fmt.Errorf("render generic worker pod template: %w", err)
			}
			data[urutauspec.WorkerPodTemplateKey(urutauspec.DefaultWorkerTemplateTarget)] = string(b)
		}
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
		Data: data,
	}, nil
}
