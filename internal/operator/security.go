package operator

import (
	"context"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	urutauspec "github.com/maltzsama/urutau/spec"
)

// This file carries the operator's Pod identity and hardening: the two
// ServiceAccounts (a coordinator that needs RBAC and a worker that needs
// none), the security contexts every managed Pod gets, and the scratch volume
// a read-only root filesystem requires. Kept out of controller.go so that file
// stays within the size ratchet.

// workerSAName is the ServiceAccount a pipeline's worker Pods run as. It is
// DISTINCT from the coordinator's (issue #603): the worker never talks to the
// Kubernetes API, so it must not carry the coordinator's Role — which can
// create and delete the pipeline's StatefulSets. A compromised worker (or a
// plugin running in it) could otherwise delete its siblings.
func workerSAName(cr *urutauv1alpha1.CDCPipeline) string {
	return cr.Name + "-worker"
}

// workerServiceAccount is the worker's identity: no Role is bound to it, and
// the token is not mounted at all, so a worker Pod cannot reach the API even
// to read (issue #603).
func workerServiceAccount(cr *urutauv1alpha1.CDCPipeline) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: workerSAName(cr), Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
		AutomountServiceAccountToken: boolPtr(false),
	}
}

// ensureIdentity applies the pipeline's two ServiceAccounts: the coordinator's
// (bound to its Role) and the worker's (bound to nothing, token not mounted —
// issue #603). Both are owned by the CR so deletion cascades.
func (r *CoordinatorReconciler) ensureIdentity(ctx context.Context, cr *urutauv1alpha1.CDCPipeline) error {
	coordSA := coordinatorServiceAccount(cr)
	workerSA := workerServiceAccount(cr)
	for _, obj := range []client.Object{coordSA, workerSA} {
		if err := controllerutil.SetControllerReference(cr, obj, r.Scheme()); err != nil {
			return err
		}
	}
	if err := r.ensure(ctx, coordSA, "coordinator service account"); err != nil {
		return err
	}
	return r.ensure(ctx, workerSA, "worker service account")
}

// boolPtr returns a pointer to b, for the many optional bool fields in the
// Kubernetes API.
func boolPtr(b bool) *bool { return &b }

// podSecurityContext is the pod-level hardening every managed Pod gets (issue
// #596): no root and the RuntimeDefault seccomp profile. The image already
// runs as the distroless nonroot user; stating it lets a namespace under the
// `restricted` Pod Security Standard admit the Pod.
func podSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot:   boolPtr(true),
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// containerSecurityContext hardens the managed container (issue #596): no
// privilege escalation, every capability dropped, and a read-only root
// filesystem. The one writable location the binaries use — plugin sockets and
// Go temp files — is /tmp, mounted as an emptyDir by tmpVolume.
func containerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: boolPtr(false),
		ReadOnlyRootFilesystem:   boolPtr(true),
		RunAsNonRoot:             boolPtr(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// tmpVolume is the writable scratch volume a read-only root filesystem needs:
// plugin unix sockets (os.TempDir) and Go's temp files all live under /tmp.
func tmpVolume() (corev1.Volume, corev1.VolumeMount) {
	return corev1.Volume{
			Name:         "tmp",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		corev1.VolumeMount{Name: "tmp", MountPath: "/tmp"}
}

// workerWorkloadNames returns the StatefulSet/Service names the coordinator
// provisions for this pipeline — one per declared table — or nil when they
// cannot be enumerated. A discovery pipeline lists its tables at boot, and an
// unparseable spec yields nil (the operator surfaces the real error elsewhere);
// in both cases the role grants get/update namespace-wide instead.
//
// The names must match coordinator.workers_k8s.go's
// spec.WorkerGroupPrefix(pipeline, target) exactly, or the coordinator would be
// denied get/update on its own workers.
func workerWorkloadNames(cr *urutauv1alpha1.CDCPipeline) []string {
	if len(cr.Spec.Definition.Inline) == 0 {
		return nil
	}
	payload, err := yaml.Marshal(cr.Spec.Definition.Inline)
	if err != nil {
		return nil
	}
	s, err := urutauspec.LoadYAML(strings.NewReader(string(payload)))
	if err != nil {
		return nil
	}
	if s.Source.Postgres != nil && s.Source.Postgres.Discover {
		return nil // targets are discovered at boot; they cannot be name-scoped
	}
	if len(s.Tables) == 0 {
		return nil
	}
	names := make([]string, 0, len(s.Tables))
	for _, t := range s.Tables {
		names = append(names, urutauspec.WorkerGroupPrefix(s.Pipeline, t.Target))
	}
	sort.Strings(names) // stable Role content, so ensure() sees no spurious drift
	return names
}

// int64Ptr returns a pointer to v (Kubernetes grace periods are *int64).
func int64Ptr(v int64) *int64 { return &v }

// intstrPtr returns a pointer to an IntOrString built from v.
func intstrPtr(v int) *intstr.IntOrString {
	x := intstr.FromInt(v)
	return &x
}

// coordinatorPodDisruptionBudget bounds voluntary disruption of the (single)
// coordinator so a node drain evicts it at most one at a time and waits for
// its graceful shutdown. maxUnavailable=1, not minAvailable=1: on a
// 1-replica StatefulSet minAvailable=1 would block every voluntary eviction
// and wedge the drain (issue #604).
func coordinatorPodDisruptionBudget(cr *urutauv1alpha1.CDCPipeline) *policyv1.PodDisruptionBudget {
	labels := selectorLabels(cr)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace, Labels: labels},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: intstrPtr(1),
			Selector:       &metav1.LabelSelector{MatchLabels: labels},
		},
	}
}

// ensureDisruptionBudget applies the coordinator's PodDisruptionBudget, owned
// by the CR so deletion cascades.
func (r *CoordinatorReconciler) ensureDisruptionBudget(ctx context.Context, cr *urutauv1alpha1.CDCPipeline) error {
	pdb := coordinatorPodDisruptionBudget(cr)
	if err := controllerutil.SetControllerReference(cr, pdb, r.Scheme()); err != nil {
		return err
	}
	return r.ensure(ctx, pdb, "coordinator pod disruption budget")
}
