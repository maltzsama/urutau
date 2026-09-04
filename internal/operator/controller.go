// Package operator reconciles CDCPipeline CRs into a coordinator
// StatefulSet (§16): one coordinator per pipeline, owning the workers it
// creates. The operator stops reconciling once status.terminated is set —
// the job stays dead until a human clears it.
package operator

import (
	"context"
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

const finalizer = "urutau.io/finalizer"

// coordinatorSAName is the service account the coordinator pod runs as.
// The reconciler creates it (with its Role and binding) in the pipeline's
// namespace so the coordinator can read and write its own CR status.
const coordinatorSAName = "urutau-coordinator"

// CoordinatorReconciler reconciles a CDCPipeline.
type CoordinatorReconciler struct {
	client.Client
	Image string
}

// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps;services,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;create
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;create

// SetupWithManager wires the reconciler into the manager.
func (r *CoordinatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&urutauv1alpha1.CDCPipeline{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

// Reconcile reconciles a CDCPipeline: it ensures the coordinator workload
// exists, respects the terminal state, and cleans up on delete.
func (r *CoordinatorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	cr := &urutauv1alpha1.CDCPipeline{}
	if err := r.Get(ctx, req.NamespacedName, cr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal: the operator PARTS WAYS (§10). Nothing recreates the job.
	if cr.Status.Terminated != nil {
		log.Info("pipeline terminated; not reconciling", "reason", cr.Status.Terminated.Reason)
		return ctrl.Result{}, nil
	}

	// Finalizer: clean up the coordinator (and, for PG, the slot) on delete.
	if cr.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(cr, finalizer) {
			controllerutil.AddFinalizer(cr, finalizer)
			if err := r.Update(ctx, cr); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		if controllerutil.ContainsFinalizer(cr, finalizer) {
			if err := r.deleteCoordinator(ctx, cr); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(cr, finalizer)
			if err := r.Update(ctx, cr); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure the coordinator identity (service account, Role, binding),
	// the headless Service, the ConfigMap (the resolved spec payload), and
	// the StatefulSet (ownerReference → cascade GC).
	sa := coordinatorServiceAccount(cr)
	if err := controllerutil.SetControllerReference(cr, sa, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensure(ctx, sa, "coordinator service account"); err != nil {
		return ctrl.Result{}, err
	}
	role := coordinatorRole(cr)
	if err := controllerutil.SetControllerReference(cr, role, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensure(ctx, role, "coordinator role"); err != nil {
		return ctrl.Result{}, err
	}
	rb := coordinatorRoleBinding(cr)
	if err := controllerutil.SetControllerReference(cr, rb, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensure(ctx, rb, "coordinator role binding"); err != nil {
		return ctrl.Result{}, err
	}

	svc := coordinatorService(cr)
	if err := controllerutil.SetControllerReference(cr, svc, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensure(ctx, svc, "coordinator service"); err != nil {
		return ctrl.Result{}, err
	}

	cm := coordinatorConfigMap(cr)
	if err := controllerutil.SetControllerReference(cr, cm, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensure(ctx, cm, "coordinator config"); err != nil {
		return ctrl.Result{}, err
	}

	sts := coordinatorStatefulSet(cr, r.Image)
	if err := controllerutil.SetControllerReference(cr, sts, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensure(ctx, sts, "coordinator"); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("coordinator ensured", "statefulset", sts.Name)

	return ctrl.Result{}, nil
}

// ensure creates the object when absent. Existing objects are left untouched:
// the workload lifecycle after creation belongs to upgrades, not reconcile.
func (r *CoordinatorReconciler) ensure(ctx context.Context, obj client.Object, what string) error {
	key := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
	err := r.Get(ctx, key, obj)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, obj); err != nil {
			return fmt.Errorf("create %s: %w", what, err)
		}
		return nil
	}
	return err
}

// deleteCoordinator removes the coordinator StatefulSet (the worker pods are
// GC'd by ownership).
func (r *CoordinatorReconciler) deleteCoordinator(ctx context.Context, cr *urutauv1alpha1.CDCPipeline) error {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      coordinatorName(cr),
		Namespace: cr.Namespace,
	}}
	err := r.Delete(ctx, sts)
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func coordinatorName(cr *urutauv1alpha1.CDCPipeline) string {
	return cr.Name + "-coordinator"
}

func pipelineLabels(cr *urutauv1alpha1.CDCPipeline) map[string]string {
	return map[string]string{"urutau.io/pipeline": cr.Name}
}

// coordinatorServiceAccount is the identity the coordinator pod runs as,
// bound (below) to a Role that lets it read and write its own CR status.
func coordinatorServiceAccount(cr *urutauv1alpha1.CDCPipeline) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorSAName, Namespace: cr.Namespace,
			Labels: pipelineLabels(cr)},
	}
}

// coordinatorRole grants the coordinator access to its own pipeline CR and
// its status subresource — the contract is that the coordinator, not the
// operator, writes status.
func coordinatorRole(cr *urutauv1alpha1.CDCPipeline) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace,
			Labels: pipelineLabels(cr)},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"urutau.io"},
			Resources: []string{"cdcpipelines", "cdcpipelines/status"},
			Verbs:     []string{"get", "update", "patch"},
		}},
	}
}

func coordinatorRoleBinding(cr *urutauv1alpha1.CDCPipeline) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace,
			Labels: pipelineLabels(cr)},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      coordinatorSAName,
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
	labels := pipelineLabels(cr)
	labels["app"] = "urutau-coordinator"
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace,
			Labels: labels},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports: []corev1.ServicePort{{
				Name: "grpc",
				Port: 50051,
			}},
		},
	}
}

// coordinatorConfigMap holds the resolved spec payload the coordinator
// reads at boot. For now the inline tables become the spec's tables; the
// planner (SDK repo) will render the full resolvedSpec.
func coordinatorConfigMap(cr *urutauv1alpha1.CDCPipeline) *corev1.ConfigMap {
	name := coordinatorName(cr)
	payload, _ := json.Marshal(cr.Spec.Definition.Tables)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cr.Namespace,
			Labels: map[string]string{"urutau.io/pipeline": cr.Name}},
		Data: map[string]string{"pipeline.json": string(payload)},
	}
}

// coordinatorStatefulSet builds the coordinator workload from the CR.
func coordinatorStatefulSet(cr *urutauv1alpha1.CDCPipeline, image string) *appsv1.StatefulSet {
	labels := pipelineLabels(cr)
	labels["app"] = "urutau-coordinator"
	name := coordinatorName(cr)

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cr.Namespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    int32Ptr(1),
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: coordinatorSAName,
					Containers: []corev1.Container{{
						Name:         "coordinator",
						Image:        image,
						Command:      []string{"urutau-coordinator", "run", "--file", "/etc/urutau/pipeline.json"},
						VolumeMounts: []corev1.VolumeMount{{Name: "spec", MountPath: "/etc/urutau"}},
					}},
					Volumes: []corev1.Volume{{
						Name: "spec",
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: name},
						}},
					}},
				},
			},
		},
	}
}

func int32Ptr(v int32) *int32 { return &v }
