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

// CoordinatorReconciler reconciles a CDCPipeline.
type CoordinatorReconciler struct {
	client.Client
	Image string
}

// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps;services;secrets,verbs=get;list;watch;create;update;patch;delete

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

	// Ensure the coordinator ConfigMap (the resolved spec payload) and the
	// StatefulSet (ownerReference → cascade GC).
	cm := coordinatorConfigMap(cr)
	if err := controllerutil.SetControllerReference(cr, cm, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	existingCM := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: cm.Namespace}, existingCM); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, cm); err != nil {
			return ctrl.Result{}, fmt.Errorf("create coordinator config: %w", err)
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}

	sts := coordinatorStatefulSet(cr, r.Image)
	if err := controllerutil.SetControllerReference(cr, sts, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	existing := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, sts); err != nil {
			return ctrl.Result{}, fmt.Errorf("create coordinator: %w", err)
		}
		log.Info("coordinator created", "statefulset", sts.Name)
	} else if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
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
	labels := map[string]string{"urutau.io/pipeline": cr.Name, "app": "urutau-coordinator"}
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
					Containers: []corev1.Container{{
						Name:    "coordinator",
						Image:   image,
						Command: []string{"urutau-coordinator", "run", "--file", "/etc/urutau/pipeline.json"},
						Env: []corev1.EnvVar{
							{Name: "URUTAU_SOURCE_URI", Value: ""},
						},
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
