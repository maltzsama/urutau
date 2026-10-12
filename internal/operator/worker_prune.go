package operator

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// workerWorkloadLabel is the label the coordinator stamps on every worker
// StatefulSet and its headless Service (see internal/coordk8s).
const workerWorkloadLabel = "urutau.io/worker"

// reconcileWorkers does the operator's part for the worker StatefulSets the
// coordinator creates: one KEDA ScaledObject per autoscalable table, targeting
// that table's StatefulSet (issue #298; a no-op unless the operator was
// started with a Prometheus address), and the removal of the workers of a
// table that left the pipeline (#835).
func (r *CoordinatorReconciler) reconcileWorkers(ctx context.Context, cr *urutauv1alpha1.CDCPipeline) error {
	if err := r.ensureScaledObjects(ctx, cr); err != nil {
		return err
	}
	return r.pruneWorkerWorkloads(ctx, cr)
}

// pruneWorkerWorkloads deletes the worker StatefulSets and Services of the
// tables that left the pipeline. The coordinator creates one StatefulSet per
// table and only ever creates or updates them, and its Role cannot list or
// delete: a removed table's pods would crash-loop against a coordinator that
// no longer plans their table, holding the CPU and memory they request for
// as long as the pipeline lives (#835).
//
// A workload is this pipeline's when it is owned by the pipeline's
// coordinator StatefulSet. A pipeline whose tables the operator cannot
// enumerate (discovery) is left alone.
func (r *CoordinatorReconciler) pruneWorkerWorkloads(ctx context.Context, cr *urutauv1alpha1.CDCPipeline) error {
	names := workerWorkloadNames(cr)
	if len(names) == 0 {
		return nil
	}
	keep := make(map[string]bool, len(names))
	for _, n := range names {
		keep[n] = true
	}
	coord := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: coordinatorName(cr), Namespace: cr.Namespace}, coord); err != nil {
		return client.IgnoreNotFound(err)
	}
	list := &appsv1.StatefulSetList{}
	if err := r.List(ctx, list, client.InNamespace(cr.Namespace), client.HasLabels{workerWorkloadLabel}); err != nil {
		return fmt.Errorf("list worker statefulsets: %w", err)
	}
	for i := range list.Items {
		sts := &list.Items[i]
		if keep[sts.Name] || !ownedByUID(sts.OwnerReferences, coord.UID) {
			continue
		}
		if err := r.Delete(ctx, sts); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete worker statefulset %s: %w", sts.Name, err)
		}
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: sts.Name, Namespace: sts.Namespace}}
		if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete worker service %s: %w", sts.Name, err)
		}
		log.FromContext(ctx).Info("worker statefulset deleted; its table left the pipeline", "statefulset", sts.Name)
	}
	return nil
}

// ownedByUID reports whether refs names an owner with this UID.
func ownedByUID(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}
