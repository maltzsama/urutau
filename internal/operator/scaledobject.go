package operator

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	urutauspec "github.com/maltzsama/urutau/spec"
)

// kedaGroupVersion is the ScaledObject GVK the operator renders. It is kept
// as an unstructured object so the operator carries no KEDA Go dependency:
// the type is only needed when KEDA is installed, and a cluster without it
// must still run every other pipeline (issue #298).
var kedaGroupVersion = schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObject"}

// kedaMetricName is the coordinator's per-table backlog gauge: the number of
// batches the table still owes. It is the signal KEDA scales on. The lag
// gauge only rises when commits STALL, so a backlog the workers are still
// draining never moves it — measured under a 50k-row burst, lag stayed ~0.1s
// while the table owed ~50k rows (issue #298).
const kedaMetricName = "urutau_coordinator_pending_batches"

// defaultKEDAThreshold is the per-replica backlog target when the operator
// flag is unset: roughly one worker per 30 outstanding batches.
const defaultKEDAThreshold = "30"

// ensureScaledObjects renders one ScaledObject per autoscalable table — a
// table that declares a partition cap (spec.workers.max), which becomes
// maxReplicaCount. It is a no-op unless the operator was started with a
// Prometheus address: that flag is the opt-in that turns KEDA autoscaling on
// (issue #298). ScaledObjects this pipeline no longer wants are pruned, so
// removing workers.max stops autoscaling rather than leaving a stale object.
func (r *CoordinatorReconciler) ensureScaledObjects(ctx context.Context, cr *urutauv1alpha1.CDCPipeline) error {
	desired := map[string]*unstructured.Unstructured{}
	if r.KEDAPrometheusAddress != "" {
		payload, err := yaml.Marshal(cr.Spec.Definition.Inline)
		if err != nil {
			return fmt.Errorf("render inline spec: %w", err)
		}
		s, err := urutauspec.LoadYAML(strings.NewReader(string(payload)))
		if err != nil {
			return fmt.Errorf("parse inline spec for autoscaling: %w", err)
		}
		for _, t := range s.Tables {
			if t.Workers == nil || t.Workers.Max <= 0 {
				continue
			}
			obj := scaledObject(cr, s.Pipeline, t, r.KEDAPrometheusAddress, r.kedaThreshold())
			desired[obj.GetName()] = obj
		}
	}

	// Best-effort prune: a List failure (KEDA absent, or a cache that cannot
	// serve the unstructured type) must not fail the pipeline reconcile.
	existing := &unstructured.UnstructuredList{}
	existing.SetGroupVersionKind(kedaGroupVersion.GroupVersion().WithKind("ScaledObjectList"))
	if err := r.List(ctx, existing, client.InNamespace(cr.Namespace), client.MatchingLabels(selectorLabels(cr))); err == nil {
		for i := range existing.Items {
			if _, ok := desired[existing.Items[i].GetName()]; !ok {
				if err := r.Delete(ctx, &existing.Items[i]); err != nil && !meta.IsNoMatchError(err) {
					log.FromContext(ctx).Info("delete stale ScaledObject failed", "object", existing.Items[i].GetName(), "err", err)
				}
			}
		}
	}

	for _, obj := range desired {
		if err := r.applyScaledObject(ctx, obj); err != nil {
			return err
		}
	}
	return nil
}

// scaledObject builds the ScaledObject for one table: it scales that table's
// worker StatefulSet ("<pipeline>-<target>") between its configured count
// and spec.workers.max, driven by the per-table lag gauge.
func scaledObject(cr *urutauv1alpha1.CDCPipeline, pipeline string, t urutauspec.Table, promAddr, threshold string) *unstructured.Unstructured {
	name := urutauspec.WorkerGroupPrefix(pipeline, t.Target)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(kedaGroupVersion)
	obj.SetName(name)
	obj.SetNamespace(cr.Namespace)
	obj.SetLabels(selectorLabels(cr))
	obj.SetOwnerReferences([]metav1.OwnerReference{
		*metav1.NewControllerRef(cr, urutauv1alpha1.GroupVersion.WithKind("CDCPipeline")),
	})
	obj.Object["spec"] = map[string]any{
		"scaleTargetRef": map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "StatefulSet",
			"name":       name,
		},
		// minReplicaCount is the table's configured count: autoscaling never
		// drops below the declared baseline, only above it.
		"minReplicaCount": int64(t.WorkerCount()),
		"maxReplicaCount": int64(t.Workers.Max),
		"triggers": []any{
			map[string]any{
				"type": "prometheus",
				"metadata": map[string]any{
					"serverAddress": promAddr,
					"metricName":    kedaMetricName,
					"query":         fmt.Sprintf("%s{table=\"%s\"}", kedaMetricName, t.Target),
					"threshold":     threshold,
				},
			},
		},
	}
	return obj
}

// applyScaledObject creates the ScaledObject if absent, or updates it in
// place when it differs — the same declarative shape the coordinator uses
// for its workloads, rather than server-side apply (which the fake client
// cannot create-with, and this object has a single owner anyway). A cluster
// without the KEDA CRD installed has no keda.sh REST mapping; that is not a
// failure of the pipeline, so it is skipped with a log line.
func (r *CoordinatorReconciler) applyScaledObject(ctx context.Context, obj *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(kedaGroupVersion)
	err := r.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, obj); err != nil {
			if meta.IsNoMatchError(err) {
				log.FromContext(ctx).Info("KEDA ScaledObject CRD not installed; skipping autoscaling", "object", obj.GetName())
				return nil
			}
			return fmt.Errorf("create ScaledObject %s: %w", obj.GetName(), err)
		}
		return nil
	}
	if err != nil {
		if meta.IsNoMatchError(err) {
			log.FromContext(ctx).Info("KEDA ScaledObject CRD not installed; skipping autoscaling", "object", obj.GetName())
			return nil
		}
		return fmt.Errorf("get ScaledObject %s: %w", obj.GetName(), err)
	}
	obj.SetResourceVersion(existing.GetResourceVersion())
	if err := r.Update(ctx, obj); err != nil {
		return fmt.Errorf("update ScaledObject %s: %w", obj.GetName(), err)
	}
	return nil
}

// kedaThreshold is the configured per-replica backlog target, or the default.
func (r *CoordinatorReconciler) kedaThreshold() string {
	if r.KEDAThreshold == "" {
		return defaultKEDAThreshold
	}
	return r.KEDAThreshold
}
