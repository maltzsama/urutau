package operator

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// SetupWebhookWithManager registers the validating webhook.
func (r *CoordinatorReconciler) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&urutauv1alpha1.CDCPipeline{}).
		WithValidator(&pipelineValidator{}).Complete()
}

// pipelineValidator validates CDCPipeline spec mutations. The hard rules
// (§5.4) reuse the same server-side validation the coordinator boot runs.
type pipelineValidator struct{}

var _ webhook.CustomValidator = (*pipelineValidator)(nil)

// ValidateCreate rejects a spec that fails the resolved-spec validation.
func (v *pipelineValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, validatePipeline(obj)
}

// ValidateUpdate allows unchanged spec; otherwise validates like create.
func (v *pipelineValidator) ValidateUpdate(ctx context.Context, old, new runtime.Object) (admission.Warnings, error) {
	return nil, validatePipeline(new)
}

// ValidateDelete is always allowed.
func (v *pipelineValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func validatePipeline(obj runtime.Object) error {
	cr, ok := obj.(*urutauv1alpha1.CDCPipeline)
	if !ok {
		return fmt.Errorf("expected CDCPipeline, got %T", obj)
	}
	// Inline tables only for now; image/s3 definition is validated once the
	// planner exists (its own repo).
	if len(cr.Spec.Definition.Tables) == 0 {
		return fmt.Errorf("spec.definition: inline tables required (planner not yet wired)")
	}
	// serverId uniqueness across CRs is enforced by the operator at
	// reconcile time (it lists all CRs); the webhook checks the shape here.
	if cr.Spec.Coordinator.Snapshot.ChunkSize < 0 {
		return fmt.Errorf("coordinator.snapshot.chunkSize must be >= 0")
	}
	return nil
}
