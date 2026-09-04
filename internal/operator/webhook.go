package operator

import (
	"bytes"
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/yaml"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	"github.com/maltzsama/urutau/internal/spec"
)

// SetupWebhookWithManager registers the validating webhook.
func (r *CoordinatorReconciler) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&urutauv1alpha1.CDCPipeline{}).
		WithValidator(&pipelineValidator{}).Complete()
}

// pipelineValidator validates CDCPipeline spec mutations. The hard rules
// reuse the same server-side validation the coordinator boot runs.
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

	// Exactly one definition source: image, s3, or inline. Anything else
	// (zero or two) is a mistake the webhook must not guess about.
	def := cr.Spec.Definition
	defined := 0
	if def.Image != "" {
		defined++
	}
	if def.S3 != "" {
		defined++
	}
	if len(def.Inline) > 0 {
		defined++
	}
	switch defined {
	case 0:
		return fmt.Errorf("spec.definition: one of image | s3 | inline required")
	case 1:
	default:
		return fmt.Errorf("spec.definition: image, s3 and inline are mutually exclusive")
	}

	// An inline definition is validated with the same rules the coordinator
	// runs at boot — validation stays single and server-side.
	if len(def.Inline) > 0 {
		b, err := yaml.Marshal(def.Inline)
		if err != nil {
			return fmt.Errorf("spec.definition.inline: %w", err)
		}
		s, err := spec.LoadYAML(bytes.NewReader(b))
		if err != nil {
			return fmt.Errorf("spec.definition.inline: %w", err)
		}
		if err := s.Validate(); err != nil {
			return fmt.Errorf("spec.definition.inline: %w", err)
		}
	}

	// serverId uniqueness across CRs is enforced by the operator at
	// reconcile time (it lists all CRs); the webhook checks the shape here.
	if cr.Spec.Coordinator.Snapshot.ChunkSize < 0 {
		return fmt.Errorf("coordinator.snapshot.chunkSize must be >= 0")
	}
	if cr.Spec.Coordinator.Snapshot.MaxParallelChunks < 0 {
		return fmt.Errorf("coordinator.snapshot.maxParallelChunks must be >= 0")
	}
	return nil
}
