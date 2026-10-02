package operator

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// secretRequeueAfter is how long a reconcile waits before rechecking a
// referenced Secret that does not exist yet (issue #504).
const secretRequeueAfter = 30 * time.Second

// errSecretNotReady marks a transient secret problem — the Secret is absent or
// its API read failed. The reconciler requeues rather than terminating, so a
// Secret created moments later is picked up without editing the CR (#504).
var errSecretNotReady = errors.New("secret not ready")

// checkSecrets validates the referenced Secrets before the StatefulSet is
// created. A Secret that is merely absent (or unreadable) is not terminal: it
// may be created moments later by an init container or an external process, so
// the caller requeues with backoff. Only a genuinely invalid reference — a
// present Secret missing a required key — terminates the pipeline (issue #504).
// stop reports that Reconcile must return (res, err) immediately.
func (r *CoordinatorReconciler) checkSecrets(ctx context.Context, cr *urutauv1alpha1.CDCPipeline) (res ctrl.Result, err error, stop bool) {
	verr := r.validateSecrets(ctx, cr)
	if verr == nil {
		return ctrl.Result{}, nil, false
	}
	if errors.Is(verr, errSecretNotReady) {
		r.eventf(cr, corev1.EventTypeWarning, "SecretNotReady", "%s", verr.Error())
		return ctrl.Result{RequeueAfter: secretRequeueAfter}, nil, true
	}
	r.eventf(cr, corev1.EventTypeWarning, "InvalidSecret", "%s", verr.Error())
	return ctrl.Result{}, r.markTerminated(ctx, cr, "SecretValidationFailed", verr.Error()), true
}
