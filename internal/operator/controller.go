// Package operator reconciles CDCPipeline CRs into a coordinator
// StatefulSet: one coordinator per pipeline, owning the workers it creates.
// The operator stops reconciling once status.terminated is set — the job
// stays dead until a human clears it. Deletion always takes precedence over
// termination so a finished pipeline can be removed.
package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

const finalizer = "urutau.io/finalizer"

// DefaultFieldManager is the Server-Side Apply field owner used when a
// CoordinatorReconciler does not set FieldManager. A deployment can override
// it (e.g. to run two controllers that manage the same objects).
const DefaultFieldManager = "urutau-operator"

// specHashAnnotation stamps the resolved spec onto the pod template so a
// ConfigMap change (which alone never restarts a pod) rolls the coordinator
// through a template update.
const specHashAnnotation = "urutau.io/spec-hash"

const (
	// coordinatorGRPCPort is the control-plane port workers dial. It is the
	// Service port and the port in coordinatorClusterAddr.
	coordinatorGRPCPort = 50051
	// defaultCoordinatorMetricsAddr is where the coordinator serves
	// /metrics, /statusz and the dashboard when the CR leaves
	// coordinator.metricsAddr empty. The operator always sets it (rather
	// than leaving it to the CR author) so the probes have an endpoint.
	defaultCoordinatorMetricsAddr = ":9090"
)

// CoordinatorReconciler is the OPERATOR's reconciler for the CDCPipeline CR:
// the process that owns and applies the coordinator workload. The name
// describes WHAT it manages — the coordinator StatefulSet/Service/ConfigMap it
// creates, updates and deletes — not what it is. Every coordinator* helper
// (coordinatorName, coordinatorStatefulSet, …) follows the same convention:
// they name the object the operator provisions.
type CoordinatorReconciler struct {
	client.Client
	Image    string
	Recorder record.EventRecorder
	// FieldManager is the Server-Side Apply field owner for the objects this
	// controller applies. Empty means DefaultFieldManager. Set it when another
	// controller also manages these objects, so their field ownership does not
	// collide.
	FieldManager string
	// KEDAPrometheusAddress is the Prometheus server the KEDA ScaledObjects
	// query for the coordinator's per-table backlog gauge. Empty disables KEDA
	// autoscaling entirely: no ScaledObject is rendered, and worker replicas
	// stay at spec.workers.number (issue #298).
	KEDAPrometheusAddress string
	// KEDAThreshold is the per-replica backlog target (outstanding batches)
	// handed to the ScaledObject trigger. Empty means defaultKEDAThreshold.
	KEDAThreshold string
}

// fieldManager returns the configured Server-Side Apply field owner, or the
// default when unset.
func (r *CoordinatorReconciler) fieldManager() string {
	if r.FieldManager != "" {
		return r.FieldManager
	}
	return DefaultFieldManager
}

// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;create;update;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;create;update;patch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;create;update;patch;delete

// SetupWithManager wires the reconciler into the manager.
func (r *CoordinatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		//nolint:staticcheck // the GetEventRecorder replacement emits events.k8s.io/v1 Events, whose RBAC the chart doesn't grant yet; core/v1 events keep working.
		r.Recorder = mgr.GetEventRecorderFor("urutau-operator")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&urutauv1alpha1.CDCPipeline{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

// Reconcile reconciles a CDCPipeline: it ensures the coordinator workload
// reflects the spec, respects the terminal state, and cleans up on delete.
func (r *CoordinatorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	cr := &urutauv1alpha1.CDCPipeline{}
	if err := r.Get(ctx, req.NamespacedName, cr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion takes precedence over termination: a finished pipeline
	// must still be removable — if the terminated short-circuit ran
	// first, the finalizer would never run and the CR would stick in
	// Terminating forever.
	if !cr.DeletionTimestamp.IsZero() {
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

	// Terminal: the operator PARTS WAYS — until the spec changes. A corrected
	// apply (ObservedGeneration < Generation) clears the terminal state and
	// reconciles again, so a bad spec is not a permanent brick (issue #248).
	if cr.Status.Terminated != nil {
		if cr.Status.ObservedGeneration >= cr.Generation {
			log.Info("pipeline terminated; not reconciling", "reason", cr.Status.Terminated.Reason)
			return ctrl.Result{}, nil
		}
		log.Info("pipeline spec changed; clearing terminal state",
			"reason", cr.Status.Terminated.Reason,
			"observed", cr.Status.ObservedGeneration, "generation", cr.Generation)
		base := cr.DeepCopy()
		cr.Status.Terminated = nil
		if err := r.Status().Patch(ctx, cr, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
	}

	if !controllerutil.ContainsFinalizer(cr, finalizer) {
		controllerutil.AddFinalizer(cr, finalizer)
		if err := r.Update(ctx, cr); err != nil {
			return ctrl.Result{}, err
		}
	}

	// A content error in the CR is a terminal state, not a retryable
	// failure: requeueing forever would burn the operator on a spec that
	// can never succeed. Mark terminated and surface an event.
	if err := r.validateSpec(cr); err != nil {
		r.eventf(cr, corev1.EventTypeWarning, "InvalidSpec", "%s", err.Error())
		return ctrl.Result{}, r.markTerminated(ctx, cr, "invalid_spec", err.Error())
	}

	// Ensure the coordinator and worker identities (service accounts). The
	// coordinator also gets a Role and binding (below); the worker gets none.
	if err := r.ensureIdentity(ctx, cr); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureDisruptionBudget(ctx, cr); err != nil {
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

	image := r.resolveImage(cr)
	cm, err := coordinatorConfigMap(cr, image)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := controllerutil.SetControllerReference(cr, cm, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensure(ctx, cm, "coordinator config"); err != nil {
		return ctrl.Result{}, err
	}

	// Validate that referenced secrets exist before creating the StatefulSet.
	// A not-ready one is requeued, an invalid reference terminates (#504).
	if res, err, stop := r.checkSecrets(ctx, cr); stop {
		return res, err
	}

	sts := coordinatorStatefulSet(cr, image)
	if err := controllerutil.SetControllerReference(cr, sts, r.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensure(ctx, sts, "coordinator"); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("coordinator ensured", "statefulset", sts.Name)

	if err := r.reconcileWorkers(ctx, cr); err != nil {
		return ctrl.Result{}, err
	}

	// Observe the reconciled generation so the operator's view of the spec
	// is distinguishable from the coordinator's running one.
	hash := specHash(cr)
	if cr.Status.ObservedGeneration != cr.Generation || cr.Status.SpecHash != hash {
		base := cr.DeepCopy()
		cr.Status.ObservedGeneration = cr.Generation
		cr.Status.SpecHash = hash
		if err := r.Status().Patch(ctx, cr, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// validateSpec holds the content checks that are terminal rather than
// retryable. The webhook already enforces structural validity; here the
// reconciler rejects what it cannot run: only the inline definition is
// wired today — image/s3 need the planner and are a terminal state, not an
// infinite-requeue error.
func (r *CoordinatorReconciler) validateSpec(cr *urutauv1alpha1.CDCPipeline) error {
	if len(cr.Spec.Definition.Inline) == 0 {
		return fmt.Errorf("definition.inline is required (image/s3 planner not implemented)")
	}
	if err := validateResourceQuantities(cr); err != nil {
		return err
	}
	if tls := cr.Spec.Coordinator.TLS; tls != nil && (tls.ServerSecret == "" || tls.ClientSecret == "") {
		return fmt.Errorf("coordinator.tls: serverSecret and clientSecret are both required")
	}
	return nil
}

// markTerminated sets the terminal status and returns any persistence error,
// so a failed write is surfaced instead of silently leaving the pipeline
// reconcilable. It patches only the fields it changes, so a concurrent
// coordinator status write (phase/runId) is not clobbered (issue #505), and is
// idempotent: a pipeline already terminated stays terminated.
func (r *CoordinatorReconciler) markTerminated(ctx context.Context, cr *urutauv1alpha1.CDCPipeline, reason, detail string) error {
	log.FromContext(ctx).Info("pipeline terminated by operator", "reason", reason, "detail", detail)
	base := cr.DeepCopy()
	cr.Status.Terminated = &urutauv1alpha1.Terminated{Reason: reason, At: time.Now().UTC().Format(time.RFC3339)}
	cr.Status.ObservedGeneration = cr.Generation
	if err := r.Status().Patch(ctx, cr, client.MergeFrom(base)); err != nil {
		log.FromContext(ctx).Error(err, "mark terminated")
		return err
	}
	return nil
}

// eventf emits a Kubernetes event when a recorder is wired (it is in the
// deployed operator; tests may omit it).
func (r *CoordinatorReconciler) eventf(cr *urutauv1alpha1.CDCPipeline, etype, reason, msg string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(cr, etype, reason, msg, args...)
}

// ensure reconciles the desired object with the cluster via Server-Side
// Apply. Apply merges the operator's fields into the live object: it does not
// clobber fields the operator does not manage — a mutating webhook's defaults
// (#252) — and it needs no reflect.DeepEqual, which always differed on the
// API-server metadata (UID, generation, managedFields) and so wrote on every
// reconcile (#251). Apply creates the object when absent and leaves
// API-server-assigned immutable fields (a Service's clusterIP) alone, since the
// desired object does not set them.
func (r *CoordinatorReconciler) ensure(ctx context.Context, desired client.Object, what string) error {
	gvk, err := apiutil.GVKForObject(desired, r.Scheme())
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	desired.GetObjectKind().SetGroupVersionKind(gvk)
	// The apply patch's data is json.Marshal(obj) (that's all client.Apply's
	// Data does); RawPatch keeps server-side apply without the deprecated
	// helper, which now requires a generated apply-configuration type.
	data, err := json.Marshal(desired)
	if err != nil {
		return fmt.Errorf("apply %s: %w", what, err)
	}
	if err := r.Patch(ctx, desired, client.RawPatch(types.ApplyPatchType, data), client.FieldOwner(r.fieldManager()), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply %s: %w", what, err)
	}
	return nil
}

// deleteCoordinator removes the coordinator StatefulSet (the worker pods are
// GC'd by ownership). The finalizer is removed as soon as the StatefulSet
// delete is issued, not when the owned Pods are gone: the cascade GC is
// asynchronous, so a pipeline recreated immediately can briefly coexist with
// the old generation's Pods (issue #254). That is accepted — the names are
// per-pipeline and the old Pods carry the old ownerReference, so they cannot
// be adopted by the new generation.
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

// specHash fingerprints the resolved spec (definition + coordinator knobs +
// secrets) so template changes roll the coordinator.
//
// Its stability depends on sigs.k8s.io/yaml.Marshal being deterministic (it
// marshals through JSON with sorted keys); a non-deterministic marshaller would
// change the hash on every reconcile and roll the coordinator needlessly
// (issue #259).
func specHash(cr *urutauv1alpha1.CDCPipeline) string {
	b, err := yaml.Marshal(cr.Spec)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func coordinatorName(cr *urutauv1alpha1.CDCPipeline) string {
	return cr.Name + "-coordinator"
}

// coordinatorSAName returns the service account the coordinator pod runs as.
// It is PER PIPELINE (named after the CR) so that deleting one pipeline's
// workload never takes another's identity with it in the same namespace.
func coordinatorSAName(cr *urutauv1alpha1.CDCPipeline) string {
	return coordinatorName(cr)
}

// selectorLabels is the FROZEN, minimal label set the StatefulSet/Service
// selectors match on. It must never grow: the k8s selector is immutable, so
// any future mutable label has to live outside it. The pipeline name is
// part of the selector because two pipelines can share a namespace.
func selectorLabels(cr *urutauv1alpha1.CDCPipeline) map[string]string {
	return map[string]string{"app": "urutau-coordinator", "urutau.io/pipeline": cr.Name}
}

// resolveImage returns the CR's own Image when declared, else the
// operator's own default (--coordinator-image) — a per-pipeline override
// over a cluster-wide default, the same precedence spec.Source.SnapshotURI
// has over URI elsewhere in this codebase.
func (r *CoordinatorReconciler) resolveImage(cr *urutauv1alpha1.CDCPipeline) string {
	if cr.Spec.Image != "" {
		return cr.Spec.Image
	}
	return r.Image
}

// validateSecrets checks that the Secrets referenced by the CR exist in the
// cluster. Missing secrets cause pods to fail at startup; catching them here
// gives a clear terminal state instead of a cryptic CrashLoopBackOff.
func (r *CoordinatorReconciler) validateSecrets(ctx context.Context, cr *urutauv1alpha1.CDCPipeline) error {
	// Each referenced Secret must carry the keys the pod mounts (issue #250):
	// uri is required for the source and catalog; the OAuth2 keys are optional
	// (coordinatorEnv marks them so); the SSH key lives under "privateKey".
	for _, n := range []struct {
		name string
		keys []string
	}{
		{cr.Spec.Secrets.Source, []string{"uri"}},
		{cr.Spec.Secrets.Catalog, []string{"uri"}},
		{cr.Spec.Secrets.SSH, []string{"privateKey"}},
	} {
		if n.name == "" {
			continue
		}
		secret := &corev1.Secret{}
		key := types.NamespacedName{Name: n.name, Namespace: cr.Namespace}
		if err := r.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%w: secret %q not found in namespace %q", errSecretNotReady, n.name, cr.Namespace)
			}
			return fmt.Errorf("%w: check secret %q: %v", errSecretNotReady, n.name, err)
		}
		for _, k := range n.keys {
			if _, ok := secret.Data[k]; !ok {
				return fmt.Errorf("secret %q is missing required key %q", n.name, k)
			}
		}
	}
	return nil
}
