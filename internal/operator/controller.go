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
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

const finalizer = "urutau.io/finalizer"

// specHashAnnotation stamps the resolved spec onto the pod template so a
// ConfigMap change (which alone never restarts a pod) rolls the coordinator
// through a template update.
const specHashAnnotation = "urutau.io/spec-hash"

// CoordinatorReconciler reconciles a CDCPipeline.
type CoordinatorReconciler struct {
	client.Client
	Image    string
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=urutau.io,resources=cdcpipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps;services,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;create;update;patch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// SetupWithManager wires the reconciler into the manager.
func (r *CoordinatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
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

	// Deleção tem precedência sobre terminação: uma pipeline terminada
	// precisa poder ser removida — se o curto-circuito de terminated viesse
	// antes, o finalizer nunca sairia e a CR travaria em Terminating.
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

	// Terminal: the operator PARTS WAYS. Nothing recreates the job.
	if cr.Status.Terminated != nil {
		log.Info("pipeline terminated; not reconciling", "reason", cr.Status.Terminated.Reason)
		return ctrl.Result{}, nil
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
		r.markTerminated(ctx, cr, "invalid_spec", err.Error())
		r.eventf(cr, corev1.EventTypeWarning, "InvalidSpec", "%s", err.Error())
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

	cm, err := coordinatorConfigMap(cr)
	if err != nil {
		return ctrl.Result{}, err
	}
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

	// Observe the reconciled generation so the operator's view of the spec
	// is distinguishable from the coordinator's running one.
	hash := specHash(cr)
	if cr.Status.ObservedGeneration != cr.Generation || cr.Status.SpecHash != hash {
		cr.Status.ObservedGeneration = cr.Generation
		cr.Status.SpecHash = hash
		if err := r.Status().Update(ctx, cr); err != nil {
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
	return nil
}

// markTerminated sets the terminal status. It is idempotent: a pipeline
// already terminated stays terminated.
func (r *CoordinatorReconciler) markTerminated(ctx context.Context, cr *urutauv1alpha1.CDCPipeline, reason, detail string) {
	log.FromContext(ctx).Info("pipeline terminated by operator", "reason", reason, "detail", detail)
	cr.Status.Terminated = &urutauv1alpha1.Terminated{Reason: reason, At: time.Now().UTC().Format(time.RFC3339)}
	cr.Status.ObservedGeneration = cr.Generation
	if err := r.Status().Update(ctx, cr); err != nil {
		log.FromContext(ctx).Error(err, "mark terminated")
	}
}

// eventf emits a Kubernetes event when a recorder is wired (it is in the
// deployed operator; tests may omit it).
func (r *CoordinatorReconciler) eventf(cr *urutauv1alpha1.CDCPipeline, etype, reason, msg string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(cr, etype, reason, msg, args...)
}

// ensure reconciles the desired object with the cluster: create when
// absent, otherwise update in place (carrying the resource version and any
// immutable fields from the live object). This is what makes the CR
// declarative — a spec change propagates on the next reconcile instead of
// requiring delete-and-recreate.
func (r *CoordinatorReconciler) ensure(ctx context.Context, desired client.Object, what string) error {
	key := types.NamespacedName{Name: desired.GetName(), Namespace: desired.GetNamespace()}
	existing, ok := desired.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("%s: deep copy", what)
	}
	err := r.Get(ctx, key, existing)
	switch {
	case apierrors.IsNotFound(err):
		if err := r.Create(ctx, desired); err != nil {
			return fmt.Errorf("create %s: %w", what, err)
		}
		return nil
	case err != nil:
		return err
	}
	// Immutable fields must survive the update: the Service cluster IP is
	// assigned once, and the StatefulSet selector/serviceName cannot change.
	switch d := desired.(type) {
	case *corev1.Service:
		e := existing.(*corev1.Service)
		d.Spec.ClusterIP = e.Spec.ClusterIP
		d.Spec.ClusterIPs = e.Spec.ClusterIPs
		d.Spec.IPFamilies = e.Spec.IPFamilies
	case *appsv1.StatefulSet:
		e := existing.(*appsv1.StatefulSet)
		if e.Spec.Selector != nil {
			d.Spec.Selector = e.Spec.Selector
		}
		if e.Spec.ServiceName != "" {
			d.Spec.ServiceName = e.Spec.ServiceName
		}
	}
	desired.SetResourceVersion(existing.GetResourceVersion())
	if err := r.Update(ctx, desired); err != nil {
		return fmt.Errorf("update %s: %w", what, err)
	}
	return nil
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

// specHash fingerprints the resolved spec (definition + coordinator knobs +
// secrets) so template changes roll the coordinator.
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

// coordinatorServiceAccount is the identity the coordinator pod runs as,
// bound (below) to a Role that lets it read and write its own CR status.
func coordinatorServiceAccount(cr *urutauv1alpha1.CDCPipeline) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorSAName(cr), Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
	}
}

// coordinatorRole grants the coordinator access to ITS OWN pipeline CR and
// status subresource only (resourceNames) — not every pipeline in the
// namespace. The contract is that the coordinator, not the operator, writes
// its status.
func coordinatorRole(cr *urutauv1alpha1.CDCPipeline) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
		Rules: []rbacv1.PolicyRule{{
			APIGroups:     []string{"urutau.io"},
			Resources:     []string{"cdcpipelines", "cdcpipelines/status"},
			ResourceNames: []string{cr.Name},
			Verbs:         []string{"get", "update", "patch"},
		}},
	}
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
func coordinatorConfigMap(cr *urutauv1alpha1.CDCPipeline) (*corev1.ConfigMap, error) {
	name := coordinatorName(cr)
	inline := cr.Spec.Definition.Inline
	if len(inline) == 0 {
		return nil, fmt.Errorf("definition.inline is required (image/s3 planner not implemented)")
	}
	payload, err := yaml.Marshal(inline)
	if err != nil {
		return nil, fmt.Errorf("render inline spec: %w", err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
		Data: map[string]string{"pipeline.yaml": string(payload)},
	}, nil
}

// coordinatorStatefulSet builds the coordinator workload from the CR. The
// pod template carries the spec hash as an annotation: a spec change alters
// the template and triggers a rolling restart, which is what makes a
// ConfigMap-only update reach the running process.
func coordinatorStatefulSet(cr *urutauv1alpha1.CDCPipeline, image string) *appsv1.StatefulSet {
	labels := selectorLabels(cr)
	name := coordinatorName(cr)

	tmpl := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: map[string]string{specHashAnnotation: specHash(cr)},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: coordinatorSAName(cr),
			Containers: []corev1.Container{{
				Name:         "coordinator",
				Image:        image,
				Command:      coordinatorCommand(cr),
				Env:          coordinatorEnv(cr),
				VolumeMounts: []corev1.VolumeMount{{Name: "spec", MountPath: "/etc/urutau"}},
			}},
			Volumes: []corev1.Volume{{
				Name: "spec",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: name},
				}},
			}},
		},
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cr.Namespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    int32Ptr(1),
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template:    tmpl,
		},
	}
}

// coordinatorCommand renders the coordinator invocation. Flags mirror the
// CoordinatorSpec knobs; only fields that are set are passed, so defaults
// live in the binary.
func coordinatorCommand(cr *urutauv1alpha1.CDCPipeline) []string {
	args := []string{"urutau-coordinator", "run", "--file", "/etc/urutau/pipeline.yaml"}
	snap := cr.Spec.Coordinator.Snapshot
	if snap.ChunkSize > 0 {
		args = append(args, "--chunk-size", strconv.Itoa(snap.ChunkSize))
	}
	if snap.MaxParallelChunks > 0 {
		args = append(args, "--max-parallel-chunks", strconv.Itoa(snap.MaxParallelChunks))
	}
	sup := cr.Spec.Coordinator.Supervision
	if sup.AckTimeout != "" {
		args = append(args, "--ack-timeout", sup.AckTimeout)
	}
	if sup.MaxResets > 0 {
		args = append(args, "--max-resets", strconv.Itoa(sup.MaxResets))
	}
	if sup.Window != "" {
		args = append(args, "--reset-window", sup.Window)
	}
	if cr.Spec.Coordinator.MetricsAddr != "" {
		args = append(args, "--metrics-addr", cr.Spec.Coordinator.MetricsAddr)
	}
	return args
}

// coordinatorEnv mounts the referenced Secrets as environment variables.
// The spec's empty credential fields resolve from these at load time
// (URUTAU_SOURCE_URI, URUTAU_SINK_*, …). uri is always required; the OAuth2
// keys are OPTIONAL so a catalog authenticated with a bearer token or sigv4
// (which has no client credentials) can start.
func coordinatorEnv(cr *urutauv1alpha1.CDCPipeline) []corev1.EnvVar {
	var env []corev1.EnvVar
	if cr.Spec.Secrets.Source != "" {
		env = append(env, corev1.EnvVar{
			Name: "URUTAU_SOURCE_URI",
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: cr.Spec.Secrets.Source},
				Key:                  "uri",
			}},
		})
	}
	if cr.Spec.Secrets.Catalog != "" {
		for _, kv := range []struct{ env, key string }{
			{"URUTAU_SINK_URI", "uri"},
			{"URUTAU_SINK_CLIENT_ID", "clientId"},
			{"URUTAU_SINK_CLIENT_SECRET", "clientSecret"},
			{"URUTAU_SINK_SCOPE", "scope"},
		} {
			ref := &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: cr.Spec.Secrets.Catalog},
				Key:                  kv.key,
			}
			if kv.key != "uri" {
				opt := true
				ref.Optional = &opt
			}
			env = append(env, corev1.EnvVar{
				Name:      kv.env,
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ref},
			})
		}
	}
	return env
}

func int32Ptr(v int32) *int32 { return &v }
