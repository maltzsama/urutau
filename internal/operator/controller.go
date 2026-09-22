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
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	"github.com/maltzsama/urutau/internal/maintenance"
	urutauspec "github.com/maltzsama/urutau/spec"
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
		cr.Status.Terminated = nil
		if err := r.Status().Update(ctx, cr); err != nil {
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
	// A missing secret would cause the pod to fail at startup with a cryptic
	// error; catching it here gives the operator a clear terminal state.
	if err := r.validateSecrets(ctx, cr); err != nil {
		r.markTerminated(ctx, cr, "SecretValidationFailed", err.Error())
		return ctrl.Result{}, nil
	}

	sts := coordinatorStatefulSet(cr, image)
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
	if err := r.Patch(ctx, desired, client.Apply, client.FieldOwner(r.fieldManager()), client.ForceOwnership); err != nil {
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
//
// It also grants Deployment management for the long-lived data workers and
// reading its own Pod, to set the ownerReference that cascades GC of
// everything it provisions when the coordinator dies. Those are
// namespace-wide (no resourceNames) because a worker Deployment does not
// exist yet when the coordinator boots and needs to create it — matching
// the pattern the operator's own ClusterRole already uses for
// statefulsets/configmaps/services.
//
// Deleting Pods is granted separately and narrowly. The ephemeral
// maintenance workers (issue #105) are bare Pods the coordinator creates
// when a turn is due and deletes once the pass ends, so it needs delete —
// but only ever on those names, which are derivable here. A pipeline with
// maintenance off gets no Pod create/delete at all.
func coordinatorRole(cr *urutauv1alpha1.CDCPipeline) *rbacv1.Role {
	rules := []rbacv1.PolicyRule{
		{
			APIGroups:     []string{"urutau.io"},
			Resources:     []string{"cdcpipelines"},
			ResourceNames: []string{cr.Name},
			Verbs:         []string{"get", "update", "patch"},
		},
		{
			// The status subresource is its own rule without resourceNames:
			// some authorizers ignore resourceNames on a subresource, so
			// scoping it to cr.Name would silently deny the coordinator its
			// own status (issue #257). It stays in the pipeline's namespace.
			APIGroups: []string{"urutau.io"},
			Resources: []string{"cdcpipelines/status"},
			Verbs:     []string{"get", "update", "patch"},
		},
		{
			APIGroups: []string{"apps"},
			Resources: []string{"deployments"},
			Verbs:     []string{"get", "create", "update"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get"},
		},
	}
	if names := maintenanceWorkerNames(cr); len(names) > 0 {
		rules = append(rules,
			// create cannot be scoped by name: RBAC matches resourceNames
			// against an existing object, and the Pod does not exist yet.
			rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"create"},
			},
			rbacv1.PolicyRule{
				APIGroups:     []string{""},
				Resources:     []string{"pods"},
				ResourceNames: names,
				Verbs:         []string{"delete"},
			},
		)
	}
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(cr), Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
		Rules: rules,
	}
}

// maintenanceWorkerNames returns the Pod names this pipeline's coordinator
// may delete — one ephemeral maintenance worker per table — or nil when
// maintenance is off, so no Pod write permission is granted at all. The
// names must match coordinator.maintenanceWorkerName exactly; a spec that
// does not parse yields nil, and the coordinator fails on the same spec
// before it could provision anything.
func maintenanceWorkerNames(cr *urutauv1alpha1.CDCPipeline) []string {
	if len(cr.Spec.Definition.Inline) == 0 {
		return nil
	}
	payload, err := yaml.Marshal(cr.Spec.Definition.Inline)
	if err != nil {
		return nil
	}
	s, err := urutauspec.LoadYAML(strings.NewReader(string(payload)))
	if err != nil || !s.Sink.MaintenanceEnabled() {
		return nil
	}
	names := make([]string, 0, len(s.Tables))
	for _, t := range s.Tables {
		names = append(names, maintenance.WorkerName(s.Pipeline, t.Target))
	}
	sort.Strings(names) // stable Role content, so ensure() sees no spurious drift
	return names
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
//
// The same ConfigMap also carries one worker pod template per table —
// "worker-pod-template.<target>.yaml" — the coordinator reads at boot to
// provision that table's worker group(s) (spec.Table.WorkerGroupNames):
// the coordinator only ever copies this template and stamps a derived
// --name onto it, it never builds a PodSpec of its own (mirrors Spark:
// the driver clones the executor pod template, it doesn't assemble one
// field by field). Emitted only when cr.Spec.Image is resolvable —
// without an image there is nothing to provision, and every existing
// pipeline that never sets spec.image is completely unaffected.
func coordinatorConfigMap(cr *urutauv1alpha1.CDCPipeline, image string) (*corev1.ConfigMap, error) {
	name := coordinatorName(cr)
	// The caller has already run validateSpec (which requires inline), so this
	// does not repeat that check (issue #255).
	inline := cr.Spec.Definition.Inline
	payload, err := yaml.Marshal(inline)
	if err != nil {
		return nil, fmt.Errorf("render inline spec: %w", err)
	}
	data := map[string]string{"pipeline.yaml": string(payload)}

	if image != "" {
		s, err := urutauspec.LoadYAML(strings.NewReader(string(payload)))
		if err != nil {
			return nil, fmt.Errorf("parse inline spec for worker provisioning: %w", err)
		}
		for _, t := range s.Tables {
			tmpl := workerPodTemplate(cr, image, t)
			b, err := yaml.Marshal(tmpl)
			if err != nil {
				return nil, fmt.Errorf("render worker pod template for %s: %w", t.Target, err)
			}
			data[urutauspec.WorkerPodTemplateKey(t.Target)] = string(b)
		}
		// A discovery pipeline lists no tables at operator time — the source
		// enumerates them at boot — so no per-table template can be rendered.
		// Emit one generic template the coordinator clones for every
		// discovered target (#152).
		if s.Source.Postgres != nil && s.Source.Postgres.Discover {
			tmpl := workerPodTemplate(cr, image, urutauspec.Table{})
			b, err := yaml.Marshal(tmpl)
			if err != nil {
				return nil, fmt.Errorf("render generic worker pod template: %w", err)
			}
			data[urutauspec.WorkerPodTemplateKey(urutauspec.DefaultWorkerTemplateTarget)] = string(b)
		}
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cr.Namespace,
			Labels: selectorLabels(cr)},
		Data: data,
	}, nil
}

// workerPodTemplate builds one table's worker Pod template. It carries NO
// --name — the coordinator stamps that on when it clones this template
// per worker group (spec.Table.WorkerGroupNames), since the coordinator,
// not the operator, knows how many partitions exist and what their
// derived names are.
func workerPodTemplate(cr *urutauv1alpha1.CDCPipeline, image string, t urutauspec.Table) corev1.PodTemplateSpec {
	labels := map[string]string{"app": "urutau-worker", "urutau.io/pipeline": cr.Name, "urutau.io/table": t.Target}
	env := coordinatorEnv(cr)
	// The catalog URI and credentials reach the worker as env from the
	// mounted Secret (coordinatorEnv); the warehouse is not a credential and
	// lives in the inline spec, so carry it across as its own env.
	if wh := inlineSinkWarehouse(cr); wh != "" {
		env = append(env, corev1.EnvVar{Name: "URUTAU_SINK_WAREHOUSE", Value: wh})
	}
	// The metrics endpoint is off unless the operator opts in: an empty
	// Worker.MetricsAddr must not start a listener, so the flag is only
	// passed when set (defaults live in the binary).
	cmd := []string{"urutau-worker", "run", "--coordinator", coordinatorClusterAddr(cr)}
	if cr.Spec.Worker.MetricsAddr != "" {
		cmd = append(cmd, "--metrics-addr", cr.Spec.Worker.MetricsAddr)
	}
	pod := corev1.PodSpec{
		ServiceAccountName: coordinatorSAName(cr),
		Containers: []corev1.Container{{
			Name:      "worker",
			Image:     image,
			Command:   cmd,
			Env:       env,
			Resources: workerResources(cr, t),
		}},
	}
	// An SSH-tunneled source needs the private key on the worker host: the
	// DSN cannot carry a DialFunc, so the coordinator ships the structured
	// postgres block (issue #170) and the worker reads the key from this
	// mount. The key is a file, not an env var, so it comes as a Secret
	// volume under the fixed name/path the inline spec's
	// source.postgres.ssh.privateKey names.
	if _, worker := sshMounts(cr); worker {
		mode := int32(0o400)
		pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name: "ssh-key", MountPath: sshMountPath, ReadOnly: true,
		})
		pod.Volumes = append(pod.Volumes, corev1.Volume{
			Name: "ssh-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  cr.Spec.Secrets.SSH,
				DefaultMode: &mode,
			}},
		})
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels},
		Spec:       pod,
	}
}

// sshMountPath is where the SSH private key Secret is mounted into every
// worker Pod; the inline spec's source.postgres.ssh.privateKey must name
// sshMountPath/privateKey (the secret key the operator expects).
const sshMountPath = "/etc/urutau/ssh"

// inlineSpec parses the CR's inline definition; nil when absent or invalid
// (the ConfigMap renderer surfaces the real error).
func inlineSpec(cr *urutauv1alpha1.CDCPipeline) *urutauspec.Spec {
	if len(cr.Spec.Definition.Inline) == 0 {
		return nil
	}
	b, err := yaml.Marshal(cr.Spec.Definition.Inline)
	if err != nil {
		return nil
	}
	s, err := urutauspec.LoadYAML(strings.NewReader(string(b)))
	if err != nil {
		return nil
	}
	return s
}

// sshMounts reports whether the SSH key Secret must be mounted, and into
// which pods. The coordinator always needs it when the source tunnels (it
// opens the replication connection); a worker needs it only when the
// structured block is shipped to it — a scoped snapshotUri makes the worker
// connect directly, so the key would be an unused credential.
func sshMounts(cr *urutauv1alpha1.CDCPipeline) (coordinator, worker bool) {
	if cr.Spec.Secrets.SSH == "" {
		return false, false
	}
	s := inlineSpec(cr)
	if s == nil || s.Source.Postgres == nil || s.Source.Postgres.SSH == nil {
		return false, false
	}
	return true, s.Source.SnapshotURI == ""
}

// inlineSinkWarehouse reads sink.warehouse out of the inline definition —
// the one catalog setting the worker needs that is NOT in a Secret.
func inlineSinkWarehouse(cr *urutauv1alpha1.CDCPipeline) string {
	sink, ok := cr.Spec.Definition.Inline["sink"].(map[string]any)
	if !ok {
		return ""
	}
	wh, _ := sink["warehouse"].(string)
	return wh
}

// coordinatorClusterAddr is the in-cluster address a worker Pod dials —
// the coordinator's own headless Service DNS name.
func coordinatorClusterAddr(cr *urutauv1alpha1.CDCPipeline) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:50051", coordinatorName(cr), cr.Namespace)
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

// coordinatorStatefulSet builds the coordinator workload from the CR. The
// pod template carries the spec hash as an annotation: a spec change alters
// the template and triggers a rolling restart, which is what makes a
// ConfigMap-only update reach the running process.
func coordinatorStatefulSet(cr *urutauv1alpha1.CDCPipeline, image string) *appsv1.StatefulSet {
	labels := selectorLabels(cr)
	name := coordinatorName(cr)
	metricsPort := coordinatorMetricsPort(cr)

	tmpl := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: map[string]string{specHashAnnotation: specHash(cr)},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: coordinatorSAName(cr),
			Containers: []corev1.Container{{
				Name:      "coordinator",
				Image:     image,
				Command:   coordinatorCommand(cr),
				Env:       coordinatorEnv(cr),
				Resources: resourceRequirements(cr.Spec.Coordinator.CPU, "", cr.Spec.Coordinator.Memory, ""),
				Ports: []corev1.ContainerPort{
					{Name: "grpc", ContainerPort: coordinatorGRPCPort},
					{Name: "metrics", ContainerPort: metricsPort},
				},
				// /statusz is the coordinator's live-state endpoint, served
				// alongside /metrics on the (operator-guaranteed) metrics
				// address. The startup probe gives a slow boot — source open,
				// resume, a large initial snapshot — room to finish without
				// the liveness probe restarting the pod mid-snapshot.
				StartupProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/statusz", Port: intstr.FromInt(int(metricsPort)),
					}},
					PeriodSeconds:    10,
					FailureThreshold: 60, // up to 10m of boot before liveness kicks in
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/statusz", Port: intstr.FromInt(int(metricsPort)),
					}},
					PeriodSeconds:    20,
					FailureThreshold: 3,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/statusz", Port: intstr.FromInt(int(metricsPort)),
					}},
					PeriodSeconds:    10,
					FailureThreshold: 3,
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
	}

	// The coordinator opens the replication connection, so it needs the SSH
	// key too when the source tunnels — not just the workers (#170).
	if coordinator, _ := sshMounts(cr); coordinator {
		mode := int32(0o400)
		tmpl.Spec.Containers[0].VolumeMounts = append(tmpl.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "ssh-key", MountPath: sshMountPath, ReadOnly: true})
		tmpl.Spec.Volumes = append(tmpl.Spec.Volumes, corev1.Volume{
			Name: "ssh-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  cr.Spec.Secrets.SSH,
				DefaultMode: &mode,
			}},
		})
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
	// Always pass a metrics address: the operator guarantees one so the
	// /statusz probes have a stable endpoint, instead of leaving it to the
	// CR author (an empty value disables /statusz entirely).
	args = append(args, "--metrics-addr", coordinatorMetricsAddr(cr))
	return args
}

// coordinatorMetricsAddr is the effective metrics/statusz listen address: the
// CR's own value, or the operator default when unset. The operator always
// sets it (rather than leaving it to the CR author) because an empty value
// disables /statusz entirely — the endpoint the probes depend on.
func coordinatorMetricsAddr(cr *urutauv1alpha1.CDCPipeline) string {
	if cr.Spec.Coordinator.MetricsAddr != "" {
		return cr.Spec.Coordinator.MetricsAddr
	}
	return defaultCoordinatorMetricsAddr
}

// coordinatorMetricsPort extracts the numeric port from the effective metrics
// address, for the container port and the probes. A malformed address falls
// back to the default's port; the coordinator fails on the same flag at boot
// anyway, and a probe on the wrong port is no worse than none.
func coordinatorMetricsPort(cr *urutauv1alpha1.CDCPipeline) int32 {
	_, portStr, err := net.SplitHostPort(coordinatorMetricsAddr(cr))
	if err != nil {
		return 9090
	}
	p, err := strconv.Atoi(portStr)
	if err != nil || p <= 0 || p > 65535 {
		return 9090
	}
	return int32(p)
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

// resourceRequirements builds a container's resource request/limit from
// Kubernetes quantity strings. cpu/memory become the request; cpu+overhead/
// memory+overhead become the limit — the request alone when no overhead is
// given (limit == request), matching CoordinatorSpec, which has no
// overhead knob. Empty cpu/memory returns an empty ResourceRequirements
// (no request or limit at all — the container is BestEffort), which is
// the caller's responsibility to avoid for anything but an explicit,
// deliberate default (see workerResources).
func resourceRequirements(cpu, cpuOverhead, memory, memOverhead string) corev1.ResourceRequirements {
	if cpu == "" && memory == "" {
		return corev1.ResourceRequirements{}
	}
	req := corev1.ResourceList{}
	lim := corev1.ResourceList{}
	if cpu != "" {
		req[corev1.ResourceCPU] = resource.MustParse(cpu)
		lim[corev1.ResourceCPU] = addQuantity(cpu, cpuOverhead)
	}
	if memory != "" {
		req[corev1.ResourceMemory] = resource.MustParse(memory)
		lim[corev1.ResourceMemory] = addQuantity(memory, memOverhead)
	}
	return corev1.ResourceRequirements{Requests: req, Limits: lim}
}

// addQuantity adds an optional overhead quantity to a base quantity;
// overhead=="" returns base unchanged (limit == request).
func addQuantity(base, overhead string) resource.Quantity {
	b := resource.MustParse(base)
	if overhead == "" {
		return b
	}
	o := resource.MustParse(overhead)
	b.Add(o)
	return b
}

// workerResources resolves one table's effective worker resources: the
// table's own spec.Table.Workers.CPU/Memory when set, else the
// pipeline-wide spec.worker default — matching CoordinatorSpec.CPU/Memory,
// a table-level override always wins over the fallback. The default
// carries WorkerDefaults' overhead; a table override does not declare its
// own overhead (spec.WorkerSpec has no overhead field), so it inherits the
// pipeline default's overhead too.
func workerResources(cr *urutauv1alpha1.CDCPipeline, t urutauspec.Table) corev1.ResourceRequirements {
	wd := cr.Spec.Worker
	cpu, memory := wd.CPU, wd.Memory
	if t.Workers != nil {
		if t.Workers.CPU != "" {
			cpu = t.Workers.CPU
		}
		if t.Workers.Memory != "" {
			memory = t.Workers.Memory
		}
	}
	return resourceRequirements(cpu, wd.CPUOverhead, memory, wd.MemoryOverhead)
}

func int32Ptr(v int32) *int32 { return &v }

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
				return fmt.Errorf("secret %q not found in namespace %q", n.name, cr.Namespace)
			}
			return fmt.Errorf("check secret %q: %w", n.name, err)
		}
		for _, k := range n.keys {
			if _, ok := secret.Data[k]; !ok {
				return fmt.Errorf("secret %q is missing required key %q", n.name, k)
			}
		}
	}
	return nil
}
