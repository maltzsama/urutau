// Kubernetes worker provisioning: the coordinator, not the operator,
// creates the worker Deployments it needs — the same driver→executor
// relationship Spark uses (the driver clones the executor pod template
// and creates the Pod; it never assembles a PodSpec field by field). The
// operator (see internal/operator) renders one worker Pod template per
// table into the coordinator's own ConfigMap; the coordinator here only
// ever reads that template, clones it, and stamps a derived --name onto
// it. It never builds credentials, resources, or an image itself.
package coordinator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	"github.com/maltzsama/urutau/spec"
)

// workerConfigDir is where the operator mounts the coordinator's
// ConfigMap (internal/operator's coordinatorStatefulSet VolumeMount) —
// the same directory pipeline.yaml itself lives in.
const workerConfigDir = "/etc/urutau"

// serviceAccountNamespaceFile is the namespace every in-cluster
// ServiceAccount projection carries — avoids a Downward API env var just
// for this.
const serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// provisionWorkers ensures one StatefulSet exists per table, with replicas
// equal to that table's partition count, using that table's own Pod template
// rendered by the operator into this coordinator's ConfigMap
// ("worker-pod-template.<target>.yaml", next to pipeline.yaml). The
// StatefulSet is named "<pipeline>-<target>", so its pods are named
// "<pipeline>-<target>-<index>" — exactly spec.Table.WorkerGroupNames — and
// KEDA has a single replica count to scale for the whole table (issue #298).
// If no such template file exists for ANY of the given names, Kubernetes
// worker provisioning is off entirely — this is the normal case for every
// pipeline that doesn't set spec.image, and the coordinator makes zero
// Kubernetes API calls.
func (c *Coordinator) provisionWorkers(ctx context.Context, workerTarget map[string]string) error {
	if len(workerTarget) == 0 {
		return nil
	}
	if !workerPodTemplateAvailable(workerTarget) {
		return nil
	}

	clientset, ns, owner, err := c.workerClientset(ctx)
	if err != nil {
		return fmt.Errorf("k8s worker provisioning: %w", err)
	}

	// One StatefulSet per table: its replica count is the number of worker
	// group names the boot path derived for that target.
	replicas := map[string]int{}
	for _, target := range workerTarget {
		replicas[target]++
	}
	targets := make([]string, 0, len(replicas))
	for target := range replicas {
		targets = append(targets, target)
	}
	sort.Strings(targets) // stable order, so boot provisioning is deterministic

	templates := map[string]corev1.PodTemplateSpec{}
	for _, target := range targets {
		tmpl, ok := templates[target]
		if !ok {
			tmpl, err = loadWorkerPodTemplate(target)
			if err != nil {
				return fmt.Errorf("k8s worker provisioning: %s: %w", target, err)
			}
			templates[target] = tmpl
		}
		name := spec.WorkerGroupPrefix(c.cfg.Spec.Pipeline, target)
		if err := ensureService(ctx, clientset, ns, workerHeadlessService(name, ns, owner)); err != nil {
			return fmt.Errorf("k8s worker provisioning: %s: %w", name, err)
		}
		sts := workerStatefulSet(name, ns, owner, tmpl, int32(replicas[target]))
		if err := ensureStatefulSet(ctx, clientset, ns, sts); err != nil {
			return fmt.Errorf("k8s worker provisioning: %s: %w", name, err)
		}
		c.log.Info("coordinator: worker statefulset ensured", "table", target, "replicas", replicas[target])
	}
	return nil
}

// workerClientset returns the coordinator's cached in-cluster Kubernetes
// client, its namespace, and the ownerReference new worker workloads carry.
// It is built once and reused: the scale reconcile loop calls it on every
// tick, and rebuilding the client each time would re-read the service
// account files and re-GET the coordinator Pod. A failure is not cached, so
// a transient boot error does not permanently disable provisioning.
func (c *Coordinator) workerClientset(ctx context.Context) (kubernetes.Interface, string, metav1.OwnerReference, error) {
	c.k8sMu.Lock()
	defer c.k8sMu.Unlock()
	if c.k8sClient != nil {
		return c.k8sClient, c.k8sNS, c.k8sOwner, nil
	}
	cs, ns, owner, err := inClusterClient(ctx)
	if err != nil {
		return nil, "", metav1.OwnerReference{}, err
	}
	c.k8sClient, c.k8sNS, c.k8sOwner = cs, ns, owner
	return cs, ns, owner, nil
}

// workerPodTemplateAvailable reports whether the operator rendered worker pod
// templates for this pipeline — the switch that turns Kubernetes worker
// provisioning on: the data workers and the ephemeral maintenance workers
// alike. When it is false the coordinator makes zero Kubernetes API calls.
func workerPodTemplateAvailable(workerTarget map[string]string) bool {
	if len(workerTarget) == 0 {
		return false
	}
	for _, f := range workerPodTemplateCandidates(anyTarget(workerTarget)) {
		if _, err := os.Stat(filepath.Join(workerConfigDir, f)); err == nil {
			return true
		}
	}
	return false
}

// inClusterClient builds the coordinator's in-cluster Kubernetes client, its
// own namespace, and the ownerReference every workload it creates carries
// back to its own Pod (so the Pod dying cascades to GC).
func inClusterClient(ctx context.Context) (kubernetes.Interface, string, metav1.OwnerReference, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, "", metav1.OwnerReference{}, fmt.Errorf("worker pod template present but not running in-cluster: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, "", metav1.OwnerReference{}, fmt.Errorf("build client: %w", err)
	}
	nsBytes, err := os.ReadFile(serviceAccountNamespaceFile)
	if err != nil {
		return nil, "", metav1.OwnerReference{}, fmt.Errorf("read own namespace: %w", err)
	}
	owner, err := coordinatorPodOwner(ctx, clientset, string(nsBytes))
	if err != nil {
		return nil, "", metav1.OwnerReference{}, err
	}
	return clientset, string(nsBytes), owner, nil
}

// anyTarget returns one value from the map — used only to probe whether
// ANY worker pod template exists on disk before doing anything else; the
// operator renders a template per table, so if one table has it, every
// table serviced by this same coordinator pipeline does too (they were
// all rendered in the same reconcile).
func anyTarget(workerTarget map[string]string) string {
	for _, target := range workerTarget {
		return target
	}
	return ""
}

// workerPodTemplateCandidates lists the template files to try for a target,
// most specific first: the table's own, then the generic discovery template
// the operator renders for a pipeline that lists no tables (#152).
func workerPodTemplateCandidates(target string) []string {
	own := spec.WorkerPodTemplateKey(target)
	if target == spec.DefaultWorkerTemplateTarget {
		return []string{own}
	}
	return []string{own, spec.WorkerPodTemplateKey(spec.DefaultWorkerTemplateTarget)}
}

func loadWorkerPodTemplate(target string) (corev1.PodTemplateSpec, error) {
	var lastErr error
	for _, f := range workerPodTemplateCandidates(target) {
		b, err := os.ReadFile(filepath.Join(workerConfigDir, f))
		if err != nil {
			lastErr = err
			continue
		}
		var tmpl corev1.PodTemplateSpec
		if err := yaml.Unmarshal(b, &tmpl); err != nil {
			return corev1.PodTemplateSpec{}, fmt.Errorf("decode pod template: %w", err)
		}
		return tmpl, nil
	}
	return corev1.PodTemplateSpec{}, lastErr
}

// coordinatorPodOwner identifies this coordinator's own Pod, found by its
// hostname (Kubernetes sets a Pod's hostname to its own name by default)
// — every worker Deployment this coordinator creates carries an
// ownerReference to it, so the Pod dying (or the StatefulSet scaling to
// zero) cascades to GC every worker it provisioned.
func coordinatorPodOwner(ctx context.Context, cs kubernetes.Interface, namespace string) (metav1.OwnerReference, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return metav1.OwnerReference{}, fmt.Errorf("hostname: %w", err)
	}
	pod, err := cs.CoreV1().Pods(namespace).Get(ctx, hostname, metav1.GetOptions{})
	if err != nil {
		return metav1.OwnerReference{}, fmt.Errorf("get own pod %s/%s: %w", namespace, hostname, err)
	}
	return metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       pod.Name,
		UID:        pod.UID,
	}, nil
}

// workerStatefulSet clones the table's own Pod template into the workload
// that runs that table's worker pool. It is named "<pipeline>-<target>", so
// a pod's hostname is "<pipeline>-<target>-<ordinal>" — exactly
// spec.Table.WorkerGroupNames[ordinal]. The pod template carries NO --name:
// the worker's --name defaults to $HOSTNAME, which Kubernetes sets to the
// pod name, so the derived name needs no stamping. The replica count is the
// table's partition count at boot; KEDA owns it from then on (issue #298).
func workerStatefulSet(name, namespace string, owner metav1.OwnerReference, tmpl corev1.PodTemplateSpec, replicas int32) *appsv1.StatefulSet {
	tmpl = *tmpl.DeepCopy()
	if tmpl.Labels == nil {
		tmpl.Labels = map[string]string{}
	}
	tmpl.Labels["urutau.io/worker"] = name

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			Labels:          tmpl.Labels,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    int32Ptr(replicas),
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"urutau.io/worker": name}},
			Template:    tmpl,
		},
	}
}

// workerHeadlessService is the governing Service a StatefulSet requires for
// its pods' stable network identity. The workers dial the coordinator, not
// each other, so it is otherwise unused — it exists so the StatefulSet spec
// is valid and its pods resolve.
func workerHeadlessService(name, namespace string, owner metav1.OwnerReference) *corev1.Service {
	labels := map[string]string{"urutau.io/worker": name}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports:     []corev1.ServicePort{{Name: "grpc", Port: 50051}},
		},
	}
}

// ensureStatefulSet creates the StatefulSet if absent, or updates it in
// place when the desired spec differs from the live one — the same
// declarative reconcile shape internal/operator's ensure() uses for the
// coordinator itself. The replica count is NOT the coordinator's to set:
// KEDA owns it, so the live count is carried forward, or the coordinator
// would fight the autoscaler on every reconcile (issue #298).
func ensureStatefulSet(ctx context.Context, cs kubernetes.Interface, namespace string, desired *appsv1.StatefulSet) error {
	statefulsets := cs.AppsV1().StatefulSets(namespace)
	existing, err := statefulsets.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err := statefulsets.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if existing.Spec.Replicas != nil {
		desired.Spec.Replicas = existing.Spec.Replicas
	}
	desired.ResourceVersion = existing.ResourceVersion
	// Selector and ServiceName are immutable once set; carry the live
	// values forward.
	desired.Spec.Selector = existing.Spec.Selector
	desired.Spec.ServiceName = existing.Spec.ServiceName
	_, err = statefulsets.Update(ctx, desired, metav1.UpdateOptions{})
	return err
}

// ensureService creates the Service if absent, or updates it in place when
// the desired spec differs from the live one. ClusterIP and its family
// fields are immutable once assigned; the live values are carried forward.
func ensureService(ctx context.Context, cs kubernetes.Interface, namespace string, desired *corev1.Service) error {
	services := cs.CoreV1().Services(namespace)
	existing, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err := services.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	desired.ResourceVersion = existing.ResourceVersion
	desired.Spec.ClusterIP = existing.Spec.ClusterIP
	desired.Spec.ClusterIPs = existing.Spec.ClusterIPs
	desired.Spec.IPFamilies = existing.Spec.IPFamilies
	desired.Spec.IPFamilyPolicy = existing.Spec.IPFamilyPolicy
	_, err = services.Update(ctx, desired, metav1.UpdateOptions{})
	return err
}

// maintenanceWorkerPod is the data worker's own Pod template plus the
// --maintenance flag, run as a bare Pod with restartPolicy: Never instead of
// a Deployment: the maintenance worker connects, runs its assigned pass
// once, and exits — it must terminate, not restart. The coordinator creates
// one of these per due turn and deletes it once the worker's session ends.
func maintenanceWorkerPod(name, namespace string, owner metav1.OwnerReference, tmpl corev1.PodTemplateSpec) *corev1.Pod {
	tmpl = *tmpl.DeepCopy()
	if tmpl.Labels == nil {
		tmpl.Labels = map[string]string{}
	}
	tmpl.Labels["urutau.io/worker"] = name
	tmpl.Spec.RestartPolicy = corev1.RestartPolicyNever
	for i := range tmpl.Spec.Containers {
		tmpl.Spec.Containers[i].Args = append(tmpl.Spec.Containers[i].Args, "--maintenance", "--name", name)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			Labels:          tmpl.Labels,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: tmpl.Spec,
	}
}

// createPod creates the Pod, tolerating AlreadyExists — a Pod from a
// previous coordinator generation may still be terminating under the same
// name, and the caller reconciles against the live Pod on its next pass
// rather than assuming this create is the one that took effect.
func createPod(ctx context.Context, cs kubernetes.Interface, namespace string, pod *corev1.Pod) error {
	_, err := cs.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// getPod returns the named Pod, or nil when it does not exist.
func getPod(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.Pod, error) {
	pod, err := cs.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return pod, nil
}

// podCanStillConnect reports whether an existing maintenance worker Pod may
// yet open its control session. A Pod that cannot is replaced rather than
// waited on, so a table is never stalled by one that will never connect.
//
// Pending covers both a Pod that is simply still being scheduled and one
// wedged on ImagePullBackOff or CrashLoopBackOff; the waiting reason tells
// them apart. Running is kept regardless of session state: the worker may be
// mid-dial, and a Running Pod that has genuinely hung is the one case left
// to the coordinator's own restart rather than guessed at with a timeout.
func podCanStillConnect(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	case corev1.PodPending:
		for _, cs := range pod.Status.ContainerStatuses {
			if w := cs.State.Waiting; w != nil && terminalWaitingReasons[w.Reason] {
				return false
			}
		}
		return true
	default:
		return true
	}
}

// terminalWaitingReasons are the container waiting reasons a maintenance Pod
// never recovers from on its own — it is replaced instead of waited on.
var terminalWaitingReasons = map[string]bool{
	"ImagePullBackOff":           true,
	"ErrImagePull":               true,
	"InvalidImageName":           true,
	"CreateContainerConfigError": true,
	"CreateContainerError":       true,
	"CrashLoopBackOff":           true,
}

// deletePod removes the ephemeral maintenance worker's Pod once its pass
// ends (the session it held closes). A Pod that is already gone is not an
// error — nothing left to clean up.
func deletePod(ctx context.Context, cs kubernetes.Interface, namespace, name string) error {
	err := cs.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func int32Ptr(v int32) *int32 { return &v }
