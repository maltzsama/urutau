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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

// workerConfigDir is where the operator mounts the coordinator's
// ConfigMap (internal/operator's coordinatorStatefulSet VolumeMount) —
// the same directory pipeline.yaml itself lives in.
const workerConfigDir = "/etc/urutau"

// serviceAccountNamespaceFile is the namespace every in-cluster
// ServiceAccount projection carries — avoids a Downward API env var just
// for this.
const serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// provisionWorkers ensures one single-replica Deployment exists for
// every worker group name in workerTarget (name -> table target), using
// that table's own Pod template rendered by the operator into this
// coordinator's ConfigMap ("worker-pod-template.<target>.yaml", next to
// pipeline.yaml). If no such template file exists for ANY of the given
// names, Kubernetes worker provisioning is off entirely — this is the
// normal case for every pipeline that doesn't set spec.image, and the
// coordinator makes zero Kubernetes API calls.
func (c *Coordinator) provisionWorkers(ctx context.Context, workerTarget map[string]string) error {
	if len(workerTarget) == 0 {
		return nil
	}
	if _, err := os.Stat(filepath.Join(workerConfigDir, workerPodTemplateFile(anyTarget(workerTarget)))); err != nil {
		return nil
	}

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("k8s worker provisioning: worker pod template present but not running in-cluster: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("k8s worker provisioning: build client: %w", err)
	}
	nsBytes, err := os.ReadFile(serviceAccountNamespaceFile)
	if err != nil {
		return fmt.Errorf("k8s worker provisioning: read own namespace: %w", err)
	}
	owner, err := coordinatorPodOwner(ctx, clientset, string(nsBytes))
	if err != nil {
		return fmt.Errorf("k8s worker provisioning: %w", err)
	}

	templates := map[string]corev1.PodTemplateSpec{}
	for name, target := range workerTarget {
		tmpl, ok := templates[target]
		if !ok {
			tmpl, err = loadWorkerPodTemplate(target)
			if err != nil {
				return fmt.Errorf("k8s worker provisioning: %s: %w", target, err)
			}
			templates[target] = tmpl
		}
		dep := workerDeployment(name, string(nsBytes), owner, tmpl)
		if err := ensureDeployment(ctx, clientset, string(nsBytes), dep); err != nil {
			return fmt.Errorf("k8s worker provisioning: %s: %w", name, err)
		}
		c.log.Info("coordinator: worker deployment ensured", "worker", name, "table", target)
	}
	return nil
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

func workerPodTemplateFile(target string) string {
	return "worker-pod-template." + target + ".yaml"
}

func loadWorkerPodTemplate(target string) (corev1.PodTemplateSpec, error) {
	b, err := os.ReadFile(filepath.Join(workerConfigDir, workerPodTemplateFile(target)))
	if err != nil {
		return corev1.PodTemplateSpec{}, err
	}
	var tmpl corev1.PodTemplateSpec
	if err := yaml.Unmarshal(b, &tmpl); err != nil {
		return corev1.PodTemplateSpec{}, fmt.Errorf("decode pod template: %w", err)
	}
	return tmpl, nil
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

// workerDeployment clones the table's own Pod template, stamping only
// the two things the operator cannot know: this worker's derived name
// (spec.Table.WorkerGroupNames) and the ownerReference back to the
// coordinator.
func workerDeployment(name, namespace string, owner metav1.OwnerReference, tmpl corev1.PodTemplateSpec) *appsv1.Deployment {
	tmpl = *tmpl.DeepCopy()
	if tmpl.Labels == nil {
		tmpl.Labels = map[string]string{}
	}
	tmpl.Labels["urutau.io/worker"] = name
	for i := range tmpl.Spec.Containers {
		tmpl.Spec.Containers[i].Args = append(tmpl.Spec.Containers[i].Args, "--name", name)
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			Labels:          tmpl.Labels,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"urutau.io/worker": name}},
			Template: tmpl,
		},
	}
}

// ensureDeployment creates the Deployment if absent, or updates it in
// place when the desired spec differs from the live one — the same
// declarative reconcile shape internal/operator's ensure() uses for the
// coordinator itself.
func ensureDeployment(ctx context.Context, cs kubernetes.Interface, namespace string, desired *appsv1.Deployment) error {
	deployments := cs.AppsV1().Deployments(namespace)
	existing, err := deployments.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err := deployments.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	desired.ResourceVersion = existing.ResourceVersion
	// The selector is immutable once set; carry the live value forward.
	desired.Spec.Selector = existing.Spec.Selector
	_, err = deployments.Update(ctx, desired, metav1.UpdateOptions{})
	return err
}

func int32Ptr(v int32) *int32 { return &v }
