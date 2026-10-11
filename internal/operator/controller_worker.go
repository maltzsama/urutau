package operator

import (
	"fmt"
	"strings"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	urutauspec "github.com/maltzsama/urutau/spec"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

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
	if cr.Spec.LogLevel != "" {
		cmd = append(cmd, "--log-level", cr.Spec.LogLevel)
	}
	// Control-plane mTLS (issue #594): the worker presents its client cert.
	cmd = append(cmd, workerTLSArgs(cr)...)
	pod := corev1.PodSpec{
		ServiceAccountName:           workerSAName(cr),
		AutomountServiceAccountToken: boolPtr(false),
		SecurityContext:              podSecurityContext(),
		Containers: []corev1.Container{{
			Name:            "worker",
			Image:           image,
			Command:         cmd,
			Env:             env,
			Resources:       workerResources(cr, t),
			SecurityContext: containerSecurityContext(),
		}},
	}
	tmpVol, tmpMount := tmpVolume()
	pod.Volumes = append(pod.Volumes, tmpVol)
	pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, tmpMount)
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
	// Control-plane mTLS (issue #594): mount the worker's client cert.
	if v, m, ok := controlPlaneTLSVolume(cr, false); ok {
		pod.Volumes = append(pod.Volumes, v)
		pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, m)
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
	// <service>.<namespace> resolves through the pod's DNS search domains, so
	// it does not hard-code the cluster domain (a cluster with a custom
	// cluster.local would not resolve the old FQDN). The port is the shared
	// constant (issue #575).
	return fmt.Sprintf("%s.%s:%d", coordinatorName(cr), cr.Namespace, coordinatorGRPCPort)
}
