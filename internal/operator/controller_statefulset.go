package operator

import (
	"net"
	"strconv"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

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
			SecurityContext:    podSecurityContext(),
			// The graceful drain (coordinator gracefulShutdown) relies on the
			// termination grace period; the 30s default can cut a large drain
			// short, so give it 60s (issue #604).
			TerminationGracePeriodSeconds: int64Ptr(60),
			Containers: []corev1.Container{{
				Name:            "coordinator",
				Image:           image,
				Command:         coordinatorCommand(cr),
				Env:             coordinatorContainerEnv(cr),
				Resources:       resourceRequirements(cr.Spec.Coordinator.CPU, "", cr.Spec.Coordinator.Memory, ""),
				SecurityContext: containerSecurityContext(),
				Ports: []corev1.ContainerPort{
					{Name: "grpc", ContainerPort: coordinatorGRPCPort},
					{Name: "metrics", ContainerPort: metricsPort},
				},
				// /readyz answers only once routing is published and the
				// stream is running, so the startup probe gives a slow boot
				// (source open, resume, a large initial snapshot) room to
				// finish without liveness restarting the pod mid-snapshot,
				// and the readiness probe keeps a coordinator that is still
				// booting out of service (issue #601).
				StartupProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/readyz", Port: intstr.FromInt(int(metricsPort)),
					}},
					PeriodSeconds:    10,
					FailureThreshold: 60, // up to 10m of boot before liveness kicks in
				},
				// /healthz answers unhealthy once the pump stops signalling
				// life, so a wedged coordinator is restarted instead of
				// staying "live" but doing nothing (issue #601).
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/healthz", Port: intstr.FromInt(int(metricsPort)),
					}},
					PeriodSeconds:    20,
					FailureThreshold: 3,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/readyz", Port: intstr.FromInt(int(metricsPort)),
					}},
					PeriodSeconds:    10,
					FailureThreshold: 3,
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "spec", MountPath: "/etc/urutau"},
					{Name: "tmp", MountPath: "/tmp"},
				},
			}},
			Volumes: []corev1.Volume{{
				Name: "spec",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: name},
				}},
			}, {
				Name:         "tmp",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
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

	// Control-plane mTLS (issue #594): mount the coordinator's server cert.
	if v, m, ok := controlPlaneTLSVolume(cr, true); ok {
		tmpl.Spec.Volumes = append(tmpl.Spec.Volumes, v)
		tmpl.Spec.Containers[0].VolumeMounts = append(tmpl.Spec.Containers[0].VolumeMounts, m)
	}

	// Kafka / schema-registry TLS (issue #598).
	srcVols, srcMounts := sourceTLSMounts(cr)
	tmpl.Spec.Volumes = append(tmpl.Spec.Volumes, srcVols...)
	tmpl.Spec.Containers[0].VolumeMounts = append(tmpl.Spec.Containers[0].VolumeMounts, srcMounts...)

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
	args = append(args, supervisionArgs(cr.Spec.Coordinator.Supervision)...)
	args = append(args, eventlogArgs(cr.Spec.Coordinator.Eventlog)...)
	if cr.Spec.LogLevel != "" {
		args = append(args, "--log-level", cr.Spec.LogLevel)
	}
	// Always pass a metrics address: the operator guarantees one so the
	// /statusz probes have a stable endpoint, instead of leaving it to the
	// CR author (an empty value disables /statusz entirely).
	args = append(args, "--metrics-addr", coordinatorMetricsAddr(cr))
	// Control-plane TLS (issue #594): when the CR references TLS Secrets, pass
	// the mounted material; otherwise opt into plaintext explicitly so the
	// coordinator's fail-closed default does not crash-loop the pipeline.
	args = append(args, coordinatorTLSArgs(cr)...)
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

// defaultCPU and defaultMemory are the fail-closed floor: a pod the spec
// leaves without cpu/memory gets a fixed request and limit instead of
// becoming BestEffort and eating the cluster. Overhead is the operator's
// decision (spec), never a urutau default, so it is only ever added on top
// of whatever base applies — specified or default.
const (
	defaultCPU    = "500m"
	defaultMemory = "1Gi"
)

func int32Ptr(v int32) *int32 { return &v }
