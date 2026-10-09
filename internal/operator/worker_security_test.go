package operator

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	urutauspec "github.com/maltzsama/urutau/spec"
)

// #603: the worker runs as its own ServiceAccount with no mounted token, so a
// compromised worker (or a plugin in it) cannot use the coordinator's RBAC to
// create or delete sibling workloads.
func TestWorkerRunsAsDedicatedTokenlessSA(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	tmpl := workerPodTemplate(cr, "urutau:v1", urutauspec.Table{Source: "shop.orders", Target: "raw.orders"})
	if got, want := tmpl.Spec.ServiceAccountName, workerSAName(cr); got != want {
		t.Fatalf("worker SA = %q, want %q", got, want)
	}
	if tmpl.Spec.ServiceAccountName == coordinatorSAName(cr) {
		t.Fatal("worker must not run as the coordinator's ServiceAccount")
	}
	if tmpl.Spec.AutomountServiceAccountToken == nil || *tmpl.Spec.AutomountServiceAccountToken {
		t.Fatal("worker must not mount a service account token")
	}
	if sa := workerServiceAccount(cr); sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
		t.Fatal("worker ServiceAccount must set automountServiceAccountToken=false")
	}
}

// #596: every managed Pod runs non-root with the RuntimeDefault seccomp
// profile, drops all capabilities, and uses a read-only root filesystem with a
// writable /tmp (the one location the binaries need).
func TestManagedPodsAreHardened(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	worker := workerPodTemplate(cr, "urutau:v1", urutauspec.Table{Source: "shop.orders", Target: "raw.orders"})
	assertHardened(t, worker.Spec, "worker")

	coord := coordinatorStatefulSet(cr, "urutau:v1")
	assertHardened(t, coord.Spec.Template.Spec, "coordinator")
}

func assertHardened(t *testing.T, pod corev1.PodSpec, who string) {
	t.Helper()
	sc := pod.SecurityContext
	if sc == nil || sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
		t.Fatalf("%s pod must set runAsNonRoot", who)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("%s pod must set the RuntimeDefault seccomp profile", who)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("%s: want 1 container, got %d", who, len(pod.Containers))
	}
	c := pod.Containers[0].SecurityContext
	if c == nil {
		t.Fatalf("%s container must set a securityContext", who)
	}
	if c.ReadOnlyRootFilesystem == nil || !*c.ReadOnlyRootFilesystem {
		t.Fatalf("%s container must use a read-only root filesystem", who)
	}
	if c.AllowPrivilegeEscalation == nil || *c.AllowPrivilegeEscalation {
		t.Fatalf("%s must not allow privilege escalation", who)
	}
	if c.Capabilities == nil || len(c.Capabilities.Drop) != 1 || c.Capabilities.Drop[0] != corev1.Capability("ALL") {
		t.Fatalf("%s must drop ALL capabilities", who)
	}
	tmp := false
	for _, m := range pod.Containers[0].VolumeMounts {
		if m.MountPath == "/tmp" {
			tmp = true
		}
	}
	if !tmp {
		t.Fatalf("%s must mount a writable /tmp", who)
	}
}

// #604: the coordinator gets an explicit termination grace period and a
// bounded PodDisruptionBudget (maxUnavailable=1, not minAvailable — a
// 1-replica singleton with minAvailable=1 would wedge a node drain).
func TestCoordinatorPDBAndGrace(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	if got := coordinatorStatefulSet(cr, "urutau:v1").Spec.Template.Spec.TerminationGracePeriodSeconds; got == nil || *got != 60 {
		t.Fatalf("terminationGracePeriodSeconds = %v, want 60", got)
	}

	pdb := coordinatorPodDisruptionBudget(cr)
	if pdb.Name != "orders-coordinator" || pdb.Namespace != "ns" {
		t.Fatalf("pdb identity = %s/%s, want ns/orders-coordinator", pdb.Namespace, pdb.Name)
	}
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Fatalf("maxUnavailable = %v, want 1", pdb.Spec.MaxUnavailable)
	}
	if pdb.Spec.MinAvailable != nil {
		t.Fatalf("minAvailable must be unset: %v", pdb.Spec.MinAvailable)
	}
	if pdb.Spec.Selector == nil || pdb.Spec.Selector.MatchLabels["urutau.io/pipeline"] != "orders" {
		t.Fatalf("pdb selector = %+v, want the pipeline labels", pdb.Spec.Selector)
	}
}

// #594: a CR with coordinator.tls mounts the server/client TLS Secrets and
// passes the TLS flags, dropping the plaintext opt-in.
func TestControlPlaneTLSWiring(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Coordinator.TLS = &urutauv1alpha1.CoordinatorTLS{ServerSecret: "srv-tls", ClientSecret: "cli-tls"}

	cmd := strings.Join(coordinatorCommand(cr), " ")
	if strings.Contains(cmd, "allow-insecure-control-plane") {
		t.Fatal("TLS set must not opt into plaintext")
	}
	for _, want := range []string{
		"--tls-cert /etc/urutau/tls/tls.crt",
		"--tls-key /etc/urutau/tls/tls.key",
		"--tls-ca /etc/urutau/tls/ca.crt",
	} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("coordinator command missing %q: %s", want, cmd)
		}
	}

	sts := coordinatorStatefulSet(cr, "urutau:v1")
	if !podSpecHasSecretVolume(sts.Spec.Template.Spec, "control-plane-tls", "srv-tls") {
		t.Fatalf("coordinator must mount the server TLS secret: %+v", sts.Spec.Template.Spec.Volumes)
	}

	tmpl := workerPodTemplate(cr, "urutau:v1", urutauspec.Table{Source: "shop.orders", Target: "raw.orders"})
	if !podSpecHasSecretVolume(tmpl.Spec, "control-plane-tls", "cli-tls") {
		t.Fatalf("worker must mount the client TLS secret: %+v", tmpl.Spec.Volumes)
	}
	wcmd := strings.Join(tmpl.Spec.Containers[0].Command, " ")
	if !strings.Contains(wcmd, "--tls-cert /etc/urutau/tls/tls.crt") {
		t.Fatalf("worker command missing TLS flags: %s", wcmd)
	}

	// Without TLS: the coordinator opts into plaintext explicitly and no TLS
	// volume is mounted.
	plain := pipelineCR("orders", "ns")
	if cmd := strings.Join(coordinatorCommand(plain), " "); !strings.Contains(cmd, "--allow-insecure-control-plane") {
		t.Fatalf("a plaintext coordinator must opt in explicitly: %s", cmd)
	}
	if _, _, ok := controlPlaneTLSVolume(plain, true); ok {
		t.Fatal("no coordinator.tls must produce no volume")
	}
}

func podSpecHasSecretVolume(spec corev1.PodSpec, name, secret string) bool {
	for _, v := range spec.Volumes {
		if v.Name == name && v.Secret != nil && v.Secret.SecretName == secret {
			return true
		}
	}
	return false
}

// #598: the Kafka and schema-registry TLS Secrets are mounted read-only into
// the coordinator at the fixed paths the inline spec names.
func TestSourceTLSMounts(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Secrets.KafkaTLS = "kafka-tls-secret"
	cr.Spec.Secrets.SchemaRegistryTLS = "sr-tls-secret"

	sts := coordinatorStatefulSet(cr, "urutau:v1")
	if !podSpecHasSecretVolume(sts.Spec.Template.Spec, "kafka-tls", "kafka-tls-secret") {
		t.Fatalf("coordinator must mount the Kafka TLS secret: %+v", sts.Spec.Template.Spec.Volumes)
	}
	if !podSpecHasSecretVolume(sts.Spec.Template.Spec, "schema-registry-tls", "sr-tls-secret") {
		t.Fatalf("coordinator must mount the schema-registry TLS secret: %+v", sts.Spec.Template.Spec.Volumes)
	}

	plain := pipelineCR("orders", "ns")
	if v, m := sourceTLSMounts(plain); len(v) != 0 || len(m) != 0 {
		t.Fatalf("no TLS secrets must produce no mounts, got %v / %v", v, m)
	}
}
