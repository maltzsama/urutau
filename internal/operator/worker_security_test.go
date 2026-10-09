package operator

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

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
