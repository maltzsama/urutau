package operator

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// Issue #465: a trail on an S3-compatible store (RustFS, MinIO) needs its
// endpoint on the coordinator's command line, and its credentials from a
// Secret, or the history server has nothing to read.
func TestCoordinatorEventlogOnAnS3CompatibleStore(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Coordinator.Eventlog = &urutauv1alpha1.EventlogSpec{
		Bucket:   "trails",
		Endpoint: "http://rustfs:9000",
		Secret:   "trail-creds",
	}
	cmd := strings.Join(coordinatorCommand(cr), " ")
	if !strings.Contains(cmd, "--eventlog-endpoint http://rustfs:9000") {
		t.Fatalf("command = %q, want --eventlog-endpoint http://rustfs:9000", cmd)
	}
	byName := map[string]corev1.EnvVar{}
	for _, e := range coordinatorContainerEnv(cr) {
		byName[e.Name] = e
	}
	for env, key := range map[string]string{"AWS_ACCESS_KEY_ID": "accessKeyId", "AWS_SECRET_ACCESS_KEY": "secretAccessKey"} {
		ref := byName[env].ValueFrom
		if ref == nil || ref.SecretKeyRef == nil || ref.SecretKeyRef.Name != "trail-creds" || ref.SecretKeyRef.Key != key {
			t.Fatalf("%s = %+v, want secret trail-creds key %s", env, byName[env], key)
		}
	}
}

// The trail credentials belong to the coordinator only: a worker Pod does not
// write the trail, so it must not carry them.
func TestTheTrailCredentialsStayOffTheWorkers(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.Coordinator.Eventlog = &urutauv1alpha1.EventlogSpec{Bucket: "trails", Secret: "trail-creds"}
	for _, e := range coordinatorEnv(cr) {
		if strings.HasPrefix(e.Name, "AWS_") {
			t.Fatalf("worker env carries %s", e.Name)
		}
	}
	plain := pipelineCR("orders", "ns")
	plain.Spec.Coordinator.Eventlog = &urutauv1alpha1.EventlogSpec{Bucket: "trails"}
	if cmd := strings.Join(coordinatorCommand(plain), " "); strings.Contains(cmd, "--eventlog-endpoint") {
		t.Fatalf("command = %q, must not carry --eventlog-endpoint when unset", cmd)
	}
	for _, e := range coordinatorContainerEnv(plain) {
		if strings.HasPrefix(e.Name, "AWS_") {
			t.Fatalf("coordinator env carries %s with no secret set", e.Name)
		}
	}
}
