package operator

import (
	"strings"
	"testing"

	urutauspec "github.com/maltzsama/urutau/spec"
)

// spec.logLevel reaches the coordinator and every worker as --log-level, so
// a pipeline's debug logs can be turned on in Kubernetes; unset, neither
// carries the flag and the binaries keep their default.
func TestLogLevelReachesCoordinatorAndWorkers(t *testing.T) {
	cr := pipelineCR("orders", "ns")
	cr.Spec.LogLevel = "debug"
	if cmd := strings.Join(coordinatorCommand(cr), " "); !strings.Contains(cmd, "--log-level debug") {
		t.Fatalf("coordinator command = %q, want --log-level debug", cmd)
	}
	tbl := urutauspec.Table{Target: "raw.orders"}
	if cmd := strings.Join(workerPodTemplate(cr, "urutau:test", tbl).Spec.Containers[0].Command, " "); !strings.Contains(cmd, "--log-level debug") {
		t.Fatalf("worker command = %q, want --log-level debug", cmd)
	}
	plain := pipelineCR("orders", "ns")
	if cmd := strings.Join(coordinatorCommand(plain), " "); strings.Contains(cmd, "--log-level") {
		t.Fatalf("coordinator command = %q, must not carry --log-level when unset", cmd)
	}
}
