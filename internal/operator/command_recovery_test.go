package operator

import (
	"strings"
	"testing"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// The coordinator's worker-recovery limits (issue #461) reach it as flags
// when the CR sets them, and are left to the binary's defaults otherwise.
func TestCoordinatorCommandPassesRecoveryLimits(t *testing.T) {
	cr := &urutauv1alpha1.CDCPipeline{}
	cr.Spec.Coordinator.Supervision = urutauv1alpha1.SupervisionSpec{MaxLossesWithoutProgress: 4, WorkerAbsenceTimeout: "7m"}
	cmd := strings.Join(coordinatorCommand(cr), " ")
	if !strings.Contains(cmd, "--max-losses-without-progress 4") || !strings.Contains(cmd, "--worker-absence-timeout 7m") {
		t.Fatalf("coordinator command = %q, want both recovery limits", cmd)
	}

	cmd = strings.Join(coordinatorCommand(&urutauv1alpha1.CDCPipeline{}), " ")
	if strings.Contains(cmd, "--max-losses-without-progress") || strings.Contains(cmd, "--worker-absence-timeout") {
		t.Fatalf("coordinator command = %q, want the binary's defaults when unset", cmd)
	}
}
