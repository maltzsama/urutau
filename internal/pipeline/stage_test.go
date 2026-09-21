package pipeline

import (
	"testing"

	"github.com/maltzsama/urutau/internal/plugin/contract"
)

// #276: roleFor must reject an unknown kind, not silently default to source.
func TestRoleForKnownAndUnknown(t *testing.T) {
	if r, err := roleFor(StageSource); err != nil || r != contract.RoleSource {
		t.Fatalf("source role = %v, %v; want RoleSource", r, err)
	}
	if r, err := roleFor(StageSink); err != nil || r != contract.RoleSink {
		t.Fatalf("sink role = %v, %v; want RoleSink", r, err)
	}
	if _, err := roleFor(StageKind("")); err == nil {
		t.Fatal("an empty stage kind must error, not default to source")
	}
	if _, err := roleFor(StageKind("transform")); err == nil {
		t.Fatal("an unknown stage kind must error")
	}
}
