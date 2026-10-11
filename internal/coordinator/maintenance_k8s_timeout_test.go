package coordinator

import (
	"testing"
	"time"
)

// Every Kubernetes call of the maintenance scheduler runs under a deadline.
// Without one a hung API server holds the serial tick, and with it every
// table's maintenance, forever.
func TestMaintenanceK8sCallsCarryADeadline(t *testing.T) {
	ctx, cancel := maintenanceK8sContext()
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("the maintenance Kubernetes context has no deadline")
	}
	if left := time.Until(deadline); left <= 0 || left > maintenanceK8sTimeout {
		t.Fatalf("deadline is %s away, want within %s", left, maintenanceK8sTimeout)
	}
}
