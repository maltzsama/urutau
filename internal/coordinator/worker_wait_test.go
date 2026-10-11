package coordinator

import (
	"testing"
	"time"
)

// A worker exits when it loses its coordinator, and the kubelet restarts it
// with a backoff that doubles up to five minutes. After a few coordinator
// restarts in a row every worker sits in that backoff. A coordinator that
// waits less than the backoff ceiling for its workers gives up before they
// can come back, exits, and resets every worker that had connected: the
// pipeline restarts in a loop with no progress.
func TestDefaultWorkerWaitOutlastsTheKubeletCrashBackoff(t *testing.T) {
	const kubeletCrashBackoffCeiling = 5 * time.Minute
	if DefaultWaitWorker <= kubeletCrashBackoffCeiling {
		t.Fatalf("DefaultWaitWorker = %s, want more than the kubelet's %s crash backoff ceiling", DefaultWaitWorker, kubeletCrashBackoffCeiling)
	}
}
