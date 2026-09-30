package pods

// The run trail (issue #465): every e2e pipeline writes its eventlog to the
// in-cluster RustFS, the store the history server reads. Since #441 the trail
// carries every coordinator and worker log line, so a run's logs survive a
// replaced Pod — the in-cluster log follower cannot keep a log once its Pod is
// gone — and the artifacts take the trail from there.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
)

const (
	trailBucket   = "urutau-history"
	trailPrefix   = "e2e"
	trailEndpoint = "http://rustfs.e2e.svc.cluster.local:9000"
	trailSecret   = "pod-e2e-trail"
)

// e2eEventlog is the CR's coordinator.eventlog block every e2e pipeline gets.
var e2eEventlog = map[string]any{
	"bucket": trailBucket, "rootPrefix": trailPrefix,
	"endpoint": trailEndpoint, "secret": trailSecret,
}

// ensureTrailSecret creates the Secret holding the trail store's credentials,
// the same RustFS keys the bucket-init Job uses.
func ensureTrailSecret(t *testing.T) {
	t.Helper()
	ensureSecret(t, testNS, trailSecret, map[string]string{
		"accessKeyId":     "urutau",
		"secretAccessKey": "urutau_dev_secret",
	})
}

// trailRoot reads the trail back from RustFS: the test runs in-cluster, so it
// reaches the store at the same address the coordinator writes to.
var trailRoot = eventlog.RootConfig{
	Bucket: trailBucket, Prefix: trailPrefix, Region: "us-east-1",
	Endpoint: trailEndpoint, AccessKey: "urutau", SecretKey: "urutau_dev_secret",
}

// dumpTrail writes every run of pipeline that started at or after since (one
// run per coordinator start) into dir/trail, one JSON event per line, and
// returns how many runs it wrote. Best effort, like the other diagnostics.
func dumpTrail(ctx context.Context, dir, pipeline string, since time.Time) (int, error) {
	runs, err := eventlog.ListRuns(ctx, trailRoot, pipeline)
	if err != nil {
		return 0, err
	}
	d := filepath.Join(dir, "trail")
	if err := os.MkdirAll(d, 0o755); err != nil {
		return 0, err
	}
	n := 0
	for _, run := range runs {
		// A run id carries its start at second precision.
		if !run.Started.IsZero() && run.Started.Before(since.Truncate(time.Second)) {
			continue
		}
		tr, err := eventlog.ReadRunTrail(ctx, trailRoot, pipeline, run.ID)
		if err != nil {
			return n, err
		}
		f, err := os.Create(filepath.Join(d, "run-"+run.ID+".jsonl"))
		if err != nil {
			return n, err
		}
		enc := json.NewEncoder(f)
		for _, ev := range tr.Events {
			line := map[string]any{"ts": ev.Timestamp, "kind": ev.Kind, "seq": ev.Seq}
			for k, v := range ev.Fields {
				line[k] = v
			}
			_ = enc.Encode(line)
		}
		_ = enc.Encode(map[string]any{"trail_sealed": tr.Sealed, "trail_missing": tr.Missing, "trail_dropped": tr.Dropped})
		_ = f.Close()
		n++
	}
	return n, nil
}
