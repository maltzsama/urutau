package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/runner"
)

// TestEventlogTrail runs the MySQL pipeline with an audit trail pointed at
// the stack's S3 store, then reads the trail back and checks the lifecycle:
// job_started first, resume/snapshot/commit in the middle, job_stopped last,
// one run_id across every line. The trail is the post-mortem record — this
// test proves it survives a real run and is readable back through plain S3.
func TestEventlogTrail(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	trailCfg := eventlog.Config{
		URI:       "s3://warehouse/e2e-events/" + fmt.Sprintf("%d", time.Now().UnixNano()),
		Region:    "us-east-1",
		Endpoint:  env("URUTAU_E2E_S3_ENDPOINT", "http://localhost:9000"),
		AccessKey: env("URUTAU_E2E_S3_KEY", "urutau"),
		SecretKey: env("URUTAU_E2E_S3_SECRET", "urutau_dev_secret"),
	}

	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 20)

	cfg := testConfig()
	cfg.Eventlog = &trailCfg

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(runCtx, loadPipeline(t), cfg) }()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(20))
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (101, 'live', 1.5)`)
	dml(t, db, `UPDATE orders SET v = 'upd' WHERE id = 1`)
	dml(t, db, `DELETE FROM orders WHERE id = 2`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(20)) // 20 - 1 (del) + 1 (live)

	stop()
	// Cancellation is the normal shutdown path; anything else is a failure.
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run: %v", err)
	}

	// The final job_stopped PUT completes before Run returns, so the
	// object is complete by the time we read it.
	lines := readTrail(t, trailCfg)
	if len(lines) < 5 {
		t.Fatalf("trail has %d lines, want >= 5 (started, resumed, snapshot x2, commits, stopped)", len(lines))
	}

	kinds := make([]string, 0, len(lines))
	runIDs := map[string]bool{}
	commits, snapshots := 0, 0
	for _, line := range lines {
		var ev map[string]any
		if err := json.Unmarshal(line, &ev); err != nil {
			t.Fatalf("trail line is not json: %v (%q)", err, line)
		}
		kind, _ := ev["kind"].(string)
		id, _ := ev["run_id"].(string)
		if kind == "" || id == "" {
			t.Fatalf("trail line missing kind/run_id: %q", line)
		}
		kinds = append(kinds, kind)
		runIDs[id] = true
		switch kind {
		case eventlog.KindCommit:
			commits++
		case eventlog.KindSnapshotStarted, eventlog.KindSnapshotDone:
			snapshots++
		}
	}
	if len(runIDs) != 1 {
		t.Errorf("trail spans %d run ids, want 1", len(runIDs))
	}
	if kinds[0] != eventlog.KindJobStarted {
		t.Errorf("first kind = %s, want %s", kinds[0], eventlog.KindJobStarted)
	}
	if last := kinds[len(kinds)-1]; last != eventlog.KindJobStopped {
		t.Errorf("last kind = %s, want %s", last, eventlog.KindJobStopped)
	}
	if commits == 0 {
		t.Error("trail carries no commit events")
	}
	if snapshots == 0 {
		t.Error("trail carries no snapshot events")
	}

	// The stop is a deliberate cancel; the trail must say so, not "error".
	var stopped map[string]any
	if err := json.Unmarshal(lines[len(lines)-1], &stopped); err != nil {
		t.Fatalf("last line: %v", err)
	}
	if stopped["reason"] != "cancelled" {
		t.Errorf("job_stopped reason = %v, want cancelled", stopped["reason"])
	}
}

// readTrail lists the run's object under the trail prefix and returns its
// JSONL lines.
func readTrail(t *testing.T, cfg eventlog.Config) [][]byte {
	t.Helper()

	rest, ok := strings.CutPrefix(cfg.URI, "s3://")
	if !ok {
		t.Fatalf("trail uri %q is not s3", cfg.URI)
	}
	bucket, prefix, _ := strings.Cut(rest, "/")

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithBaseEndpoint(cfg.Endpoint),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")
	})

	lst, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("list trail objects: %v", err)
	}
	if len(lst.Contents) != 1 {
		var keys []string
		for _, o := range lst.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		t.Fatalf("trail prefix holds %d objects, want 1: %v", len(lst.Contents), keys)
	}

	obj, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    lst.Contents[0].Key,
	})
	if err != nil {
		t.Fatalf("get trail object: %v", err)
	}
	defer func() { _ = obj.Body.Close() }()
	body, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatalf("read trail object: %v", err)
	}

	var lines [][]byte
	for _, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
		lines = append(lines, []byte(line))
	}
	return lines
}
