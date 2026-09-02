package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// CheckpointConfig points the async position checkpoint at its S3 store.
// The manifest is convenience only: recovery reads the Iceberg table
// property (the source of truth) on each worker's Hello, so a lost or stale
// checkpoint never blocks anything (design §6).
type CheckpointConfig struct {
	URI      string // s3://<bucket>/<prefix>
	Interval time.Duration

	Endpoint  string
	AccessKey string
	SecretKey string
}

// checkpoint writes per-worker PositionManifests to S3 on an interval.
type checkpoint struct {
	interval time.Duration
	client   *s3.Client
	bucket   string
	prefix   string
}

func newCheckpoint(ctx context.Context, cfg CheckpointConfig) (*checkpoint, error) {
	bucket, prefix, err := parseS3URI(cfg.URI)
	if err != nil {
		return nil, err
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.AccessKey != "" && cfg.SecretKey != "" {
			o.Credentials = credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")
		}
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})
	return &checkpoint{interval: interval, client: client, bucket: bucket, prefix: strings.TrimSuffix(prefix, "/")}, nil
}

// run writes the manifests until ctx is done. Async and best-effort by
// contract: a failed checkpoint is logged, never fatal.
func (c *checkpoint) run(ctx context.Context, runID string, index map[string]*positionIndex, log *slog.Logger) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for worker, idx := range index {
				m := idx.Manifest()
				body, err := json.Marshal(m)
				if err != nil {
					log.Warn("checkpoint: marshal", "worker", worker, "err", err)
					continue
				}
				key := fmt.Sprintf("%s/%s/%s/manifest.json", c.prefix, runID, worker)
				if _, err := c.client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(c.bucket),
					Key:    aws.String(key),
					Body:   strings.NewReader(string(body)),
				}); err != nil {
					log.Warn("checkpoint: put", "key", key, "err", err)
				}
			}
		}
	}
}

func parseS3URI(uri string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", "", fmt.Errorf("checkpoint: store uri %q must be s3://<bucket>/<prefix>", uri)
	}
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("checkpoint: store uri %q lacks a bucket", uri)
	}
	return bucket, prefix, nil
}
