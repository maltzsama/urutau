// Package eventlog writes a per-run JSONL audit trail to S3: one object
// per run, lifecycle and commit events appended as they happen. The trail
// is the post-mortem record — what ran, when, from where, with which
// positions — cheap enough to keep forever. Emits are best-effort by
// contract: a lost trail must never fail the pipeline.
//
// Every Emit uploads the whole buffer (one atomic PUT per event). At CDC
// commit rates the object stays tiny and every upload replaces the last —
// a crash at any instant leaves a consistent trail up to the previous
// event, never a torn line.
package eventlog

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config points the eventlog at its S3 store. Credentials follow the
// standard AWS chain (env, shared config, IMDS); Endpoint overrides the
// API target for MinIO-style stores and implies path-style addressing.
type Config struct {
	// URI is the store root: s3://<bucket>/<prefix>.
	URI string
	// Region defaults to us-east-1 when unset.
	Region string
	// Endpoint overrides the S3 API target; empty uses the AWS default.
	Endpoint string
	// AccessKey/SecretKey override the credential chain when both set.
	AccessKey string
	SecretKey string
}

// Event kinds emitted by the runner.
const (
	KindJobStarted      = "job_started"
	KindResume          = "resume"
	KindSnapshotStarted = "snapshot_started"
	KindSnapshotDone    = "snapshot_done"
	KindCommit          = "commit"
	KindJobStopped      = "job_stopped"
	KindWorkerCreated   = "worker_created"
	KindWorkerReset     = "worker_reset"
	KindJobTerminated   = "job_terminated"
	KindSchemaDrift     = "schema_drift"
	KindDeleteDropped   = "delete_dropped"
)

// Run accumulates one run's events and uploads the trail object as it
// grows. Safe for concurrent use — the coordinator emits KindCommit from
// per-commit goroutines while the run loop emits lifecycle events, so
// multi-writer is the real shape, not a documentation nicety.
type Run struct {
	id      string
	bucket  string
	key     string
	putter  putter
	mu      sync.Mutex // buffer + closed + emitted
	putMu   sync.Mutex // serializes PUTs (see Emit for the lock order)
	buf     []byte
	closed  bool
	emitted int
}

// putter abstracts the S3 PutObject call (unit tests use a fake).
type putter interface {
	Put(ctx context.Context, bucket, key string, body []byte) error
}

// New resolves the config, derives the run id, and returns the writer.
// The object itself appears on the first Emit — the runner emits
// job_started immediately after construction, so a crash anywhere later
// still leaves a trail.
func New(ctx context.Context, cfg Config) (*Run, error) {
	bucket, prefix, err := parseURI(cfg.URI)
	if err != nil {
		return nil, err
	}
	awsCfg, err := loadAWSConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("eventlog: aws config: %w", err)
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
	id := newRunID()
	return &Run{
		id:     id,
		bucket: bucket,
		key:    strings.TrimSuffix(prefix, "/") + "/run-" + id + "/events.jsonl",
		putter: &s3Putter{client: client},
	}, nil
}

// NewWithPutter builds a run around a custom putter (unit tests).
func NewWithPutter(bucket, prefix string, p putter) *Run {
	id := newRunID()
	return &Run{
		id:     id,
		bucket: bucket,
		key:    strings.TrimSuffix(prefix, "/") + "/run-" + id + "/events.jsonl",
		putter: p,
	}
}

// ID returns the run identifier.
func (r *Run) ID() string { return r.id }

// ObjectKey returns the S3 key the trail is written to.
func (r *Run) ObjectKey() string { return r.key }

// Emitted reports how many events the run accepted.
func (r *Run) Emitted() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.emitted
}

// putTimeout bounds a single S3 PutObject so a slow endpoint stalls only
// this event, not the entire pipeline.
const putTimeout = 10 * time.Second

// Emit appends one event and uploads the trail. Fields are free-form; ts,
// run_id, and kind are added automatically. Best-effort by contract:
// callers log failures and carry on.
func (r *Run) Emit(ctx context.Context, kind string, fields map[string]any) error {
	ev := make(map[string]any, len(fields)+3)
	for k, v := range fields {
		ev[k] = v
	}
	ev["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	ev["run_id"] = r.id
	ev["kind"] = kind

	line, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("eventlog: marshal %s: %w", kind, err)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return fmt.Errorf("eventlog: run %s is closed", r.id)
	}
	r.buf = append(r.buf, line...)
	r.buf = append(r.buf, '\n')
	r.emitted++
	body := slices.Clone(r.buf)
	key := r.key
	// putMu is acquired BEFORE releasing mu. The race that forces this
	// order is between two Emits: both clone the body under mu, and whoever
	// releases mu first could reach putMu after the other — inverting the
	// PUT order relative to the append order, so a smaller PUT could land
	// last on S3's last-writer-wins and the event would vanish. Holding
	// putMu across the PUT (and mu until it is taken) makes PUT order ==
	// append order by construction.
	r.putMu.Lock()
	r.mu.Unlock()

	putCtx, cancel := context.WithTimeout(ctx, putTimeout)
	defer cancel()
	err = r.putter.Put(putCtx, r.bucket, key, body)
	r.putMu.Unlock()
	if err != nil {
		return fmt.Errorf("eventlog: put %s/%s: %w", r.bucket, key, err)
	}
	return nil
}

// Close seals the run; further emits fail. The last Emit already uploaded
// the final buffer, so Close only flips the flag.
func (r *Run) Close() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
}

func newRunID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Randomness is a nicety; the second-precision timestamp already
		// separates boots.
		return time.Now().UTC().Format("20060102T150405Z")
	}
	return time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(b[:])
}

func parseURI(uri string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", "", fmt.Errorf("eventlog: store uri %q must be s3://<bucket>/<prefix>", uri)
	}
	bucket, prefix, ok = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("eventlog: store uri %q lacks a bucket", uri)
	}
	if !ok {
		prefix = ""
	}
	return bucket, prefix, nil
}

func loadAWSConfig(ctx context.Context, cfg Config) (aws.Config, error) {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if cfg.Endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(cfg.Endpoint))
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

// s3Putter adapts the S3 client to the putter interface.
type s3Putter struct {
	client *s3.Client
}

func (p *s3Putter) Put(ctx context.Context, bucket, key string, body []byte) error {
	_, err := p.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	return err
}
