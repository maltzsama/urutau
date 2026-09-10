// Package eventlog writes a per-run JSONL audit trail to S3: lifecycle and
// commit events appended as they happen, in objects rotated by size. Keys
// are fixed-width (events-000000.jsonl, events-000001.jsonl, …), so they
// sort in creation order as plain strings. The trail is the post-mortem
// record — what ran, when, from where, with which positions — cheap enough
// to keep forever. Emits are best-effort by contract: a lost trail must
// never fail the pipeline. A rotated object whose PUT fails is retained and
// retried (see Run.pending), so a transient never drops a sealed object.
//
// Every Emit uploads the whole current object (one atomic PUT per event).
// At CDC commit rates the object stays tiny and every upload replaces the
// last — a crash at any instant leaves a consistent trail up to the
// previous event, never a torn line. Rotation caps the cumulative bytes a
// long run would otherwise re-upload quadratically.
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
	// MaxObjectBytes rotates the trail object once the buffer reaches this
	// size: the crossing event travels in the object being closed, and the
	// next Emit opens the next one. Zero or negative is clamped to the
	// default (8 MiB) — never a rotation per event.
	MaxObjectBytes int64
}

// defaultMaxObjectBytes is the rotation threshold when the config is
// silent: large enough that a commit-rate run rotates rarely, small enough
// that each PUT stays cheap.
const defaultMaxObjectBytes = int64(8 << 20)

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
	id     string
	bucket string
	// baseKey is the run's key prefix (".../run-<id>/") — immutable after
	// New. Rotated object keys are always derived from it: deriving from
	// the current key corrupts from the second rotation on (its
	// "events-NNNNNN.jsonl" suffix no longer matches).
	baseKey        string
	key            string // current object; mutated under mu on rotation
	seq            int    // rotations so far; 0 before the first
	maxObjectBytes int64
	putter         putter
	mu             sync.Mutex // buffer + closed + emitted + key/seq + pending
	putMu          sync.Mutex // serializes PUTs (see Emit for the lock order)
	buf            []byte
	closed         bool
	emitted        int
	// pending holds rotated objects whose PUT failed, in creation order.
	// They are retried — before the next own PUT and by Close — because the
	// buffer that produced them is gone: a rotated object must never be
	// dropped (R-1). Rotation is DEFERRED while the backlog is non-empty, so
	// it only holds the objects sealed in the window before the first
	// failure propagated (bounded by concurrent Emits), never an unbounded
	// stream. Mutated only under mu, and never while putMu is held (mu is
	// re-acquired after putMu is released — no lock-order violation).
	pending []pendingObject
}

// pendingObject is a rotated object awaiting (re)delivery.
type pendingObject struct {
	key  string
	body []byte
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
	max := cfg.MaxObjectBytes
	if max <= 0 {
		max = defaultMaxObjectBytes
	}
	base := strings.TrimSuffix(prefix, "/") + "/run-" + id + "/"
	return &Run{
		id:             id,
		bucket:         bucket,
		baseKey:        base,
		key:            base + "events-000000.jsonl",
		seq:            0,
		maxObjectBytes: max,
		putter:         &s3Putter{client: client},
	}, nil
}

// NewWithPutter builds a run around a custom putter (unit tests).
// maxObjectBytes <= 0 is clamped to the default (8 MiB).
func NewWithPutter(bucket, prefix string, maxObjectBytes int64, p putter) *Run {
	if maxObjectBytes <= 0 {
		maxObjectBytes = defaultMaxObjectBytes
	}
	id := newRunID()
	base := strings.TrimSuffix(prefix, "/") + "/run-" + id + "/"
	return &Run{
		id:             id,
		bucket:         bucket,
		baseKey:        base,
		key:            base + "events-000000.jsonl",
		seq:            0,
		maxObjectBytes: maxObjectBytes,
		putter:         p,
	}
}

// ID returns the run identifier.
func (r *Run) ID() string { return r.id }

// ObjectKey returns the S3 key of the trail object currently being written.
// With rotation the key changes over the run's life, so it is read under
// the append lock.
func (r *Run) ObjectKey() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.key
}

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
	// Drain the undelivered rotated objects; they are retried, in order,
	// before this emit's own PUT.
	pending := r.pending
	r.pending = nil
	r.buf = append(r.buf, line...)
	r.buf = append(r.buf, '\n')
	r.emitted++
	body := slices.Clone(r.buf)
	key := r.key
	// Rotation: the event that crosses the threshold travels in the object
	// being closed; the next Emit opens the next one. One rotation = one
	// PUT, zero extra. The key is ALWAYS derived from baseKey — deriving
	// from the current key corrupts from the second rotation on. buf/key/seq
	// mutate only under mu.
	//
	// Rotation is DEFERRED while the backlog is non-empty: sealing a new
	// object behind a failing PUT would orphan its buffer (the loss R-1
	// fixes). The buffer keeps accumulating instead, so nothing is sealed
	// until delivery works.
	rotated := len(pending) == 0 && len(r.buf) >= int(r.maxObjectBytes)
	if rotated {
		r.seq++
		r.buf = nil
		r.key = r.baseKey + fmt.Sprintf("events-%06d.jsonl", r.seq)
	}
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
	// Deliver the backlog first, in order. On the first failure, requeue the
	// failed object and everything after it — nothing is lost. The own PUT
	// is skipped (the network is down) and the buffer survives, because
	// rotation was deferred whenever a backlog existed.
	for i := range pending {
		if err := r.putter.Put(putCtx, r.bucket, pending[i].key, pending[i].body); err != nil {
			r.putMu.Unlock()
			r.mu.Lock()
			r.pending = append(append([]pendingObject{}, pending[i:]...), r.pending...)
			r.mu.Unlock()
			return fmt.Errorf("eventlog: put %s/%s: %w", r.bucket, pending[i].key, err)
		}
	}
	err = r.putter.Put(putCtx, r.bucket, key, body)
	r.putMu.Unlock()
	if err != nil {
		if rotated {
			// The rotated object's buffer is gone: retain it so the next
			// Emit or Close retries it — a rotated object must never be
			// dropped. A non-rotated current object's buffer survives, so
			// the next Emit re-PUTs it as before.
			r.mu.Lock()
			r.pending = append(r.pending, pendingObject{key: key, body: body})
			r.mu.Unlock()
		}
		return fmt.Errorf("eventlog: put %s/%s: %w", r.bucket, key, err)
	}
	return nil
}

// Close seals the run: further emits fail, and the final buffer — plus any
// rotated object still awaiting delivery — is re-uploaded best-effort. A
// failed last Emit left its line in the buffer (the contract is best-effort,
// the event stays accepted), and without the re-flush a graceful shutdown
// would lose exactly that final event. The close PUTs are serialized against
// in-flight Emits through putMu, so the objects never land smaller than what
// was accepted.
//
// Lock order: mu is released BEFORE putMu is taken — the inverse of Emit,
// and safe here. The Emit race (both Emits clone the body and compete for
// putMu) does not exist for the one-shot close, and the closed flag set
// under mu prevents re-entry. Serializing with in-flight PUTs comes from
// putMu itself; holding mu during the wait would pin the append lock for
// up to one putTimeout for no additional correctness.
func (r *Run) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	pending := r.pending
	r.pending = nil
	body := slices.Clone(r.buf)
	key := r.key
	r.mu.Unlock()

	r.putMu.Lock()
	defer r.putMu.Unlock()
	if len(pending) == 0 && len(body) == 0 {
		return
	}
	putCtx, cancel := context.WithTimeout(context.Background(), putTimeout)
	defer cancel()
	// Deliver the backlog first, then the current buffer — best-effort.
	for _, po := range pending {
		_ = r.putter.Put(putCtx, r.bucket, po.key, po.body)
	}
	if len(body) > 0 {
		_ = r.putter.Put(putCtx, r.bucket, key, body)
	}
}

func newRunID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means the boot environment is broken: a
		// deterministic fallback would collide keys across boots in the
		// same second and one trail would overwrite the other (the
		// randTicket precedent — same policy, same reason).
		panic(fmt.Sprintf("eventlog: crypto/rand: %v", err))
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
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/x-ndjson"),
	})
	return err
}
