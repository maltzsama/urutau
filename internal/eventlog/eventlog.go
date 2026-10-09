// Package eventlog writes a per-run JSONL audit trail to S3: lifecycle and
// commit events appended as they happen, in objects sealed by a background
// flusher. Keys are fixed-width (events-000000.jsonl, events-000001.jsonl, …),
// so they sort in creation order as plain strings. The trail is the post-mortem
// record — what ran, when, from where, with which positions — cheap enough
// to keep forever. Emits are best-effort by contract: a lost trail must
// never fail the pipeline.
//
// # Key convention
//
// The object layout is a shared, structured root so a reader can discover
// pipelines and runs with nothing but S3 list calls (no database):
//
//	s3://<bucket>/<prefix>/<pipeline>/run-<id>/events-NNNNNN.jsonl
//
// Listing <prefix>/ yields the pipeline names; listing <prefix>/<pipeline>/
// yields the run ids. Config.Pipeline supplies the segment; an empty
// Pipeline omits it (<prefix>/run-<id>/), for callers that are not a named
// pipeline.
//
// # Write model
//
// Emit and EmitBatch only enqueue: they marshal the event (assigning its
// sequence number) and hand the line to a bounded queue, never touching S3 on
// the caller's goroutine. A single background flusher drains that queue into a
// buffer and, when the buffer reaches MaxObjectBytes or the flush interval
// elapses, uploads it as one immutable object under the next key. An object is
// written exactly once and never rewritten, so a run re-uploads bytes in
// O(total bytes), not O(events × object size) — the quadratic cost that made
// every Emit re-PUT the whole buffer (issue #548). A failed upload keeps its
// buffer and is retried on the next flush; the buffer is capped so an S3
// outage drops the oldest lines (with an events_dropped marker) instead of
// growing without bound.
//
// Lifecycle events that must be durable — job_started, job_stopped — call
// Flush (or Close) so the object is on S3 before the caller continues.
package eventlog

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
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
	// Pipeline is the pipeline name, inserted as a path segment between the
	// URI prefix and the run: <prefix>/<pipeline>/run-<id>/. It is what
	// makes the trail discoverable by listing alone (see the package doc).
	// Empty omits the segment, for callers that are not a named pipeline.
	Pipeline string
	// Region defaults to us-east-1 when unset.
	Region string
	// Endpoint overrides the S3 API target; empty uses the AWS default.
	Endpoint string
	// AccessKey/SecretKey override the credential chain when both set.
	AccessKey string
	SecretKey string
	// MaxObjectBytes caps one flushed object: the flusher seals and uploads
	// the current object once it reaches this many bytes. Zero or negative is
	// clamped to the default (8 MiB) — never a flush per event.
	MaxObjectBytes int64
	// FlushInterval is how often a non-empty buffer is uploaded. A burst that
	// reaches MaxObjectBytes flushes sooner. Zero or negative is clamped to
	// the default (1s).
	FlushInterval time.Duration
	// QueueSize bounds the in-memory queue of lines awaiting the flusher.
	// When it is full, further events are dropped and counted (a best-effort
	// trail never blocks the pipeline). Zero or negative is clamped to the
	// default.
	QueueSize int
	// Logger receives the best-effort warnings (a dropped event, a failed
	// upload, a drop under outage). Nil means slog.Default().
	Logger *slog.Logger
}

// defaultMaxObjectBytes is the object-size cap when the config is silent:
// large enough that a commit-rate run seals rarely, small enough that each
// PUT stays cheap.
const defaultMaxObjectBytes = int64(8 << 20)

// defaultFlushInterval is how often a non-empty buffer is uploaded when the
// config is silent (issue #548: one PUT per second, like the log trail).
const defaultFlushInterval = time.Second

// defaultQueueSize bounds the lines awaiting the flusher when the config is
// silent: a burst between flushes fits, and a stalled flusher cannot grow the
// process without limit.
const defaultQueueSize = 8192

// maxBufferFactor caps the unflushed buffer at MaxObjectBytes × this. While an
// upload keeps failing the buffer grows with the outage; without a cap it can
// OOM the coordinator. Overflow drops the oldest lines and warns (issue #230).
const maxBufferFactor = 4

// putTimeout bounds a single S3 PutObject so a slow endpoint stalls only the
// flusher, not the pipeline.
const putTimeout = 10 * time.Second

func orDefaultLogger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
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
	// KindDestructiveDDL records a TRUNCATE or destructive DDL the source
	// stream carried but the engine did not propagate to the sink: the sink
	// diverges from the source until an operator reconciles it (issue #671).
	KindDestructiveDDL = "destructive_ddl"
	// KindLog is one structured process log record (a slog line) the
	// coordinator's log trail appends. It carries level/msg/attrs — the
	// run's operational log, distinct from the lifecycle events above, so a
	// postmortem reads cause and context from the same trail (issue: logs
	// belong in S3, not only in the live buffer).
	KindLog = "log"
	// KindWorkerRetired records an owner removed by a scale-in; its key
	// range is inherited by the remaining owners.
	KindWorkerRetired = "worker_retired"
	// KindTableRepartitioned records a live re-slice of one table's
	// partition ranges (issue #312).
	KindTableRepartitioned = "table_repartitioned"
	// KindEventsDropped is a synthetic marker the flusher inserts where it
	// discarded buffered lines under a failing upload: without it the trail
	// reads as if the pipeline simply did nothing during the gap
	// (issue #333).
	KindEventsDropped = "events_dropped"
	// KindRunSealed is the terminal marker Close appends: it records the final
	// emitted count, so a reader can verify it read every event and can tell a
	// sealed run from an abandoned one (issue #333).
	KindRunSealed = "run_sealed"
)

// Run accumulates one run's events and uploads the trail as it grows. Safe
// for concurrent use — the coordinator emits KindCommit from per-commit
// callbacks while the run loop emits lifecycle events, so multi-writer is the
// real shape, not a documentation nicety. Emit and EmitBatch marshal under mu
// (so sequence and queue order match) and hand the line to a single flusher
// goroutine; only that goroutine touches the buffer, the object key and the
// putter.
type Run struct {
	id     string
	bucket string
	// baseKey is the run's key prefix (".../run-<id>/") — immutable after
	// New. Rotated object keys are always derived from it.
	baseKey        string
	maxObjectBytes int64
	maxBufferBytes int64
	flushEvery     time.Duration
	putter         putter
	log            *slog.Logger

	// mu guards the emit-side state (emitted/closed/closing) and the flusher's
	// published key/seq, so ObjectKey/Emitted/Emitted can read them safely. It
	// is never held across an S3 call.
	mu      sync.Mutex
	emitted int
	closed  bool
	closing bool
	key     string // next object key; advanced under mu after a successful PUT
	seq     int    // objects sealed so far; the next key is derived from it

	// dropped counts events not enqueued because the queue was full.
	dropped atomic.Int64

	// Flusher-owned state. buf accumulates marshaled lines awaiting a
	// successful PUT; it is reset only after a PUT succeeds and retains its
	// lines (bounded) after a failure.
	ch       chan []byte
	buf      []byte
	flushReq chan chan error
	stop     chan struct{}
	done     chan struct{}
	closeErr error
}

// putter abstracts the S3 PutObject call (unit tests use a fake).
type putter interface {
	Put(ctx context.Context, bucket, key string, body []byte) error
}

// New resolves the config, derives the run id, and returns the writer with its
// flusher running. The first object appears on the first flush — the runner
// emits job_started and calls Flush immediately after construction, so a crash
// anywhere later still leaves a trail.
func New(ctx context.Context, cfg Config) (*Run, error) {
	bucket, prefix, err := parseURI(cfg.URI)
	if err != nil {
		return nil, err
	}
	client, err := newS3Client(ctx, cfg.Region, cfg.Endpoint, cfg.AccessKey, cfg.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("eventlog: aws config: %w", err)
	}
	id := newRunID()
	return newRun(bucket, runBaseKey(prefix, cfg.Pipeline, id), id,
		cfg.MaxObjectBytes, cfg.FlushInterval, cfg.QueueSize,
		&s3Putter{client: client}, orDefaultLogger(cfg.Logger)), nil
}

// NewWithPutter builds a run around a custom putter (unit tests). The flush
// size and interval default to the same values as New.
func NewWithPutter(bucket, prefix string, maxObjectBytes int64, p putter) *Run {
	id := newRunID()
	return newRun(bucket, runBaseKey(prefix, "", id), id, maxObjectBytes, 0, 0, p, slog.Default())
}

// newRun wires the writer and starts its flusher goroutine.
func newRun(bucket, base, id string, maxObjectBytes int64, every time.Duration, queueSize int, p putter, log *slog.Logger) *Run {
	if maxObjectBytes <= 0 {
		maxObjectBytes = defaultMaxObjectBytes
	}
	if every <= 0 {
		every = defaultFlushInterval
	}
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	r := &Run{
		id:             id,
		bucket:         bucket,
		baseKey:        base,
		maxObjectBytes: maxObjectBytes,
		maxBufferBytes: maxObjectBytes * maxBufferFactor,
		flushEvery:     every,
		putter:         p,
		log:            log,
		key:            base + "events-000000.jsonl",
		ch:             make(chan []byte, queueSize),
		flushReq:       make(chan chan error),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}
	go r.flushLoop()
	return r
}

// runBaseKey is one run's immutable key prefix:
// <prefix>[/<pipeline>]/run-<id>/ . The pipeline segment is what a
// database-free reader lists under to discover pipelines, then runs.
func runBaseKey(prefix, pipeline, id string) string {
	parts := make([]string, 0, 2)
	if p := strings.Trim(prefix, "/"); p != "" {
		parts = append(parts, p)
	}
	if p := strings.Trim(pipeline, "/"); p != "" {
		parts = append(parts, p)
	}
	base := strings.Join(parts, "/")
	if base != "" {
		base += "/"
	}
	return base + "run-" + id + "/"
}

// ID returns the run identifier.
func (r *Run) ID() string { return r.id }

// ObjectKey returns the key the next successful flush will write. With the
// flusher advancing it after each upload, it changes over the run's life, so
// it is read under the append lock.
func (r *Run) ObjectKey() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.key
}

// Emitted reports how many events the run accepted (enqueued or dropped).
func (r *Run) Emitted() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.emitted
}

// Dropped reports how many events were not enqueued because the queue was
// full. A non-zero count means the trail is missing events by contract.
func (r *Run) Dropped() int64 { return r.dropped.Load() }

// Emit enqueues one event. Fields are free-form; ts (unless the caller
// supplies one — the log trail stamps its own records), run_id, kind and seq
// are added automatically. It is non-blocking: when the queue is full the
// event is dropped and counted. Best-effort by contract — a lost trail must
// never fail the pipeline. The ctx is accepted for API stability and is not
// consulted; durability is via Flush or Close.
func (r *Run) Emit(_ context.Context, kind string, fields map[string]any) error {
	return r.enqueue(kind, []map[string]any{fields})
}

// EmitBatch enqueues several events of one kind together. It is what a log
// trail needs: one queue slot per batch instead of one per line. An empty
// batch is a no-op.
func (r *Run) EmitBatch(_ context.Context, kind string, batch []map[string]any) error {
	if len(batch) == 0 {
		return nil
	}
	return r.enqueue(kind, batch)
}

// enqueue marshals the batch (assigning sequence numbers under mu, so the
// queue order matches the sequence order) and hands each line to the flusher,
// dropping and counting when the queue is full.
func (r *Run) enqueue(kind string, batch []map[string]any) error {
	r.mu.Lock()
	if r.closed || r.closing {
		r.mu.Unlock()
		return fmt.Errorf("eventlog: run %s is closed", r.id)
	}
	lines, seq, err := r.marshalLocked(kind, batch)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.emitted = seq
	var dropped int64
	for _, line := range lines {
		entry := append(line, '\n')
		select {
		case r.ch <- entry:
		default:
			dropped++
		}
	}
	r.mu.Unlock()

	if dropped > 0 {
		n := r.dropped.Add(dropped)
		r.log.Warn("eventlog: queue full; events dropped",
			"run", r.id, "dropped", n, "queued", len(r.ch))
	}
	return nil
}

// flushLoop is the single goroutine that owns the buffer and the putter. It
// batches queued lines and uploads on the size cap, the interval, an explicit
// Flush, or shutdown — in that order, so a burst flushes before the timer.
func (r *Run) flushLoop() {
	defer close(r.done)
	ticker := time.NewTicker(r.flushEvery)
	defer ticker.Stop()
	for {
		select {
		case line := <-r.ch:
			r.buf = append(r.buf, line...)
			r.flushIfFull()
		case <-ticker.C:
			_ = r.flush()
		case errc := <-r.flushReq:
			r.drain()
			errc <- r.flush()
		case <-r.stop:
			r.drain()
			r.seal()
			return
		}
	}
}

// drain pulls every currently-queued line into the buffer without blocking.
func (r *Run) drain() {
	for {
		select {
		case line := <-r.ch:
			r.buf = append(r.buf, line...)
		default:
			return
		}
	}
}

// flushIfFull uploads early once the buffer reaches the object cap, so a burst
// between ticks does not grow one object without limit.
func (r *Run) flushIfFull() {
	if int64(len(r.buf)) >= r.maxObjectBytes {
		_ = r.flush()
	}
}

// flush uploads the current buffer as one immutable object and, on success,
// advances to the next key. On failure the buffer is retained (bounded) and
// the upload is retried on the next flush.
func (r *Run) flush() error {
	if len(r.buf) == 0 {
		return nil
	}
	key := r.key // flusher-owned: no other goroutine writes it
	putCtx, cancel := context.WithTimeout(context.Background(), putTimeout)
	defer cancel()
	if err := r.putter.Put(putCtx, r.bucket, key, r.buf); err != nil {
		r.boundBuffer()
		r.log.Warn("eventlog: put", "key", key, "err", err)
		return fmt.Errorf("eventlog: put %s/%s: %w", r.bucket, key, err)
	}
	r.buf = nil
	r.advance()
	return nil
}

// advance publishes the next object key after a successful upload.
func (r *Run) advance() {
	r.mu.Lock()
	r.seq++
	r.key = r.baseKey + fmt.Sprintf("events-%06d.jsonl", r.seq)
	r.mu.Unlock()
}

// boundBuffer trims the buffer to the last MaxObjectBytes bytes, aligned to a
// line boundary, when a failing upload let it grow past the hard cap. The
// lines it replaces are gone, so it leaves an events_dropped marker: without
// it the trail reads as if the pipeline simply did nothing during the gap
// (issues #230, #333).
func (r *Run) boundBuffer() {
	if int64(len(r.buf)) <= r.maxBufferBytes {
		return
	}
	target := len(r.buf) - int(r.maxObjectBytes)
	if target <= 0 {
		return
	}
	if i := bytes.IndexByte(r.buf[target:], '\n'); i >= 0 {
		target += i + 1
	} else {
		target = len(r.buf)
	}
	dropped := target
	rest := r.buf[target:]
	next := make([]byte, 0, len(rest)+128)
	if marker, err := json.Marshal(map[string]any{
		"ts":     time.Now().UTC().Format(time.RFC3339Nano),
		"run_id": r.id,
		"kind":   KindEventsDropped,
		"bytes":  dropped,
	}); err == nil {
		next = append(next, marker...)
		next = append(next, '\n')
	}
	next = append(next, rest...)
	r.buf = next
	r.log.Warn("eventlog: dropped buffered events under a failing upload",
		"run", r.id, "bytes", dropped)
}

// seal appends the run_sealed terminal marker and uploads the final object.
func (r *Run) seal() {
	r.mu.Lock()
	emitted := r.emitted
	r.mu.Unlock()
	// The terminal marker records the final emitted count, so a reader can
	// verify it read every event and can tell a sealed run from an abandoned
	// one (issue #333). It is written even for a run that emitted nothing.
	if marker, err := json.Marshal(map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"run_id":  r.id,
		"kind":    KindRunSealed,
		"emitted": emitted,
	}); err == nil {
		r.buf = append(r.buf, marker...)
		r.buf = append(r.buf, '\n')
	}
	r.closeErr = r.flush()
}

// Flush uploads everything currently queued and returns the upload error, if
// any. Callers on a lifecycle path use it to make an event durable before they
// continue. A closed run yields an error.
func (r *Run) Flush(ctx context.Context) error {
	errc := make(chan error, 1)
	select {
	case r.flushReq <- errc:
	case <-r.done:
		return fmt.Errorf("eventlog: run %s is closed", r.id)
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-errc:
		return err
	case <-r.done:
		return fmt.Errorf("eventlog: run %s is closed", r.id)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close seals the run: no further emits are accepted, the queue is drained,
// and the final buffer — plus the run_sealed marker — is uploaded. The
// returned error surfaces a best-effort PUT failure, so a graceful shutdown
// that lost the trail's tail is not silent (issue #231). Idempotent.
func (r *Run) Close() error {
	r.mu.Lock()
	if r.closed || r.closing {
		closing := r.closing
		r.mu.Unlock()
		if closing {
			<-r.done
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.closeErr
	}
	r.closing = true // no new Emit may start
	r.mu.Unlock()

	close(r.stop)
	<-r.done

	r.mu.Lock()
	r.closed = true
	err := r.closeErr
	r.mu.Unlock()
	return err
}

// marshalLocked renders batch as JSONL lines carrying consecutive seqs after
// the events already appended. It reads only r.emitted and r.id, so it runs
// with mu held; on failure nothing has been mutated and the caller leaves the
// trail untouched. ts defaults to now, but a caller-supplied one wins, so a
// batched log record keeps its own timestamp instead of the flush time.
func (r *Run) marshalLocked(kind string, batch []map[string]any) ([][]byte, int, error) {
	lines := make([][]byte, 0, len(batch))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	seq := r.emitted
	for _, fields := range batch {
		seq++
		ev := make(map[string]any, len(fields)+4)
		for k, v := range fields {
			ev[k] = v
		}
		if _, ok := ev["ts"]; !ok {
			ev["ts"] = now
		}
		ev["run_id"] = r.id
		ev["kind"] = kind
		ev["seq"] = seq
		line, err := json.Marshal(ev)
		if err != nil {
			return nil, 0, fmt.Errorf("eventlog: marshal %s: %w", kind, err)
		}
		lines = append(lines, line)
	}
	return lines, seq, nil
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
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("eventlog: store uri %q lacks a bucket", uri)
	}
	// Normalize a trailing slash so s3://bucket and s3://bucket/ derive the
	// same base key (issue #232).
	prefix = strings.Trim(prefix, "/")
	return bucket, prefix, nil
}

// newS3Client builds the S3 client both the writer and the reader use.
// Credentials follow the standard AWS chain unless an explicit key pair is
// given; Endpoint overrides the API target for MinIO-style stores and implies
// path-style addressing.
func newS3Client(ctx context.Context, region, endpoint, accessKey, secretKey string) (*s3.Client, error) {
	awsCfg, err := loadAWSConfig(ctx, region, endpoint)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if accessKey != "" && secretKey != "" {
			o.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		}
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

func loadAWSConfig(ctx context.Context, region, endpoint string) (aws.Config, error) {
	if region == "" {
		region = "us-east-1"
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(endpoint))
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
