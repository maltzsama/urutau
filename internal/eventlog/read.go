package eventlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ErrNotFound reports that a pipeline or run does not exist under the root,
// so an HTTP caller can answer 404 instead of an empty 200 (issue #329).
var ErrNotFound = errors.New("eventlog: not found")

// ErrInvalidID reports a pipeline or run identifier that is not a safe single
// path segment. It is embedded in an S3 key prefix, so this is the
// confinement boundary (issue #335).
var ErrInvalidID = errors.New("eventlog: invalid identifier")

// RootConfig points a reader at the shared eventlog root
// (s3://<bucket>/<prefix>). It carries the same connection knobs as Config;
// unlike Config it is not tied to one pipeline — discovery spans the root.
type RootConfig struct {
	Bucket    string
	Prefix    string
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
}

// ParseRoot parses s3://<bucket>/<prefix> into a RootConfig. Region, Endpoint
// and credentials are left for the caller to fill.
func ParseRoot(uri string) (RootConfig, error) {
	bucket, prefix, err := parseURI(uri)
	if err != nil {
		return RootConfig{}, err
	}
	return RootConfig{Bucket: bucket, Prefix: prefix}, nil
}

// PipelineSummary is one pipeline discovered under the root.
type PipelineSummary struct {
	Name string
}

// RunSummary is one run of a pipeline. Started is parsed from the run id's
// timestamp prefix; it is the zero time for an id that does not carry one.
type RunSummary struct {
	ID      string
	Started time.Time
}

// Event is one decoded JSONL line. Fields carries every key besides the
// reserved ts/run_id/kind/seq, mirroring the free-form map Emit accepts.
type Event struct {
	Timestamp time.Time
	RunID     string
	Kind      string
	// Seq is the writer's monotonic per-event sequence number (1-based),
	// added by Emit. Zero on a synthetic marker (events_dropped, run_sealed)
	// and on lines written before seq existed. A gap in the seqs is lost
	// events (issue #333).
	Seq    int
	Fields map[string]any
}

// Trail is a run's decoded events plus the completeness signals the writer
// leaves in the trail (issue #333).
type Trail struct {
	Events []Event
	// Sealed reports whether the run wrote its run_sealed terminal marker.
	// False means the run was abandoned (a crash, a kill) and the trail may
	// be missing its tail.
	Sealed bool
	// Emitted is the writer's final event count, from the terminal marker.
	// Zero when the run is not sealed.
	Emitted int
	// Dropped reports an events_dropped marker: the writer overflowed its
	// buffer under an S3 backlog and lost lines.
	Dropped bool
	// Missing is how many events the writer accepted that the trail does not
	// contain: Emitted minus the events read when sealed, otherwise the gaps
	// in the event seq numbers.
	Missing int
}

// lister abstracts the S3 list + get calls (unit tests use a fake). List
// returns the object keys and, when delimiter is non-empty, the common
// "directory" prefixes directly under prefix.
type lister interface {
	List(ctx context.Context, bucket, prefix, delimiter string) (objects, prefixes []string, err error)
	Get(ctx context.Context, bucket, key string) ([]byte, error)
}

// ListPipelines returns every pipeline with a trail under the root. One list
// call: the shared key convention puts each pipeline under its own prefix.
func ListPipelines(ctx context.Context, cfg RootConfig) ([]PipelineSummary, error) {
	l, err := newLister(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return listPipelines(ctx, l, cfg)
}

// ListRuns returns every run of one pipeline, oldest first.
func ListRuns(ctx context.Context, cfg RootConfig, pipeline string) ([]RunSummary, error) {
	l, err := newLister(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return listRuns(ctx, l, cfg, pipeline)
}

// ReadRun returns a run's events in order: every rotated object under the
// run's prefix, concatenated. Each object is a full re-upload of the events
// since the previous rotation, so the objects never overlap.
func ReadRun(ctx context.Context, cfg RootConfig, pipeline, runID string) ([]Event, error) {
	t, err := ReadRunTrail(ctx, cfg, pipeline, runID)
	if err != nil {
		return nil, err
	}
	return t.Events, nil
}

// ReadRunTrail returns a run's events plus the completeness signals the writer
// left in the trail (issue #333). A missing run yields ErrNotFound, so an HTTP
// caller can answer 404 (issue #329).
func ReadRunTrail(ctx context.Context, cfg RootConfig, pipeline, runID string) (Trail, error) {
	l, err := newLister(ctx, cfg)
	if err != nil {
		return Trail{}, err
	}
	return readRunTrail(ctx, l, cfg, pipeline, runID)
}

func listPipelines(ctx context.Context, l lister, cfg RootConfig) ([]PipelineSummary, error) {
	root := rootPrefix(cfg.Prefix)
	_, prefixes, err := l.List(ctx, cfg.Bucket, root, "/")
	if err != nil {
		return nil, err
	}
	out := make([]PipelineSummary, 0, len(prefixes))
	for _, p := range prefixes {
		name := strings.Trim(strings.TrimPrefix(p, root), "/")
		if name == "" {
			continue
		}
		out = append(out, PipelineSummary{Name: name})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func listRuns(ctx context.Context, l lister, cfg RootConfig, pipeline string) ([]RunSummary, error) {
	if !ValidSegment(pipeline) {
		return nil, fmt.Errorf("%w: pipeline %q", ErrInvalidID, pipeline)
	}
	prefix := pipelinePrefix(cfg.Prefix, pipeline)
	_, prefixes, err := l.List(ctx, cfg.Bucket, prefix, "/")
	if err != nil {
		return nil, err
	}
	out := make([]RunSummary, 0, len(prefixes))
	for _, p := range prefixes {
		dir := strings.Trim(strings.TrimPrefix(p, prefix), "/")
		id, ok := strings.CutPrefix(dir, "run-")
		if !ok || id == "" {
			continue
		}
		out = append(out, RunSummary{ID: id, Started: runStarted(id)})
	}
	// No runs under the prefix is either an unknown pipeline or a genuinely
	// empty one; only the first is an error (issue #329).
	if len(out) == 0 {
		known, err := listPipelines(ctx, l, cfg)
		if err != nil {
			return nil, err
		}
		found := false
		for _, p := range known {
			if p.Name == pipeline {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, pipeline)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func readRun(ctx context.Context, l lister, cfg RootConfig, pipeline, runID string) ([]Event, error) {
	t, err := readRunTrail(ctx, l, cfg, pipeline, runID)
	if err != nil {
		return nil, err
	}
	return t.Events, nil
}

func readRunTrail(ctx context.Context, l lister, cfg RootConfig, pipeline, runID string) (Trail, error) {
	if !ValidSegment(pipeline) {
		return Trail{}, fmt.Errorf("%w: pipeline %q", ErrInvalidID, pipeline)
	}
	if !ValidSegment(runID) {
		return Trail{}, fmt.Errorf("%w: run %q", ErrInvalidID, runID)
	}
	prefix := pipelinePrefix(cfg.Prefix, pipeline) + "run-" + runID + "/"
	objects, _, err := l.List(ctx, cfg.Bucket, prefix, "")
	if err != nil {
		return Trail{}, err
	}
	// Fixed-width names (events-000000.jsonl, …) sort in creation order as
	// plain strings, so the events concatenate in the order they were emitted.
	sort.Strings(objects)
	var out []Event
	for _, key := range objects {
		if !strings.HasSuffix(key, ".jsonl") {
			continue
		}
		body, err := l.Get(ctx, cfg.Bucket, key)
		if err != nil {
			return Trail{}, fmt.Errorf("eventlog: get %s: %w", key, err)
		}
		events, err := parseEvents(body)
		if err != nil {
			return Trail{}, fmt.Errorf("eventlog: parse %s: %w", key, err)
		}
		out = append(out, events...)
	}
	if len(out) == 0 {
		return Trail{}, fmt.Errorf("%w: run %q of pipeline %q", ErrNotFound, runID, pipeline)
	}
	dropped, missing, emitted, sealed := summarize(out)
	return Trail{Events: out, Sealed: sealed, Emitted: emitted, Dropped: dropped, Missing: missing}, nil
}

// summarize folds the writer's completeness signals out of a decoded trail:
// whether it was dropped or sealed, the final emitted count, and how many
// accepted events are missing (issue #333).
func summarize(events []Event) (dropped bool, missing, emitted int, sealed bool) {
	minSeq, maxSeq, count := 0, 0, 0
	for _, e := range events {
		switch e.Kind {
		case KindEventsDropped:
			dropped = true
			continue
		case KindRunSealed:
			sealed = true
			if n, ok := e.Fields["emitted"].(float64); ok {
				emitted = int(n)
			}
			continue
		}
		if e.Seq <= 0 {
			continue
		}
		count++
		if minSeq == 0 || e.Seq < minSeq {
			minSeq = e.Seq
		}
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
	}
	switch {
	case sealed && emitted > count:
		missing = emitted - count
	case minSeq > 0:
		missing = (maxSeq - minSeq + 1) - count
	}
	return dropped, missing, emitted, sealed
}

// ValidSegment reports whether s is a safe single path segment to embed in an
// S3 key prefix: non-empty, no separators, no "." or "..", no control
// characters (issue #335). Exported so the HTTP layer can reject a bad
// identifier before it reaches the store.
func ValidSegment(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	if strings.ContainsAny(s, `/\`) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// parseEvents decodes a JSONL object. A malformed line fails the read rather
// than being skipped: a torn trail is a bug the reader should surface.
func parseEvents(body []byte) ([]Event, error) {
	// One line per newline (the last may lack one) is a good capacity hint;
	// otherwise the slice grows repeatedly (issue #332).
	out := make([]Event, 0, bytes.Count(body, []byte("\n"))+1)
	for _, line := range bytes.Split(body, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			return nil, err
		}
		ev := Event{}
		if v, ok := raw["ts"].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				ev.Timestamp = t
			}
		}
		ev.RunID, _ = raw["run_id"].(string)
		ev.Kind, _ = raw["kind"].(string)
		if n, ok := raw["seq"].(float64); ok {
			ev.Seq = int(n)
		}
		// Reuse the decoded map for Fields instead of copying into a second
		// one: delete the reserved keys in place (issue #332).
		delete(raw, "ts")
		delete(raw, "run_id")
		delete(raw, "kind")
		delete(raw, "seq")
		ev.Fields = raw
		if len(raw) == 0 {
			ev.Fields = map[string]any{}
		}
		out = append(out, ev)
	}
	return out, nil
}

// rootPrefix normalizes the root to a listable prefix: "" or "<prefix>/".
func rootPrefix(prefix string) string {
	p := strings.Trim(prefix, "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

// pipelinePrefix is the list prefix for one pipeline's runs.
func pipelinePrefix(prefix, pipeline string) string {
	return rootPrefix(prefix) + strings.Trim(pipeline, "/") + "/"
}

// runStarted parses the timestamp prefix newRunID writes
// (20060102T150405), the cheapest source of a run's start time.
func runStarted(id string) time.Time {
	if len(id) < 15 {
		return time.Time{}
	}
	t, err := time.Parse("20060102T150405", id[:15])
	if err != nil {
		return time.Time{}
	}
	return t
}

func newLister(ctx context.Context, cfg RootConfig) (lister, error) {
	client, err := newS3Client(ctx, cfg.Region, cfg.Endpoint, cfg.AccessKey, cfg.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("eventlog: aws config: %w", err)
	}
	return &s3Lister{client: client}, nil
}

// s3Lister adapts the S3 client to the lister interface, following
// ContinuationToken so a listing larger than one page is complete.
type s3Lister struct {
	client *s3.Client
}

func (l *s3Lister) List(ctx context.Context, bucket, prefix, delimiter string) (objects, prefixes []string, err error) {
	var token *string
	for {
		in := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)}
		if delimiter != "" {
			in.Delimiter = aws.String(delimiter)
		}
		if token != nil {
			in.ContinuationToken = token
		}
		out, err := l.client.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, nil, err
		}
		for _, o := range out.Contents {
			if o.Key != nil {
				objects = append(objects, *o.Key)
			}
		}
		for _, p := range out.CommonPrefixes {
			if p.Prefix != nil {
				prefixes = append(prefixes, *p.Prefix)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			return objects, prefixes, nil
		}
		token = out.NextContinuationToken
	}
}

func (l *s3Lister) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = out.Body.Close() }()
	return io.ReadAll(out.Body)
}
