package eventlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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
// reserved ts/run_id/kind, mirroring the free-form map Emit accepts.
type Event struct {
	Timestamp time.Time
	RunID     string
	Kind      string
	Fields    map[string]any
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
	l, err := newLister(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return readRun(ctx, l, cfg, pipeline, runID)
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
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func readRun(ctx context.Context, l lister, cfg RootConfig, pipeline, runID string) ([]Event, error) {
	prefix := pipelinePrefix(cfg.Prefix, pipeline) + "run-" + strings.Trim(runID, "/") + "/"
	objects, _, err := l.List(ctx, cfg.Bucket, prefix, "")
	if err != nil {
		return nil, err
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
			return nil, fmt.Errorf("eventlog: get %s: %w", key, err)
		}
		events, err := parseEvents(body)
		if err != nil {
			return nil, fmt.Errorf("eventlog: parse %s: %w", key, err)
		}
		out = append(out, events...)
	}
	return out, nil
}

// parseEvents decodes a JSONL object. A malformed line fails the read rather
// than being skipped: a torn trail is a bug the reader should surface.
func parseEvents(body []byte) ([]Event, error) {
	var out []Event
	for _, line := range bytes.Split(body, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			return nil, err
		}
		ev := Event{Fields: map[string]any{}}
		if v, ok := raw["ts"].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				ev.Timestamp = t
			}
		}
		ev.RunID, _ = raw["run_id"].(string)
		ev.Kind, _ = raw["kind"].(string)
		for k, v := range raw {
			switch k {
			case "ts", "run_id", "kind":
			default:
				ev.Fields[k] = v
			}
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
