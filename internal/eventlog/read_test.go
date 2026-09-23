package eventlog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeLister serves a fixed key→body map and derives common prefixes from the
// keys, so the reader's discovery logic is exercised without S3.
type fakeLister struct {
	objects map[string]string
}

func (f *fakeLister) List(_ context.Context, _, prefix, delimiter string) (objects, prefixes []string, err error) {
	seen := map[string]bool{}
	for k := range f.objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if delimiter != "" {
			if i := strings.Index(k[len(prefix):], delimiter); i >= 0 {
				p := prefix + k[len(prefix):][:i+len(delimiter)]
				if !seen[p] {
					seen[p] = true
					prefixes = append(prefixes, p)
				}
				continue
			}
		}
		objects = append(objects, k)
	}
	return objects, prefixes, nil
}

func (f *fakeLister) Get(_ context.Context, _, key string) ([]byte, error) {
	return []byte(f.objects[key]), nil
}

func testLister() *fakeLister {
	return &fakeLister{objects: map[string]string{
		"urutau/shop/run-20260101T000000-aa/events-000000.jsonl":  `{"ts":"2026-01-01T00:00:00Z","run_id":"20260101T000000-aa","kind":"job_started","pipeline":"shop"}` + "\n",
		"urutau/shop/run-20260101T000000-aa/events-000001.jsonl":  `{"ts":"2026-01-01T00:00:01Z","run_id":"20260101T000000-aa","kind":"commit","table":"orders"}` + "\n",
		"urutau/shop/run-20260102T000000-bb/events-000000.jsonl":  `{"ts":"2026-01-02T00:00:00Z","run_id":"20260102T000000-bb","kind":"job_started"}` + "\n",
		"urutau/other/run-20260103T000000-cc/events-000000.jsonl": `{"ts":"2026-01-03T00:00:00Z","run_id":"20260103T000000-cc","kind":"job_started"}` + "\n",
	}}
}

func TestListPipelines(t *testing.T) {
	got, err := listPipelines(context.Background(), testLister(), RootConfig{Bucket: "b", Prefix: "urutau"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "other" || got[1].Name != "shop" {
		t.Fatalf("pipelines = %+v, want [other shop] (sorted)", got)
	}
}

func TestListRuns(t *testing.T) {
	got, err := listRuns(context.Background(), testLister(), RootConfig{Bucket: "b", Prefix: "urutau"}, "shop")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("runs = %+v, want 2", got)
	}
	if got[0].ID != "20260101T000000-aa" || got[1].ID != "20260102T000000-bb" {
		t.Fatalf("runs = %+v, want oldest first", got)
	}
	want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !got[0].Started.Equal(want) {
		t.Fatalf("started = %v, want %v (parsed from the run id)", got[0].Started, want)
	}
}

func TestReadRunConcatenatesRotations(t *testing.T) {
	got, err := readRun(context.Background(), testLister(), RootConfig{Bucket: "b", Prefix: "urutau"}, "shop", "20260101T000000-aa")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("events = %d, want 2 (both rotated objects)", len(got))
	}
	if got[0].Kind != "job_started" || got[1].Kind != "commit" {
		t.Fatalf("kinds = %q, %q; want job_started, commit (in order)", got[0].Kind, got[1].Kind)
	}
	if got[0].Fields["pipeline"] != "shop" {
		t.Fatalf("fields = %+v, want pipeline=shop", got[0].Fields)
	}
	// ts/run_id/kind are lifted out of Fields, not duplicated in it.
	for _, reserved := range []string{"ts", "run_id", "kind"} {
		if _, ok := got[0].Fields[reserved]; ok {
			t.Fatalf("Fields must not carry %q: %+v", reserved, got[0].Fields)
		}
	}
	if !got[1].Timestamp.Equal(time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)) {
		t.Fatalf("timestamp = %v", got[1].Timestamp)
	}
}

func TestParseRoot(t *testing.T) {
	cfg, err := ParseRoot("s3://bucket/urutau/trails")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Bucket != "bucket" || cfg.Prefix != "urutau/trails" {
		t.Fatalf("ParseRoot = %+v", cfg)
	}
	if _, err := ParseRoot("http://nope"); err == nil {
		t.Fatal("a non-s3 URI must fail")
	}
}

// #333: the reader reports a seq gap and a seal marker through Trail, so a
// truncated trail is not indistinguishable from a complete one.
func TestReadRunTrailReportsCompleteness(t *testing.T) {
	l := &fakeLister{objects: map[string]string{
		"urutau/shop/run-20260101T000000-aa/events-000000.jsonl": `{"ts":"2026-01-01T00:00:00Z","run_id":"r","kind":"job_started","seq":1}` + "\n" +
			`{"ts":"2026-01-01T00:00:01Z","run_id":"r","kind":"commit","seq":3}` + "\n" +
			`{"ts":"2026-01-01T00:00:02Z","run_id":"r","kind":"run_sealed","emitted":4}` + "\n",
	}}
	tr, err := readRunTrail(context.Background(), l, RootConfig{Bucket: "b", Prefix: "urutau"}, "shop", "20260101T000000-aa")
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Sealed || tr.Emitted != 4 {
		t.Fatalf("seal = %v emitted = %d, want sealed with 4", tr.Sealed, tr.Emitted)
	}
	if tr.Missing != 2 {
		t.Fatalf("missing = %d, want 2 (seq 2 and 4 absent)", tr.Missing)
	}
}

// #333: an unsealed run is reported as such — the trail may be missing its
// tail.
func TestReadRunTrailUnsealed(t *testing.T) {
	tr, err := readRunTrail(context.Background(), testLister(), RootConfig{Bucket: "b", Prefix: "urutau"}, "shop", "20260101T000000-aa")
	if err != nil {
		t.Fatal(err)
	}
	if tr.Sealed {
		t.Fatal("a trail with no run_sealed marker must report Sealed=false")
	}
}

// #329: a nonexistent run is ErrNotFound, not an empty success.
func TestReadRunNotFound(t *testing.T) {
	_, err := readRun(context.Background(), testLister(), RootConfig{Bucket: "b", Prefix: "urutau"}, "shop", "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// #329: an unknown pipeline is ErrNotFound, not an empty run list.
func TestListRunsUnknownPipelineNotFound(t *testing.T) {
	_, err := listRuns(context.Background(), testLister(), RootConfig{Bucket: "b", Prefix: "urutau"}, "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// #335: an unsafe path segment is rejected before it reaches a key prefix.
func TestReadRejectsUnsafeSegment(t *testing.T) {
	for _, id := range []string{"", ".", "..", "../../etc", "a/b", `a\b`} {
		if _, err := readRunTrail(context.Background(), testLister(), RootConfig{Bucket: "b", Prefix: "urutau"}, "shop", id); !errors.Is(err, ErrInvalidID) {
			t.Errorf("runID %q: err = %v, want ErrInvalidID", id, err)
		}
		if _, err := listRuns(context.Background(), testLister(), RootConfig{Bucket: "b", Prefix: "urutau"}, id); !errors.Is(err, ErrInvalidID) {
			t.Errorf("pipeline %q: err = %v, want ErrInvalidID", id, err)
		}
	}
}
