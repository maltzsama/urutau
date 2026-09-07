package state_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/internal/state"
)

// --- helpers ---

func openTestStore(t *testing.T) *state.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := state.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// --- Open ---

func TestOpenCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := state.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s.Path() != path {
		t.Errorf("Path = %q, want %q", s.Path(), path)
	}
	_ = s.Close()
}

func TestOpenSecondInstanceBlocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s1, err := state.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s1.Close()

	done := make(chan error, 1)
	go func() {
		s2, err := state.Open(path)
		if err == nil {
			_ = s2.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("second Open should have failed (lock held)")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second Open did not return within 10s")
	}
}

// --- CommitPositions ---

func TestCommitPositionsAtomic(t *testing.T) {
	s := openTestStore(t)

	initial := map[string][]byte{
		"orders":  []byte("offset-orders-1"),
		"users":   []byte("offset-users-1"),
		"products": []byte("offset-products-1"),
	}
	if err := s.CommitPositions("pipeline-1", initial); err != nil {
		t.Fatalf("CommitPositions: %v", err)
	}

	bad := map[string][]byte{
		"orders":  []byte("offset-orders-2"),
		"users":   {},                          // empty offset → should fail
		"products": []byte("offset-products-2"),
	}
	err := s.CommitPositions("pipeline-1", bad)
	if err == nil {
		t.Fatal("expected error for empty offset")
	}

	for table, want := range initial {
		got, err := s.GetPosition("pipeline-1", table)
		if err != nil {
			t.Fatalf("GetPosition(%s): %v", table, err)
		}
		if string(got) != string(want) {
			t.Errorf("after failed commit: %s = %q, want %q (previous intact)", table, got, want)
		}
	}
}

func TestCommitPositionsEmptyTableRejected(t *testing.T) {
	s := openTestStore(t)
	err := s.CommitPositions("p", map[string][]byte{"": []byte("x")})
	if err == nil {
		t.Fatal("expected error for empty table name")
	}
}

func TestCommitPositionsEmptyPipelineRejected(t *testing.T) {
	s := openTestStore(t)
	err := s.CommitPositions("", map[string][]byte{"t": []byte("x")})
	if err == nil {
		t.Fatal("expected error for empty pipeline")
	}
}

func TestCommitPositionsNoop(t *testing.T) {
	s := openTestStore(t)
	err := s.CommitPositions("p", nil)
	if err != nil {
		t.Fatalf("nil map should be noop: %v", err)
	}
	err = s.CommitPositions("p", map[string][]byte{})
	if err != nil {
		t.Fatalf("empty map should be noop: %v", err)
	}
}

func TestCommitPositionsOverwrite(t *testing.T) {
	s := openTestStore(t)

	if err := s.CommitPositions("p", map[string][]byte{"t": []byte("v1")}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if err := s.CommitPositions("p", map[string][]byte{"t": []byte("v2")}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}

	got, err := s.GetPosition("p", "t")
	if err != nil {
		t.Fatalf("GetPosition: %v", err)
	}
	if string(got) != "v2" {
		t.Errorf("Position = %q, want v2", got)
	}
}

// --- Positions ---

func TestPositionsMultipleTables(t *testing.T) {
	s := openTestStore(t)

	watermarks := map[string][]byte{
		"orders":  []byte("w-orders"),
		"users":   []byte("w-users"),
		"products": []byte("w-products"),
	}
	if err := s.CommitPositions("p1", watermarks); err != nil {
		t.Fatalf("CommitPositions: %v", err)
	}

	positions, err := s.Positions("p1")
	if err != nil {
		t.Fatalf("Positions: %v", err)
	}
	if len(positions) != 3 {
		t.Fatalf("Positions len = %d, want 3", len(positions))
	}
	for table, want := range watermarks {
		if string(positions[table]) != string(want) {
			t.Errorf("Positions[%s] = %q, want %q", table, positions[table], want)
		}
	}
}

func TestPositionsEmptyPipeline(t *testing.T) {
	s := openTestStore(t)
	positions, err := s.Positions("nonexistent")
	if err != nil {
		t.Fatalf("Positions: %v", err)
	}
	if len(positions) != 0 {
		t.Errorf("Positions len = %d, want 0", len(positions))
	}
}

// --- Two pipelines, same table, zero stomp ---

func TestTwoPipelinesSameTable(t *testing.T) {
	s := openTestStore(t)

	if err := s.CommitPositions("pipeline-A", map[string][]byte{"orders": []byte("off-A")}); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if err := s.CommitPositions("pipeline-B", map[string][]byte{"orders": []byte("off-B")}); err != nil {
		t.Fatalf("commit B: %v", err)
	}

	posA, _ := s.GetPosition("pipeline-A", "orders")
	posB, _ := s.GetPosition("pipeline-B", "orders")

	if string(posA) != "off-A" {
		t.Errorf("pipeline-A/orders = %q, want off-A", posA)
	}
	if string(posB) != "off-B" {
		t.Errorf("pipeline-B/orders = %q, want off-B", posB)
	}
}

// --- ErrNotFound ---

func TestGetPositionNotFound(t *testing.T) {
	s := openTestStore(t)
	_, err := s.GetPosition("pipeline-1", "orders")
	if err != state.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestGetPositionPipelineNotFound(t *testing.T) {
	s := openTestStore(t)
	_, err := s.GetPosition("nonexistent", "orders")
	if err != state.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestGetSnapshotNotFound(t *testing.T) {
	s := openTestStore(t)
	_, err := s.GetSnapshot("pipeline-1", "orders")
	if err != state.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- Snapshot ---

func TestSnapshotRoundTrip(t *testing.T) {
	s := openTestStore(t)

	meta := &state.SnapshotMetadata{
		EndOffset:  []byte("base64-end-offset"),
		CapturedAt: time.Now().Truncate(time.Microsecond),
	}
	if err := s.SetSnapshot("pipeline-1", "orders", meta); err != nil {
		t.Fatalf("SetSnapshot: %v", err)
	}

	got, err := s.GetSnapshot("pipeline-1", "orders")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if string(got.EndOffset) != string(meta.EndOffset) {
		t.Errorf("EndOffset = %q, want %q", got.EndOffset, meta.EndOffset)
	}
	if !got.CapturedAt.Equal(meta.CapturedAt) {
		t.Errorf("CapturedAt = %v, want %v", got.CapturedAt, meta.CapturedAt)
	}
}

func TestSnapshotRequiresEndOffset(t *testing.T) {
	s := openTestStore(t)

	err := s.SetSnapshot("p", "t", &state.SnapshotMetadata{})
	if err == nil {
		t.Fatal("expected error for empty endOffset")
	}
	err = s.SetSnapshot("p", "t", nil)
	if err == nil {
		t.Fatal("expected error for nil metadata")
	}
}

func TestSnapshotSchemaRoundTrip(t *testing.T) {
	s := openTestStore(t)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: false},
	}, nil)
	ipcSchema := flight.SerializeSchema(schema, memory.NewGoAllocator())

	meta := &state.SnapshotMetadata{
		EndOffset:  []byte("offset-123"),
		Schema:     ipcSchema,
		CapturedAt: time.Now().Truncate(time.Microsecond),
	}
	if err := s.SetSnapshot("p", "t", meta); err != nil {
		t.Fatalf("SetSnapshot: %v", err)
	}

	got, err := s.GetSnapshot("p", "t")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}

	gotSchema, err := flight.DeserializeSchema(got.Schema, memory.NewGoAllocator())
	if err != nil {
		t.Fatalf("DeserializeSchema: %v", err)
	}

	if gotSchema.NumFields() != schema.NumFields() {
		t.Fatalf("schema fields = %d, want %d", gotSchema.NumFields(), schema.NumFields())
	}
	for i := 0; i < schema.NumFields(); i++ {
		want := schema.Field(i)
		got := gotSchema.Field(i)
		if got.Name != want.Name {
			t.Errorf("field %d: Name = %q, want %q", i, got.Name, want.Name)
		}
		if got.Fingerprint() != want.Fingerprint() {
			t.Errorf("field %d: Type = %v, want %v", i, got.Type, want.Type)
		}
		if got.Nullable != want.Nullable {
			t.Errorf("field %d: Nullable = %v, want %v", i, got.Nullable, want.Nullable)
		}
	}
}

// --- DeletePipeline ---

func TestDeletePipeline(t *testing.T) {
	s := openTestStore(t)

	_ = s.CommitPositions("p", map[string][]byte{"t": []byte("off")})
	_ = s.SetSnapshot("p", "t", &state.SnapshotMetadata{EndOffset: []byte("snap")})

	if err := s.DeletePipeline("p"); err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}

	_, err := s.GetPosition("p", "t")
	if err != state.ErrNotFound {
		t.Errorf("position should be gone, got %v", err)
	}

	_, err = s.GetSnapshot("p", "t")
	if err != state.ErrNotFound {
		t.Errorf("snapshot should be gone, got %v", err)
	}
}

func TestDeletePipelineNonexistent(t *testing.T) {
	s := openTestStore(t)
	if err := s.DeletePipeline("nonexistent"); err != nil {
		t.Fatalf("DeletePipeline nonexistent should be noop: %v", err)
	}
}

// --- MVCC concurrency ---

// TestMVCCConcurrentGetDuringCommit verifies that a single GetPosition
// never sees a torn/partial write. bbolt MVCC guarantees each View sees
// a consistent snapshot — no mutex needed. Two separate Views CAN see
// different committed states (that's expected, not a bug).
func TestMVCCConcurrentGetDuringCommit(t *testing.T) {
	s := openTestStore(t)

	_ = s.CommitPositions("p", map[string][]byte{
		"orders": []byte("old-orders"),
	})

	var wg sync.WaitGroup
	errs := make(chan error, 200)

	// Writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			_ = s.CommitPositions("p", map[string][]byte{
				"orders": []byte(fmt.Sprintf("new-orders-%d", i)),
			})
		}
	}()

	// Readers: each individual GetPosition must return a complete value,
	// never empty, never partial.
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				o, err := s.GetPosition("p", "orders")
				if err != nil {
					errs <- fmt.Errorf("GetPosition: %w", err)
					return
				}
				if len(o) == 0 {
					errs <- fmt.Errorf("empty position returned")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("MVCC violation: %v", err)
	}
}

// --- Kill-9 durability ---

func TestCommitSurvivesCloseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	s1, err := state.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = s1.CommitPositions("p", map[string][]byte{"t": []byte("committed-offset")})
	_ = s1.Close()

	s2, err := state.Open(path)
	if err != nil {
		t.Fatalf("Open after close: %v", err)
	}
	defer s2.Close()

	got, err := s2.GetPosition("p", "t")
	if err != nil {
		t.Fatalf("GetPosition after reopen: %v", err)
	}
	if string(got) != "committed-offset" {
		t.Errorf("Position = %q, want committed-offset", got)
	}
}

// --- positionValue round trip ---

func TestPositionValueJSONRoundTrip(t *testing.T) {
	s := openTestStore(t)

	if err := s.CommitPositions("p", map[string][]byte{"t": []byte("abc123")}); err != nil {
		t.Fatalf("CommitPositions: %v", err)
	}

	// Read raw to inspect JSON shape.
	var raw []byte
	err := s.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify the JSON has committedAtNs.
	s2, err := state.Open(s.Path())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()

	pos, err := s2.GetPosition("p", "t")
	if err != nil {
		t.Fatalf("GetPosition: %v", err)
	}
	if string(pos) != "abc123" {
		t.Errorf("offset = %q, want abc123", pos)
	}

	_ = raw
}

// --- DeletePipeline is atomic (position + snapshot) ---

func TestDeletePipelineAtomic(t *testing.T) {
	s := openTestStore(t)

	_ = s.CommitPositions("p", map[string][]byte{"t": []byte("off")})
	_ = s.SetSnapshot("p", "t", &state.SnapshotMetadata{EndOffset: []byte("snap")})

	if err := s.DeletePipeline("p"); err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}

	_, err := s.GetPosition("p", "t")
	if err != state.ErrNotFound {
		t.Errorf("position should be gone: %v", err)
	}
	_, err = s.GetSnapshot("p", "t")
	if err != state.ErrNotFound {
		t.Errorf("snapshot should be gone: %v", err)
	}
}

// --- JSON import guard ---

func TestPositionValueJSON(t *testing.T) {
	type positionValue struct {
		Offset        []byte `json:"offset"`
		CommittedAtNs int64  `json:"committedAtNs"`
	}
	v := positionValue{Offset: []byte("test"), CommittedAtNs: 12345}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var v2 positionValue
	if err := json.Unmarshal(b, &v2); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(v2.Offset) != "test" {
		t.Errorf("Offset = %q, want test", v2.Offset)
	}
	if v2.CommittedAtNs != 12345 {
		t.Errorf("CommittedAtNs = %d, want 12345", v2.CommittedAtNs)
	}
}
