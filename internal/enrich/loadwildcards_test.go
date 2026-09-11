package enrich

// Regression tests for issue #56(a): a wildcard reference's real
// destination columns must be known before a schema owner extends the
// wire schema, not only after the first async refresh. LoadWildcards (per
// Stage) and LoadWildcardColumns (no Stage, for the coordinator) close
// that drift window.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/spec"
)

func starCfg(mutate func(*spec.Enrich)) spec.Enrich {
	cfg := spec.Enrich{
		Table: "users",
		Source: spec.EnrichSource{
			URI:   "mysql://refdb/internal",
			Query: "SELECT id, name, tier FROM users",
		},
		On:       map[string]string{"user_ref": "id"},
		Select:   []string{"*"},
		JoinType: "left",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

// LoadWildcards warms only wildcard refs; RefColumns() is correct
// immediately after it returns, with no Start/poll needed.
func TestLoadWildcardsResolvesRealColumnsBeforeStart(t *testing.T) {
	cfg := starCfg(nil)
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	fl := fakeRows(t, usersRows())
	if err := s.UseLoader(cfg.Table, fl); err != nil {
		t.Fatalf("use loader: %v", err)
	}
	if got := s.RefColumns(); len(got) != 0 {
		t.Fatalf("RefColumns before load = %v, want empty (wildcard, not yet loaded)", got)
	}
	if err := s.LoadWildcards(context.Background()); err != nil {
		t.Fatalf("LoadWildcards: %v", err)
	}
	if !s.Ready() {
		t.Fatal("stage not ready immediately after LoadWildcards")
	}
	got := s.RefColumns()
	want := map[string]bool{"users.id": true, "users.name": true, "users.tier": true}
	if len(got) != len(want) {
		t.Fatalf("RefColumns after LoadWildcards = %v, want 3 real columns", got)
	}
	for _, c := range got {
		if !want[c] {
			t.Fatalf("RefColumns after LoadWildcards contains unexpected %q (got %v)", c, got)
		}
	}
}

// LoadWildcards leaves an explicit-select reference completely untouched —
// no load, no RefColumns change, no interference with its own async Start.
func TestLoadWildcardsSkipsExplicitSelect(t *testing.T) {
	cfg := refCfg(nil) // explicit select: name, tier
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	fl := fakeRows(t, usersRows())
	if err := s.UseLoader(cfg.Table, fl); err != nil {
		t.Fatalf("use loader: %v", err)
	}
	if err := s.LoadWildcards(context.Background()); err != nil {
		t.Fatalf("LoadWildcards: %v", err)
	}
	if got := fl.loadsCount(); got != 0 {
		t.Fatalf("LoadWildcards loaded an explicit-select reference: %d calls", got)
	}
	if s.Ready() {
		t.Fatal("explicit-select reference reported ready before Start ever ran")
	}
}

// Start must not re-issue a load for a reference LoadWildcards already
// warmed — one query on the happy boot path, not two.
func TestStartSkipsRedundantLoadAfterLoadWildcards(t *testing.T) {
	cfg := starCfg(nil)
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	fl := fakeRows(t, usersRows())
	if err := s.UseLoader(cfg.Table, fl); err != nil {
		t.Fatalf("use loader: %v", err)
	}
	if err := s.LoadWildcards(context.Background()); err != nil {
		t.Fatalf("LoadWildcards: %v", err)
	}
	if got := fl.loadsCount(); got != 1 {
		t.Fatalf("loads after LoadWildcards = %d, want 1", got)
	}
	s.Start(context.Background())
	t.Cleanup(s.Stop)
	// Start's first-load branch runs inside its own goroutine; give it a
	// moment to fire (or skip, per the fix) before asserting the count.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := fl.loadsCount(); got != 1 {
		t.Fatalf("loads after Start = %d, want 1 (no redundant reload)", got)
	}
}

// LoadWildcards propagates a load error, naming the failing reference.
func TestLoadWildcardsFailsLoudlyNamingReference(t *testing.T) {
	cfg := starCfg(nil)
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	wantErr := errors.New("connection refused")
	if err := s.UseLoader(cfg.Table, fakeErr(wantErr)); err != nil {
		t.Fatalf("use loader: %v", err)
	}
	err = s.LoadWildcards(context.Background())
	if err == nil {
		t.Fatal("LoadWildcards: want error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("LoadWildcards error = %v, want wrapping %v", err, wantErr)
	}
	if got := err.Error(); !strings.Contains(got, cfg.Table) {
		t.Fatalf("LoadWildcards error %q does not name reference %q", got, cfg.Table)
	}
}

// LoadWildcardColumns has no UseLoader-style test seam (it builds its own
// NewSQLLoader per call, for the coordinator, which has no Stage to
// inject a fake loader into) — so this only exercises what's reachable
// without a real reference DB: an explicit-only cfg list returns exactly
// RefColumnsFor's result untouched (no I/O attempted), and a wildcard cfg
// whose connection fails propagates the error naming the reference. The
// happy-path wildcard discovery (real columns merged in) is covered at
// the Stage level by TestLoadWildcardsResolvesRealColumnsBeforeStart, and
// end to end by the runner test using the same buildImage machinery.
func TestLoadWildcardColumnsExplicitOnlyIsUntouched(t *testing.T) {
	explicit := refCfg(func(c *spec.Enrich) {
		c.Table = "orders"
		c.On = map[string]string{"order_ref": "id"}
	})
	got, err := LoadWildcardColumns(context.Background(), []spec.Enrich{explicit})
	if err != nil {
		t.Fatalf("LoadWildcardColumns: %v", err)
	}
	want := RefColumnsFor([]spec.Enrich{explicit})
	if len(got) != len(want) {
		t.Fatalf("LoadWildcardColumns(explicit only) = %v, want %v", got, want)
	}
}

// An unsupported URI scheme fails NewSQLLoader immediately, no network
// round trip needed — a deterministic stand-in for "the reference is
// unreachable" that doesn't depend on DNS/connect timeouts in CI.
func TestLoadWildcardColumnsFailsLoudlyNamingReference(t *testing.T) {
	star := starCfg(func(c *spec.Enrich) {
		c.Source.URI = "sqlite://not-a-supported-scheme"
	})
	_, err := LoadWildcardColumns(context.Background(), []spec.Enrich{star})
	if err == nil {
		t.Fatal("LoadWildcardColumns: want error for an unreachable reference, got nil")
	}
	if !strings.Contains(err.Error(), star.Table) {
		t.Fatalf("LoadWildcardColumns error %q does not name reference %q", err.Error(), star.Table)
	}
}
