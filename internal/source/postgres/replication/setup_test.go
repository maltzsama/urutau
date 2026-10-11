package replication

import (
	"testing"

	"github.com/maltzsama/urutau/source"
)

func TestEnsureSetupBadSlotName(t *testing.T) {
	err := EnsureSetup(t.Context(), nil, "bad-name!", []source.TableRef{{Source: "public.t"}}, "pgoutput", "full")
	if err == nil {
		t.Error("bad slot name: want error")
	}
}

func TestEnsureSetupEmptyTables(t *testing.T) {
	err := EnsureSetup(t.Context(), nil, "slot", nil, "pgoutput", "full")
	if err == nil {
		t.Error("empty tables: want error")
	}
}

func TestEnsureSetupBareSourceName(t *testing.T) {
	err := EnsureSetup(t.Context(), nil, "slot", []source.TableRef{{Source: "no_dot"}}, "pgoutput", "full")
	if err == nil {
		t.Error("bare source name: want error")
	}
}

func TestEnsureSetupValidSlotName(t *testing.T) {
	// Valid slot name passes validation; the nil DB causes a panic later,
	// so just verify the slot-name regex accepts it.
	if !slotNameRe.MatchString("_valid_slot_123") {
		t.Error("_valid_slot_123 should match slot name regex")
	}
	if !slotNameRe.MatchString("mySlot") {
		t.Error("mySlot should match slot name regex")
	}
	if slotNameRe.MatchString("bad-name!") {
		t.Error("bad-name! should not match slot name regex")
	}
	if slotNameRe.MatchString("1starts_digit") {
		t.Error("starts with digit should not match")
	}
}

// "keep" must not touch the database at all — the nil DB proves no query runs
// (issue #571).
func TestEnsureReplicaIdentityKeepSkipsDB(t *testing.T) {
	if err := ensureReplicaIdentity(t.Context(), nil, "public.t", "public", "t", "keep"); err != nil {
		t.Fatalf("keep must not touch the DB: %v", err)
	}
}
