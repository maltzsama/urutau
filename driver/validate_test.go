package driver

import (
	"strings"
	"testing"

	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// #672: onDelete: record (the append-only default) requires a source that
// carries a before image on deletes. The boot path checks the declared
// capability instead of hard-coding the source kind.
func TestValidateOnDeleteImage(t *testing.T) {
	s := &spec.Spec{
		Source: spec.Source{Kind: "kafka"},
		Sink:   spec.Sink{Defaults: spec.Defaults{WriteMode: spec.WriteModeAppend}},
		Tables: []spec.Table{{Source: "t", Target: "raw.t"}},
	}
	if err := validateOnDeleteImage(s, source.Capabilities{BeforeImage: false}); err == nil || !strings.Contains(err.Error(), "onDelete") {
		t.Fatalf("err = %v, want an onDelete failure", err)
	}

	s.Tables[0].OnDelete = spec.OnDeleteSkip
	if err := validateOnDeleteImage(s, source.Capabilities{BeforeImage: false}); err != nil {
		t.Fatalf("onDelete: skip must be accepted: %v", err)
	}

	s.Tables[0].OnDelete = spec.OnDeleteRecord
	if err := validateOnDeleteImage(s, source.Capabilities{BeforeImage: true}); err != nil {
		t.Fatalf("a source with a before image must be accepted: %v", err)
	}

	// Upsert tables do not use the before image for delete handling.
	s.Tables[0].WriteMode = spec.WriteModeUpsert
	s.Tables[0].OnDelete = spec.OnDeleteRecord
	if err := validateOnDeleteImage(s, source.Capabilities{BeforeImage: false}); err != nil {
		t.Fatalf("upsert must be exempt: %v", err)
	}
}
