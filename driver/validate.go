package driver

import (
	"fmt"

	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// validateOnDeleteImage rejects onDelete: record (the default for append-only
// tables) when the source declares it cannot carry a before image on deletes
// (issue #672). spec.Validate cannot check this: driver imports spec, not the
// reverse, so only the boot path has both the spec and the source's declared
// capabilities. A source that declares BeforeImage=false and an operator who
// still asked to record the deleted row would otherwise get a stream that
// silently drops or nulls the delete.
func validateOnDeleteImage(s *spec.Spec, caps source.Capabilities) error {
	if caps.BeforeImage {
		return nil
	}
	for i := range s.Tables {
		t := s.Tables[i]
		mode := spec.EffectiveWriteMode(t, s.Sink.Defaults.WriteMode)
		if mode != spec.WriteModeAppend && mode != spec.WriteModeAppendIdempotent {
			continue
		}
		if t.OnDelete == spec.OnDeleteRecord || t.OnDelete == "" {
			return fmt.Errorf("source %q declares no before image on deletes: %s.onDelete must be %q",
				s.Source.Kind, t.Target, spec.OnDeleteSkip)
		}
	}
	return nil
}
