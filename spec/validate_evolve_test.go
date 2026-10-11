package spec

import (
	"strings"
	"testing"
)

func TestValidateDeleteMode(t *testing.T) {
	for _, tc := range []struct {
		name, sinkType string
		mode           DeleteMode
		wantProblem    string
	}{
		{"default", "iceberg+rest", "", ""},
		{"equality", "iceberg+rest", DeleteModeEquality, ""},
		{"positional on iceberg", "iceberg+rest", DeleteModePositional, ""},
		{"unknown value", "iceberg+rest", "vectors", `sink.deleteMode: unknown "vectors"`},
		{"positional on clickhouse", "clickhouse", DeleteModePositional, "only the Iceberg sink writes positional deletes"},
		{"equality on clickhouse", "clickhouse", DeleteModeEquality, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var problems []string
			validateDeleteMode(tc.mode, tc.sinkType, &problems)
			got := strings.Join(problems, "; ")
			if tc.wantProblem == "" && got != "" {
				t.Fatalf("problems = %q, want none", got)
			}
			if tc.wantProblem != "" && !strings.Contains(got, tc.wantProblem) {
				t.Fatalf("problems = %q, want one containing %q", got, tc.wantProblem)
			}
		})
	}
}

// The delete mode is rejected at validation, with the rest of the spec, not
// discovered at runtime.
func TestValidateRejectsAnUnknownDeleteMode(t *testing.T) {
	s := validSpec()
	s.Sink.DeleteMode = "vectors"
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "sink.deleteMode") {
		t.Fatalf("Validate = %v, want a sink.deleteMode problem", err)
	}
}
