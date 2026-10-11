package spec

import (
	"fmt"
	"strings"
)

// validateEvolveSchema rejects sink.evolveSchema on a sink that cannot evolve
// its target schema. Only the Iceberg sink implements evolution, so any other
// sink would accept the option and silently ignore it — the operator would
// believe the schema adapts while the sink keeps failing closed (issue #672).
func validateEvolveSchema(enabled bool, sinkType string, problems *[]string) {
	if !enabled || sinkSupportsEvolveSchema(sinkType) {
		return
	}
	*problems = append(*problems, fmt.Sprintf(
		"sink.evolveSchema: only the Iceberg sink evolves the target schema, not sink.type %q", sinkType))
}

// sinkSupportsEvolveSchema reports whether a sink type implements additive
// schema evolution. It is the same Iceberg family as maintenance; spec cannot
// ask the driver registry (driver imports spec, not the reverse), so the
// sink-type name is matched here, like sinkSupportsMaintenance.
func sinkSupportsEvolveSchema(sinkType string) bool {
	return sinkType == "iceberg" || strings.HasPrefix(sinkType, "iceberg+")
}

// validateDeleteMode rejects an unknown sink.deleteMode, and a positional one
// on a sink that has no deletion vectors — the knob would do nothing there.
func validateDeleteMode(mode DeleteMode, sinkType string, problems *[]string) {
	switch mode {
	case "", DeleteModeEquality:
		return
	case DeleteModePositional:
	default:
		*problems = append(*problems, fmt.Sprintf("sink.deleteMode: unknown %q (equality | positional)", mode))
		return
	}
	if !sinkSupportsEvolveSchema(sinkType) {
		*problems = append(*problems, fmt.Sprintf(
			"sink.deleteMode: only the Iceberg sink writes positional deletes, not sink.type %q", sinkType))
	}
}
