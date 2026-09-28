package operator

import (
	"strconv"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// supervisionArgs renders the coordinator's worker-supervision flags; only
// fields that are set are passed, so defaults live in the binary.
func supervisionArgs(sup urutauv1alpha1.SupervisionSpec) []string {
	var args []string
	if sup.AckTimeout != "" {
		args = append(args, "--ack-timeout", sup.AckTimeout)
	}
	if sup.MaxResets > 0 {
		args = append(args, "--max-resets", strconv.Itoa(sup.MaxResets))
	}
	if sup.Window != "" {
		args = append(args, "--reset-window", sup.Window)
	}
	if sup.MaxLossesWithoutProgress > 0 {
		args = append(args, "--max-losses-without-progress", strconv.Itoa(sup.MaxLossesWithoutProgress))
	}
	if sup.WorkerAbsenceTimeout != "" {
		args = append(args, "--worker-absence-timeout", sup.WorkerAbsenceTimeout)
	}
	return args
}
