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
	if sup.MaxConsecutiveCrashes > 0 {
		args = append(args, "--max-consecutive-crashes", strconv.Itoa(sup.MaxConsecutiveCrashes))
	}
	if sup.WorkerDeliveryTimeout != "" {
		args = append(args, "--worker-delivery-timeout", sup.WorkerDeliveryTimeout)
	}
	return args
}
