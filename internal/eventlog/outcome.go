package eventlog

// Outcome classifies how a run ended, from the last terminal event in its
// trail. It is independent of Trail.Sealed: a run can be sealed (the writer
// closed cleanly) and still have failed, and an unsealed run's outcome may be
// unknown — its trail was truncated before a terminal marker (issue #350).
type Outcome string

const (
	// OutcomeSucceeded: the run stopped cleanly (job_stopped reason
	// "shutdown").
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeFailed: the run ended on an error — startup_failed, snapshot,
	// session, stream, error, or a job_terminated "crashloop".
	OutcomeFailed Outcome = "failed"
	// OutcomeCancelled: an operator cancel (reason "cancelled").
	OutcomeCancelled Outcome = "cancelled"
	// OutcomeUnknown: no terminal event in the trail (unsealed, or written
	// before this classification existed).
	OutcomeUnknown Outcome = "unknown"
)

// outcomeOf derives a run's Outcome from its events: the reason on the last
// job_stopped / job_terminated event. The taxonomy maps the coordinator's and
// runner's existing reason values (issue #350) — no new emission.
func outcomeOf(events []Event) Outcome {
	for i := len(events) - 1; i >= 0; i-- {
		switch events[i].Kind {
		case KindJobTerminated:
			// job_terminated reasons: "cancelled" | "crashloop".
			if reasonOf(events[i]) == "cancelled" {
				return OutcomeCancelled
			}
			return OutcomeFailed
		case KindJobStopped:
			switch reasonOf(events[i]) {
			case "shutdown":
				return OutcomeSucceeded
			case "cancelled":
				return OutcomeCancelled
			default:
				// startup_failed | snapshot | session | stream | error.
				return OutcomeFailed
			}
		}
	}
	return OutcomeUnknown
}

// reasonOf returns an event's "reason" field, or "" when absent.
func reasonOf(e Event) string {
	if e.Fields == nil {
		return ""
	}
	r, _ := e.Fields["reason"].(string)
	return r
}
