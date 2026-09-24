package eventlog

import "testing"

func TestOutcomeOf(t *testing.T) {
	cases := []struct {
		name   string
		kind   string
		reason string
		want   Outcome
	}{
		{"clean shutdown", KindJobStopped, "shutdown", OutcomeSucceeded},
		{"operator cancel", KindJobTerminated, "cancelled", OutcomeCancelled},
		{"job_stopped cancelled", KindJobStopped, "cancelled", OutcomeCancelled},
		{"crashloop", KindJobTerminated, "crashloop", OutcomeFailed},
		{"stream error", KindJobStopped, "stream", OutcomeFailed},
		{"session error", KindJobStopped, "session", OutcomeFailed},
		{"snapshot error", KindJobStopped, "snapshot", OutcomeFailed},
		{"startup failed", KindJobStopped, "startup_failed", OutcomeFailed},
		{"generic error", KindJobStopped, "error", OutcomeFailed},
	}
	for _, tc := range cases {
		got := outcomeOf([]Event{{Kind: tc.kind, Fields: map[string]any{"reason": tc.reason}}})
		if got != tc.want {
			t.Errorf("%s: outcomeOf(%s/%s) = %q, want %q", tc.name, tc.kind, tc.reason, got, tc.want)
		}
	}
}

func TestOutcomeOfNoTerminalIsUnknown(t *testing.T) {
	if got := outcomeOf([]Event{{Kind: KindCommit}, {Kind: KindSnapshotDone}}); got != OutcomeUnknown {
		t.Errorf("no terminal event: outcomeOf = %q, want unknown", got)
	}
	if got := outcomeOf(nil); got != OutcomeUnknown {
		t.Errorf("empty trail: outcomeOf = %q, want unknown", got)
	}
}

func TestOutcomeOfLastTerminalWins(t *testing.T) {
	got := outcomeOf([]Event{
		{Kind: KindJobStopped, Fields: map[string]any{"reason": "stream"}},
		{Kind: KindJobTerminated, Fields: map[string]any{"reason": "cancelled"}},
	})
	if got != OutcomeCancelled {
		t.Errorf("last terminal wins: outcomeOf = %q, want cancelled", got)
	}
}
