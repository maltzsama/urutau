package pods

import "testing"

// Issue #465: a Pod replaced under the same name (a chaos pod-kill on a
// StatefulSet) starts again at restart 0. Its log must get its own file, not
// be taken for the run already followed — the chaos run chaos-1M-947abde
// lost the coordinator's logs of 08:53–09:15 and 09:17–end that way.
func TestAReplacedPodGetsItsOwnLogFile(t *testing.T) {
	first, ok1 := parsePodLine("coord-0\tuid-aaaaaaaa-1\tRunning\tcoordinator=0,")
	again, ok2 := parsePodLine("coord-0\tuid-bbbbbbbb-2\tRunning\tcoordinator=0,")
	if !ok1 || !ok2 || len(first) != 1 || len(again) != 1 {
		t.Fatalf("parse: %v %v %v %v", first, ok1, again, ok2)
	}
	if first[0].key() == again[0].key() {
		t.Fatalf("both Pods share the key %q", first[0].key())
	}
	if first[0].fileName() == again[0].fileName() {
		t.Fatalf("both Pods share the file %q", first[0].fileName())
	}
	if _, ok := parsePodLine("coord-0\tuid-a\tPending\t"); ok {
		t.Fatal("a Pod that is not Running has no log to follow")
	}
}
