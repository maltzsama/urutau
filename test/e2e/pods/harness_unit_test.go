package pods

import "testing"

// #691: teardown must recognize a lingering Pod or StatefulSet by name prefix,
// so a still-terminating pipeline is not mistaken for a clean namespace.
func TestHasResourceWithPrefix(t *testing.T) {
	listing := "pod-smoke-coordinator-0\npod-smoke-raw-orders-abc-0\npod-crash-coordinator-0\n"
	if !hasResourceWithPrefix(listing, "pod-smoke-") {
		t.Fatal("a lingering pipeline resource must be detected")
	}
	if hasResourceWithPrefix(listing, "pod-other-") {
		t.Fatal("an unrelated prefix must not match")
	}
	if hasResourceWithPrefix("", "pod-smoke-") {
		t.Fatal("an empty listing has no resources")
	}
	// A prefix must anchor at the name start, not match a substring later in
	// the line.
	if hasResourceWithPrefix("pod-crash-coordinator-0\n", "crash-") {
		t.Fatal("a substring prefix must not match")
	}
}
