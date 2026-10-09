package main

import "testing"

func TestLoopbackAddr(t *testing.T) {
	for _, ok := range []string{"127.0.0.1:6060", "localhost:6060", "[::1]:6060"} {
		if !loopbackAddr(ok) {
			t.Fatalf("%q must be recognized as loopback", ok)
		}
	}
	for _, bad := range []string{"0.0.0.0:6060", ":6060", "10.0.0.1:6060", "notanaddr"} {
		if loopbackAddr(bad) {
			t.Fatalf("%q must not be recognized as loopback", bad)
		}
	}
}

// #604: the profiler is loopback-only, so a public --debug-addr is rejected.
func TestValidateDebugAddrRejectsNonLoopback(t *testing.T) {
	f := &coordinatorFlags{debugAddr: "0.0.0.0:6060", allowInsecure: true}
	if err := f.validate(); err == nil {
		t.Fatal("a non-loopback --debug-addr must be rejected")
	}
	f.debugAddr = "127.0.0.1:6060"
	if err := f.validate(); err != nil {
		t.Fatalf("a loopback --debug-addr must be accepted: %v", err)
	}
}
