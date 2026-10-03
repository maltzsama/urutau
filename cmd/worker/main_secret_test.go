package main

import (
	"bytes"
	"strings"
	"testing"
)

// #597: the client secret falls back to the environment only after flag
// parsing, so a usage dump (--help, or a mistyped flag) never prints it.
func TestClientSecretIsNotPrintedInUsage(t *testing.T) {
	const secret = "s3cr3t-do-not-print"
	t.Setenv("URUTAU_SINK_CLIENT_SECRET", secret)

	for _, args := range [][]string{{"--help"}, {"--client-secret-typo"}} {
		cmd := runCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs(args)
		_ = cmd.Execute()
		if strings.Contains(out.String(), secret) {
			t.Fatalf("%v leaked the client secret in its output:\n%s", args, out.String())
		}
	}
}

// The environment still reaches the sink config when the flag is absent.
func TestClientSecretFallsBackToEnv(t *testing.T) {
	const secret = "from-the-environment"
	t.Setenv("URUTAU_SINK_CLIENT_SECRET", secret)

	f := &workerFlags{coordinator: "127.0.0.1:50051", name: "w0", catalogURI: "http://catalog", logLevel: "info", logFormat: "text"}
	cfg, err := f.config()
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if got := cfg.Sink.Options["client_secret"]; got != secret {
		t.Fatalf("client_secret = %q, want the environment value", got)
	}
}
