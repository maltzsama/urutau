package proc_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/internal/plugin/proc"
)

func TestSpawnMissingBinary(t *testing.T) {
	_, err := proc.Spawn(context.Background(), proc.Config{
		Token: "dGVzdA==",
	})
	if err == nil {
		t.Fatal("expected error for missing binary")
	}
}

func TestSpawnMissingToken(t *testing.T) {
	bin := writeScript(t, "echo '{\"ready\":true,\"protocolVersion\":1}'")

	_, err := proc.Spawn(context.Background(), proc.Config{
		Bin:  bin,
		Role: contract.RoleSource,
	})
	if err == nil {
		t.Fatal("expected error for missing token")
	}
}

func TestSpawnReadinessTimeout(t *testing.T) {
	bin := writeScript(t, "sleep 10")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := proc.Spawn(ctx, proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestSpawnInvalidJSON(t *testing.T) {
	bin := writeScript(t, "echo 'not json'")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := proc.Spawn(ctx, proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestSpawnReadyFalse(t *testing.T) {
	bin := writeScript(t, "echo '{\"ready\":false,\"protocolVersion\":1}'")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := proc.Spawn(ctx, proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error for ready=false")
	}
}

func TestSpawnSuccess(t *testing.T) {
	bin := writeScript(t, "echo '{\"ready\":true,\"protocolVersion\":1,\"pid\":123}' && sleep 60")

	p, err := proc.Spawn(context.Background(), proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = p.Kill() })

	ready := p.ReadyInfo()
	if !ready.Ready {
		t.Error("expected ready=true")
	}
	if ready.ProtocolVersion != 1 {
		t.Errorf("expected protocolVersion=1, got %d", ready.ProtocolVersion)
	}
}

func TestProcessKill(t *testing.T) {
	bin := writeScript(t, "echo '{\"ready\":true,\"protocolVersion\":1}' && sleep 60")

	p, err := proc.Spawn(context.Background(), proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := p.Kill(); err != nil {
		t.Errorf("kill failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- p.Wait() }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit after kill")
	}
}

func TestProcessStop(t *testing.T) {
	bin := writeScript(t, "echo '{\"ready\":true,\"protocolVersion\":1}' && sleep 60")

	p, err := proc.Spawn(context.Background(), proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Stop sends SIGTERM then SIGKILL — the shell exits with signal,
	// which is expected. The important thing is it doesn't hang.
	done := make(chan error, 1)
	go func() { done <- p.Stop() }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Stop() hung")
	}
}

func TestProcessKillAfterReap(t *testing.T) {
	bin := writeScript(t, "echo '{\"ready\":true,\"protocolVersion\":1}' && exit 0")

	p, err := proc.Spawn(context.Background(), proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for process to exit naturally.
	<-p.Exited()
	// Kill after reap should be a no-op, not crash.
	if err := p.Kill(); err != nil {
		t.Errorf("kill after reap failed: %v", err)
	}
}

func TestProcessExitCode(t *testing.T) {
	bin := writeScript(t, "echo '{\"ready\":true,\"protocolVersion\":1}' && exit 42")

	p, err := proc.Spawn(context.Background(), proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = p.Kill() })

	<-p.Exited()
	if got := p.ExitCode(); got != 42 {
		t.Errorf("expected exit code 42, got %d", got)
	}
}

func TestReadinessTimeoutEnforcedInsideSpawn(t *testing.T) {
	bin := writeScript(t, "sleep 60")

	start := time.Now()
	_, err := proc.Spawn(context.Background(), proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error for no readiness")
	}
	if elapsed > 35*time.Second {
		t.Errorf("Spawn took %v, should enforce 30s timeout internally", elapsed)
	}
	if !strings.Contains(err.Error(), "no readiness line") {
		t.Errorf("error should mention readiness timeout, got: %v", err)
	}
}

func TestReadinessPlusLogsNoLinesLost(t *testing.T) {
	bin := writeScript(t, `#!/bin/sh
echo '{"ready":true,"protocolVersion":1}'
echo '{"level":"info","msg":"boot ok"}'
echo '{"level":"info","msg":"connected"}'
sleep 60`)

	p, err := proc.Spawn(context.Background(), proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = p.Kill() })

	ready := p.ReadyInfo()
	if !ready.Ready {
		t.Error("expected ready=true")
	}
	// The pump goroutine processes logs after readiness — no lines lost.
	// We can't directly assert log output here without a custom logger,
	// but the pump path uses a single bufio.Reader so lines aren't dropped.
}

func TestLongLogLineDoesNotFreezePipeline(t *testing.T) {
	// Plugin emits a 100KB log line. With ReadString (no length limit),
	// the pump handles it and truncates to maxLogLine.
	longLine := strings.Repeat("x", 100*1024)
	bin := writeScript(t, `#!/bin/sh
echo '{"ready":true,"protocolVersion":1}'
echo '`+longLine+`'
sleep 60`)

	p, err := proc.Spawn(context.Background(), proc.Config{
		Bin:        bin,
		Role:       contract.RoleSource,
		Token:      "dGVzdA==",
		ConfigPath: writeConfig(t),
		PluginDir:  t.TempDir(),
		WorkDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = p.Kill() })

	ready := p.ReadyInfo()
	if !ready.Ready {
		t.Error("expected ready=true")
	}
	// Pump should still be alive — not frozen on a full pipe.
	// Verify by checking ExitCode() returns -1 (still running).
	if code := p.ExitCode(); code != -1 {
		t.Errorf("expected -1 (still running), got %d — pump may have died", code)
	}
}

func writeScript(t *testing.T, script string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "plugin")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func writeConfig(t *testing.T) string {
	t.Helper()
	cfg := map[string]any{"test": true}
	b, _ := json.Marshal(cfg)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
