package proc_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	defer os.Remove(bin)

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
	defer os.Remove(bin)

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
	defer os.Remove(bin)

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
	defer os.Remove(bin)

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
	defer os.Remove(bin)

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
	defer p.Kill()

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
	defer os.Remove(bin)

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

func writeScript(t *testing.T, script string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "plugin")
	content := "#!/bin/sh\n" + script + "\n"
	if err := os.WriteFile(bin, []byte(content), 0o755); err != nil {
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
