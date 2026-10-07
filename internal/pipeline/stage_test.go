package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/maltzsama/urutau/internal/plugin/contract"
)

// #276: roleFor must reject an unknown kind, not silently default to source.
func TestRoleForKnownAndUnknown(t *testing.T) {
	if r, err := roleFor(StageSource); err != nil || r != contract.RoleSource {
		t.Fatalf("source role = %v, %v; want RoleSource", r, err)
	}
	if r, err := roleFor(StageSink); err != nil || r != contract.RoleSink {
		t.Fatalf("sink role = %v, %v; want RoleSink", r, err)
	}
	if _, err := roleFor(StageKind("")); err == nil {
		t.Fatal("an empty stage kind must error, not default to source")
	}
	if _, err := roleFor(StageKind("transform")); err == nil {
		t.Fatal("an unknown stage kind must error")
	}
}

// Two stages must not share a work dir — otherwise an external source and sink
// collide on one socket. An empty WorkDir asks Spawn for a private temp dir,
// which Stop removes (issue #570).
func TestSpawnGivesEachStageItsOwnWorkDir(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "plugin")
	script := "#!/bin/sh\necho '{\"ready\":true,\"protocolVersion\":1}'\nsleep 5\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := StageConfig{Kind: StageSource, Bin: bin, Token: "dGVzdA=="}

	s1, err := Spawn(context.Background(), cfg)
	if err != nil {
		t.Fatalf("spawn 1: %v", err)
	}
	defer func() { _ = s1.Stop(context.Background()) }()
	s2, err := Spawn(context.Background(), cfg)
	if err != nil {
		t.Fatalf("spawn 2: %v", err)
	}
	defer func() { _ = s2.Stop(context.Background()) }()

	if s1.tempWorkDir == "" || s2.tempWorkDir == "" {
		t.Fatalf("empty WorkDir must yield a private temp dir: %q, %q", s1.tempWorkDir, s2.tempWorkDir)
	}
	if s1.tempWorkDir == s2.tempWorkDir {
		t.Fatal("two stages must not share a work dir (and thus a socket)")
	}

	dir := s1.tempWorkDir
	// A terminated process reports a non-nil wait error; only the dir removal
	// matters here.
	_ = s1.Stop(context.Background())
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("Stop must remove the private work dir: err=%v", err)
	}
}
