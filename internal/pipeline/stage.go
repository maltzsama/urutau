// Package pipeline defines the stage abstraction that bridges external
// plugin processes to the runner's source/sink interfaces. A Stage owns
// one subprocess lifecycle (spawn → connect → run → shutdown) and exposes
// the standard source.Source or sink.Sink contract.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/internal/plugin/proc"
)

// StageKind identifies whether a stage wraps a source or a sink plugin.
type StageKind string

const (
	StageSource StageKind = "source"
	StageSink   StageKind = "sink"
)

// StageConfig is the complete configuration for one plugin stage: the
// binary to spawn, auth credentials, and file paths.
type StageConfig struct {
	Kind       StageKind
	Bin        string // plugin binary path
	Token      string // auth token
	ConfigPath string // plugin config file
	PluginDir  string // plugin data directory
	WorkDir    string // working directory (sockets live here)
	Logger     *slog.Logger
}

// Stage couples a plugin process to its Flight client. It is the
// lifecycle owner: Spawn starts the binary, Connect dials the Flight
// service, and Close tears everything down in order.
//
// Death signals are distinct (issue #274):
//   - Exited() is the OS process's exit channel, valid immediately after
//     Spawn.
//   - Dead is the Flight client's unhealthy channel and DeadErr its reason;
//     both are set by Connect. Before Connect they are nil — select on
//     Exited() (not Dead) to detect a process that died before dialling.
type Stage struct {
	Cfg    StageConfig
	Proc   *proc.Process
	Client *client.Client
	Logger *slog.Logger
	// Dead is closed when the Flight client becomes unhealthy; set by
	// Connect. DeadErr returns its reason. Both are nil until Connect.
	Dead    <-chan struct{}
	DeadErr func() error
}

// Spawn starts the plugin subprocess and waits for readiness. The returned
// Stage's Dead/DeadErr are nil until Connect (issue #274); use Exited() to
// watch for a process that dies before Connect.
func Spawn(ctx context.Context, cfg StageConfig) (*Stage, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	role, err := roleFor(cfg.Kind)
	if err != nil {
		return nil, err
	}
	p, err := proc.Spawn(ctx, proc.Config{
		Bin:        cfg.Bin,
		Role:       role,
		Token:      cfg.Token,
		ConfigPath: cfg.ConfigPath,
		PluginDir:  cfg.PluginDir,
		WorkDir:    cfg.WorkDir,
		Logger:     cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	return &Stage{
		Cfg:    cfg,
		Proc:   p,
		Logger: cfg.Logger,
	}, nil
}

// Connect dials the Flight service on the running process.
func (s *Stage) Connect(ctx context.Context) error {
	c, err := client.Connect(ctx, s.Proc.Addr(), s.Cfg.Token, s.Logger)
	if err != nil {
		return err
	}
	s.Client = c
	s.Dead = c.Dead()
	s.DeadErr = c.DeadErr
	return nil
}

// Stop gracefully shuts down the stage: Flight shutdown, then process
// termination with a hard-kill fallback. It is safe to call after Spawn even
// if Connect failed (Client is nil) — proc.Stop is idempotent. The Flight
// shutdown error is propagated, not swallowed: a graceful shutdown whose
// plugin Shutdown never landed must not look like a clean stop (issue #275).
func (s *Stage) Stop(ctx context.Context) error {
	var shutdownErr error
	if s.Client != nil {
		shutdownErr = s.Client.Shutdown(ctx)
		_ = s.Client.Close()
	}
	return errors.Join(shutdownErr, s.Proc.Stop())
}

// Exited returns a channel closed when the plugin process exits. It is valid
// from Spawn (unlike Dead, which Connect sets).
func (s *Stage) Exited() <-chan struct{} { return s.Proc.Exited() }

// roleFor maps a stage kind to its plugin role. An unknown kind is a config
// error, not a code invariant: return it rather than defaulting to source or
// panicking (issue #276).
func roleFor(k StageKind) (contract.Role, error) {
	switch k {
	case StageSource:
		return contract.RoleSource, nil
	case StageSink:
		return contract.RoleSink, nil
	default:
		return "", fmt.Errorf("pipeline: unknown stage kind %q", k)
	}
}
