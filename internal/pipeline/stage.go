// Package pipeline defines the stage abstraction that bridges external
// plugin processes to the runner's source/sink interfaces. A Stage owns
// one subprocess lifecycle (spawn → connect → run → shutdown) and exposes
// the standard source.Source or sink.Sink contract.
package pipeline

import (
	"context"
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
type Stage struct {
	Cfg     StageConfig
	Proc    *proc.Process
	Client  *client.Client
	Logger  *slog.Logger
	Dead    <-chan struct{} // closed when the stage becomes unhealthy
	DeadErr func() error    // the reason for death
}

// Spawn starts the plugin subprocess and waits for readiness.
func Spawn(ctx context.Context, cfg StageConfig) (*Stage, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	p, err := proc.Spawn(ctx, proc.Config{
		Bin:        cfg.Bin,
		Role:       roleFor(cfg.Kind),
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
// termination with a hard-kill fallback.
func (s *Stage) Stop(ctx context.Context) error {
	if s.Client != nil {
		_ = s.Client.Shutdown(ctx)
		_ = s.Client.Close()
	}
	return s.Proc.Stop()
}

// Exited returns a channel closed when the plugin process exits.
func (s *Stage) Exited() <-chan struct{} { return s.Proc.Exited() }

func roleFor(k StageKind) contract.Role {
	if k == StageSink {
		return contract.RoleSink
	}
	return contract.RoleSource
}
