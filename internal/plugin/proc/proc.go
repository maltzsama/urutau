// Package proc manages external plugin processes: spawn, readiness, orphan
// protection, and lifecycle.
package proc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/maltzsama/urutau/internal/plugin/contract"
)

type Ready struct {
	Ready           bool   `json:"ready"`
	ProtocolVersion int    `json:"protocolVersion"`
	PID             int    `json:"pid,omitempty"`
	Port            string `json:"port,omitempty"`
}

type Config struct {
	Bin        string
	Role       contract.Role
	Token      string
	ConfigPath string
	PluginDir  string
	WorkDir    string
	Logger     *slog.Logger
}

type Process struct {
	cfg    Config
	cmd    *exec.Cmd
	ready  Ready
	addr   string
	socket string
	mu     sync.Mutex
}

func Spawn(ctx context.Context, cfg Config) (*Process, error) {
	if cfg.Bin == "" {
		return nil, fmt.Errorf("plugin binary path required")
	}
	if cfg.Token == "" {
		return nil, fmt.Errorf("plugin token required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	useSocket := true
	socketPath := filepath.Join(cfg.WorkDir, "plugin.sock")
	bindAddr := ""

	if isWindows() {
		useSocket = false
		bindAddr = "127.0.0.1:0"
	}

	env := []string{
		fmt.Sprintf("URUTAU_STAGE=%s", cfg.Role),
		fmt.Sprintf("URUTAU_TOKEN=%s", cfg.Token),
		fmt.Sprintf("URUTAU_CONFIG=%s", cfg.ConfigPath),
		fmt.Sprintf("URUTAU_PLUGIN_DIR=%s", cfg.PluginDir),
		fmt.Sprintf("URUTAU_PROTOCOL_VERSION=%d", contract.ProtocolVersion),
	}
	if useSocket {
		env = append(env, fmt.Sprintf("URUTAU_SOCKET=%s", socketPath))
	} else {
		env = append(env, fmt.Sprintf("URUTAU_BIND=%s", bindAddr))
	}

	cmd := exec.CommandContext(ctx, cfg.Bin)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stderr = &logWriter{cfg.Logger}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start plugin: %w", err)
	}

	p := &Process{
		cfg: cfg,
		cmd: cmd,
	}

	ready, err := readReady(ctx, stdout)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("plugin readiness: %w", err)
	}
	p.ready = *ready

	if !isWindows() {
		p.socket = socketPath
		p.addr = socketPath
	} else {
		p.addr = bindAddr
	}

	// Drain remaining stdout in background
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
		}
	}()

	cfg.Logger.Info("plugin ready",
		"pid", ready.PID,
		"protocol", ready.ProtocolVersion,
		"addr", p.addr,
	)

	return p, nil
}

func readReady(ctx context.Context, r io.Reader) (*Ready, error) {
	type result struct {
		ready *Ready
		err   error
	}
	ch := make(chan result, 1)

	go func() {
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 8*1024), 8*1024)
		if !scanner.Scan() {
			ch <- result{err: fmt.Errorf("no readiness line from plugin")}
			return
		}
		line := scanner.Bytes()
		var ready Ready
		if err := json.Unmarshal(line, &ready); err != nil {
			ch <- result{err: fmt.Errorf("invalid readiness JSON: %w", err)}
			return
		}
		if !ready.Ready {
			ch <- result{err: fmt.Errorf("readiness line has ready=false")}
			return
		}
		ch <- result{ready: &ready}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		return r.ready, r.err
	}
}

func (p *Process) Addr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addr
}

func (p *Process) Socket() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.socket
}

func (p *Process) ReadyInfo() Ready {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ready
}

func (p *Process) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd.Process == nil {
		return nil
	}
	return p.cmd.Process.Kill()
}

func (p *Process) Wait() error {
	return p.cmd.Wait()
}

func (p *Process) ExitCode() int {
	if p.cmd.Process == nil {
		return -1
	}
	state := p.cmd.ProcessState
	if state == nil {
		return -1
	}
	return state.ExitCode()
}

func isWindows() bool {
	return filepath.VolumeName(`C:\`) != ""
}

type logWriter struct {
	logger *slog.Logger
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	w.logger.Error(string(p), "source", "plugin-stderr")
	return len(p), nil
}
