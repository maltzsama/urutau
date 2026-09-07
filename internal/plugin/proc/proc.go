// Package proc manages external plugin processes: spawn, readiness, log
// pumping, and lifecycle. Platform behavior lives in sys_linux.go,
// sys_darwin.go, and sys_windows.go.
package proc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maltzsama/urutau/internal/plugin/contract"
)

const (
	readinessTimeout = 30 * time.Second
	hardStopGrace    = 10 * time.Second
	maxLogLine       = 16 * 1024
)

type Ready struct {
	Ready           bool `json:"ready"`
	ProtocolVersion int  `json:"protocolVersion"`
	PID             int  `json:"pid,omitempty"`
	Port            int  `json:"port,omitempty"` // contract: integer, TCP mode only
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
	reaped bool

	waitErr error
	exited  chan struct{}
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
	if err := os.MkdirAll(cfg.WorkDir, 0o700); err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}

	useTCP := runtime.GOOS == "windows"
	socketPath := filepath.Join(cfg.WorkDir, "plugin.sock")
	if !useTCP && len(socketPath) > 100 {
		return nil, fmt.Errorf("socket path too long (%d bytes): %s", len(socketPath), socketPath)
	}

	env := append(os.Environ(),
		fmt.Sprintf("URUTAU_STAGE=%s", cfg.Role),
		"URUTAU_TOKEN="+cfg.Token,
		"URUTAU_CONFIG="+cfg.ConfigPath,
		"URUTAU_PLUGIN_DIR="+cfg.PluginDir,
		fmt.Sprintf("URUTAU_PROTOCOL_VERSION=%d", contract.ProtocolVersion),
	)
	if useTCP {
		env = append(env, "URUTAU_BIND=127.0.0.1:0")
	} else {
		env = append(env, "URUTAU_SOCKET="+socketPath)
	}

	cmd := exec.Command(cfg.Bin)
	cmd.Env = env
	setSysProcAttr(cmd)

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		stdoutR.Close()
		stdoutW.Close()
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	if err := cmd.Start(); err != nil {
		stdoutR.Close()
		stdoutW.Close()
		stderrR.Close()
		stderrW.Close()
		return nil, fmt.Errorf("start plugin: %w", err)
	}
	stdoutW.Close()
	stderrW.Close()

	p := &Process{cfg: cfg, cmd: cmd, exited: make(chan struct{})}

	readyCh := make(chan readyResult, 1)
	go pump(stdoutR, cfg.Logger, "stdout", readyCh)
	go pump(stderrR, cfg.Logger, "stderr", nil)

	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		p.reaped = true
		p.mu.Unlock()
		p.waitErr = err
		close(p.exited)
	}()

	if err := waitReady(ctx, readyCh, &p.ready); err != nil {
		p.Kill()
		<-p.exited
		return nil, fmt.Errorf("plugin startup: %w", err)
	}
	if p.ready.ProtocolVersion != contract.ProtocolVersion {
		p.Kill()
		<-p.exited
		return nil, fmt.Errorf("plugin speaks protocol v%d, urutau speaks v%d",
			p.ready.ProtocolVersion, contract.ProtocolVersion)
	}

	if useTCP {
		if p.ready.Port <= 0 {
			p.Kill()
			<-p.exited
			return nil, fmt.Errorf("TCP mode: plugin did not report a port")
		}
		p.addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(p.ready.Port))
	} else {
		p.socket = socketPath
		p.addr = socketPath
	}

	cfg.Logger.Info("plugin ready",
		"pid", p.cmd.Process.Pid,
		"addr", p.addr,
		"protocol", p.ready.ProtocolVersion,
	)
	return p, nil
}

type readyResult struct {
	ready Ready
	err   error
}

func waitReady(ctx context.Context, ch <-chan readyResult, out *Ready) error {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return fmt.Errorf("no readiness line within %s: %w", readinessTimeout, ctx.Err())
	case res := <-ch:
		if res.err != nil {
			return res.err
		}
		*out = res.ready
		return nil
	}
}

func pump(r io.ReadCloser, logger *slog.Logger, stream string, readyCh chan<- readyResult) {
	defer r.Close()
	br := bufio.NewReaderSize(r, 64*1024)
	pending := readyCh != nil
	for {
		line, err := readLine(br)
		if line != "" {
			if pending {
				pending = false
				readyCh <- parseReady(line)
				continue
			}
			if len(line) > maxLogLine {
				line = line[:maxLogLine] + "…(truncated)"
			}
			logger.Info(line, "stream", stream)
		}
		if err != nil {
			if pending {
				readyCh <- readyResult{err: fmt.Errorf("stdout closed before readiness line: %w", err)}
			}
			return
		}
	}
}

func parseReady(line string) readyResult {
	if len(line) > 8*1024 {
		return readyResult{err: fmt.Errorf("readiness line exceeds 8KB")}
	}
	var r Ready
	if err := json.Unmarshal([]byte(line), &r); err != nil {
		return readyResult{err: fmt.Errorf("invalid readiness JSON (%q): %w", line, err)}
	}
	if !r.Ready {
		return readyResult{err: fmt.Errorf("readiness line has ready=false")}
	}
	return readyResult{ready: r}
}

func readLine(br *bufio.Reader) (string, error) {
	s, err := br.ReadString('\n')
	return strings.TrimRight(s, "\r\n"), err
}

func (p *Process) Addr() string      { return p.addr }
func (p *Process) Socket() string    { return p.socket }
func (p *Process) ReadyInfo() Ready  { return p.ready }

// Exited returns a channel that is closed when the process exits.
func (p *Process) Exited() <-chan struct{} { return p.exited }

func (p *Process) Wait() error {
	<-p.exited
	return p.waitErr
}

func (p *Process) ExitCode() int {
	select {
	case <-p.exited:
	default:
		return -1
	}
	if ee, ok := p.waitErr.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	if p.waitErr != nil {
		return -1
	}
	return 0
}

func (p *Process) Terminate() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reaped {
		return nil
	}
	return terminateGroup(p.cmd.Process.Pid)
}

func (p *Process) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reaped {
		return nil
	}
	return killGroup(p.cmd.Process.Pid)
}

func (p *Process) Stop() error {
	p.Terminate()
	select {
	case <-p.exited:
	case <-time.After(hardStopGrace):
		p.Kill()
	}
	return p.Wait()
}
