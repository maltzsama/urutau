//go:build linux

package proc

import (
	"os/exec"
	"syscall"
)

func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
		Setpgid:   true,
	}
}

func terminateGroup(pid int) error { return syscall.Kill(-pid, syscall.SIGTERM) }
func killGroup(pid int) error      { return syscall.Kill(-pid, syscall.SIGKILL) }
