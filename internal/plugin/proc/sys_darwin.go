//go:build darwin

package proc

import (
	"os/exec"
	"syscall"
)

func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateGroup(pid int) error { return syscall.Kill(-pid, syscall.SIGTERM) }
func killGroup(pid int) error      { return syscall.Kill(-pid, syscall.SIGKILL) }
