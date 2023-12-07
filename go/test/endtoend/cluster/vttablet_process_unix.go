//go:build !windows

package cluster

import "syscall"

// ToggleProfiling enables or disables the configured CPU profiler on this vttablet
func (vttablet *VttabletProcess) ToggleProfiling() error {
	return vttablet.proc.Process.Signal(syscall.SIGUSR1)
}
