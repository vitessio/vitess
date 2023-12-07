//go:build !windows

package syscallutil

import (
	"syscall"
)

func Kill(pid int, signum syscall.Signal) (err error) {
	return syscall.Kill(pid, signum)
}
