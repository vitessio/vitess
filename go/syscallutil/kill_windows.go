//go:build windows

package syscallutil

import (
	"errors"
	"syscall"
)

func Kill(pid int, signum syscall.Signal) (err error) {
	return errors.New("kill is not supported on windows")
}
