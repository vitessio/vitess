//go:build windows

package cluster

import (
	"errors"
)

// ToggleProfiling enables or disables the configured CPU profiler on this vttablet
func (vttablet *VttabletProcess) ToggleProfiling() error {
	return errors.New("not implemented")
}
