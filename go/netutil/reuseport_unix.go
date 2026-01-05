//go:build !windows

/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package netutil

import (
	"context"
	"fmt"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// ListenReusePort binds a host:port and sets SO_REUSEPORT on the listener.
// SO_REUSEPORT allows multiple processes to bind to the same port, enabling
// kernel-level load balancing of incoming connections.
//
// Requires Linux 3.9+ or equivalent kernel support.
func ListenReusePort(network, address string) (net.Listener, error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return net.Listen(network, address)
	}

	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
			if err != nil {
				return err
			}
			if opErr != nil {
				return fmt.Errorf("failed to set SO_REUSEPORT on %s %s: %w", network, address, opErr)
			}
			return nil
		},
	}

	return lc.Listen(context.Background(), network, address)
}
