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
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestListenReusePort(t *testing.T) {
	l1, err := ListenReusePort("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l1.Close()

	// Bind to the same address. This should be possible with SO_REUSEPORT.
	addr := l1.Addr().String()
	l2, err := ListenReusePort("tcp", addr)
	require.NoError(t, err)
	defer l2.Close()

	tcpListener := l1.(*net.TCPListener)
	file, err := tcpListener.File()
	require.NoError(t, err)
	defer file.Close()

	val, err := unix.GetsockoptInt(int(file.Fd()), unix.SOL_SOCKET, unix.SO_REUSEPORT)
	require.NoError(t, err)
	require.Equalf(t, 1, val, "SO_REUSEPORT not set: got %d, want 1", val)
}
