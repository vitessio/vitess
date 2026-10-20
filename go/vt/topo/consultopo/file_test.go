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

package consultopo

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGetEndsWithContext verifies that Get returns when its ctx is done, even if the Consul server never
// answers, so that callers can bound a read that hangs.
func TestGetEndsWithContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// Accept connections and never answer on them.
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	})

	s, err := NewServer("cell", listener.Addr().String(), "root")
	require.NoError(t, err)
	t.Cleanup(s.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := s.Get(ctx, "file")
		done <- err
	}()
	require.Eventually(t, func() bool { return len(done) > 0 }, 30*time.Second, 10*time.Millisecond,
		"Get did not return after its ctx was done")
	require.ErrorIs(t, <-done, context.DeadlineExceeded)
}
