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

	"vitess.io/vitess/go/vt/topo"
)

// TestKVCallsEndWithContext verifies that each Consul KV call returns when its ctx is done, even if the
// Consul server never answers, so that callers can bound a call that hangs.
func TestKVCallsEndWithContext(t *testing.T) {
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
	mp, err := s.NewLeaderParticipation("election", "id")
	require.NoError(t, err)

	tests := []struct {
		name string
		call func(ctx context.Context) error
	}{{
		name: "Create",
		call: func(ctx context.Context) error {
			_, err := s.Create(ctx, "file", []byte("contents"))
			return err
		},
	}, {
		name: "Update",
		call: func(ctx context.Context) error {
			_, err := s.Update(ctx, "file", []byte("contents"), nil)
			return err
		},
	}, {
		name: "Get",
		call: func(ctx context.Context) error {
			_, _, err := s.Get(ctx, "file")
			return err
		},
	}, {
		name: "List",
		call: func(ctx context.Context) error {
			_, err := s.List(ctx, "file")
			return err
		},
	}, {
		name: "Delete",
		call: func(ctx context.Context) error {
			return s.Delete(ctx, "file", nil)
		},
	}, {
		name: "ListDir",
		call: func(ctx context.Context) error {
			_, err := s.ListDir(ctx, "dir", false)
			return err
		},
	}, {
		name: "GetCurrentLeaderID",
		call: func(ctx context.Context) error {
			_, err := mp.GetCurrentLeaderID(ctx)
			return err
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			t.Cleanup(cancel)
			done := make(chan error, 1)
			go func() {
				done <- tt.call(ctx)
			}()
			require.Eventually(t, func() bool { return len(done) > 0 }, 30*time.Second, 10*time.Millisecond,
				"%s did not return after its ctx was done", tt.name)
			err := <-done
			require.True(t, topo.IsErrType(err, topo.Timeout), "expected a topo.Timeout error, got %v", err)
		})
	}
}
