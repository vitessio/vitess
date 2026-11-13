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
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

// kvCall is one Consul KV call of a Server, for table tests over all of them.
type kvCall struct {
	name string
	call func(ctx context.Context) error
}

// kvCalls returns every KV call of s that passes its ctx to Consul.
func kvCalls(t *testing.T, s *Server) []kvCall {
	mp, err := s.NewLeaderParticipation("election", "id")
	require.NoError(t, err)
	return []kvCall{{
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
}

// TestKVCallsEndWithContext verifies that each Consul KV call returns when its ctx is done, even if the
// Consul server never answers, so that callers can bound a call that hangs.
func TestKVCallsEndWithContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// Accept connections and never answer on them.
	var conns []net.Conn
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conns = append(conns, conn)
		}
	}()
	t.Cleanup(func() {
		// Closing the listener makes Accept fail; wait for the loop to exit before closing what it accepted.
		listener.Close()
		<-accepted
		for _, conn := range conns {
			conn.Close()
		}
	})

	s, err := NewServer("cell", listener.Addr().String(), "root")
	require.NoError(t, err)
	t.Cleanup(s.Close)

	for _, tt := range kvCalls(t, s) {
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

// TestKVCallsKeepDialTimeoutUnderLiveContext verifies that when dialing the Consul server times out while the
// caller's ctx is still live, each KV call returns the transport error rather than a topo.Timeout. A dial
// timeout also matches context.DeadlineExceeded, and srvtopo drops the values it has cached once a watch
// fails with a topo error past the cache TTL, so a vtgate whose packets to Consul are dropped would otherwise
// stop serving SrvKeyspace and SrvVSchema instead of keeping its last known values.
func TestKVCallsKeepDialTimeoutUnderLiveContext(t *testing.T) {
	timeoutErr := dialTimeoutError(t)
	cfg := api.DefaultConfig()
	cfg.Address = "127.0.0.1:1"
	cfg.Transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		return nil, timeoutErr
	}
	client, err := api.NewClient(cfg)
	require.NoError(t, err)
	s := &Server{
		client: client,
		kv:     client.KV(),
		root:   "root",
		locks:  make(map[string]*lockInstance),
	}
	t.Cleanup(s.Close)

	for _, tt := range kvCalls(t, s) {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(t.Context())
			require.Error(t, err)
			var opErr *net.OpError
			require.ErrorAs(t, err, &opErr, "expected the dial error, got %v", err)
			assert.True(t, opErr.Timeout())
			assert.False(t, topo.IsErrType(err, topo.Timeout), "a dial timeout under a live ctx became a topo.Timeout: %v", err)
		})
	}
}

// dialTimeoutError returns the error that net returns when a dial times out, by dialing a listener with a ctx
// whose deadline has passed. net reports that the same way as a Dialer whose Timeout ran out.
func dialTimeoutError(t *testing.T) error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	expired, cancel := context.WithDeadline(t.Context(), time.Now())
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(expired, "tcp", listener.Addr().String())
	if err == nil {
		conn.Close()
	}
	require.Error(t, err, "dialing with an expired ctx did not fail")
	return err
}
