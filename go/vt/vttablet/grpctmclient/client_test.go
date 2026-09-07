/*
Copyright 2024 The Vitess Authors.

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

package grpctmclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/protoutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	tabletmanagerservicepb "vitess.io/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type executeMultiFetchClient struct {
	tabletmanagerservicepb.TabletManagerClient
	request *tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest
}

func (client *executeMultiFetchClient) ExecuteMultiFetchAsDba(
	ctx context.Context,
	request *tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest,
	opts ...grpc.CallOption,
) (*tabletmanagerdatapb.ExecuteMultiFetchAsDbaResponse, error) {
	client.request = request
	return &tabletmanagerdatapb.ExecuteMultiFetchAsDbaResponse{}, nil
}

type executeMultiFetchDialer struct {
	client tabletmanagerservicepb.TabletManagerClient
}

func (dialer *executeMultiFetchDialer) dial(
	ctx context.Context,
	tablet *topodatapb.Tablet,
) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	return dialer.client, executeMultiFetchCloser{}, nil
}

func (dialer *executeMultiFetchDialer) Close() {}

type executeMultiFetchCloser struct{}

func (executeMultiFetchCloser) Close() error {
	return nil
}

// TestExecuteMultiFetchAsDbaSessionVariables verifies the gRPC client forwards
// ordered session variable assignments.
func TestExecuteMultiFetchAsDbaSessionVariables(t *testing.T) {
	rpcClient := &executeMultiFetchClient{}
	client := &Client{dialer: &executeMultiFetchDialer{client: rpcClient}}
	tablet := &topodatapb.Tablet{Keyspace: "commerce"}
	sessionVariables := []*tabletmanagerdatapb.SessionVariable{
		{Name: "innodb_strict_mode", Value: "off"},
		{Name: "sql_mode", Value: "ANSI"},
	}

	_, err := client.ExecuteMultiFetchAsDba(
		t.Context(),
		tablet,
		false,
		&tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest{
			Sql:              []byte("create table t (id int primary key)"),
			SessionVariables: sessionVariables,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, rpcClient.request)
	assert.Equal(t, sessionVariables, rpcClient.request.SessionVariables)
}

func TestDialDedicatedPool(t *testing.T) {
	ctx := t.Context()
	client := NewClient()
	tablet := &topodatapb.Tablet{
		Hostname: "localhost",
		PortMap: map[string]int32{
			"grpc": 15991,
		},
	}
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	t.Run("dialPool", func(t *testing.T) {
		poolDialer, ok := client.dialer.(poolDialer)
		require.True(t, ok)

		cli, invalidator, err := poolDialer.dialDedicatedPool(ctx, dialPoolGroupThrottler, tablet)
		require.NoError(t, err)
		assert.NotNil(t, invalidator)
		assert.NotNil(t, cli)
		_, invalidatorTwo, err := poolDialer.dialDedicatedPool(ctx, dialPoolGroupThrottler, tablet)
		require.NoError(t, err)
		// Ensure that running both the invalidators doesn't cause any issues.
		invalidator()
		invalidatorTwo()
		_, _, err = poolDialer.dialDedicatedPool(ctx, dialPoolGroupThrottler, tablet)
		assert.NoError(t, err)
	})

	var cachedTmc *tmc
	t.Run("maps", func(t *testing.T) {
		rpcClient, ok := client.dialer.(*grpcClient)
		require.True(t, ok)
		assert.NotEmpty(t, rpcClient.rpcDialPoolMap)
		assert.NotEmpty(t, rpcClient.rpcDialPoolMap[dialPoolGroupThrottler])
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupVTOrc])

		entry := rpcClient.rpcDialPoolMap[dialPoolGroupThrottler][addr]
		assert.NotNil(t, entry)
		assert.NotNil(t, entry.tmc)
		assert.Contains(t, []connectivity.State{connectivity.Connecting, connectivity.TransientFailure}, entry.tmc.cc.GetState())

		cachedTmc = entry.tmc
	})

	t.Run("CheckThrottler", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		req := &tabletmanagerdatapb.CheckThrottlerRequest{}
		_, err := client.CheckThrottler(ctx, tablet, req)
		assert.Error(t, err)
	})
	t.Run("GetThrottlerStatus", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		req := &tabletmanagerdatapb.GetThrottlerStatusRequest{}
		_, err := client.GetThrottlerStatus(ctx, tablet, req)
		assert.Error(t, err)
	})
	t.Run("empty map", func(t *testing.T) {
		rpcClient, ok := client.dialer.(*grpcClient)
		require.True(t, ok)
		assert.NotEmpty(t, rpcClient.rpcDialPoolMap)
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupThrottler])
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupVTOrc])

		assert.Equal(t, connectivity.Shutdown, cachedTmc.cc.GetState())
	})
}

// TestCloseShardHealthPool verifies that CloseShardHealthPool releases the pooled ping connections
// (so the shard-health monitor does not leak its connection to the primary on stop) while leaving
// other dial-pool groups untouched.
func TestCloseShardHealthPool(t *testing.T) {
	ctx := t.Context()
	client := NewClient()
	pingTablet := &topodatapb.Tablet{Hostname: "localhost", PortMap: map[string]int32{"grpc": 15994}}
	throttlerTablet := &topodatapb.Tablet{Hostname: "localhost", PortMap: map[string]int32{"grpc": 15995}}

	poolDialer, ok := client.dialer.(poolDialer)
	require.True(t, ok)
	rpcClient, ok := client.dialer.(*grpcClient)
	require.True(t, ok)

	// Open a pooled connection in the ping group (what the monitor uses) and one in another group.
	_, _, err := poolDialer.dialDedicatedPool(ctx, dialPoolGroupPing, pingTablet)
	require.NoError(t, err)
	_, _, err = poolDialer.dialDedicatedPool(ctx, dialPoolGroupThrottler, throttlerTablet)
	require.NoError(t, err)

	rpcClient.rpcDialPoolMapMu.Lock()
	require.NotEmpty(t, rpcClient.rpcDialPoolMap[dialPoolGroupPing])
	rpcClient.rpcDialPoolMapMu.Unlock()

	// Open a second pooled ping connection, then evict just the first tablet's
	// entry: the second entry and other groups must be left intact.
	otherPingTablet := &topodatapb.Tablet{Hostname: "localhost", PortMap: map[string]int32{"grpc": 15996}}
	_, _, err = poolDialer.dialDedicatedPool(ctx, dialPoolGroupPing, pingTablet)
	require.NoError(t, err)
	_, _, err = poolDialer.dialDedicatedPool(ctx, dialPoolGroupPing, otherPingTablet)
	require.NoError(t, err)

	client.CloseShardHealthPoolEntry(pingTablet)

	rpcClient.rpcDialPoolMapMu.Lock()
	assert.Len(t, rpcClient.rpcDialPoolMap[dialPoolGroupPing], 1, "only the evicted tablet's ping pool entry may be removed")
	rpcClient.rpcDialPoolMapMu.Unlock()
	// Evicting an address with no pool entry is a no-op.
	client.CloseShardHealthPoolEntry(pingTablet)

	// Eviction is best-effort: nil or partially-populated tablet records (no
	// address to construct) must be a no-op rather than a panic, and must not
	// disturb the remaining pool entry.
	client.CloseShardHealthPoolEntry(nil)
	client.CloseShardHealthPoolEntry(&topodatapb.Tablet{PortMap: map[string]int32{"grpc": 15996}})
	client.CloseShardHealthPoolEntry(&topodatapb.Tablet{Hostname: "localhost"})
	client.CloseShardHealthPoolEntry(&topodatapb.Tablet{Hostname: "localhost", PortMap: map[string]int32{"grpc": -1}})
	rpcClient.rpcDialPoolMapMu.Lock()
	assert.Len(t, rpcClient.rpcDialPoolMap[dialPoolGroupPing], 1, "invalid tablet records must not evict anything")
	rpcClient.rpcDialPoolMapMu.Unlock()

	client.CloseShardHealthPool()

	rpcClient.rpcDialPoolMapMu.Lock()
	defer rpcClient.rpcDialPoolMapMu.Unlock()
	assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupPing], "ping pool must be closed and emptied")
	assert.NotEmpty(t, rpcClient.rpcDialPoolMap[dialPoolGroupThrottler], "other dial-pool groups must be left intact")
}

// TestShouldInvalidatePooledConn verifies that only connection-level failures invalidate a pooled
// conn. A DeadlineExceeded/Canceled (a slow but alive peer, or a cancelled probe) must keep the
// conn so a momentary stall does not close + redial the pool; an Unavailable (or any other error)
// still invalidates so a genuinely dead peer redials.
func TestShouldInvalidatePooledConn(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"deadline exceeded keeps the conn", status.Error(codes.DeadlineExceeded, "timed out"), false},
		{"canceled keeps the conn", status.Error(codes.Canceled, "canceled"), false},
		{"unavailable invalidates", status.Error(codes.Unavailable, "connection refused"), true},
		{"non-status error invalidates", errors.New("boom"), true},
		// Raw and wrapped context errors are not gRPC status errors (they map
		// to codes.Unknown) but indicate caller-side shutdown or timeout, not
		// a broken connection: they must keep the conn.
		{"raw context.Canceled keeps the conn", context.Canceled, false},
		{"raw context.DeadlineExceeded keeps the conn", context.DeadlineExceeded, false},
		{"wrapped context.Canceled keeps the conn", fmt.Errorf("ping: %w", context.Canceled), false},
		{"wrapped context.DeadlineExceeded keeps the conn", fmt.Errorf("ping: %w", context.DeadlineExceeded), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldInvalidatePooledConn(tt.err))
		})
	}
}

// TestDialDedicatedPoolInvalidatorOnlyDeletesOwnEntry verifies a pooled-conn invalidator deletes
// only the entry it was created for. If a concurrent caller has already replaced that addr's entry
// with a fresh one, the stale invalidator must leave the new entry in place — otherwise it removes
// a pooled connection it never owned, orphaning that conn (only its own was closed).
func TestDialDedicatedPoolInvalidatorOnlyDeletesOwnEntry(t *testing.T) {
	ctx := t.Context()
	client := NewClient()
	tablet := &topodatapb.Tablet{
		Hostname: "localhost",
		PortMap:  map[string]int32{"grpc": 15993},
	}
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))

	rpcClient, ok := client.dialer.(*grpcClient)
	require.True(t, ok)
	poolDialer, ok := client.dialer.(poolDialer)
	require.True(t, ok)

	// Dial entry A and grab its invalidator.
	_, invalidatorA, err := poolDialer.dialDedicatedPool(ctx, dialPoolGroupPing, tablet)
	require.NoError(t, err)
	require.NotNil(t, invalidatorA)

	// Simulate a concurrent caller having replaced the addr's entry with a fresh entry B.
	entryB := &tmcEntry{}
	rpcClient.rpcDialPoolMapMu.Lock()
	rpcClient.rpcDialPoolMap[dialPoolGroupPing][addr] = entryB
	rpcClient.rpcDialPoolMapMu.Unlock()

	// A's invalidator must NOT delete B — different identity.
	invalidatorA()

	rpcClient.rpcDialPoolMapMu.Lock()
	got := rpcClient.rpcDialPoolMap[dialPoolGroupPing][addr]
	rpcClient.rpcDialPoolMapMu.Unlock()
	assert.Same(t, entryB, got, "invalidator must not delete a concurrently-installed entry it does not own")
}

func TestDialPool(t *testing.T) {
	ctx := t.Context()
	client := NewClient()
	tablet := &topodatapb.Tablet{
		Hostname: "localhost",
		PortMap: map[string]int32{
			"grpc": 15991,
		},
	}
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
	t.Run("dialPool", func(t *testing.T) {
		poolDialer, ok := client.dialer.(poolDialer)
		require.True(t, ok)

		cli, err := poolDialer.dialPool(ctx, tablet)
		require.NoError(t, err)
		assert.NotNil(t, cli)
	})

	var cachedTmc *tmc
	t.Run("maps", func(t *testing.T) {
		rpcClient, ok := client.dialer.(*grpcClient)
		require.True(t, ok)
		assert.Empty(t, rpcClient.rpcDialPoolMap)
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupThrottler])
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupVTOrc])

		assert.NotEmpty(t, rpcClient.rpcClientMap)
		assert.NotEmpty(t, rpcClient.rpcClientMap[addr])

		ch := rpcClient.rpcClientMap[addr]
		cachedTmc = <-ch
		ch <- cachedTmc

		assert.NotNil(t, cachedTmc)
		assert.Contains(t, []connectivity.State{connectivity.Connecting, connectivity.TransientFailure}, cachedTmc.cc.GetState())
	})

	t.Run("CheckThrottler", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		req := &tabletmanagerdatapb.CheckThrottlerRequest{}
		_, err := client.CheckThrottler(ctx, tablet, req)
		assert.Error(t, err)
	})

	t.Run("post throttler maps", func(t *testing.T) {
		rpcClient, ok := client.dialer.(*grpcClient)
		require.True(t, ok)

		func() {
			rpcClient.rpcDialPoolMapMu.Lock()
			defer rpcClient.rpcDialPoolMapMu.Unlock()

			assert.NotEmpty(t, rpcClient.rpcDialPoolMap)
			assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupThrottler])
			assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupVTOrc])
		}()

		func() {
			rpcClient.rpcClientMapMu.Lock()
			defer rpcClient.rpcClientMapMu.Unlock()

			assert.NotEmpty(t, rpcClient.rpcClientMap)
			assert.NotEmpty(t, rpcClient.rpcClientMap[addr])
		}()

		assert.Contains(t, []connectivity.State{connectivity.Connecting, connectivity.TransientFailure}, cachedTmc.cc.GetState())
	})

	t.Run("ExecuteFetchAsDba", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		req := &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{}
		_, err := client.ExecuteFetchAsDba(ctx, tablet, true, req)
		assert.Error(t, err)
	})

	t.Run("post ExecuteFetchAsDba maps", func(t *testing.T) {
		rpcClient, ok := client.dialer.(*grpcClient)
		require.True(t, ok)

		func() {
			rpcClient.rpcDialPoolMapMu.Lock()
			defer rpcClient.rpcDialPoolMapMu.Unlock()

			assert.NotEmpty(t, rpcClient.rpcDialPoolMap)
			assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupThrottler])
			assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupVTOrc])
		}()

		func() {
			rpcClient.rpcClientMapMu.Lock()
			defer rpcClient.rpcClientMapMu.Unlock()
			// The default pools are unaffected. Invalidator does not run, connections are not closed.
			assert.NotEmpty(t, rpcClient.rpcClientMap)
			assert.NotEmpty(t, rpcClient.rpcClientMap[addr])
		}()

		assert.NotNil(t, cachedTmc)
		assert.Contains(t, []connectivity.State{connectivity.Connecting, connectivity.TransientFailure}, cachedTmc.cc.GetState())
	})
}

func TestValidateTablet(t *testing.T) {
	t.Parallel()

	t.Run("valid", func(t *testing.T) {
		tablet := &topodatapb.Tablet{
			Hostname: t.Name(),
			PortMap: map[string]int32{
				"grpc": 12345,
			},
		}
		require.NoError(t, validateTablet(tablet))
	})

	t.Run("is shutdown", func(t *testing.T) {
		tablet := &topodatapb.Tablet{
			TabletShutdownTime: protoutil.TimeToProto(time.Now()),
		}
		require.ErrorContains(t, validateTablet(tablet), "tablet is shutdown")
	})

	t.Run("invalid - empty Hostname", func(t *testing.T) {
		tablet := &topodatapb.Tablet{
			Hostname: "",
		}
		require.ErrorContains(t, validateTablet(tablet), "empty tablet hostname")
	})

	t.Run("invalid - nil PortMap", func(t *testing.T) {
		tablet := &topodatapb.Tablet{
			Hostname: t.Name(),
			PortMap:  nil,
		}
		require.ErrorContains(t, validateTablet(tablet), "no tablet port map")
	})

	t.Run("invalid - bad port", func(t *testing.T) {
		tablet := &topodatapb.Tablet{
			Hostname: t.Name(),
			PortMap: map[string]int32{
				"grpc": 0,
			},
		}
		require.Error(t, validateTablet(tablet), "invalid tablet grpc port")
	})
}

// TestDialDedicatedPoolEvictsFailedEntry verifies that a pooled dial that failed
// to initialize is not cached forever. Each dedicated-pool entry is guarded by a
// sync.Once, so a first createTmc failure would otherwise make every subsequent
// dialDedicatedPool for that address return the same error without ever redialing,
// permanently marking a reachable tablet as down until the client is recreated.
// The failed entry must be evicted so the next dial reconnects.
func TestDialDedicatedPoolEvictsFailedEntry(t *testing.T) {
	ctx := t.Context()
	client := NewClient()
	tablet := &topodatapb.Tablet{
		Hostname: "localhost",
		PortMap:  map[string]int32{"grpc": 15992},
	}
	addr := netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))

	rpcClient, ok := client.dialer.(*grpcClient)
	require.True(t, ok)
	poolDialer, ok := client.dialer.(poolDialer)
	require.True(t, ok)

	// Simulate a first dial that failed: install an entry whose once has already
	// fired with an error, exactly as a createTmc failure would leave it.
	failed := &tmcEntry{}
	failed.once.Do(func() { failed.err = errors.New("simulated dial failure") })
	rpcClient.rpcDialPoolMapMu.Lock()
	rpcClient.rpcDialPoolMap = map[DialPoolGroup]addrTmcMap{
		dialPoolGroupThrottler: {addr: failed},
	}
	rpcClient.rpcDialPoolMapMu.Unlock()

	// A dial that finds the failed entry must surface the error AND evict it.
	_, _, err := poolDialer.dialDedicatedPool(ctx, dialPoolGroupThrottler, tablet)
	require.ErrorContains(t, err, "simulated dial failure")

	rpcClient.rpcDialPoolMapMu.Lock()
	_, stillCached := rpcClient.rpcDialPoolMap[dialPoolGroupThrottler][addr]
	rpcClient.rpcDialPoolMapMu.Unlock()
	assert.False(t, stillCached, "a failed pooled-dial entry must be evicted so the next dial redials")

	// The next dial redials and succeeds (grpc dials lazily), proving the cached
	// failure no longer permanently blocks recovery.
	cli, invalidator, err := poolDialer.dialDedicatedPool(ctx, dialPoolGroupThrottler, tablet)
	require.NoError(t, err)
	assert.NotNil(t, cli)
	assert.NotNil(t, invalidator)
}
