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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"

	"vitess.io/vitess/go/netutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestDialDedicatedPool(t *testing.T) {
	ctx := context.Background()
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
		assert.NoError(t, err)
		assert.NotNil(t, invalidator)
		assert.NotNil(t, cli)
		_, invalidatorTwo, err := poolDialer.dialDedicatedPool(ctx, dialPoolGroupThrottler, tablet)
		assert.NoError(t, err)
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

		c := rpcClient.rpcDialPoolMap[dialPoolGroupThrottler][addr]
		assert.NotNil(t, c)
		assert.Contains(t, []connectivity.State{connectivity.Connecting, connectivity.TransientFailure}, c.cc.GetState())

		cachedTmc = c
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

func TestDialPool(t *testing.T) {
	ctx := context.Background()
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
		assert.NoError(t, err)
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

		rpcClient.mu.Lock()
		defer rpcClient.mu.Unlock()

		assert.NotEmpty(t, rpcClient.rpcDialPoolMap)
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupThrottler])
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupVTOrc])

		assert.NotEmpty(t, rpcClient.rpcClientMap)
		assert.NotEmpty(t, rpcClient.rpcClientMap[addr])

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

		rpcClient.mu.Lock()
		defer rpcClient.mu.Unlock()

		assert.NotEmpty(t, rpcClient.rpcDialPoolMap)
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupThrottler])
		assert.Empty(t, rpcClient.rpcDialPoolMap[dialPoolGroupVTOrc])

		// The default pools are unaffected. Invalidator does not run, connections are not closed.
		assert.NotEmpty(t, rpcClient.rpcClientMap)
		assert.NotEmpty(t, rpcClient.rpcClientMap[addr])

		assert.NotNil(t, cachedTmc)
		assert.Contains(t, []connectivity.State{connectivity.Connecting, connectivity.TransientFailure}, cachedTmc.cc.GetState())
	})
}
<<<<<<< HEAD
||||||| parent of 40cb03dd64 (grpctmclient: evict a failed dedicated-pool dial so the next call redials (#20414))

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
=======

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
>>>>>>> 40cb03dd64 (grpctmclient: evict a failed dedicated-pool dial so the next call redials (#20414))
