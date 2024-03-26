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
