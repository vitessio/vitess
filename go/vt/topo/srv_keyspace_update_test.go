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

package topo_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestUpdateSrvKeyspaceGossipConfig(t *testing.T) {
	ctx := t.Context()
	keyspace := "ks"
	cells := []string{"zone1", "zone2", "zone3"}
	ts := memorytopo.NewServer(ctx, cells...)
	defer ts.Close()

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
	for _, cell := range cells {
		require.NoError(t, ts.UpdateSrvKeyspace(ctx, cell, keyspace, &topodatapb.SrvKeyspace{}))
	}

	lockCtx, unlock, err := ts.LockKeyspace(ctx, keyspace, "Locking for tests")
	require.NoError(t, err)
	var unlockErr error
	defer unlock(&unlockErr)

	updatedCells, err := ts.UpdateSrvKeyspaceGossipConfig(lockCtx, keyspace, nil, func(*topodatapb.GossipConfig) *topodatapb.GossipConfig {
		return &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 5,
			PingInterval: "2s",
			MaxUpdateAge: "10s",
		}
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, cells, updatedCells)

	for _, cell := range cells {
		srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
		require.NoError(t, err)
		require.NotNil(t, srvKeyspace.GossipConfig)
		assert.True(t, srvKeyspace.GossipConfig.Enabled)
		assert.Equal(t, float64(5), srvKeyspace.GossipConfig.PhiThreshold)
		assert.Equal(t, "2s", srvKeyspace.GossipConfig.PingInterval)
		assert.Equal(t, "10s", srvKeyspace.GossipConfig.MaxUpdateAge)
	}
}

func TestUpdateSrvKeyspaceThrottlerConfig(t *testing.T) {
	ctx := t.Context()
	keyspace := "ks"
	cells := []string{"zone1", "zone2", "zone3"}
	ts := memorytopo.NewServer(ctx, cells...)
	defer ts.Close()

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
	for _, cell := range cells {
		require.NoError(t, ts.UpdateSrvKeyspace(ctx, cell, keyspace, &topodatapb.SrvKeyspace{}))
	}

	lockCtx, unlock, err := ts.LockKeyspace(ctx, keyspace, "Locking for tests")
	require.NoError(t, err)
	var unlockErr error
	defer unlock(&unlockErr)

	updatedCells, err := ts.UpdateSrvKeyspaceThrottlerConfig(lockCtx, keyspace, nil, func(*topodatapb.ThrottlerConfig) *topodatapb.ThrottlerConfig {
		return &topodatapb.ThrottlerConfig{
			Enabled:   true,
			Threshold: 1.5,
		}
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, cells, updatedCells)

	for _, cell := range cells {
		srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
		require.NoError(t, err)
		require.NotNil(t, srvKeyspace.ThrottlerConfig)
		assert.True(t, srvKeyspace.ThrottlerConfig.Enabled)
		assert.Equal(t, 1.5, srvKeyspace.ThrottlerConfig.Threshold)
	}
}
