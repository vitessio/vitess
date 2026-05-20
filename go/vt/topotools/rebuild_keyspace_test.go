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

package topotools

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/logutil"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestRebuildKeyspaceCopiesQueryThrottlerConfig verifies that RebuildKeyspace
// propagates Keyspace.QueryThrottlerConfig into each cell's SrvKeyspace.
// Without this, a serving-graph rebuild silently drops the QueryThrottlerConfig
// and tablets watching SrvKeyspace fall back to the default NoOp strategy.
func TestRebuildKeyspaceCopiesQueryThrottlerConfig(t *testing.T) {
	ctx := t.Context()
	cell := "zone1"
	keyspace := "test_keyspace"

	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	queryThrottlerConfig := &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		DryRun:   true,
	}

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{
		QueryThrottlerConfig: queryThrottlerConfig,
	}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, "0"))

	require.NoError(t, RebuildKeyspace(ctx, logutil.NewMemoryLogger(), ts, keyspace, []string{cell}, false))

	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)
	assert.True(t, proto.Equal(queryThrottlerConfig, srvKeyspace.GetQueryThrottlerConfig()),
		"SrvKeyspace.QueryThrottlerConfig mismatch: want %v, got %v", queryThrottlerConfig, srvKeyspace.GetQueryThrottlerConfig())
}
