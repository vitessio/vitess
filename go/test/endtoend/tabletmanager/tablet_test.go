/*
Copyright 2020 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// dbTestKeyspace is a keyspace whose database is never created, so that
// tablets serving it come up without one.
const dbTestKeyspace = "dbtest"

// newDBTestTablet starts one more tablet for the dbtest keyspace, creating the
// keyspace when it is the first one.
func newDBTestTablet(ctx context.Context, t *testing.T) *vitesst.Tablet {
	t.Helper()

	if clusterInstance.Keyspace(dbTestKeyspace) == nil {
		keyspace, err := clusterInstance.AddKeyspace(ctx, vitesst.WithKeyspace(dbTestKeyspace).
			WithShardNames("0").
			WithoutPrimaryElection())
		require.NoError(t, err)
		return keyspace.Shard("0").Tablets()[0]
	}

	tablet, err := clusterInstance.AddTablet(ctx, cell, dbTestKeyspace, "0", "replica")
	require.NoError(t, err)
	return tablet
}

// TestEnsureDB tests that vttablet creates the db as needed
func TestEnsureDB(t *testing.T) {
	ctx := t.Context()

	// Create new tablet. It won't be able to serve because there's no db.
	tablet := newDBTestTablet(ctx, t)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))

	// Make it the primary.
	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "TabletExternallyReparented", tablet.Alias())
	require.ErrorContains(t, err, "exit code 1")

	// It is still NOT_SERVING because the db is read-only.
	assert.Equal(t, "NOT_SERVING", tabletStatus(t, tablet))
	status := statusDetails(t, tablet)
	assert.Contains(t, status, "read-only")

	// Switch to read-write and verify that we go serving.
	// Note: for TabletExternallyReparented, we expect SetWritable to be called by the user
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "SetWritable", tablet.Alias(), "true")
	require.NoError(t, err)
	err = tablet.WaitForTabletStatus(ctx, vttabletStateTimeout, "SERVING")
	require.NoError(t, err)
	killTablets(ctx, tablet)
}

// TestGRPCErrorCode_UNAVAILABLE tests that vttablet returns correct gRPC codes,
// in this case codes.Unavailable/vtrpcpb.Code_UNAVAILABLE when mysqld is down.
func TestGRPCErrorCode_UNAVAILABLE(t *testing.T) {
	// Create new tablet. It won't be able to serve because there's no db.
	tablet := newDBTestTablet(t.Context(), t)
	defer killTablets(t.Context(), tablet)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))

	// kill the mysql process
	err := tablet.StopMySQL(t.Context())
	require.NoError(t, err)

	// confirm we get vtrpcpb.Code_UNAVAILABLE when calling FullStatus,
	// because this will try and fail to connect to mysql
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
	defer cancel()
	_, err = tmcFullStatus(ctx, tablet)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
}

// TestResetReplicationParameters tests that the RPC ResetReplicationParameters works as intended.
func TestResetReplicationParameters(t *testing.T) {
	ctx := t.Context()

	// Create new tablet. It won't be able to serve because there's no db.
	tablet := newDBTestTablet(ctx, t)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))

	// Set a replication source on the tablet and start replication
	for _, query := range []string{
		"stop replica",
		"change replication source to source_host = 'localhost', source_port = 123, get_source_public_key = 1",
		"start replica",
	} {
		_, err := tablet.QueryTabletWithDB(ctx, query, "")
		require.NoError(t, err)
	}

	// Check the replica status.
	res, err := tablet.QueryTabletWithDB(ctx, "show replica status", "")
	require.NoError(t, err)
	// This is expected to return 1 row result
	require.Len(t, res.Rows, 1)

	// Reset the replication parameters on the tablet
	err = tmcResetReplicationParameters(ctx, tablet)
	require.NoError(t, err)

	// Recheck the replica status and this time is should be empty
	res, err = tablet.QueryTabletWithDB(ctx, "show replica status", "")
	require.NoError(t, err)
	require.Len(t, res.Rows, 0)
}

// TestGetGlobalStatusVars tests the GetGlobalStatusVars RPC
func TestGetGlobalStatusVars(t *testing.T) {
	ctx := t.Context()
	statusValues, err := tmcGetGlobalStatusVars(ctx, replicaTablet, []string{"Innodb_buffer_pool_pages_data", "unknown_value"})
	require.NoError(t, err)
	require.Len(t, statusValues, 1)
	checkValueGreaterZero(t, statusValues, "Innodb_buffer_pool_pages_data")

	statusValues, err = tmcGetGlobalStatusVars(ctx, replicaTablet, []string{"Uptime", "Innodb_buffer_pool_pages_data"})
	require.NoError(t, err)
	require.Len(t, statusValues, 2)
	checkValueGreaterZero(t, statusValues, "Innodb_buffer_pool_pages_data")
	checkValueGreaterZero(t, statusValues, "Uptime")

	statusValues, err = tmcGetGlobalStatusVars(ctx, replicaTablet, nil)
	require.NoError(t, err)
	require.Greater(t, len(statusValues), 250)
	checkValueGreaterZero(t, statusValues, "Innodb_buffer_pool_pages_data")
	checkValueGreaterZero(t, statusValues, "Innodb_buffer_pool_pages_free")
	checkValueGreaterZero(t, statusValues, "Uptime")
}

// TestStopReplicationAndGetStatus tests the StopReplicationAndGetStatus RPC.
func TestStopReplicationAndGetStatus(t *testing.T) {
	// Create new tablet
	tablet, err := clusterInstance.AddTablet(t.Context(), cell, keyspaceName, shardName, "replica")
	require.NoError(t, err)
	defer killTablets(t.Context(), tablet)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))

	// Setup semi-sync on keyspace and wait for tablet to enable semi-sync.
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=semi_sync")
	require.NoError(t, err)
	defer func() {
		tmcStartReplication(t.Context(), tablet)
		clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy=none")
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
		defer cancel()
		resp, err := tmcFullStatus(ctx, tablet)
		require.NoError(c, err)
		require.True(c, resp.SemiSyncReplicaEnabled)
		require.True(c, resp.SemiSyncReplicaStatus)
	}, time.Second*45, time.Second)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
	defer cancel()
	resp, err := tmcStopReplicationAndGetStatus(ctx, tablet, replicationdatapb.StopReplicationMode_IOTHREADONLY)
	require.NoError(t, err)
	require.NotNil(t, resp.Before)
	require.True(t, resp.Before.SemiSyncReplicaEnabled)
	require.True(t, resp.Before.SemiSyncReplicaStatus)
	require.False(t, resp.Before.SemiSyncPrimaryEnabled)
	require.False(t, resp.Before.SemiSyncPrimaryStatus)
}

func checkValueGreaterZero(t *testing.T, statusValues map[string]string, val string) {
	valInMap, err := strconv.Atoi(statusValues[val])
	require.NoError(t, err)
	require.Greater(t, valInMap, 0)
}
