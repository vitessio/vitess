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

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestEnsureDB tests that vttablet creates the db as needed
func TestEnsureDB(t *testing.T) {
	// Create new tablet
	tablet := clusterInstance.NewVttabletInstance("replica", 0, "")
	mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	require.NoError(t, err)

	tablet.MysqlctlProcess = *mysqlctlProcess
	err = tablet.MysqlctlProcess.Start()
	require.NoError(t, err)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))
	// Start vttablet process as replica. It won't be able to serve because there's no db.
	err = clusterInstance.StartVttablet(tablet, false, "NOT_SERVING", false, cell, "dbtest", hostname, "0")
	require.NoError(t, err)

	// Make it the primary.
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("TabletExternallyReparented", tablet.Alias)
	require.EqualError(t, err, "exit status 1")

	// It is still NOT_SERVING because the db is read-only.
	assert.Equal(t, "NOT_SERVING", tablet.VttabletProcess.GetTabletStatus())
	status := tablet.VttabletProcess.GetStatusDetails()
	assert.Contains(t, status, "read-only")

	// Switch to read-write and verify that we go serving.
	// Note: for TabletExternallyReparented, we expect SetWritable to be called by the user
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("SetWritable", tablet.Alias, "true")
	require.NoError(t, err)
	err = tablet.VttabletProcess.WaitForTabletStatus("SERVING")
	require.NoError(t, err)
	killTablets(tablet)
}

// TestGRPCErrorCode_UNAVAILABLE tests that vttablet returns correct gRPC codes,
// in this case codes.Unavailable/vtrpcpb.Code_UNAVAILABLE when mysqld is down.
func TestGRPCErrorCode_UNAVAILABLE(t *testing.T) {
	// Create new tablet
	tablet := clusterInstance.NewVttabletInstance("replica", 0, "")
	defer killTablets(tablet)
	mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	require.NoError(t, err)

	tablet.MysqlctlProcess = *mysqlctlProcess
	err = tablet.MysqlctlProcess.Start()
	require.NoError(t, err)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))
	// Start vttablet process as replica. It won't be able to serve because there's no db.
	err = clusterInstance.StartVttablet(tablet, false, "SERVING", false, cell, "dbtest", hostname, "0")
	require.NoError(t, err)

	// kill the mysql process
	err = tablet.MysqlctlProcess.Stop()
	require.NoError(t, err)

	// confirm we get vtrpcpb.Code_UNAVAILABLE when calling FullStatus,
	// because this will try and fail to connect to mysql
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tmClient := tmc.NewClient()
	vttablet := getTablet(tablet.GrpcPort)
	_, err = tmClient.FullStatus(ctx, vttablet)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
}

// TestResetReplicationParameters tests that the RPC ResetReplicationParameters works as intended.
func TestResetReplicationParameters(t *testing.T) {
	// Create new tablet
	tablet := clusterInstance.NewVttabletInstance("replica", 0, "")
	mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	require.NoError(t, err)
	tablet.MysqlctlProcess = *mysqlctlProcess
	err = tablet.MysqlctlProcess.Start()
	require.NoError(t, err)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))
	// Start vttablet process as replica. It won't be able to serve because there's no db.
	err = clusterInstance.StartVttablet(tablet, false, "NOT_SERVING", false, cell, "dbtest", hostname, "0")
	require.NoError(t, err)

	// Set a replication source on the tablet and start replication
	err = tablet.VttabletProcess.QueryTabletMultiple([]string{"stop replica", "change replication source to source_host = 'localhost', source_port = 123, get_source_public_key = 1", "start replica"}, keyspaceName, false)
	require.NoError(t, err)

	// Check the replica status.
	res, err := tablet.VttabletProcess.QueryTablet("show replica status", keyspaceName, false)
	require.NoError(t, err)
	// This is expected to return 1 row result
	require.Len(t, res.Rows, 1)

	// Reset the replication parameters on the tablet
	err = tmcResetReplicationParameters(context.Background(), tablet.GrpcPort)
	require.NoError(t, err)

	// Recheck the replica status and this time is should be empty
	res, err = tablet.VttabletProcess.QueryTablet("show replica status", keyspaceName, false)
	require.NoError(t, err)
	require.Len(t, res.Rows, 0)
}

// TestGetGlobalStatusVars tests the GetGlobalStatusVars RPC
func TestGetGlobalStatusVars(t *testing.T) {
	ctx := context.Background()
	statusValues, err := tmcGetGlobalStatusVars(ctx, replicaTablet.GrpcPort, []string{"Innodb_buffer_pool_pages_data", "unknown_value"})
	require.NoError(t, err)
	require.Len(t, statusValues, 1)
	checkValueGreaterZero(t, statusValues, "Innodb_buffer_pool_pages_data")

	statusValues, err = tmcGetGlobalStatusVars(ctx, replicaTablet.GrpcPort, []string{"Uptime", "Innodb_buffer_pool_pages_data"})
	require.NoError(t, err)
	require.Len(t, statusValues, 2)
	checkValueGreaterZero(t, statusValues, "Innodb_buffer_pool_pages_data")
	checkValueGreaterZero(t, statusValues, "Uptime")

	statusValues, err = tmcGetGlobalStatusVars(ctx, replicaTablet.GrpcPort, nil)
	require.NoError(t, err)
	require.Greater(t, len(statusValues), 250)
	checkValueGreaterZero(t, statusValues, "Innodb_buffer_pool_pages_data")
	checkValueGreaterZero(t, statusValues, "Innodb_buffer_pool_pages_free")
	checkValueGreaterZero(t, statusValues, "Uptime")
}

func checkValueGreaterZero(t *testing.T, statusValues map[string]string, val string) {
	valInMap, err := strconv.Atoi(statusValues[val])
	require.NoError(t, err)
	require.Greater(t, valInMap, 0)
}
