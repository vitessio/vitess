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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

// TestEnsureDB tests that vttablet creates the db as needed
func TestEnsureDB(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create new tablet
	tablet := clusterInstance.NewVttabletInstance("replica", 0, "")
	tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	err := tablet.MysqlctlProcess.Start()
	require.NoError(t, err)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))
	// Start vttablet process as replica. It won't be able to serve because there's no db.
	err = clusterInstance.StartVttablet(tablet, "NOT_SERVING", false, cell, "dbtest", hostname, "0")
	require.NoError(t, err)

	// Make it the primary.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented", tablet.Alias)
	require.EqualError(t, err, "exit status 1")

	// It is still NOT_SERVING because the db is read-only.
	assert.Equal(t, "NOT_SERVING", tablet.VttabletProcess.GetTabletStatus())
	status := tablet.VttabletProcess.GetStatusDetails()
	assert.Contains(t, status, "read-only")

	// Switch to read-write and verify that we go serving.
	// Note: for TabletExternallyReparented, we expect SetReadWrite to be called by the user
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadWrite", tablet.Alias)
	require.NoError(t, err)
	err = tablet.VttabletProcess.WaitForTabletStatus("SERVING")
	require.NoError(t, err)
	killTablets(t, tablet)
}

// TestResetReplicationParameters tests that the RPC ResetReplicationParameters works as intended.
func TestResetReplicationParameters(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create new tablet
	tablet := clusterInstance.NewVttabletInstance("replica", 0, "")
	tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
	err := tablet.MysqlctlProcess.Start()
	require.NoError(t, err)

	log.Info(fmt.Sprintf("Started vttablet %v", tablet))
	// Start vttablet process as replica. It won't be able to serve because there's no db.
	err = clusterInstance.StartVttablet(tablet, "NOT_SERVING", false, cell, "dbtest", hostname, "0")
	require.NoError(t, err)

	// Set a replication source on the tablet and start replication
	_, err = tablet.VttabletProcess.QueryTablet("stop slave;change master to master_host = 'localhost', master_port = 123;start slave;", keyspaceName, false)
	require.NoError(t, err)

	// Check the replica status.
	res, err := tablet.VttabletProcess.QueryTablet("show slave status", keyspaceName, false)
	require.NoError(t, err)
	// This is expected to return 1 row result
	require.Len(t, res.Rows, 1)

	// Reset the replication parameters on the tablet
	err = tmcResetReplicationParameters(context.Background(), tablet.GrpcPort)
	require.NoError(t, err)

	// Recheck the replica status and this time is should be empty
	res, err = tablet.VttabletProcess.QueryTablet("show slave status", keyspaceName, false)
	require.NoError(t, err)
	require.Len(t, res.Rows, 0)
}
