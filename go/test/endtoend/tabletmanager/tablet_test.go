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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

// TestLocalMetadata tests the contents of local_metadata table after vttablet startup
func TestLocalMetadata(t *testing.T) {
	defer cluster.PanicHandler(t)
	// by default tablets are started with -restore_from_backup
	// so metadata should exist
	cluster.VerifyLocalMetadata(t, &replicaTablet, keyspaceName, shardName, cell)

	// Create new tablet
	rTablet := clusterInstance.GetVttabletInstance("replica", 0, "")

	// Init Tablet
	err := clusterInstance.VtctlclientProcess.InitTablet(rTablet, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	clusterInstance.VtTabletExtraArgs = []string{
		"-lock_tables_timeout", "5s",
		"-init_populate_metadata",
	}
	rTablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(rTablet.TabletUID, rTablet.MySQLPort, clusterInstance.TmpDirectory)
	err = rTablet.MysqlctlProcess.Start()
	require.Nil(t, err)

	log.Info(fmt.Sprintf("Started vttablet %v", rTablet))
	// SupportsBackup=False prevents vttablet from trying to restore
	// Start vttablet process
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	cluster.VerifyLocalMetadata(t, rTablet, keyspaceName, shardName, cell)

	// Create another new tablet
	rTablet2 := clusterInstance.GetVttabletInstance("replica", 0, "")

	// Init Tablet
	err = clusterInstance.VtctlclientProcess.InitTablet(rTablet2, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	// start with -init_populate_metadata false (default)
	clusterInstance.VtTabletExtraArgs = []string{
		"-lock_tables_timeout", "5s",
	}
	rTablet2.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(rTablet2.TabletUID, rTablet2.MySQLPort, clusterInstance.TmpDirectory)
	err = rTablet2.MysqlctlProcess.Start()
	require.Nil(t, err)

	log.Info(fmt.Sprintf("Started vttablet %v", rTablet2))
	// SupportsBackup=False prevents vttablet from trying to restore
	// Start vttablet process
	err = clusterInstance.StartVttablet(rTablet2, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	// check that tablet did _not_ get populated
	qr, err := rTablet2.VttabletProcess.QueryTablet("select * from _vt.local_metadata", keyspaceName, false)
	require.Nil(t, err)
	require.Nil(t, qr.Rows)

	// Reset the VtTabletExtraArgs and kill tablets
	clusterInstance.VtTabletExtraArgs = []string{}
	killTablets(t, rTablet, rTablet2)
}
