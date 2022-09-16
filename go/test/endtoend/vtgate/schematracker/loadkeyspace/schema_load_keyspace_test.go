/*
Copyright 2021 The Vitess Authors.

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

package loadkeyspace

import (
	"os"
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	sqlSchema       = `
		create table vt_user (
			id bigint,
			name varchar(64),
			primary key (id)
		) Engine=InnoDB;
			
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;

		create table test_table (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

// 1. Create cluster with vttablet, having schema change signal set to true
// 2. Create vtgate with schema change tracker to false
// 3. vtgate during its initialization will not be able to get schema changes from vttablet. Even though it
// initializes without any error.
// 4. But when you check for authoritative column list it will be empty. Indicating that vtgate didn't received
// any schema changes.
func TestBlockedLoadKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	var err error

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	err = clusterInstance.StartTopo()
	require.NoError(t, err)

	// Start keyspace without the --queryserver-config-schema-change-signal flag
	keyspace := &cluster.Keyspace{
		Name:      keyspaceName,
		SchemaSQL: sqlSchema,
	}
	clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal=false", "--use_super_read_only=true"}
	err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false)
	require.NoError(t, err)

	// Start vtgate with the schema_change_signal flag
	clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	// wait for addKeyspaceToTracker to timeout
	time.Sleep(30 * time.Second)

	// check warning logs
	logDir := clusterInstance.VtgateProcess.LogDir
	all, err := os.ReadFile(path.Join(logDir, "vtgate-stderr.txt"))
	require.NoError(t, err)
	require.Contains(t, string(all), "")

	err = utils.WaitForAuthoritative(t, clusterInstance.VtgateProcess, keyspaceName, "test_table")
	require.Error(t, err, "not able to find test_table in schema tracker")

	// This error should not be logged as the initial load itself failed.
	require.NotContains(t, string(all), "Unable to add keyspace to tracker")
}

// 1. Create cluster with vttablet, having schema change signal set to true
// 2. Create vtgate with schema change tracker to true
// 3. vtgate during its initialization should be able to learn about schema changes in vttablet successfully
func TestBlockedLoadKeyspaceWithScehmaChangeEnabled(t *testing.T) {
	defer cluster.PanicHandler(t)
	var err error

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	err = clusterInstance.StartTopo()
	require.NoError(t, err)

	// Start keyspace without the --queryserver-config-schema-change-signal flag
	keyspace := &cluster.Keyspace{
		Name:      keyspaceName,
		SchemaSQL: sqlSchema,
	}
	clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal=true", "--use_super_read_only=true"}
	err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false)
	require.NoError(t, err)

	// Start vtgate with the schema_change_signal flag
	clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	// wait for addKeyspaceToTracker to timeout
	time.Sleep(30 * time.Second)

	// check warning logs
	logDir := clusterInstance.VtgateProcess.LogDir
	all, err := os.ReadFile(path.Join(logDir, "vtgate-stderr.txt"))
	require.NoError(t, err)
	require.Contains(t, string(all), "")

	err = utils.WaitForAuthoritative(t, clusterInstance.VtgateProcess, keyspaceName, "test_table")
	require.NoError(t, err)

	// This error should not be logged as the initial load itself failed.
	require.NotContains(t, string(all), "Unable to add keyspace to tracker")
}

func TestLoadKeyspaceWithNoTablet(t *testing.T) {
	defer cluster.PanicHandler(t)
	var err error

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	err = clusterInstance.StartTopo()
	require.NoError(t, err)

	// create keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceName,
		SchemaSQL: sqlSchema,
	}
	clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal"}
	err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false)
	require.NoError(t, err)

	// teardown vttablets
	for _, vttablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		err = vttablet.VttabletProcess.TearDown()
		require.NoError(t, err)
	}

	// Start vtgate with the schema_change_signal flag
	clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	// check warning logs
	logDir := clusterInstance.VtgateProcess.LogDir
	all, err := os.ReadFile(path.Join(logDir, "vtgate-stderr.txt"))
	require.NoError(t, err)
	require.Contains(t, string(all), "Unable to get initial schema reload")
}

func TestNoInitialKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	var err error

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	err = clusterInstance.StartTopo()
	require.NoError(t, err)

	// Start vtgate with the schema_change_signal flag
	clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	logDir := clusterInstance.VtgateProcess.LogDir

	// teardown vtgate to flush logs
	err = clusterInstance.VtgateProcess.TearDown()
	require.NoError(t, err)

	// check info logs
	all, err := os.ReadFile(path.Join(logDir, "vtgate.INFO"))
	require.NoError(t, err)
	require.Contains(t, string(all), "No keyspace to load")
}
