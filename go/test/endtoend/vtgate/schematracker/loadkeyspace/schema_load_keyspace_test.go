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

func TestBlockedLoadKeyspace(t *testing.T) {
	t.Skip("this test is not valid anymore since schemacopy is being created by default and inspite of the tablet signal being setup to false")

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
	clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal=false"}
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
	require.Contains(t, string(all), "Unable to get initial schema reload")

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
