/*
Copyright 2019 The Vitess Authors.

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
package master

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"

	"vitess.io/vitess/go/json2"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	masterTablet    cluster.Vttablet
	replicaTablet   cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "ks"
	shardName       = "0"
	cell            = "zone1"
	sqlSchema       = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB;
`

	vSchema = `
	{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "t1": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
  }`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Set extra tablet args for lock timeout
		clusterInstance.VtTabletExtraArgs = []string{
			"-lock_tables_timeout", "5s",
			"-watch_replication_stream",
			"-enable_replication_reporter",
		}
		// We do not need semiSync for this test case.
		clusterInstance.EnableSemiSync = false

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1
		}

		// Collect table paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "master" {
				masterTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestRepeatedInitShardMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Test that using InitShardMaster can go back and forth between 2 hosts.

	// Make replica tablet as master
	err := clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, replicaTablet.TabletUID)
	require.Nil(t, err)

	// Run health check on both, make sure they are both healthy.
	// Also make sure the types are correct.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", masterTablet.Alias)
	require.Nil(t, err)
	checkHealth(t, masterTablet.HTTPPort, false)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTablet.Alias)
	require.Nil(t, err)
	checkHealth(t, replicaTablet.HTTPPort, false)

	checkTabletType(t, masterTablet.Alias, "REPLICA")
	checkTabletType(t, replicaTablet.Alias, "MASTER")

	// Come back to the original tablet.
	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, masterTablet.TabletUID)
	require.Nil(t, err)

	// Run health check on both, make sure they are both healthy.
	// Also make sure the types are correct.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", masterTablet.Alias)
	require.Nil(t, err)
	checkHealth(t, masterTablet.HTTPPort, false)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTablet.Alias)
	require.Nil(t, err)
	checkHealth(t, replicaTablet.HTTPPort, false)

	checkTabletType(t, masterTablet.Alias, "MASTER")
	checkTabletType(t, replicaTablet.Alias, "REPLICA")
}

func TestMasterRestartSetsTERTimestamp(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Test that TER timestamp is set when we restart the MASTER vttablet.
	// TER = TabletExternallyReparented.
	// See StreamHealthResponse.tablet_externally_reparented_timestamp for details.

	// Make replica as master
	err := clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, replicaTablet.TabletUID)
	require.Nil(t, err)

	err = replicaTablet.VttabletProcess.WaitForTabletType("SERVING")
	require.Nil(t, err)

	// Capture the current TER.
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"VtTabletStreamHealth", "-count", "1", replicaTablet.Alias)
	require.Nil(t, err)

	var streamHealthRes1 querypb.StreamHealthResponse
	err = json.Unmarshal([]byte(result), &streamHealthRes1)
	require.Nil(t, err)
	actualType := streamHealthRes1.GetTarget().GetTabletType()
	tabletType := topodatapb.TabletType_value["MASTER"]
	got := fmt.Sprintf("%d", actualType)
	want := fmt.Sprintf("%d", tabletType)
	assert.Equal(t, want, got)
	assert.NotNil(t, streamHealthRes1.GetTabletExternallyReparentedTimestamp())
	assert.True(t, streamHealthRes1.GetTabletExternallyReparentedTimestamp() > 0,
		"TER on MASTER must be set after InitShardMaster")

	// Restart the MASTER vttablet and test again

	// kill the newly promoted master tablet
	err = replicaTablet.VttabletProcess.TearDown()
	require.Nil(t, err)

	// Start Vttablet
	err = clusterInstance.StartVttablet(&replicaTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err)

	// Make sure that the TER did not change
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"VtTabletStreamHealth", "-count", "1", replicaTablet.Alias)
	require.Nil(t, err)

	var streamHealthRes2 querypb.StreamHealthResponse
	err = json.Unmarshal([]byte(result), &streamHealthRes2)
	require.Nil(t, err)

	actualType = streamHealthRes2.GetTarget().GetTabletType()
	tabletType = topodatapb.TabletType_value["MASTER"]
	got = fmt.Sprintf("%d", actualType)
	want = fmt.Sprintf("%d", tabletType)
	assert.Equal(t, want, got)

	assert.NotNil(t, streamHealthRes2.GetTabletExternallyReparentedTimestamp())
	assert.True(t, streamHealthRes2.GetTabletExternallyReparentedTimestamp() == streamHealthRes1.GetTabletExternallyReparentedTimestamp(),
		fmt.Sprintf("When the MASTER vttablet was restarted, "+
			"the TER timestamp must be set by reading the old value from the tablet record. Old: %d, New: %d",
			streamHealthRes1.GetTabletExternallyReparentedTimestamp(),
			streamHealthRes2.GetTabletExternallyReparentedTimestamp()))

	// Reset master
	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, masterTablet.TabletUID)
	require.Nil(t, err)
	err = masterTablet.VttabletProcess.WaitForTabletType("SERVING")
	require.Nil(t, err)

}

func checkHealth(t *testing.T, port int, shouldError bool) {
	url := fmt.Sprintf("http://localhost:%d/healthz", port)
	resp, err := http.Get(url)
	require.Nil(t, err)
	if shouldError {
		assert.True(t, resp.StatusCode > 400)
	} else {
		assert.Equal(t, 200, resp.StatusCode)
	}
}

func checkTabletType(t *testing.T, tabletAlias string, typeWant string) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tabletAlias)
	require.Nil(t, err)

	var tablet topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tablet)
	require.Nil(t, err)

	actualType := tablet.GetType()
	got := fmt.Sprintf("%d", actualType)

	tabletType := topodatapb.TabletType_value[typeWant]
	want := fmt.Sprintf("%d", tabletType)

	assert.Equal(t, want, got)
}
