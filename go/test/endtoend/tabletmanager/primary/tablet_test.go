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
package primary

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	primaryTablet   cluster.Vttablet
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
			"--lock_tables_timeout", "5s",
			"--watch_replication_stream",
			"--enable_replication_reporter",
		}

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
			if tablet.Type == "primary" {
				primaryTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestRepeatedInitShardPrimary(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Test that using InitShardPrimary can go back and forth between 2 hosts.

	// Make replica tablet as primary
	err := clusterInstance.VtctlclientProcess.InitShardPrimary(keyspaceName, shardName, cell, replicaTablet.TabletUID)
	require.NoError(t, err)

	// Run health check on both, make sure they are both healthy.
	// Also make sure the types are correct.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", primaryTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, primaryTablet.HTTPPort, false)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, replicaTablet.HTTPPort, false)

	checkTabletType(t, primaryTablet.Alias, "REPLICA")
	checkTabletType(t, replicaTablet.Alias, "PRIMARY")

	// Come back to the original tablet.
	err = clusterInstance.VtctlclientProcess.InitShardPrimary(keyspaceName, shardName, cell, primaryTablet.TabletUID)
	require.NoError(t, err)

	// Run health check on both, make sure they are both healthy.
	// Also make sure the types are correct.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", primaryTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, primaryTablet.HTTPPort, false)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", replicaTablet.Alias)
	require.NoError(t, err)
	checkHealth(t, replicaTablet.HTTPPort, false)

	checkTabletType(t, primaryTablet.Alias, "PRIMARY")
	checkTabletType(t, replicaTablet.Alias, "REPLICA")
}

func TestPrimaryRestartSetsTERTimestamp(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Test that TER timestamp is set when we restart the PRIMARY vttablet.
	// TER = TabletExternallyReparented.
	// See StreamHealthResponse.tablet_externally_reparented_timestamp for details.

	// Make replica as primary
	err := clusterInstance.VtctlclientProcess.InitShardPrimary(keyspaceName, shardName, cell, replicaTablet.TabletUID)
	require.NoError(t, err)

	err = replicaTablet.VttabletProcess.WaitForTabletStatus("SERVING")
	require.NoError(t, err)

	// Capture the current TER.
	shrs, err := clusterInstance.StreamTabletHealth(context.Background(), &replicaTablet, 1)
	require.NoError(t, err)

	streamHealthRes1 := shrs[0]
	actualType := streamHealthRes1.GetTarget().GetTabletType()
	tabletType := topodatapb.TabletType_value["PRIMARY"]
	got := fmt.Sprintf("%d", actualType)
	want := fmt.Sprintf("%d", tabletType)
	assert.Equal(t, want, got)
	assert.NotNil(t, streamHealthRes1.GetTabletExternallyReparentedTimestamp())
	assert.True(t, streamHealthRes1.GetTabletExternallyReparentedTimestamp() > 0,
		"TER on PRIMARY must be set after InitShardPrimary")

	// Restart the PRIMARY vttablet and test again

	// kill the newly promoted primary tablet
	err = replicaTablet.VttabletProcess.TearDown()
	require.NoError(t, err)

	// Start Vttablet
	err = clusterInstance.StartVttablet(&replicaTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.NoError(t, err)

	// Make sure that the TER did not change
	shrs, err = clusterInstance.StreamTabletHealth(context.Background(), &replicaTablet, 1)
	require.NoError(t, err)

	streamHealthRes2 := shrs[0]

	actualType = streamHealthRes2.GetTarget().GetTabletType()
	tabletType = topodatapb.TabletType_value["PRIMARY"]
	got = fmt.Sprintf("%d", actualType)
	want = fmt.Sprintf("%d", tabletType)
	assert.Equal(t, want, got)

	assert.NotNil(t, streamHealthRes2.GetTabletExternallyReparentedTimestamp())
	assert.True(t, streamHealthRes2.GetTabletExternallyReparentedTimestamp() == streamHealthRes1.GetTabletExternallyReparentedTimestamp(),
		fmt.Sprintf("When the PRIMARY vttablet was restarted, "+
			"the TER timestamp must be set by reading the old value from the tablet record. Old: %d, New: %d",
			streamHealthRes1.GetTabletExternallyReparentedTimestamp(),
			streamHealthRes2.GetTabletExternallyReparentedTimestamp()))

	// Reset primary
	err = clusterInstance.VtctlclientProcess.InitShardPrimary(keyspaceName, shardName, cell, primaryTablet.TabletUID)
	require.NoError(t, err)
	err = primaryTablet.VttabletProcess.WaitForTabletStatus("SERVING")
	require.NoError(t, err)

}

func checkHealth(t *testing.T, port int, shouldError bool) {
	url := fmt.Sprintf("http://localhost:%d/healthz", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	if shouldError {
		assert.True(t, resp.StatusCode > 400)
	} else {
		assert.Equal(t, 200, resp.StatusCode)
	}
}

func checkTabletType(t *testing.T, tabletAlias string, typeWant string) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetTablet", tabletAlias)
	require.NoError(t, err)

	var tablet topodatapb.Tablet
	err = json2.Unmarshal([]byte(result), &tablet)
	require.NoError(t, err)

	actualType := tablet.GetType()
	got := fmt.Sprintf("%d", actualType)

	tabletType := topodatapb.TabletType_value[typeWant]
	want := fmt.Sprintf("%d", tabletType)

	assert.Equal(t, want, got)
}
