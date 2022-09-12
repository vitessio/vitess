/*
Copyright 2022 The Vitess Authors.

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
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	tmClient        *tmc.Client
	primaryTablet   cluster.Vttablet
	replicaTablet   cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	sqlSchema       = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB DEFAULT CHARSET=utf8;
	CREATE VIEW v1 AS SELECT id, value FROM t1;
`

	vSchema = `
	{
    "sharded": false,
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
			if tablet.Type == "primary" {
				primaryTablet = *tablet
			} else {
				replicaTablet = *tablet
			}
		}

		// create tablet manager client
		tmClient = tmc.NewClient()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func tmcGetReplicationStatus(ctx context.Context, tabletGrpcPort int) (*replicationdatapb.Status, error) {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.ReplicationStatus(ctx, vtablet)
}

func getTablet(tabletGrpcPort int) *tabletpb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
}

// resurrectTablet is used to resurrect the given tablet
func resurrectTablet(t *testing.T, tab cluster.Vttablet) {
	// initialize config again to regenerate the my.cnf file which has the port to use
	_, err := tab.MysqlctlProcess.ExecuteCommandWithOutput("--log_dir", tab.MysqlctlProcess.LogDirectory,
		"--tablet_uid", fmt.Sprintf("%d", tab.MysqlctlProcess.TabletUID),
		"--mysql_port", fmt.Sprintf("%d", tab.MysqlctlProcess.MySQLPort),
		"init_config")
	require.NoError(t, err)

	tab.MysqlctlProcess.InitMysql = false
	err = tab.MysqlctlProcess.Start()
	require.NoError(t, err)

	// Start the tablet
	tab.VttabletProcess.ServingStatus = "SERVING"
	err = tab.VttabletProcess.Setup()
	require.NoError(t, err)
}

// stopTablet stops the tablet
func stopTablet(t *testing.T, tab cluster.Vttablet) {
	err := tab.VttabletProcess.TearDownWithTimeout(30 * time.Second)
	require.NoError(t, err)
	err = tab.MysqlctlProcess.Stop()
	require.NoError(t, err)
}

func waitForSourcePort(ctx context.Context, t *testing.T, tablet cluster.Vttablet, expectedPort int32) error {
	timeout := time.Now().Add(15 * time.Second)
	for time.Now().Before(timeout) {
		// Check that initially replication is setup correctly on the replica tablet
		replicaStatus, err := tmcGetReplicationStatus(ctx, tablet.GrpcPort)
		require.NoError(t, err)
		if replicaStatus.SourcePort == expectedPort {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("time out before source port became %v for %v", expectedPort, tablet.Alias)
}

func TestReplicationRepairAfterPrimaryTabletChange(t *testing.T) {
	ctx := context.Background()
	// Check that initially replication is setup correctly on the replica tablet
	err := waitForSourcePort(ctx, t, replicaTablet, int32(primaryTablet.MySQLPort))
	require.NoError(t, err)

	// Stop the primary tablet
	stopTablet(t, primaryTablet)

	// Change the MySQL port of the primary tablet
	newMysqlPort := clusterInstance.GetAndReservePort()
	primaryTablet.MySQLPort = newMysqlPort
	primaryTablet.MysqlctlProcess.MySQLPort = newMysqlPort

	// Start the primary tablet again
	resurrectTablet(t, primaryTablet)

	// Let replication manager repair replication
	err = waitForSourcePort(ctx, t, replicaTablet, int32(newMysqlPort))
	require.NoError(t, err)
}
