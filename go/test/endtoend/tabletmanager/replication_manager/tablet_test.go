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
	"path"
	"strconv"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/sidecardb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
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
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false, clusterInstance.Cell); err != nil {
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
	_, err := tab.MysqlctlProcess.ExecuteCommandWithOutput(
		"--tablet-uid", strconv.Itoa(tab.MysqlctlProcess.TabletUID),
		"--mysql-port", strconv.Itoa(tab.MysqlctlProcess.MySQLPort),
		"init_config",
	)
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
		if err == nil && replicaStatus.SourcePort == expectedPort {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("time out before source port became %v for %v", expectedPort, tablet.Alias)
}

func getSidecarDBDDLQueryCount(tablet *cluster.VttabletProcess) (int64, error) {
	vars := tablet.GetVars()
	key := sidecardb.StatsKeyQueryCount
	val, ok := vars[key]
	if !ok {
		return 0, fmt.Errorf("%s not found in debug/vars", key)
	}
	return int64(val.(float64)), nil
}

func TestReplicationRepairAfterPrimaryTabletChange(t *testing.T) {
	ctx := t.Context()
	// Check that initially replication is setup correctly on the replica tablet
	err := waitForSourcePort(ctx, t, replicaTablet, int32(primaryTablet.MySQLPort))
	require.NoError(t, err)

	sidecarDDLCount, err := getSidecarDBDDLQueryCount(primaryTablet.VttabletProcess)
	require.NoError(t, err)
	// sidecar db should create all _vt tables when vttablet started
	require.Positive(t, sidecarDDLCount)

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

	sidecarDDLCount, err = getSidecarDBDDLQueryCount(primaryTablet.VttabletProcess)
	require.NoError(t, err)
	// sidecardb should find the desired _vt schema and not apply any new creates or upgrades when the tablet comes up again
	require.Equal(t, int64(0), sidecarDDLCount)
}

func TestReparentJournalInfo(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for _, vttablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		length, err := tmClient.ReadReparentJournalInfo(ctx, getTablet(vttablet.GrpcPort))
		require.NoError(t, err)
		require.EqualValues(t, 1, length)
	}
}

// runSQL runs a query directly against the MySQL instance of the given
// tablet, as the dba user.
func runSQL(ctx context.Context, t *testing.T, tablet cluster.Vttablet, query string) *sqltypes.Result {
	t.Helper()
	connParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", tablet.TabletUID)),
	}
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn.Close()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}

// TestFatalReplicationErrorMakesTabletUnhealthy breaks replication
// unrecoverably: the primary purges binlogs the replica still needs, so the
// replica's IO thread stops with the fatal binlog read error (1236, recorded
// as errno 13114 by MySQL 8.0.26+). The replica must be reported unhealthy
// right away, instead of quietly serving an estimated replication lag until
// the unhealthy threshold is crossed.
//
// This test intentionally leaves the replica unable to replicate (the purged
// transactions cannot be recovered), so it must remain the last test in this
// package.
func TestFatalReplicationErrorMakesTabletUnhealthy(t *testing.T) {
	ctx := t.Context()

	err := waitForSourcePort(ctx, t, replicaTablet, int32(primaryTablet.MySQLPort))
	require.NoError(t, err)
	require.NoError(t, replicaTablet.VttabletProcess.WaitForTabletStatus("SERVING"))

	// Stop replication, move the primary ahead, and purge its binlogs so the
	// replica can never catch up.
	err = tmClient.StopReplication(ctx, getTablet(replicaTablet.GrpcPort))
	require.NoError(t, err)

	runSQL(ctx, t, primaryTablet, "insert into vt_ks.t1(id, value) values (100, 'unreachable')")
	runSQL(ctx, t, primaryTablet, "flush binary logs")
	binlogs := runSQL(ctx, t, primaryTablet, "show binary logs")
	require.NotEmpty(t, binlogs.Rows)
	lastFile := binlogs.Rows[len(binlogs.Rows)-1][0].ToString()
	runSQL(ctx, t, primaryTablet, fmt.Sprintf("purge binary logs to '%s'", lastFile))

	err = tmClient.StartReplication(ctx, getTablet(replicaTablet.GrpcPort), false)
	require.NoError(t, err)

	// The IO thread now stops with the fatal purged-binlogs error. The tablet
	// must go unhealthy well before the replication lag unhealthy threshold
	// would catch it.
	err = replicaTablet.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"NOT_SERVING"}, 2*time.Minute)
	require.NoError(t, err)

	status, err := tmcGetReplicationStatus(ctx, replicaTablet.GrpcPort)
	require.NoError(t, err)
	assert.Contains(t, status.LastIoError, "Got fatal error 1236")
}
