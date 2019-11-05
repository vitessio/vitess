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

package tabletmanager

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	tmClient              *tmc.Client
	vtParams              mysql.ConnParams
	masterTabletParams    mysql.ConnParams
	replicaTabletParams   mysql.ConnParams
	replicaTabletGrpcPort int
	masterTabletGrpcPort  int
	hostname              = "localhost"
	keyspaceName          = "ks"
	dbName                = "vt_" + keyspaceName
	username              = "vt_dba"
	cell                  = "zone1"
	sqlSchema             = `
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
	flag.Parse()

	exitCode := func() int {
		clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// List of users authorized to execute vschema ddl operations
		clusterInstance.VtGateExtraArgs = []string{"-vschema_ddl_authorized_users=%"}
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

		// Start vtgate
		if err = clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		// Collect table paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		var masterTabletPath string
		var replicaTabletPath string
		for _, tablet := range tablets {
			path := fmt.Sprintf(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tablet.TabletUID)))
			if tablet.Type == "master" {
				masterTabletPath = path
				masterTabletGrpcPort = tablet.GrpcPort
			} else {
				replicaTabletPath = path
				replicaTabletGrpcPort = tablet.GrpcPort
			}
		}

		// Set mysql tablet params
		masterTabletParams = mysql.ConnParams{
			Uname:      username,
			DbName:     dbName,
			UnixSocket: masterTabletPath + "/mysql.sock",
		}
		replicaTabletParams = mysql.ConnParams{
			Uname:      username,
			DbName:     dbName,
			UnixSocket: replicaTabletPath + "/mysql.sock",
		}

		// create tablet manager client
		tmClient = tmc.NewClient()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}

func tmcLockTables(ctx context.Context, tabletGrpcPort int) error {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.LockTables(ctx, vtablet)
}

func tmcUnlockTables(ctx context.Context, tabletGrpcPort int) error {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.UnlockTables(ctx, vtablet)
}

func tmcStopSlave(ctx context.Context, tabletGrpcPort int) error {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.StopSlave(ctx, vtablet)
}

func tmcStartSlave(ctx context.Context, tabletGrpcPort int) error {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.StartSlave(ctx, vtablet)
}

func tmcMasterPosition(ctx context.Context, tabletGrpcPort int) (string, error) {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.MasterPosition(ctx, vtablet)
}

func tmcStartSlaveUntilAfter(ctx context.Context, tabletGrpcPort int, positon string, waittime time.Duration) error {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.StartSlaveUntilAfter(ctx, vtablet, positon, waittime)
}

func getTablet(tabletGrpcPort int) *tabletpb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
}
