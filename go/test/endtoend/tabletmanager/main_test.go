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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	clusterInstance     *cluster.LocalProcessCluster
	tmClient            *tmc.Client
	masterTabletParams  mysql.ConnParams
	replicaTabletParams mysql.ConnParams
	masterTablet        cluster.Vttablet
	replicaTablet       cluster.Vttablet
	rdonlyTablet        cluster.Vttablet
	hostname            = "localhost"
	keyspaceName        = "ks"
	shardName           = "0"
	keyspaceShard       = "ks/" + shardName
	dbName              = "vt_" + keyspaceName
	username            = "vt_dba"
	cell                = "zone1"
	sqlSchema           = `
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

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, true); err != nil {
			return 1
		}

		// Start vtgate
		if err = clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		// Collect table paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "master" {
				masterTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
			} else {
				rdonlyTablet = *tablet
			}
		}

		// Set mysql tablet params
		masterTabletParams = mysql.ConnParams{
			Uname:      username,
			DbName:     dbName,
			UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", masterTablet.TabletUID)),
		}
		replicaTabletParams = mysql.ConnParams{
			Uname:      username,
			DbName:     dbName,
			UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", replicaTablet.TabletUID)),
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
	require.Nil(t, err)
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

func tmcStopReplication(ctx context.Context, tabletGrpcPort int) error {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.StopReplication(ctx, vtablet)
}

func tmcStartReplication(ctx context.Context, tabletGrpcPort int) error {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.StartReplication(ctx, vtablet)
}

func tmcMasterPosition(ctx context.Context, tabletGrpcPort int) (string, error) {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.MasterPosition(ctx, vtablet)
}

func tmcStartReplicationUntilAfter(ctx context.Context, tabletGrpcPort int, positon string, waittime time.Duration) error {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.StartReplicationUntilAfter(ctx, vtablet, positon, waittime)
}

func getTablet(tabletGrpcPort int) *tabletpb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
}
