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
	"vitess.io/vitess/go/test/endtoend/cluster"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	clusterInstance                  *cluster.LocalProcessCluster
	tmClient                         *tmc.Client
	primaryTabletParams              mysql.ConnParams
	replicaTabletParams              mysql.ConnParams
	primaryTablet                    cluster.Vttablet
	replicaTablet                    cluster.Vttablet
	rdonlyTablet                     cluster.Vttablet
	hostname                         = "localhost"
	keyspaceName                     = "ks"
	shardName                        = "0"
	keyspaceShard                    = "ks/" + shardName
	dbName                           = "vt_" + keyspaceName
	username                         = "vt_dba"
	cell                             = "zone1"
	tabletHealthcheckRefreshInterval = 5 * time.Second
	tabletUnhealthyThreshold         = tabletHealthcheckRefreshInterval * 2
	sqlSchema                        = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB DEFAULT CHARSET=utf8;
	CREATE VIEW v1 AS SELECT id, value FROM t1;
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
		clusterInstance.VtGateExtraArgs = []string{
			"--vschema_ddl_authorized_users=%",
			"--enable-views",
			"--discovery_low_replication_lag", tabletUnhealthyThreshold.String(),
		}
		// Set extra tablet args for lock timeout
		clusterInstance.VtTabletExtraArgs = []string{
			"--lock_tables_timeout", "5s",
			"--watch_replication_stream",
			"--heartbeat_enable",
			"--health_check_interval", tabletHealthcheckRefreshInterval.String(),
			"--unhealthy_threshold", tabletUnhealthyThreshold.String(),
		}

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
			if tablet.Type == "primary" {
				primaryTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
			} else {
				rdonlyTablet = *tablet
			}
		}

		// Set mysql tablet params
		primaryTabletParams = mysql.ConnParams{
			Uname:      username,
			DbName:     dbName,
			UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", primaryTablet.TabletUID)),
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
	return tmClient.StartReplication(ctx, vtablet, false)
}

func tmcResetReplicationParameters(ctx context.Context, tabletGrpcPort int) error {
	vttablet := getTablet(tabletGrpcPort)
	return tmClient.ResetReplicationParameters(ctx, vttablet)
}

func tmcPrimaryPosition(ctx context.Context, tabletGrpcPort int) (string, error) {
	vtablet := getTablet(tabletGrpcPort)
	return tmClient.PrimaryPosition(ctx, vtablet)
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
