/*
Copyright 2026 The Vitess Authors.

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
package querythrottler

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldclient"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtctldClient    vtctldclient.VtctldClient
	primaryTablet   *cluster.Vttablet
	replicaTablet   *cluster.Vttablet
	vtParams        mysql.ConnParams
	hostname        = "localhost"
	keyspaceName    = "ks"
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
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--lock-tables-timeout", "5s",
			"--watch-replication-stream",
			"--enable-replication-reporter",
			"--heartbeat-interval", "250ms",
			"--heartbeat-on-demand-duration", "5s",
		}

		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false, clusterInstance.Cell); err != nil {
			return 1
		}

		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "primary" {
				primaryTablet = tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = tablet
			}
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		if err := vtgateInstance.Setup(); err != nil {
			return 1
		}
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		clusterInstance.VtctldClientProcess = *cluster.VtctldClientProcessInstance(
			clusterInstance.VtctldProcess.GrpcPort,
			clusterInstance.TopoPort,
			"localhost",
			clusterInstance.TmpDirectory,
		)

		ctx := context.Background()
		vtctldAddr := fmt.Sprintf("%s:%d", hostname, clusterInstance.VtctldProcess.GrpcPort)
		vtctldClient, err = grpcvtctldclient.NewWithDialOpts(
			ctx, vtctldAddr, grpcclient.FailFast(false),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return 1
		}
		defer vtctldClient.Close()

		return m.Run()
	}()
	os.Exit(exitCode)
}
