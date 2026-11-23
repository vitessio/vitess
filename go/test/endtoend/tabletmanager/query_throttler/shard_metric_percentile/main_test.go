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
package shardmetricpercentile

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

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
	replicaTablets  []*cluster.Vttablet
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
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		if err := clusterInstance.StartTopo(); err != nil {
			fmt.Fprintf(os.Stderr, "starting topo: %v\n", err)
			return 1
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--lock-tables-timeout", "5s",
			"--enable-replication-reporter",
			"--heartbeat-interval", "250ms",
			"--heartbeat-on-demand-duration", "5s",
			"--query-throttler-cache-update-interval", "1s",
		}

		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, replicaCount, false, clusterInstance.Cell); err != nil {
			fmt.Fprintf(os.Stderr, "starting keyspace %s: %v\n", keyspaceName, err)
			return 1
		}

		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			switch tablet.Type {
			case "primary":
				primaryTablet = tablet
			case "replica":
				replicaTablets = append(replicaTablets, tablet)
			}
		}
		if primaryTablet == nil || len(replicaTablets) != replicaCount {
			fmt.Fprintf(os.Stderr, "expected a primary and %d replicas, got primary=%v and %d replicas\n", replicaCount, primaryTablet != nil, len(replicaTablets))
			return 1
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		if err := vtgateInstance.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "starting vtgate: %v\n", err)
			return 1
		}
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		vtctldAddr := fmt.Sprintf("%s:%d", hostname, clusterInstance.VtctldProcess.GrpcPort)
		var err error
		vtctldClient, err = grpcvtctldclient.NewWithDialOpts(
			ctx, vtctldAddr, grpcclient.FailFast(false),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "connecting to vtctld at %s: %v\n", vtctldAddr, err)
			return 1
		}
		defer vtctldClient.Close()

		return m.Run()
	}()
	os.Exit(exitCode)
}
