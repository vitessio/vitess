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

package binlogdump

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "test_keyspace"
	sqlSchema       = `create table binlog_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
	) Engine=InnoDB;

	create table large_blob_test (
		id bigint auto_increment,
		data longblob,
		primary key (id)
	) Engine=InnoDB;`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		ctx := context.Background()

		// Set gRPC max message size to 64MB for both vttablet and vtgate.
		// This is needed to stream large binlog events (>16MB default).
		grpcMaxMsgSize := "--grpc-max-message-size=67108864" // 64MB

		cluster, err := vitesst.NewCluster(
			vitesst.WithKeyspace(keyspaceName).WithShardNames("0").WithReplicas(0).WithSchema(sqlSchema),
			vitesst.WithVTTabletArgs(grpcMaxMsgSize, "--pprof-http"),
			vitesst.WithVTGateArgs(grpcMaxMsgSize, "--enable-binlog-dump", "--binlog-dump-authorized-users=%", "--pprof-http"),
		)
		if err != nil {
			return 1, err
		}

		cleanup, err := cluster.Start(ctx)
		if cleanup != nil {
			defer cleanup(ctx)
		}
		if err != nil {
			return 1, err
		}

		clusterInstance = cluster

		addr, err := cluster.VTGate().MySQLAddr(ctx)
		if err != nil {
			return 1, err
		}
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return 1, err
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return 1, err
		}
		vtParams = mysql.ConnParams{Host: host, Port: port}

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}
