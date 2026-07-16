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
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
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

func setup(target testing.TB) {
	target.Helper()

	// Set gRPC max message size to 64MB for both vttablet and vtgate.
	// This is needed to stream large binlog events (>16MB default).
	grpcMaxMsgSize := "--grpc-max-message-size=67108864" // 64MB

	cluster, err := vitesst.NewCluster(target,
		vitesst.WithKeyspace(keyspaceName).WithShardNames("0").WithReplicas(0).WithSchema(sqlSchema),
		vitesst.WithVTTabletArgs(grpcMaxMsgSize, "--pprof-http"),
		vitesst.WithVTGateArgs(grpcMaxMsgSize, "--enable-binlog-dump", "--binlog-dump-authorized-users=%", "--pprof-http"),
	)
	require.NoError(target, err)

	cleanup, err := cluster.Start(target, target.Context())
	target.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(target.Context()), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(ctx); cleanupErr != nil {
			target.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(target, err)

	clusterInstance = cluster

	addr, err := cluster.VTGate().MySQLAddr(target.Context())
	require.NoError(target, err)
	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(target, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(target, err)
	vtParams = mysql.ConnParams{Host: host, Port: port}
}
