/*
Copyright 2025 The Vitess Authors.

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
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	hostname        = "localhost"
	keyspaceName    = "test_keyspace"
	cell            = "zone1"
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
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Set gRPC max message size to 64MB for both vttablet and vtgate
		// This is needed to stream large binlog events (>16MB default)
		grpcMaxMsgSize := "--grpc-max-message-size=67108864" // 64MB
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, grpcMaxMsgSize)
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, grpcMaxMsgSize)

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false, clusterInstance.Cell); err != nil {
			return 1, err
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}
