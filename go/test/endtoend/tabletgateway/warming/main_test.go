/*
Copyright 2024 The Vitess Authors.

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

package warming

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance   *cluster.LocalProcessCluster
	vtParams          mysql.ConnParams
	keyspaceName      = "commerce"
	vtgateGrpcAddress string
	cell              = "zone1"

	sqlSchema = `
		create table test_table (
			id bigint not null auto_increment,
			val varchar(128),
			primary key(id)
		) ENGINE=InnoDB;
	`

	vSchema = `{
		"tables": {
			"test_table": {}
		}
	}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")

		// Configure VTTablet with fast health checks
		clusterInstance.VtTabletExtraArgs = []string{
			"--health_check_interval", "1s",
			"--shutdown_grace_period", "3s",
		}

		// Configure VTGate with warming balancer mode
		clusterInstance.VtGateExtraArgs = []string{
			"--vtgate-balancer-mode", "warming",
			"--balancer-warming-period", "2m", // Short period for testing
			"--balancer-warming-traffic-percent", "10",
		}

		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			fmt.Printf("Failed to start topo: %v\n", err)
			return 1
		}

		// Start keyspace with 2 replicas for testing traffic distribution
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		// StartUnshardedKeyspace(keyspace, replicaCount, rdonly)
		// We want 1 primary + 2 replicas for proper testing
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 2, false)
		if err != nil {
			fmt.Printf("Failed to start keyspace: %v\n", err)
			return 1
		}

		// Start vtgate
		vtgateInstance := clusterInstance.NewVtgateInstance()
		err = vtgateInstance.Setup()
		if err != nil {
			fmt.Printf("Failed to start vtgate: %v\n", err)
			return 1
		}
		clusterInstance.VtgateProcess = *vtgateInstance

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		vtgateGrpcAddress = fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateGrpcPort)

		return m.Run()
	}()
	os.Exit(exitCode)
}
