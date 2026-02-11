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

package vstreamclient

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
	keyspaceName      = "customer"
	vtgateGrpcAddress string
	cell              = "zone1"
	sqlSchema         = `create table customer(
			id bigint not null auto_increment,
			email varchar(128),
			primary key(id)
		) ENGINE=InnoDB;`

	vSchema = `{
						"tables": {
							"customer": {}
						}
					}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		clusterInstance.VtTabletExtraArgs = []string{"--health_check_interval", "1s", "--shutdown_grace_period", "3s"}
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1, err
		}

		// Start keyspace
		customerKeyspace := &cluster.Keyspace{
			Name:      "customer",
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*customerKeyspace, 1, true, cell)
		if err != nil {
			return 1, err
		}

		commerceKeyspace := &cluster.Keyspace{
			Name:      "commerce",
			SchemaSQL: "",
			VSchema:   "",
		}
		err = clusterInstance.StartUnshardedKeyspace(*commerceKeyspace, 1, true, cell)
		if err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// Start vtgate
		err = vtgateInstance.Setup()
		if err != nil {
			return 1, err
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		vtgateGrpcAddress = fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateGrpcPort)
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	os.Exit(exitcode)
}
