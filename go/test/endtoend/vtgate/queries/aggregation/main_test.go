/*
Copyright 2021 The Vitess Authors.

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

package aggregation

import (
	_ "embed"
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	keyspaceName    = "ks_aggr"
	cell            = "test_aggr"

	//go:embed schema.sql
	schemaSQL string

	//go:embed vschema.json
	vschema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			log.Fatalf("topo err: %v", err.Error())
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vschema,
		}
		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal", "--log_operator_traffic=true"}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal"}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			log.Fatalf("Error starting keyspace: %v", err.Error())
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--enable_system_settings=true")
		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			log.Fatalf("Error starting vtgate: %v", err.Error())
		}

		vtParams = clusterInstance.GetVTParams(keyspaceName)

		// create mysql instance and connection parameters
		conn, closer, err := utils.NewMySQL(clusterInstance, keyspaceName, schemaSQL)
		if err != nil {
			log.Fatalf("Error creating mysql instance: %v", err)
		}
		defer closer()
		mysqlParams = conn

		return m.Run()
	}()
	os.Exit(exitCode)
}
