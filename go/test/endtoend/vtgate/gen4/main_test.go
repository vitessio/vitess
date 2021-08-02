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

package vtgate

import (
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	SchemaSQL       = `create table t1(
	id bigint,
	col bigint,
	primary key(id)
) Engine=InnoDB;

create table t2(
	id bigint,
	tcol1 varchar(50),
	tcol2 varchar(50),
	primary key(id)
) Engine=InnoDB;

create table t3(
	id bigint,
	tcol1 varchar(50),
	tcol2 varchar(50),
	primary key(id)
) Engine=InnoDB;
`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "xxhash": {
      "type": "xxhash"
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "xxhash"
        }
      ]
    },
    "t2": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "xxhash"
        }
      ],
      "columns": [
        {
          "name": "tcol1",
          "type": "VARCHAR"
        }
      ]
    },
    "t3": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "xxhash"
        }
      ],
      "columns": [
        {
          "name": "tcol1",
          "type": "VARCHAR"
        }
      ]
    }
  }
}`

	routingRules = `
{"rules": [
  {
    "from_table": "ks.t1000",
	"to_tables": ["ks.t1"]
  }
]}
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true)
		if err != nil {
			return 1
		}

		// apply routing rules
		err = clusterInstance.VtctlclientProcess.ApplyRoutingRules(routingRules)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildVSchemaGraph")
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"-planner_version", "Gen4"} // enable Gen4 planner.
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}
