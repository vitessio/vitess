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

package orderby

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
	KeyspaceName    = "ks_orderby"
	Cell            = "test_orderby"
	SchemaSQL       = `create table t1(
	id1 bigint,
	id2 bigint,
	primary key(id1)
) Engine=InnoDB;

create table t1_id2_idx(
	id2 bigint,
	keyspace_id varbinary(10),
	primary key(id2)
) Engine=InnoDB;

create table t4(
	id1 bigint,
	id2 varchar(10),
	primary key(id1)
) ENGINE=InnoDB DEFAULT charset=utf8mb4 COLLATE=utf8mb4_general_ci;

create table t4_id2_idx(
	id2 varchar(10),
	id1 bigint,
	keyspace_id varbinary(50),
    primary key(id2, id1)
) Engine=InnoDB DEFAULT charset=utf8mb4 COLLATE=utf8mb4_general_ci;
`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    },
    "unicode_loose_md5" : {
	  "type": "unicode_loose_md5"
    },
    "t1_id2_vdx": {
      "type": "consistent_lookup_unique",
      "params": {
        "table": "t1_id2_idx",
        "from": "id2",
        "to": "keyspace_id"
      },
      "owner": "t1"
    },
     "t4_id2_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "t4_id2_idx",
        "from": "id2,id1",
        "to": "keyspace_id"
      },
      "owner": "t4"
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        },
        {
          "column": "id2",
          "name": "t1_id2_vdx"
        }
      ]
    },
    "t1_id2_idx": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "hash"
        }
      ]
    },
	"t4": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        },
        {
          "columns": ["id2", "id1"],
          "name": "t4_id2_vdx"
        }
      ]
    },
    "t4_id2_idx": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "unicode_loose_md5"
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
		clusterInstance.VtGateExtraArgs = []string{"-schema_change_signal"}
		clusterInstance.VtTabletExtraArgs = []string{"-queryserver-config-schema-change-signal", "-queryserver-config-schema-change-signal-interval", "0.1"}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true)
		if err != nil {
			return 1
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "-enable_system_settings=true")
		// Start vtgate
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
