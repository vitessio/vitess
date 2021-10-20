/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	SchemaSQL       = `
create table t7_xxhash(
	uid varchar(50),
	phone bigint,
    msg varchar(100),
    primary key(uid)
) Engine=InnoDB;

create table t7_xxhash_idx(
	phone bigint,
	keyspace_id varbinary(50),
	primary key(phone, keyspace_id)
) Engine=InnoDB;
`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "unicode_loose_xxhash" : {
	  "type": "unicode_loose_xxhash"
    },
    "t7_xxhash_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "t7_xxhash_idx",
        "from": "phone",
        "to": "keyspace_id",
        "ignore_nulls": "true"
      },
      "owner": "t7_xxhash"
    }
  },
  "tables": {
	"t7_xxhash": {
      "column_vindexes": [
        {
          "column": "uid",
          "name": "unicode_loose_xxhash"
        },
        {
          "column": "phone",
          "name": "t7_xxhash_vdx"
        }
      ]
    },
    "t7_xxhash_idx": {
      "column_vindexes": [
        {
          "column": "phone",
          "name": "unicode_loose_xxhash"
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
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGatePlannerVersion = planbuilder.V3 // enable V3 planner.
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
