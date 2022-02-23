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

package insert

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
	sKs             = "sks"
	uKs             = "uks"
	Cell            = "test"
	sSchemaSQL      = `create table s_tbl(
	id bigint,
	num bigint,
	primary key(id)
) Engine=InnoDB;

`

	sVSchema = `
{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "s_tbl": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ]
    }
  }
}`

	uSchemaSQL = `create table u_tbl(
	id bigint,
	num bigint,
	primary key(id)
) Engine=InnoDB;

`

	uVSchema = `
{
  "tables": {
    "u_tbl": {}
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

		// Start unsharded keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      uKs,
			SchemaSQL: uSchemaSQL,
			VSchema:   uVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*uKeyspace, 0, false)
		if err != nil {
			return 1
		}

		// Start sharded keyspace
		sKeyspace := &cluster.Keyspace{
			Name:      sKs,
			SchemaSQL: sSchemaSQL,
			VSchema:   sVSchema,
		}
		err = clusterInstance.StartKeyspace(*sKeyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

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
