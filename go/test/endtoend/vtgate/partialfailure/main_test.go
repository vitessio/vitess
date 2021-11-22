/*
Copyright 2020 The Vitess Authors.

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

package reservedconn

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/vtgate/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
	sqlSchema       = `
	create table test(
		id bigint,
		val1 varchar(16),
		val2 int,
		val3 float,
		primary key(id)
	)Engine=InnoDB;

CREATE TABLE test_vdx (
    val1 varchar(16) NOT NULL,
    keyspace_id binary(8),
    UNIQUE KEY (val1)
) ENGINE=Innodb;
`

	vSchema = `
		{	
			"sharded":true,
			"vindexes": {
				"hash_index": {
					"type": "hash"
				},
				"lookup1": {
					"type": "consistent_lookup",
					"params": {
						"table": "test_vdx",
						"from": "val1",
						"to": "keyspace_id",
						"ignore_nulls": "true"
					},
					"owner": "test"
				},
				"unicode_vdx":{
					"type": "unicode_loose_md5"
                }
			},	
			"tables": {
				"test":{
					"column_vindexes": [
						{
							"column": "id",
							"name": "hash_index"
						},
						{
							"column": "val1",
							"name": "lookup1"
						}
					]
				},
				"test_vdx":{
					"column_vindexes": [
						{
							"column": "val1",
							"name": "unicode_vdx"
						}
					]
				}
			}
		}
	`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-40", "40-80", "80-c0", "c0-"}, 0, false); err != nil {

			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
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

func TestPartialQueryFailure(t *testing.T) {
	tcases := []struct {
		mode string
	}{
		{"set workload = oltp"},
		{"set workload = oltp; set sql_mode = ''"},
		{"set workload = olap"},
		{"set workload = olap; set sql_mode = ''"},
	}

	for _, tc := range tcases {
		t.Run(tc.mode, func(t *testing.T) {
			conn, err := mysql.Connect(context.Background(), &vtParams)
			require.NoError(t, err)
			defer conn.Close()

			// setup the mode
			utils.Exec(t, conn, tc.mode)

			// cleanup previous run data from table.
			utils.Exec(t, conn, `delete from test`)

			utils.Exec(t, conn, `begin`)
			utils.Exec(t, conn, `insert into test(id, val1) values (1,'A'),(2,'B'),(3,'C')`)
			utils.AssertContainsError(t, conn, `insert into test(id, val1) values (1,'D'),(4,'E')`, `failed to execute due to partial DML execution`)
			utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
			utils.Exec(t, conn, `commit`)
			utils.AssertMatches(t, conn, `select id, val1 from test order by id`, `[[INT64(1) VARCHAR("A")] [INT64(2) VARCHAR("B")] [INT64(3) VARCHAR("C")]]`)
		})
	}
}
