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

package setstatement

import (
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
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
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1
		}

		// Start vtgate
		vtgateProcess := clusterInstance.NewVtgateInstance()
		vtgateProcess.SysVarSetEnabled = true
		if err := vtgateProcess.Setup(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func exec(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}

func checkedExec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}
