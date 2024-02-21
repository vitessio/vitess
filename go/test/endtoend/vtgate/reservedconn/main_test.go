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
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/assert"

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

var enableSettingsPool bool

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	code := runAllTests(m)
	if code != 0 {
		os.Exit(code)
	}

	println("running with settings pool enabled")
	// run again with settings pool enabled.
	enableSettingsPool = true
	code = runAllTests(m)
	os.Exit(code)
}

func runAllTests(m *testing.M) int {
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
	clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-transaction-timeout", "5s"}
	if enableSettingsPool {
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--queryserver-enable-settings-pool")
	}
	if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
		return 1
	}

	// Start vtgate
	// This test requires setting the mysql_server_version vtgate flag
	// to 5.7 regardless of the actual MySQL version used for the tests.
	clusterInstance.VtGateExtraArgs = []string{"--lock_heartbeat_time", "2s", "--mysql_server_version", "5.7.0"}
	clusterInstance.VtGatePlannerVersion = querypb.ExecuteOptions_Gen4
	if err := clusterInstance.StartVtgate(); err != nil {
		return 1
	}

	vtParams = mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	return m.Run()
}

func assertIsEmpty(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()
	qr := utils.Exec(t, conn, query)
	assert.Empty(t, qr.Rows)
}

func assertResponseMatch(t *testing.T, conn *mysql.Conn, query1, query2 string) {
	qr1 := utils.Exec(t, conn, query1)
	got1 := fmt.Sprintf("%v", qr1.Rows)

	qr2 := utils.Exec(t, conn, query2)
	got2 := fmt.Sprintf("%v", qr2.Rows)

	assert.Equal(t, got1, got2)
}
