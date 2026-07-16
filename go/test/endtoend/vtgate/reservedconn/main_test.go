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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
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

func setup(t testing.TB) {
	t.Helper()
	ctx := t.Context()

	// This test requires setting the mysql-server-version vtgate flag
	// to 5.7 regardless of the actual MySQL version used for the tests.
	cluster, err := vitesst.NewCluster(t,
		vitesst.WithVTTabletArgs("--queryserver-config-transaction-timeout", "5s"),
		vitesst.WithVTGateArgs(
			"--lock-heartbeat-time", "2s",
			"--mysql-server-version", "5.7.0",
			"--mysql-server-socket-path", "/tmp/mysql.sock",
		),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithSchema(sqlSchema).
			WithVSchema(vSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}

func assertIsEmpty(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()
	qr := vitesst.Exec(t, conn, query)
	assert.Empty(t, qr.Rows)
}

func assertResponseMatch(t *testing.T, conn *mysql.Conn, query1, query2 string) {
	qr1 := vitesst.Exec(t, conn, query1)
	got1 := fmt.Sprintf("%v", qr1.Rows)

	qr2 := vitesst.Exec(t, conn, query2)
	got2 := fmt.Sprintf("%v", qr2.Rows)

	assert.Equal(t, got1, got2)
}
