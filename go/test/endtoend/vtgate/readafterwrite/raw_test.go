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

package readafterwrite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	keyspaceName = "ks"
	sqlSchema    = `
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

func setup(t *testing.T) mysql.ConnParams {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithSchema(sqlSchema).
			WithVSchema(vSchema),
		vitesst.WithVTTabletArgs("--queryserver-config-transaction-timeout", "5s"),
		vitesst.WithVTGateArgs("--enable-system-settings", "--lock-heartbeat-time", "2s"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	return cluster.VTParams(ctx, "")
}

func TestRAWSettings(t *testing.T) {
	vtParams := setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.AssertMatches(t, conn, `select @@read_after_write_gtid, @@read_after_write_timeout, @@session_track_gtids`, `[[VARCHAR("") FLOAT64(0) VARCHAR("off")]]`)
	vitesst.Exec(t, conn, `set read_after_write_gtid = 'some-gtid:1', read_after_write_timeout = 0.2, session_track_gtids = own_gtid`)
	vitesst.AssertMatches(t, conn, `select @@read_after_write_gtid, @@read_after_write_timeout, @@session_track_gtids`, `[[VARCHAR("some-gtid:1") FLOAT64(0.2) VARCHAR("own_gtid")]]`)
}
