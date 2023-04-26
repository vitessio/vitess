/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	wfName            = "testwf"
	optionsJS         = `{"core_options": {"auto_retry": true}}`
	vdiffTestCols     = "id|vdiff_uuid|workflow|keyspace|shard|db_name|state|options|last_error"
	vdiffTestColTypes = "int64|varchar|varbinary|varbinary|varchar|varbinary|varbinary|json|varbinary"
	singleRowAffected = &sqltypes.Result{RowsAffected: 1}
	noResults         = &sqltypes.Result{}
	testSchema        = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:              "t1",
				Columns:           []string{"c1", "c2"},
				PrimaryKeyColumns: []string{"c1"},
				Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			},
		},
	}
)

func TestEngineOpen(t *testing.T) {
	UUID := uuid.New().String()
	source := `keyspace:"testsrc" shard:"0" filter:{rules:{match:"t1" filter:"select * from t1"}}`
	tests := []struct {
		name  string
		state VDiffState
	}{
		// This needs to be started, for the first time, on open
		{
			name:  "pending vdiff",
			state: PendingState,
		},
		// This needs to be restarted on open as it was previously started
		// but was unable to terminate normally (e.g. crash) in the previous
		// engine.
		{
			name:  "started vdiff",
			state: StartedState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablet := addTablet(100)
			tablet.Type = topodatapb.TabletType_PRIMARY
			defer deleteTablet(tablet)
			resetBinlogClient()
			dbClient := binlogplayer.NewMockDBClient(t)
			dbClientFactory := func() binlogplayer.DBClient { return dbClient }
			vde := &Engine{
				controllers:             make(map[int64]*controller),
				ts:                      env.TopoServ,
				thisTablet:              tablet,
				dbClientFactoryFiltered: dbClientFactory,
				dbClientFactoryDba:      dbClientFactory,
				dbName:                  vdiffdb,
			}
			require.False(t, vde.IsOpen())

			initialQR := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				vdiffTestCols,
				vdiffTestColTypes,
			),
				fmt.Sprintf("1|%s|%s|%s|%s|%s|%s|%s|", UUID, wfName, env.KeyspaceName, env.ShardName, vdiffdb, tt.state, optionsJS),
			)

			dbClient.ExpectRequest("select * from _vt.vdiff where state in ('started','pending')", initialQR, nil)

			dbClient.ExpectRequest("select * from _vt.vdiff where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				vdiffTestCols,
				vdiffTestColTypes,
			),
				fmt.Sprintf("1|%s|%s|%s|%s|%s|%s|%s|", UUID, wfName, env.KeyspaceName, env.ShardName, vdiffdb, tt.state, optionsJS),
			), nil)

			dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where workflow = '%s' and db_name = '%s'", wfName, vdiffdb), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type",
				"int64|varbinary|blob|varbinary|varbinary|int64|int64|varbinary|varbinary|int64|int64|varbinary|varbinary|varbinary|int64|varbinary|int64|int64|int64|varchar|int64",
			),
				fmt.Sprintf("1|%s|%s|MySQL56/f69ed286-6909-11ed-8342-0a50724f3211:1-110||9223372036854775807|9223372036854775807||PRIMARY,REPLICA|1669511347|0|Running||%s|200||1669511347|1|0||1", wfName, source, vdiffdb),
			), nil)

			dbClient.ExpectRequest("update _vt.vdiff set state = 'started', last_error = '' , started_at = utc_timestamp() where id = 1", singleRowAffected, nil)
			dbClient.ExpectRequest("insert into _vt.vdiff_log(vdiff_id, message) values (1, 'State changed to: started')", singleRowAffected, nil)
			dbClient.ExpectRequest(`select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = 1 and vdt.table_name = 't1'`, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"lastpk|mismatch|report",
				"varbinary|int64|json",
			),
				`fields:{name:"c1" type:INT64 table:"t1" org_table:"t1" database:"vt_customer" org_name:"c1" column_length:20 charset:63 flags:53251} rows:{lengths:1 values:"1"}|0|{}`,
			), nil)
			dbClient.ExpectRequest("select table_name as table_name, table_rows as table_rows from INFORMATION_SCHEMA.TABLES where table_schema = 'vdiff_test' and table_name in ('t1')", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"table_name|table_rows",
				"varchar|int64",
			),
				"t1|1",
			), nil)
			dbClient.ExpectRequest(`select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = 1 and vdt.table_name = 't1'`, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"lastpk|mismatch|report",
				"varbinary|int64|json",
			),
				`fields:{name:"c1" type:INT64 table:"t1" org_table:"t1" database:"vt_customer" org_name:"c1" column_length:20 charset:63 flags:53251} rows:{lengths:1 values:"1"}|0|{"TableName": "t1", "MatchingRows": 1, "ProcessedRows": 1, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
			), nil)

			// Now let's short circuit the vdiff as we know that the open has worked as expected.
			shortCircuitTestAfterQuery("update _vt.vdiff_table set table_rows = 1 where vdiff_id = 1 and table_name = 't1'", dbClient)

			vde.Open(context.Background(), vreplEngine)
			defer vde.Close()
			assert.True(t, vde.IsOpen())
			assert.Equal(t, 1, len(vde.controllers))
			dbClient.Wait()
		})
	}
}

func TestEngineRetryErroredVDiffs(t *testing.T) {
	UUID := uuid.New().String()
	source := `keyspace:"testsrc" shard:"0" filter:{rules:{match:"t1" filter:"select * from t1"}}`
	expectedControllerCnt := 0
	tests := []struct {
		name              string
		retryQueryResults *sqltypes.Result
		expectRetry       bool
	}{
		{
			name:              "nothing to retry",
			retryQueryResults: noResults,
		},
		{
			name: "non-ephemeral error",
			retryQueryResults: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				vdiffTestCols,
				vdiffTestColTypes,
			),
				fmt.Sprintf("1|%s|%s|%s|%s|%s|error|%s|%v", UUID, wfName, env.KeyspaceName, env.ShardName, vdiffdb, optionsJS,
					mysql.NewSQLError(mysql.ERNoSuchTable, "42S02", "Table 'foo' doesn't exist")),
			),
		},
		{
			name: "ephemeral error",
			retryQueryResults: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				vdiffTestCols,
				vdiffTestColTypes,
			),
				fmt.Sprintf("1|%s|%s|%s|%s|%s|error|%s|%v", UUID, wfName, env.KeyspaceName, env.ShardName, vdiffdb, optionsJS,
					mysql.NewSQLError(mysql.ERLockWaitTimeout, "HY000", "Lock wait timeout exceeded; try restarting transaction")),
			),
			expectRetry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablet := addTablet(100)
			tablet.Type = topodatapb.TabletType_PRIMARY
			defer deleteTablet(tablet)
			resetBinlogClient()
			dbClient := binlogplayer.NewMockDBClient(t)
			dbClientFactory := func() binlogplayer.DBClient { return dbClient }
			vde := &Engine{
				controllers:             make(map[int64]*controller),
				ts:                      env.TopoServ,
				thisTablet:              tablet,
				dbClientFactoryFiltered: dbClientFactory,
				dbClientFactoryDba:      dbClientFactory,
				dbName:                  vdiffdb,
			}
			require.False(t, vde.IsOpen())

			dbClient.ExpectRequest("select * from _vt.vdiff where state in ('started','pending')", noResults, nil)
			vde.Open(context.Background(), vreplEngine)
			defer vde.Close()
			assert.True(t, vde.IsOpen())
			assert.Equal(t, 0, len(vde.controllers))

			dbClient.ExpectRequest("select * from _vt.vdiff where state = 'error' and options->>'$.core_options.auto_retry' = 'true'", tt.retryQueryResults, nil)
			// Right now this only supports a single row as with multiple rows we have
			// multiple controllers in separate goroutines and the order is not
			// guaranteed. If we want to support multiple rows here then we'll need to
			// switch to using the queryhistory package. That will also require building
			// out that package to support MockDBClient and its Expect* functions
			// (query+results+err) as right now it only supports a real DBClient and
			// checks for query execution.
			for _, row := range tt.retryQueryResults.Rows {
				id := row[0].ToString()
				if tt.expectRetry {
					dbClient.ExpectRequestRE("update _vt.vdiff as vd left join _vt.vdiff_table as vdt on \\(vd.id = vdt.vdiff_id\\) set vd.state = 'pending'.*", singleRowAffected, nil)
					dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vdiff where id = %s", id), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						vdiffTestCols,
						vdiffTestColTypes,
					),
						fmt.Sprintf("%s|%s|%s|%s|%s|%s|pending|%s|", id, UUID, wfName, env.KeyspaceName, env.ShardName, vdiffdb, optionsJS),
					), nil)
					dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where workflow = '%s' and db_name = '%s'", wfName, vdiffdb), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"id|workflow|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type",
						"int64|varbinary|blob|varbinary|varbinary|int64|int64|varbinary|varbinary|int64|int64|varbinary|varbinary|varbinary|int64|varbinary|int64|int64|int64|varchar|int64",
					),
						fmt.Sprintf("%s|%s|%s|MySQL56/f69ed286-6909-11ed-8342-0a50724f3211:1-110||9223372036854775807|9223372036854775807||PRIMARY,REPLICA|1669511347|0|Running||%s|200||1669511347|1|0||1", id, wfName, source, vdiffdb),
					), nil)

					// At this point we know that we kicked off the expected retry so we can short circit the vdiff.
					shortCircuitTestAfterQuery(fmt.Sprintf("update _vt.vdiff set state = 'started', last_error = '' , started_at = utc_timestamp() where id = %s", id), dbClient)

					expectedControllerCnt++
				}
			}

			err := vde.retryVDiffs(vde.ctx)
			assert.NoError(t, err)
			assert.Equal(t, expectedControllerCnt, len(vde.controllers))
			dbClient.Wait()
		})
	}

}
